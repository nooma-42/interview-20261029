//go:build integration

package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rockets/internal/domain"
	"rockets/internal/httpapi"
	"rockets/internal/store"
)

var schemaCounter atomic.Int64

func database(t *testing.T) (*store.Store, string) {
	t.Helper()
	raw := os.Getenv("TEST_DATABASE_URL")
	if raw == "" {
		t.Fatal("TEST_DATABASE_URL is required for integration tests")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(cfg.ConnConfig.Database, "_test") {
		t.Fatal("integration database name must end in _test")
	}
	ctx := context.Background()
	admin, err := store.Open(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), schemaCounter.Add(1))
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	q.Set("search_path", schema)
	parsed.RawQuery = q.Encode()
	connection := parsed.String()
	db, err := store.Open(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Pool.Close()
		_, _ = admin.Pool.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Pool.Close()
	})
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = db.Migrate(ctx); err != nil {
		t.Fatal("migration not idempotent", err)
	}
	return db, connection
}
func api(t *testing.T, db *store.Store) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(httpapi.New(db, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(s.Close)
	return s
}
func message(channel string, n int64, kind, payload string) string {
	// Deliberately decreasing timestamps prove sequence, rather than time, controls replay.
	return fmt.Sprintf(`{"metadata":{"channel":%q,"messageNumber":%d,"messageTime":%q,"messageType":%q},"message":%s}`, channel, n, time.Date(2022, 1, 1, 0, 0, 0, 123, time.UTC).Add(-time.Duration(n)*time.Second).Format(time.RFC3339Nano), kind, payload)
}
func launch(channel string) string {
	return message(channel, 1, domain.Launched, `{"type":"Falcon-9","launchSpeed":100,"mission":"m"}`)
}
func speed(channel string, n int64) string {
	return message(channel, n, domain.SpeedIncreased, `{"by":1}`)
}
func post(t *testing.T, base, body string, want int) store.Result {
	t.Helper()
	resp, err := http.Post(base+"/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("want %d got %d: %s", want, resp.StatusCode, raw)
	}
	var result store.Result
	if want < 300 {
		if err = json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
	}
	return result
}
func get(t *testing.T, db *store.Store, channel string) domain.Rocket {
	t.Helper()
	r, err := db.Get(context.Background(), channel)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func replay(t *testing.T, db *store.Store) store.ReplayResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := db.ProcessNext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func drain(t *testing.T, db *store.Store) {
	t.Helper()
	for replay(t, db).Channel != "" {
	}
}
func startReplay(t *testing.T, db *store.Store) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		db.RunReplay(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), func(int64) {})
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("replay did not stop")
		}
	}
}
func awaitApplied(t *testing.T, db *store.Store, channel string, last int64) domain.Rocket {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r := get(t, db, channel)
		if r.LastApplied == last {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("replay did not reach %d: %+v", last, r)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func TestSequenceHTTPAndDeduplication(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	cases := []struct {
		n, last int64
	}{{1, 1}, {3, 1}, {4, 1}, {2, 4}}
	for _, c := range cases {
		body := speed("a", c.n)
		if c.n == 1 {
			body = launch("a")
		}
		post(t, s.URL, body, 202)
		drain(t, db)
		if r := get(t, db, "a"); r.LastApplied != c.last {
			t.Fatalf("%+v", r)
		}
	}
	r := get(t, db, "a")
	if *r.Speed != 103 || r.LastApplied != 4 {
		t.Fatalf("%+v", r)
	}
	if result := post(t, s.URL, speed("a", 2), 202); !result.Duplicate {
		t.Fatal(result)
	}
	post(t, s.URL, strings.Replace(speed("a", 2), `"by":1`, `"by":99`, 1), 409)
	post(t, s.URL, strings.Replace(speed("a", 2), "123Z", "124Z", 1), 409)
	post(t, s.URL, speed("a", 8), 202)
	if result := post(t, s.URL, speed("a", 8), 202); !result.Duplicate {
		t.Fatal(result)
	}
	var count int
	if err := db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM rocket_events WHERE channel='a'`).Scan(&count); err != nil || count != 5 {
		t.Fatal(count, err)
	}
	resp, err := http.Get(s.URL + "/rockets/missing")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatal(resp.Status)
	}
}

type receiveQueries struct {
	replays, updates, lockedReads atomic.Int64
}

func (q *receiveQueries) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT message_number,payload") {
		q.replays.Add(1)
	}
	if strings.HasPrefix(data.SQL, "UPDATE rockets") {
		q.updates.Add(1)
	}
	if strings.Contains(data.SQL, "FOR UPDATE") {
		q.lockedReads.Add(1)
	}
	return ctx
}

func (*receiveQueries) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestReceiveFastPaths(t *testing.T) {
	db, _ := database(t)
	queries := &receiveQueries{}
	cfg := db.Pool.Config()
	cfg.ConnConfig.Tracer = queries
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	traced := &store.Store{Pool: pool}
	for _, c := range []struct {
		name, body string
		duplicate  bool
	}{
		{"launch", launch("fast"), false},
		{"pending", speed("fast", 3), false},
		{"pending duplicate", speed("fast", 3), true},
		{"more pending", speed("fast", 4), false},
		{"launch duplicate", launch("fast"), true},
		{"gap filled", speed("fast", 2), false},
		{"in order", speed("fast", 5), false},
		{"duplicate", speed("fast", 5), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			queries.replays.Store(0)
			queries.updates.Store(0)
			queries.lockedReads.Store(0)
			event, err := domain.Parse([]byte(c.body))
			if err != nil {
				t.Fatal(err)
			}
			result, err := traced.Receive(context.Background(), event)
			if err != nil {
				t.Fatal(err)
			}
			if result.Channel != "fast" || result.Duplicate != c.duplicate {
				t.Fatalf("unexpected result: %+v", result)
			}
			if queries.replays.Load() != 0 || queries.updates.Load() != 0 || queries.lockedReads.Load() != 0 {
				t.Fatal("receive read or changed replay state")
			}
		})
	}
	drain(t, db)
	r := get(t, db, "fast")
	if r.Speed == nil || *r.Speed != 104 || r.LastApplied != 5 {
		t.Fatalf("unexpected snapshot: %+v", r)
	}
}

func TestLateLaunchMultipleGapsAndRestart(t *testing.T) {
	db, connection := database(t)
	s := api(t, db)
	for _, n := range []int64{6, 4, 2} {
		post(t, s.URL, speed("late", n), 202)
	}
	r := get(t, db, "late")
	if r.Status != "awaiting_launch" || r.Type != nil || r.Speed != nil || r.Mission != nil || r.LastMessageTime != nil {
		t.Fatal(r)
	}
	s.Close()
	db.Pool.Close()
	reopened, err := store.Open(context.Background(), connection)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Pool.Close()
	s = api(t, reopened)
	for _, c := range []struct{ n, last int64 }{{1, 2}, {3, 4}, {5, 6}} {
		body := speed("late", c.n)
		if c.n == 1 {
			body = launch("late")
		}
		post(t, s.URL, body, 202)
		drain(t, reopened)
		if r := get(t, reopened, "late"); r.LastApplied != c.last {
			t.Fatal(r)
		}
	}
	if r = get(t, reopened, "late"); *r.Speed != 105 {
		t.Fatal(r)
	}
}
func TestTwoInstancesConcurrentAndAtomicReads(t *testing.T) {
	db, connection := database(t)
	second, err := store.Open(context.Background(), connection)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Pool.Close()
	defer startReplay(t, db)()
	defer startReplay(t, second)()
	servers := []*httptest.Server{api(t, db), api(t, second)}
	const rockets = 8
	const events = 70
	bodies := make([]string, 0, rockets*events*2)
	for r := 0; r < rockets; r++ {
		channel := fmt.Sprintf("c%02d", r)
		for n := int64(1); n <= events; n++ {
			body := speed(channel, n)
			if n == 1 {
				body = launch(channel)
			}
			bodies = append(bodies, body, body)
		}
	}
	rand.New(rand.NewSource(42)).Shuffle(len(bodies), func(i, j int) { bodies[i], bodies[j] = bodies[j], bodies[i] })
	tasks := make(chan string)
	errs := make(chan error, len(bodies))
	done := make(chan struct{})
	var reads sync.WaitGroup
	reads.Add(1)
	go func() {
		defer reads.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			r, err := second.Get(context.Background(), "c00")
			if err == store.ErrNotFound {
				continue
			}
			if err != nil {
				errs <- err
				return
			}
			if r.LastApplied > 0 && (r.Speed == nil || *r.Speed != 99+r.LastApplied || r.Status != "launched") {
				errs <- fmt.Errorf("partial snapshot: %+v", r)
				return
			}
		}
	}()
	var workers sync.WaitGroup
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for body := range tasks {
				resp, err := http.Post(servers[i%2].URL+"/messages", "application/json", strings.NewReader(body))
				if err != nil {
					errs <- err
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 202 {
					errs <- fmt.Errorf("status %d", resp.StatusCode)
				}
			}
		}(i)
	}
	for _, body := range bodies {
		tasks <- body
	}
	close(tasks)
	workers.Wait()
	close(done)
	reads.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for i := 0; i < rockets; i++ {
		r := awaitApplied(t, db, fmt.Sprintf("c%02d", i), events)
		if r.LastApplied != events || *r.Speed != 99+events {
			t.Fatal(r)
		}
	}
}

func TestReceiveWhileReplayRowLocked(t *testing.T) {
	db, _ := database(t)
	post(t, api(t, db).URL, launch("parallel"), 202)
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `SELECT channel FROM rockets WHERE channel='parallel' FOR NO KEY UPDATE`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, number := range []int64{2, 3} {
		wg.Add(1)
		go func(number int64) {
			defer wg.Done()
			event, err := domain.Parse([]byte(speed("parallel", number)))
			if err == nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err = db.Receive(ctx, event)
			}
			errs <- err
		}(number)
	}
	completed := make(chan struct{})
	go func() { wg.Wait(); close(completed) }()
	select {
	case <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("receive blocked on replay row lock")
	}
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	drain(t, db)
	if r := get(t, db, "parallel"); r.LastApplied != 3 || *r.Speed != 102 {
		t.Fatal(r)
	}
}

func TestConcurrentSameKey(t *testing.T) {
	db, _ := database(t)
	post(t, api(t, db).URL, launch("same"), 202)
	bodies := []string{speed("same", 2), strings.Replace(speed("same", 2), `"by":1`, `"by":2`, 1)}
	results := make(chan struct {
		result store.Result
		err    error
	}, 2)
	start := make(chan struct{})
	for _, body := range bodies {
		go func(body string) {
			<-start
			event, err := domain.Parse([]byte(body))
			if err != nil {
				results <- struct {
					result store.Result
					err    error
				}{err: err}
				return
			}
			result, err := db.Receive(context.Background(), event)
			results <- struct {
				result store.Result
				err    error
			}{result, err}
		}(body)
	}
	close(start)
	a, b := <-results, <-results
	if a.err == nil && b.err == nil || a.err != nil && b.err != nil {
		t.Fatalf("same key must have one winner: %v, %v", a.err, b.err)
	}
	loser := a.err
	if loser == nil {
		loser = b.err
	}
	if loser != store.ErrConflict {
		t.Fatal(loser)
	}
	var count int
	if err := db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM rocket_events WHERE channel='same' AND message_number=2`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// Concurrent identical retries must both succeed and report one duplicate.
	event, err := domain.Parse([]byte(speed("same", 3)))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		go func() {
			result, err := db.Receive(context.Background(), event)
			results <- struct {
				result store.Result
				err    error
			}{result, err}
		}()
	}
	a, b = <-results, <-results
	if a.err != nil || b.err != nil || a.result.Duplicate == b.result.Duplicate {
		t.Fatalf("identical retries: %+v, %+v", a, b)
	}
}
func TestRollbackOnInsertAndCommitFailure(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprint(deferred), func(t *testing.T) {
			db, _ := database(t)
			s := api(t, db)
			post(t, s.URL, launch("a"), 202)
			drain(t, db)
			sql := `CREATE FUNCTION reject_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected write failure'; END $$;`
			if deferred {
				sql += `CREATE CONSTRAINT TRIGGER injected_failure AFTER INSERT ON rocket_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_change()`
			} else {
				sql += `CREATE TRIGGER injected_failure BEFORE INSERT ON rocket_events FOR EACH ROW EXECUTE FUNCTION reject_change()`
			}
			if _, err := db.Pool.Exec(context.Background(), sql); err != nil {
				t.Fatal(err)
			}
			post(t, s.URL, speed("a", 2), 503)
			r := get(t, db, "a")
			if r.LastApplied != 1 || *r.Speed != 100 {
				t.Fatal(r)
			}
			var count int
			_ = db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM rocket_events`).Scan(&count)
			if count != 1 {
				t.Fatal("event escaped rollback", count)
			}
			if _, err := db.Pool.Exec(context.Background(), `DROP TRIGGER injected_failure ON rocket_events`); err != nil {
				t.Fatal(err)
			}
			post(t, s.URL, speed("a", 2), 202)
			drain(t, db)
			if r = get(t, db, "a"); *r.Speed != 101 || r.LastApplied != 2 {
				t.Fatal(r)
			}
		})
	}
}
func TestOverflowBlocksOnlyItsChannel(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	post(t, s.URL, strings.Replace(launch("a"), `"launchSpeed":100`, fmt.Sprintf(`"launchSpeed":%d`, int64(math.MaxInt64)), 1), 202)
	drain(t, db)
	post(t, s.URL, speed("a", 3), 202)
	post(t, s.URL, message("a", 2, domain.MissionChanged, `{"newMission":"changed"}`), 202)
	result := replay(t, db)
	if result.Applied != 1 || result.Blocked == nil {
		t.Fatal(result)
	}
	r := get(t, db, "a")
	if r.LastApplied != 2 || *r.Mission != "changed" || r.ReplayError == nil || !strings.Contains(*r.ReplayError, "messageNumber 3") {
		t.Fatal(r)
	}
	// New arrivals and duplicates do not reset the poison-event cooldown.
	post(t, s.URL, speed("a", 4), 202)
	post(t, s.URL, speed("a", 3), 202)
	post(t, s.URL, launch("b"), 202)
	if result = replay(t, db); result.Channel != "b" || result.Applied != 1 {
		t.Fatal(result)
	}
	if result = replay(t, db); result.Channel != "" {
		t.Fatal("blocked channel retried too early", result)
	}
	if _, err := db.Pool.Exec(context.Background(), `UPDATE rockets SET replay_after='-infinity' WHERE channel='a'`); err != nil {
		t.Fatal(err)
	}
	if result = replay(t, db); result.Blocked == nil || result.Applied != 0 {
		t.Fatal(result)
	}
}
func TestSortPaginationAndNulls(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	post(t, s.URL, launch("b"), 202)
	post(t, s.URL, launch("a"), 202)
	drain(t, db)
	post(t, s.URL, speed("c", 2), 202)
	post(t, s.URL, speed("b", 2), 202)
	drain(t, db)
	for _, c := range []struct{ sort, order, want string }{{"channel", "asc", "abc"}, {"channel", "desc", "cba"}, {"speed", "asc", "abc"}, {"speed", "desc", "bac"}, {"mission", "desc", "abc"}, {"lastMessageTime", "asc", "bac"}, {"lastMessageTime", "desc", "abc"}} {
		resp, err := http.Get(s.URL + "/rockets?sort=" + c.sort + "&order=" + c.order)
		if err != nil {
			t.Fatal(err)
		}
		var list []domain.Rocket
		err = json.NewDecoder(resp.Body).Decode(&list)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		var got string
		for _, r := range list {
			got += r.Channel
		}
		if got != c.want {
			t.Errorf("%s %s got %q want %q", c.sort, c.order, got, c.want)
		}
	}
	list, err := db.List(context.Background(), store.ListOptions{Sort: "channel", Order: "asc", Limit: 1, Offset: 1})
	if err != nil || len(list) != 1 || list[0].Channel != "b" {
		t.Fatal(list, err)
	}
	list, err = db.List(context.Background(), store.ListOptions{Sort: "channel", Order: "asc", Limit: 1, Offset: 100})
	if err != nil || len(list) != 0 {
		t.Fatal(list, err)
	}
	if _, err = db.List(context.Background(), store.ListOptions{Sort: "speed; DROP TABLE rockets", Order: "asc", Limit: 1}); err == nil {
		t.Fatal("sort injection")
	}
}

func TestReplayBatchesResumeAfterRestartWithoutNewMessages(t *testing.T) {
	db, connection := database(t)
	s := api(t, db)
	const total = 520
	for n := int64(total); n >= 2; n-- {
		post(t, s.URL, speed("batch", n), 202)
	}
	if result := replay(t, db); result.Channel != "" {
		t.Fatal("gap should not be runnable", result)
	}
	post(t, s.URL, launch("batch"), 202)
	if result := replay(t, db); result.Applied != store.ReplayBatchSize {
		t.Fatal(result)
	}
	if r := get(t, db, "batch"); r.LastApplied != store.ReplayBatchSize {
		t.Fatal(r)
	}
	s.Close()
	db.Pool.Close()
	reopened, err := store.Open(context.Background(), connection)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Pool.Close()
	defer startReplay(t, reopened)()
	if r := awaitApplied(t, reopened, "batch", total); *r.Speed != 99+total {
		t.Fatal(r)
	}
}

func TestReplaySkipsLockedChannelsAndRotatesBatches(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	post(t, s.URL, launch("a"), 202)
	post(t, s.URL, launch("b"), 202)
	for n := int64(2); n <= store.ReplayBatchSize+1; n++ {
		post(t, s.URL, speed("a", n), 202)
	}
	tx, err := db.Pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), `SELECT channel FROM rockets WHERE channel='a' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	if result := replay(t, db); result.Channel != "b" {
		t.Fatal(result)
	}
	if err = tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if result := replay(t, db); result.Channel != "a" || result.Applied != store.ReplayBatchSize {
		t.Fatal(result)
	}
	post(t, s.URL, launch("c"), 202)
	if result := replay(t, db); result.Channel != "c" {
		t.Fatal("hot channel starved a new channel", result)
	}
	drain(t, db)
}

func TestReplayRollbackPreservesEarlierBatches(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprint(deferred), func(t *testing.T) {
			db, _ := database(t)
			s := api(t, db)
			post(t, s.URL, launch("a"), 202)
			const total = store.ReplayBatchSize + 2
			for n := int64(2); n <= total; n++ {
				post(t, s.URL, speed("a", n), 202)
			}
			if result := replay(t, db); result.Applied != store.ReplayBatchSize {
				t.Fatal(result)
			}
			sql := `CREATE FUNCTION reject_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected replay failure'; END $$;`
			if deferred {
				sql += `CREATE CONSTRAINT TRIGGER injected_failure AFTER UPDATE ON rockets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_replay()`
			} else {
				sql += `CREATE TRIGGER injected_failure BEFORE UPDATE ON rockets FOR EACH ROW EXECUTE FUNCTION reject_replay()`
			}
			if _, err := db.Pool.Exec(context.Background(), sql); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ProcessNext(context.Background()); err == nil {
				t.Fatal("expected replay failure")
			}
			if r := get(t, db, "a"); r.LastApplied != store.ReplayBatchSize || *r.Speed != 99+store.ReplayBatchSize {
				t.Fatal(r)
			}
			if _, err := db.Pool.Exec(context.Background(), `DROP TRIGGER injected_failure ON rockets`); err != nil {
				t.Fatal(err)
			}
			drain(t, db)
			if r := get(t, db, "a"); r.LastApplied != total || *r.Speed != 99+total {
				t.Fatal(r)
			}
		})
	}
}

func TestReadinessRequiresMigrations(t *testing.T) {
	db, _ := database(t)
	if err := db.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(context.Background(), `DELETE FROM schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if err := db.Ready(context.Background()); err == nil {
		t.Fatal("ready with missing migration record")
	}
	if _, err := db.Pool.Exec(context.Background(), `INSERT INTO schema_migrations(version) VALUES ('other.sql')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Ready(context.Background()); err == nil {
		t.Fatal("ready with wrong migration record")
	}
}

func TestReplayCheckpointAtMaxInt64(t *testing.T) {
	db, _ := database(t)
	post(t, api(t, db).URL, launch("max"), 202)
	if _, err := db.Pool.Exec(context.Background(), `UPDATE rockets SET last_applied=$1 WHERE channel='max'`, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	if result := replay(t, db); result.Channel != "" {
		t.Fatal(result)
	}
}
