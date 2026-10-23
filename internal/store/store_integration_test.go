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
func TestSequenceHTTPAndDeduplication(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	cases := []struct {
		n             int64
		status        int
		last, pending int64
	}{{1, 200, 1, 0}, {3, 202, 1, 1}, {4, 202, 1, 2}, {2, 200, 4, 0}}
	for _, c := range cases {
		body := speed("a", c.n)
		if c.n == 1 {
			body = launch("a")
		}
		result := post(t, s.URL, body, c.status)
		if result.LastApplied != c.last || result.Pending != c.pending {
			t.Fatalf("%+v", result)
		}
	}
	r := get(t, db, "a")
	if *r.Speed != 103 || r.LastApplied != 4 || r.Pending != 0 || r.MaxReceived != 4 {
		t.Fatalf("%+v", r)
	}
	if result := post(t, s.URL, speed("a", 2), 200); !result.Duplicate || result.Applied != 0 {
		t.Fatal(result)
	}
	post(t, s.URL, strings.Replace(speed("a", 2), `"by":1`, `"by":99`, 1), 409)
	post(t, s.URL, strings.Replace(speed("a", 2), "123Z", "124Z", 1), 409)
	post(t, s.URL, speed("a", 8), 202)
	if result := post(t, s.URL, speed("a", 8), 202); !result.Duplicate || result.Pending != 1 {
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
func TestLateLaunchMultipleGapsAndRestart(t *testing.T) {
	db, connection := database(t)
	s := api(t, db)
	for _, n := range []int64{6, 4, 2} {
		post(t, s.URL, speed("late", n), 202)
	}
	r := get(t, db, "late")
	if r.Status != "awaiting_launch" || r.Type != nil || r.Speed != nil || r.Mission != nil || r.LastMessageTime != nil || r.Pending != 3 {
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
	for _, c := range []struct{ n, last, pending int64 }{{1, 2, 2}, {3, 4, 1}, {5, 6, 0}} {
		body := speed("late", c.n)
		if c.n == 1 {
			body = launch("late")
		}
		result := post(t, s.URL, body, 200)
		if result.LastApplied != c.last || result.Pending != c.pending {
			t.Fatal(result)
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
				if resp.StatusCode != 200 && resp.StatusCode != 202 {
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
		r := get(t, db, fmt.Sprintf("c%02d", i))
		if r.LastApplied != events || r.Pending != 0 || *r.Speed != 99+events || r.ReceivedCount != events {
			t.Fatal(r)
		}
	}
	n, err := db.PendingCount(context.Background())
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
}
func TestRollbackOnUpdateAndCommitFailure(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprint(deferred), func(t *testing.T) {
			db, _ := database(t)
			s := api(t, db)
			post(t, s.URL, launch("a"), 200)
			sql := `CREATE FUNCTION reject_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected write failure'; END $$;`
			if deferred {
				sql += `CREATE CONSTRAINT TRIGGER injected_failure AFTER INSERT ON rocket_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_change()`
			} else {
				sql += `CREATE TRIGGER injected_failure BEFORE UPDATE ON rockets FOR EACH ROW EXECUTE FUNCTION reject_change()`
			}
			if _, err := db.Pool.Exec(context.Background(), sql); err != nil {
				t.Fatal(err)
			}
			post(t, s.URL, speed("a", 2), 503)
			r := get(t, db, "a")
			if r.LastApplied != 1 || *r.Speed != 100 || r.ReceivedCount != 1 {
				t.Fatal(r)
			}
			var count int
			_ = db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM rocket_events`).Scan(&count)
			if count != 1 {
				t.Fatal("event escaped rollback", count)
			}
			table := "rockets"
			if deferred {
				table = "rocket_events"
			}
			if _, err := db.Pool.Exec(context.Background(), `DROP TRIGGER injected_failure ON `+table); err != nil {
				t.Fatal(err)
			}
			post(t, s.URL, speed("a", 2), 200)
			if r = get(t, db, "a"); *r.Speed != 101 || r.LastApplied != 2 {
				t.Fatal(r)
			}
		})
	}
}
func TestOverflowRollsBackGapFillingRequest(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	post(t, s.URL, strings.Replace(launch("a"), `"launchSpeed":100`, fmt.Sprintf(`"launchSpeed":%d`, int64(math.MaxInt64)), 1), 200)
	post(t, s.URL, speed("a", 3), 202)
	post(t, s.URL, message("a", 2, domain.MissionChanged, `{"newMission":"changed"}`), 422)
	r := get(t, db, "a")
	if r.LastApplied != 1 || r.ReceivedCount != 2 || *r.Mission != "m" || *r.Speed != math.MaxInt64 {
		t.Fatal(r)
	}
}
func TestSortPaginationAndNulls(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	post(t, s.URL, launch("b"), 200)
	post(t, s.URL, launch("a"), 200)
	post(t, s.URL, speed("c", 2), 202)
	post(t, s.URL, speed("b", 2), 200)
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

func TestReplayAcrossFetchBatches(t *testing.T) {
	db, _ := database(t)
	s := api(t, db)
	const total = 520
	for n := int64(total); n >= 2; n-- {
		post(t, s.URL, speed("batch", n), 202)
	}
	result := post(t, s.URL, launch("batch"), 200)
	if result.LastApplied != total || result.Pending != 0 {
		t.Fatal(result)
	}
	if r := get(t, db, "batch"); *r.Speed != 99+total {
		t.Fatal(r)
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
}
