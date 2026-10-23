package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"rockets/internal/domain"
)

var (
	ErrConflict = errors.New("messageNumber already exists with different content")
	ErrNotFound = errors.New("rocket not found")
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	// Keep connections bounded per instance; DATABASE_URL can override pool_max_conns.
	if !strings.Contains(url, "pool_max_conns") {
		cfg.MaxConns = 20
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// Serialize migration jobs across instances. DDL and version records commit together.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(743927104)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, entry.Name()).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, entry.Name()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// rollback has its own bounded context because the request may already be canceled.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (s *Store) Ready(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	versions := make([]string, 0, len(entries))
	for _, entry := range entries {
		versions = append(versions, entry.Name())
	}
	var applied int
	err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version=ANY($1::text[])`, versions).Scan(&applied)
	if err != nil {
		return err
	}
	if applied != len(versions) {
		return errors.New("migrations are incomplete")
	}
	return nil
}

const columns = `channel, rocket_type, speed, mission, status, explosion_reason, last_message_time, last_applied, max_received, received_count`

func scan(row pgx.Row) (domain.Rocket, error) {
	var r domain.Rocket
	err := row.Scan(&r.Channel, &r.Type, &r.Speed, &r.Mission, &r.Status, &r.ExplosionReason, &r.LastMessageTime, &r.LastApplied, &r.MaxReceived, &r.ReceivedCount)
	r.Pending = r.ReceivedCount - r.LastApplied
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return r, err
}

type Result struct {
	Channel     string `json:"channel"`
	LastApplied int64  `json:"lastAppliedMessageNumber"`
	Pending     int64  `json:"pendingMessageCount"`
	Duplicate   bool   `json:"duplicate"`
	Applied     int64  `json:"-"`
}

func (s *Store) Receive(ctx context.Context, e domain.Event) (Result, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer rollback(tx)
	channel := e.Metadata.Channel
	if _, err = tx.Exec(ctx, `INSERT INTO rockets(channel) VALUES($1) ON CONFLICT DO NOTHING`, channel); err != nil {
		return Result{}, err
	}
	r, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM rockets WHERE channel=$1 FOR UPDATE`, channel))
	if err != nil {
		return Result{}, err
	}
	previous := r.LastApplied
	data := e.JSON()
	tag, err := tx.Exec(ctx, `INSERT INTO rocket_events(channel,message_number,event_type,message_time,payload) VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING`, channel, e.Metadata.Number, e.Metadata.Type, e.Metadata.Time, string(data))
	if err != nil {
		return Result{}, err
	}
	duplicate := tag.RowsAffected() == 0
	if duplicate {
		var equal bool
		if err = tx.QueryRow(ctx, `SELECT payload=$3::jsonb FROM rocket_events WHERE channel=$1 AND message_number=$2`, channel, e.Metadata.Number, string(data)).Scan(&equal); err != nil {
			return Result{}, err
		}
		if !equal {
			return Result{}, ErrConflict
		}
	} else {
		r.ReceivedCount++
		r.MaxReceived = max(r.MaxReceived, e.Metadata.Number)
	}
	// Bound each fetch. A gap stops replay immediately, even with a large pending backlog.
	for r.LastApplied < math.MaxInt64 {
		rows, err := tx.Query(ctx, `SELECT message_number,payload FROM rocket_events WHERE channel=$1 AND message_number>$2 ORDER BY message_number LIMIT 256`, channel, r.LastApplied)
		if err != nil {
			return Result{}, err
		}
		applied := 0
		var replayErr error
		for rows.Next() {
			var number int64
			var raw []byte
			if replayErr = rows.Scan(&number, &raw); replayErr != nil {
				break
			}
			if number != r.LastApplied+1 {
				break
			}
			var pending domain.Event
			pending, replayErr = domain.Parse(raw)
			if replayErr != nil {
				break
			}
			r, replayErr = domain.Apply(r, pending)
			if replayErr != nil {
				break
			}
			applied++
		}
		rows.Close()
		if replayErr != nil {
			return Result{}, replayErr
		}
		if err = rows.Err(); err != nil {
			return Result{}, err
		}
		if applied < 256 {
			break
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE rockets SET rocket_type=$2,speed=$3,mission=$4,status=$5,explosion_reason=$6,last_message_time=$7,last_applied=$8,max_received=$9,received_count=$10 WHERE channel=$1`, channel, r.Type, r.Speed, r.Mission, r.Status, r.ExplosionReason, r.LastMessageTime, r.LastApplied, r.MaxReceived, r.ReceivedCount); err != nil {
		return Result{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Result{}, err
	}
	return Result{Channel: channel, LastApplied: r.LastApplied, Pending: r.ReceivedCount - r.LastApplied, Duplicate: duplicate, Applied: r.LastApplied - previous}, nil
}

func (s *Store) Get(ctx context.Context, channel string) (domain.Rocket, error) {
	return scan(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM rockets WHERE channel=$1`, channel))
}

type ListOptions struct {
	Sort, Order   string
	Limit, Offset int
}

func (s *Store) List(ctx context.Context, o ListOptions) ([]domain.Rocket, error) {
	// These allowlists are also enforced here so callers cannot inject SQL.
	fields := map[string]string{"channel": "channel", "speed": "speed", "mission": "mission", "lastMessageTime": "last_message_time"}
	field, ok := fields[o.Sort]
	if !ok || (o.Order != "asc" && o.Order != "desc") || o.Limit < 1 || o.Limit > 1000 || o.Offset < 0 {
		return nil, errors.New("invalid list options")
	}
	order := field + " " + o.Order + " NULLS LAST"
	if o.Sort != "channel" {
		order += ", channel ASC"
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+columns+` FROM rockets ORDER BY `+order+` LIMIT $1 OFFSET $2`, o.Limit, o.Offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.Rocket, 0)
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) PendingCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx, `SELECT COALESCE(SUM(received_count-last_applied),0)::bigint FROM rockets`).Scan(&n)
	return n, err
}
