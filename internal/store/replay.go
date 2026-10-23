package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"rockets/internal/domain"
)

const ReplayBatchSize = 256

type ReplayResult struct {
	Channel string
	Applied int64
	Blocked *string
}

// ProcessNext commits at most one batch. The rocket row lock excludes other replay workers.
func (s *Store) ProcessNext(ctx context.Context) (ReplayResult, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ReplayResult{}, err
	}
	defer rollback(tx)
	// Oldest attempt first gives each runnable channel a turn; gaps never enter the queue.
	// shortcut: candidate selection scans rockets; add scheduling indexes when fleet size warrants it.
	r, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM rockets r
 WHERE replay_after <= clock_timestamp()
 AND EXISTS (SELECT 1 FROM rocket_events e WHERE e.channel=r.channel
 AND e.message_number=CASE WHEN r.last_applied<9223372036854775807 THEN r.last_applied+1 END)
 ORDER BY replay_after, channel LIMIT 1 FOR NO KEY UPDATE OF r SKIP LOCKED`))
	if errors.Is(err, ErrNotFound) {
		return ReplayResult{}, nil
	}
	if err != nil {
		return ReplayResult{}, err
	}
	result := ReplayResult{Channel: r.Channel}
	rows, err := tx.Query(ctx, `SELECT message_number,payload FROM rocket_events WHERE channel=$1 AND message_number>$2 ORDER BY message_number LIMIT $3`, r.Channel, r.LastApplied, ReplayBatchSize)
	if err != nil {
		return ReplayResult{}, err
	}
	for rows.Next() {
		var number int64
		var raw []byte
		if err = rows.Scan(&number, &raw); err != nil {
			break
		}
		if number != r.LastApplied+1 {
			break
		}
		var event domain.Event
		event, err = domain.Parse(raw)
		if err == nil {
			r, err = domain.Apply(r, event)
		}
		if err != nil {
			message := fmt.Sprintf("messageNumber %d: %v", number, err)
			result.Blocked = &message
			err = nil
			break
		}
		result.Applied++
	}
	rows.Close()
	if err != nil {
		return ReplayResult{}, err
	}
	if err = rows.Err(); err != nil {
		return ReplayResult{}, err
	}
	// Keep the valid prefix and expose a poisoned event without retrying it in a busy loop.
	if _, err = tx.Exec(ctx, `UPDATE rockets SET rocket_type=$2,speed=$3,mission=$4,status=$5,
 explosion_reason=$6,last_message_time=$7,last_applied=$8,replay_error=$9,
 replay_after=clock_timestamp()+CASE WHEN $9::text IS NULL THEN interval '0' ELSE interval '30 seconds' END
 WHERE channel=$1`, r.Channel, r.Type, r.Speed, r.Mission, r.Status, r.ExplosionReason, r.LastMessageTime, r.LastApplied, result.Blocked); err != nil {
		return ReplayResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ReplayResult{}, err
	}
	return result, nil
}

// RunReplay discovers durable work on startup and after idle periods; notifications are unnecessary.
func (s *Store) RunReplay(ctx context.Context, logger *slog.Logger, applied func(int64)) {
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		result, err := s.ProcessNext(batchCtx)
		cancel()
		if err == nil && result.Channel != "" {
			applied(result.Applied)
			if result.Blocked != nil {
				logger.Error("rocket replay blocked", "channel", result.Channel, "error", *result.Blocked)
			}
		}
		if ctx.Err() != nil {
			return
		}
		delay := 100 * time.Millisecond
		if err != nil {
			logger.Error("background replay failed", "error", err)
			delay = time.Second
		} else if result.Channel != "" {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
