package store

import (
	"cmp"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Status string

const (
	Waiting Status = "waiting"
	Running Status = "running"
	Done    Status = "done"
	Failed  Status = "failed"
)

type Event struct {
	ID         int64           `json:"id"`
	AgentID    string          `json:"agent_id"`
	EventID    string          `json:"event_id"`
	Payload    json.RawMessage `json:"payload"`
	Status     Status          `json:"status"`
	Attempts   int             `json:"attempts"`
	LastError  *string         `json:"last_error,omitempty"`
	ReceivedAt time.Time       `json:"received_at"`
}

type Lease struct {
	Event
	Attempt int
}

var ErrLeaseLost = errors.New("lease lost")

func PoolConfig(url string, maxConns int32) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	return cfg, nil
}

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.db.Exec(ctx, schema)
	return err
}

func (s *Store) Insert(ctx context.Context, agentID, eventID string, payload []byte) (bool, error) {
	isNew, err := s.InsertBatch(ctx, []NewEvent{{AgentID: agentID, EventID: eventID, Payload: payload}})
	if err != nil {
		return false, err
	}
	return isNew[0], nil
}

type NewEvent struct {
	AgentID string
	EventID string
	Payload []byte
}

func (s *Store) InsertBatch(ctx context.Context, events []NewEvent) ([]bool, error) {
	sorted := slices.Clone(events)
	slices.SortStableFunc(sorted, func(a, b NewEvent) int {
		return cmp.Or(strings.Compare(a.AgentID, b.AgentID), strings.Compare(a.EventID, b.EventID))
	})
	agentIDs := make([]string, len(sorted))
	eventIDs := make([]string, len(sorted))
	payloads := make([]string, len(sorted))
	for i, e := range sorted {
		agentIDs[i], eventIDs[i], payloads[i] = e.AgentID, e.EventID, string(e.Payload)
	}
	rows, err := s.db.Query(ctx, `
		INSERT INTO events (agent_id, event_id, payload)
		SELECT * FROM unnest($1::text[], $2::text[], $3::jsonb[])
		ON CONFLICT (agent_id, event_id) DO NOTHING
		RETURNING agent_id, event_id`,
		agentIDs, eventIDs, payloads)
	if err != nil {
		return nil, err
	}
	inserted := map[[2]string]bool{}
	var agentID, eventID string
	_, err = pgx.ForEachRow(rows, []any{&agentID, &eventID}, func() error {
		inserted[[2]string{agentID, eventID}] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	isNew := make([]bool, len(events))
	for i, e := range events {
		k := [2]string{e.AgentID, e.EventID}
		if inserted[k] {
			isNew[i] = true
			delete(inserted, k)
		}
	}
	return isNew, nil
}

func (s *Store) Claim(ctx context.Context, cursor string, reservation time.Duration) (*Lease, error) {
	l, err := s.claimNext(ctx, cursor, "", reservation)
	if l != nil || err != nil || cursor == "" {
		return l, err
	}
	return s.claimNext(ctx, "", cursor, reservation)
}

func (s *Store) claimNext(ctx context.Context, after, upTo string, reservation time.Duration) (*Lease, error) {
	var l Lease
	err := s.db.QueryRow(ctx, `
		UPDATE events
		SET status = 'running',
		    attempts = attempts + 1,
		    locked_until = now() + $3 * interval '1 millisecond'
		WHERE id = (
			SELECT id FROM events
			WHERE status = 'waiting'
			  AND agent_id > $1
			  AND ($2::text = '' OR agent_id <= $2)
			ORDER BY agent_id, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, agent_id, event_id, payload, status, attempts, last_error, received_at`,
		after, upTo, reservation.Milliseconds()).Scan(
		&l.ID, &l.AgentID, &l.EventID, &l.Payload, &l.Status, &l.Attempts, &l.LastError, &l.ReceivedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	l.Attempt = l.Attempts
	return &l, nil
}

func (s *Store) Renew(ctx context.Context, l *Lease, reservation time.Duration) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE events SET locked_until = now() + $3 * interval '1 millisecond'
		WHERE id = $1 AND attempts = $2 AND status = 'running'`,
		l.ID, l.Attempt, reservation.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Complete(ctx context.Context, l *Lease) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE events SET status = 'done', locked_until = NULL, last_error = NULL
		WHERE id = $1 AND attempts = $2 AND status = 'running'`,
		l.ID, l.Attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, l *Lease, reason string) error {
	tag, err := s.db.Exec(ctx, `
		UPDATE events SET status = 'failed', locked_until = NULL, last_error = $3
		WHERE id = $1 AND attempts = $2 AND status = 'running'`,
		l.ID, l.Attempt, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) ReleaseExpired(ctx context.Context) (int, error) {
	tag, err := s.db.Exec(ctx, `
		UPDATE events
		SET status = 'waiting',
		    locked_until = NULL,
		    last_error = 'reservation expired: the worker stopped before finishing'
		WHERE status = 'running' AND locked_until < now()`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *Store) List(ctx context.Context, agentID string, status Status, limit int) ([]Event, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, agent_id, event_id, payload, status, attempts, last_error, received_at
		FROM events
		WHERE agent_id = $1 AND status = $2
		ORDER BY id
		LIMIT $3`, agentID, status, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanEvent)
}

type Stats struct {
	Waiting       int     `json:"waiting"`
	Running       int     `json:"running"`
	Done          int     `json:"done"`
	Failed        int     `json:"failed"`
	OldestWaiting float64 `json:"oldest_waiting_seconds"`
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.db.QueryRow(ctx, `
		SELECT
		  count(*) FILTER (WHERE status = 'waiting'),
		  count(*) FILTER (WHERE status = 'running'),
		  count(*) FILTER (WHERE status = 'done'),
		  count(*) FILTER (WHERE status = 'failed'),
		  coalesce(extract(epoch FROM now() - min(received_at) FILTER (WHERE status = 'waiting')), 0)
		FROM events`).Scan(&st.Waiting, &st.Running, &st.Done, &st.Failed, &st.OldestWaiting)
	return st, err
}

func scanEvent(row pgx.CollectableRow) (Event, error) {
	var e Event
	err := row.Scan(&e.ID, &e.AgentID, &e.EventID, &e.Payload, &e.Status, &e.Attempts, &e.LastError, &e.ReceivedAt)
	return e, err
}
