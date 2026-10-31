package worker_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"triggerinbox/internal/agent"
	"triggerinbox/internal/store"
	"triggerinbox/internal/testdb"
	"triggerinbox/internal/worker"
)

type runnerFunc func(ctx context.Context, e store.Event) error

func (f runnerFunc) Run(ctx context.Context, e store.Event) error { return f(ctx, e) }

func config() worker.Config {
	return worker.Config{
		Workers:      10,
		Reservation:  300 * time.Millisecond,
		Heartbeat:    100 * time.Millisecond,
		PollInterval: 10 * time.Millisecond,
	}
}

func start(t *testing.T, s *store.Store, r agent.Runner, cfg worker.Config) {
	t.Helper()
	p := worker.New(s, r, cfg, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { p.Run(ctx) })
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
}

func insert(t *testing.T, s *store.Store, agentID string, n int) {
	t.Helper()
	for i := range n {
		if _, err := s.Insert(t.Context(), agentID, fmt.Sprintf("%s-%d", agentID, i), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, s *store.Store, what string, ok func(store.Stats) bool) store.Stats {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st, err := s.Stats(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if ok(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %+v", what, st)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestEveryEventRunsOnce(t *testing.T) {
	s, _ := testdb.New(t)
	effects := agent.NewEffects()
	insert(t, s, "stripe", 50)
	insert(t, s, "slack", 50)

	start(t, s, &agent.Sim{MinDuration: 5 * time.Millisecond, MaxDuration: 20 * time.Millisecond, Effects: effects}, config())
	waitFor(t, s, "all done", func(st store.Stats) bool { return st.Done == 100 })

	sum := effects.Summary()
	if len(sum.Count) != 100 {
		t.Fatalf("%d events had an effect, want 100", len(sum.Count))
	}
	for k, n := range sum.Count {
		if n != 1 {
			t.Fatalf("%v had %d effects", k, n)
		}
	}
}

func TestAFailedRunIsRecordedAndNotRetried(t *testing.T) {
	s, _ := testdb.New(t)
	insert(t, s, "slack", 1)
	var runs atomic.Int32
	failing := runnerFunc(func(context.Context, store.Event) error {
		runs.Add(1)
		return errors.New("agent error")
	})

	start(t, s, failing, config())
	waitFor(t, s, "failed", func(st store.Stats) bool { return st.Failed == 1 })
	time.Sleep(100 * time.Millisecond)

	if runs.Load() != 1 {
		t.Fatalf("%d runs, want 1", runs.Load())
	}
	failed, _ := s.List(t.Context(), "slack", store.Failed, 1)
	if *failed[0].LastError != "agent error" {
		t.Fatalf("last error %q, want the agent's error", *failed[0].LastError)
	}
}

func TestALongRunKeepsItsReservation(t *testing.T) {
	s, _ := testdb.New(t)
	insert(t, s, "webinar", 1)
	var runs atomic.Int32
	long := runnerFunc(func(ctx context.Context, _ store.Event) error {
		runs.Add(1)
		select {
		case <-time.After(5 * config().Reservation):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	start(t, s, long, config())
	waitFor(t, s, "done", func(st store.Stats) bool { return st.Done == 1 })

	if runs.Load() != 1 {
		t.Fatalf("%d runs, want 1: the heartbeat should keep the event reserved", runs.Load())
	}
	done, _ := s.List(t.Context(), "webinar", store.Done, 1)
	if done[0].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", done[0].Attempts)
	}
}

func TestARunWhoseReservationWasTakenOverIsStopped(t *testing.T) {
	s, db := testdb.New(t)
	insert(t, s, "stripe", 1)
	started := make(chan struct{}, 2)
	stopped := make(chan struct{}, 2)
	blocking := runnerFunc(func(ctx context.Context, _ store.Event) error {
		notify(started)
		<-ctx.Done()
		notify(stopped)
		return ctx.Err()
	})

	start(t, s, blocking, config())
	receive(t, started, "first run to start")
	if _, err := db.Exec(t.Context(), "UPDATE events SET status = 'waiting', locked_until = NULL"); err != nil {
		t.Fatal(err)
	}
	receive(t, started, "another worker to take the event")
	receive(t, stopped, "the first run to stop after losing its reservation")
}

func notify(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func receive(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestAnEventOfACrashedWorkerRunsAgain(t *testing.T) {
	s, _ := testdb.New(t)
	insert(t, s, "stripe", 1)
	var runs atomic.Int32
	crashOnce := runnerFunc(func(context.Context, store.Event) error {
		if runs.Add(1) == 1 {
			return agent.ErrWorkerCrash
		}
		return nil
	})

	start(t, s, crashOnce, config())
	waitFor(t, s, "done", func(st store.Stats) bool { return st.Done == 1 })

	done, _ := s.List(t.Context(), "stripe", store.Done, 1)
	if done[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2: the crashed worker and the next one", done[0].Attempts)
	}
}

func TestTwoProcessesNeverShareAnEvent(t *testing.T) {
	s, _ := testdb.New(t)
	effects := agent.NewEffects()
	insert(t, s, "webinar", 200)
	sim := &agent.Sim{MinDuration: 5 * time.Millisecond, MaxDuration: 15 * time.Millisecond, Effects: effects}

	start(t, s, sim, config())
	start(t, s, sim, config())
	waitFor(t, s, "all done", func(st store.Stats) bool { return st.Done == 200 })

	sum := effects.Summary()
	if sum.Overlaps != 0 {
		t.Fatalf("%d times an event ran twice at once", sum.Overlaps)
	}
	if sum.MaxRunning > 2*config().Workers {
		t.Fatalf("%d runs at once, two processes have %d workers", sum.MaxRunning, 2*config().Workers)
	}
}
