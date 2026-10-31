package store_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"triggerinbox/internal/store"
	"triggerinbox/internal/testdb"
)

const lease = time.Minute

func insert(t *testing.T, s *store.Store, agentID string, n int) {
	t.Helper()
	for i := range n {
		if _, err := s.Insert(t.Context(), agentID, fmt.Sprintf("%s-%d", agentID, i), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestInsertStoresADuplicateOnce(t *testing.T) {
	s, _ := testdb.New(t)
	ctx := t.Context()

	first, err := s.Insert(ctx, "stripe", "evt_1", []byte(`{"id":"evt_1"}`))
	if err != nil || !first {
		t.Fatalf("first insert: new=%v err=%v", first, err)
	}
	second, err := s.Insert(ctx, "stripe", "evt_1", []byte(`{"id":"evt_1"}`))
	if err != nil || second {
		t.Fatalf("duplicate insert: new=%v err=%v", second, err)
	}
	other, err := s.Insert(ctx, "slack", "evt_1", []byte(`{}`))
	if err != nil || !other {
		t.Fatalf("other agent: new=%v err=%v", other, err)
	}

	st, _ := s.Stats(ctx)
	if st.Waiting != 2 {
		t.Fatalf("waiting = %d, want 2", st.Waiting)
	}
}

func TestInsertBatchStoresADuplicateInsideTheBatchOnce(t *testing.T) {
	s, _ := testdb.New(t)
	ctx := t.Context()
	if _, err := s.Insert(ctx, "stripe", "evt_old", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	isNew, err := s.InsertBatch(ctx, []store.NewEvent{
		{AgentID: "stripe", EventID: "evt_1", Payload: []byte(`{"n":1}`)},
		{AgentID: "stripe", EventID: "evt_old", Payload: []byte(`{}`)},
		{AgentID: "stripe", EventID: "evt_1", Payload: []byte(`{"n":2}`)},
		{AgentID: "slack", EventID: "evt_1", Payload: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []bool{true, false, false, true}; fmt.Sprint(isNew) != fmt.Sprint(want) {
		t.Fatalf("isNew = %v, want %v", isNew, want)
	}
	st, _ := s.Stats(ctx)
	if st.Waiting != 3 {
		t.Fatalf("waiting = %d, want 3", st.Waiting)
	}
}

func TestConcurrentBatchesWithTheSameEventsNeverDeadlock(t *testing.T) {
	s, _ := testdb.New(t)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for round := range 30 {
				batch := make([]store.NewEvent, 40)
				for i, n := range rand.Perm(40) {
					batch[i] = store.NewEvent{AgentID: "stripe", EventID: fmt.Sprintf("evt_%d_%d", round, n), Payload: []byte(`{}`)}
				}
				if _, err := s.InsertBatch(t.Context(), batch); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestClaimNeverHandsOutAnEventTwice(t *testing.T) {
	s, _ := testdb.New(t)
	for a := range 10 {
		insert(t, s, fmt.Sprintf("agent-%d", a), 20)
	}

	var mu sync.Mutex
	claimed := map[int64]int{}
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			cursor := ""
			for {
				l, err := s.Claim(t.Context(), cursor, lease)
				if err != nil {
					t.Error(err)
					return
				}
				if l == nil {
					return
				}
				cursor = l.AgentID
				mu.Lock()
				claimed[l.ID]++
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(claimed) != 200 {
		t.Fatalf("claimed %d events, want 200", len(claimed))
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("event %d claimed %d times", id, n)
		}
	}
}

func TestClaimGivesEveryAgentATurn(t *testing.T) {
	s, _ := testdb.New(t)
	insert(t, s, "a-flood", 500)
	insert(t, s, "b-quiet", 1)

	first, err := s.Claim(t.Context(), "", lease)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Claim(t.Context(), first.AgentID, lease)
	if err != nil {
		t.Fatal(err)
	}
	if second.AgentID != "b-quiet" {
		t.Fatalf("second claim went to %s; the quiet agent waits behind the flood", second.AgentID)
	}
}

func TestExpiredLeaseIsReleasedAndTheOldOwnerFenced(t *testing.T) {
	s, _ := testdb.New(t)
	ctx := t.Context()
	insert(t, s, "stripe", 1)

	old, err := s.Claim(ctx, "", 50*time.Millisecond)
	if err != nil || old == nil {
		t.Fatalf("claim: %v %v", old, err)
	}
	time.Sleep(100 * time.Millisecond)

	n, err := s.ReleaseExpired(ctx)
	if err != nil || n != 1 {
		t.Fatalf("release: n=%d err=%v", n, err)
	}
	fresh, err := s.Claim(ctx, "", lease)
	if err != nil || fresh == nil || fresh.Attempt != 2 {
		t.Fatalf("reclaim: %+v %v", fresh, err)
	}

	if err := s.Renew(ctx, old, lease); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old renew: %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, old); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("old complete: %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, fresh); err != nil {
		t.Fatalf("new owner complete: %v", err)
	}
}

func TestRenewKeepsTheReservation(t *testing.T) {
	s, _ := testdb.New(t)
	ctx := t.Context()
	insert(t, s, "stripe", 1)

	l, err := s.Claim(ctx, "", 50*time.Millisecond)
	if err != nil || l == nil {
		t.Fatalf("claim: %v %v", l, err)
	}
	if err := s.Renew(ctx, l, lease); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	if n, err := s.ReleaseExpired(ctx); err != nil || n != 0 {
		t.Fatalf("release: n=%d err=%v, want the renewed reservation kept", n, err)
	}
}

func TestAFailedRunIsMarkedFailedAndNotClaimedAgain(t *testing.T) {
	s, _ := testdb.New(t)
	ctx := t.Context()
	insert(t, s, "slack", 1)

	l, err := s.Claim(ctx, "", lease)
	if err != nil || l == nil {
		t.Fatalf("claim: %v %v", l, err)
	}
	if err := s.Fail(ctx, l, "agent error"); err != nil {
		t.Fatal(err)
	}

	failed, err := s.List(ctx, "slack", store.Failed, 10)
	if err != nil || len(failed) != 1 || *failed[0].LastError != "agent error" {
		t.Fatalf("failed list: %+v %v", failed, err)
	}
	if l, _ := s.Claim(ctx, "", lease); l != nil {
		t.Fatalf("a failed event was claimed again")
	}
}
