package inbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"triggerinbox/internal/inbox"
	"triggerinbox/internal/store"
	"triggerinbox/internal/testdb"
)

func server(t *testing.T, s *store.Store) *httptest.Server {
	t.Helper()
	return serve(t, inbox.New(s, slog.New(slog.DiscardHandler)))
}

func serve(t *testing.T, in *inbox.Inbox) *httptest.Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	in.Start(ctx)
	srv := httptest.NewServer(in.Handler())
	t.Cleanup(func() {
		srv.Close()
		cancel()
	})
	return srv
}

func post(t *testing.T, url, body string, header map[string]string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

func TestADuplicateIsStoredOnceAndStillAnswered200(t *testing.T) {
	s, _ := testdb.New(t)
	srv := server(t, s)

	code, body := post(t, srv.URL+"/triggers/stripe", `{"id":"evt_1","type":"invoice.payment_failed"}`, nil)
	if code != 200 || !strings.Contains(body, "stored") {
		t.Fatalf("first delivery: %d %s", code, body)
	}
	code, body = post(t, srv.URL+"/triggers/stripe", `{"id":"evt_1","type":"invoice.payment_failed"}`, nil)
	if code != 200 || !strings.Contains(body, "duplicate") {
		t.Fatalf("second delivery: %d %s", code, body)
	}

	st, _ := s.Stats(t.Context())
	if st.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", st.Waiting)
	}
}

func TestABurstWithDuplicatesIsStoredExactlyOnce(t *testing.T) {
	s, _ := testdb.New(t)
	srv := server(t, s)

	var wg sync.WaitGroup
	senders := make(chan struct{}, 50)
	codes := make([]int, 1000)
	for i := range 1000 {
		senders <- struct{}{}
		wg.Go(func() {
			defer func() { <-senders }()
			id := i
			if i%10 == 9 {
				id = i - 9
			}
			res, err := http.Post(srv.URL+"/triggers/webinar", "application/json", strings.NewReader(fmt.Sprintf(`{"id":"reg_%d"}`, id)))
			if err != nil {
				return
			}
			res.Body.Close()
			codes[i] = res.StatusCode
		})
	}
	wg.Wait()

	for i, code := range codes {
		if code != 200 {
			t.Fatalf("delivery %d answered %d", i, code)
		}
	}
	st, _ := s.Stats(t.Context())
	if st.Waiting != 900 {
		t.Fatalf("stored %d events, want 900 unique", st.Waiting)
	}
}

func TestTheEventIDComesFromTheSender(t *testing.T) {
	s, _ := testdb.New(t)
	srv := server(t, s)

	post(t, srv.URL+"/triggers/stripe", `{"id":"evt_9"}`, nil)
	post(t, srv.URL+"/triggers/slack", `{"event_id":"Ev01","type":"event_callback"}`, nil)
	post(t, srv.URL+"/triggers/webinar", `{"email":"a@example.com"}`, map[string]string{"X-Event-ID": "reg_88"})

	for agentID, eventID := range map[string]string{"stripe": "evt_9", "slack": "Ev01", "webinar": "reg_88"} {
		events, err := s.List(t.Context(), agentID, store.Waiting, 10)
		if err != nil || len(events) != 1 || events[0].EventID != eventID {
			t.Fatalf("%s: %+v %v, want event id %s", agentID, events, err, eventID)
		}
	}
}

func TestInvalidDeliveriesAreRejected(t *testing.T) {
	s, _ := testdb.New(t)
	srv := server(t, s)

	if code, _ := post(t, srv.URL+"/triggers/stripe", `not json`, nil); code != 400 {
		t.Fatalf("non-JSON body: %d, want 400", code)
	}
	if code, _ := post(t, srv.URL+"/triggers/stripe", `{"type":"x"}`, nil); code != 400 {
		t.Fatalf("missing event id: %d, want 400", code)
	}
}

func TestADownDatabaseAnswers503NotAFalse200(t *testing.T) {
	s, pool := testdb.New(t)
	in := inbox.New(s, slog.New(slog.DiscardHandler))
	in.SaveTimeout = time.Second
	srv := serve(t, in)

	pool.Close()
	if code, _ := post(t, srv.URL+"/triggers/stripe", `{"id":"evt_1"}`, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503 so the sender retries", code)
	}
}

func TestFailedEventsCanBeListed(t *testing.T) {
	s, _ := testdb.New(t)
	srv := server(t, s)
	post(t, srv.URL+"/triggers/webinar", `{"id":"reg_1"}`, nil)
	l, err := s.Claim(t.Context(), "", time.Minute)
	if err != nil || l == nil {
		t.Fatalf("claim: %v %v", l, err)
	}
	if err := s.Fail(t.Context(), l, "agent error"); err != nil {
		t.Fatal(err)
	}

	res, err := http.Get(srv.URL + "/agents/webinar/events?status=failed")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var events []store.Event
	if err := json.NewDecoder(res.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].EventID != "reg_1" || *events[0].LastError != "agent error" {
		t.Fatalf("failed events: %+v", events)
	}
}
