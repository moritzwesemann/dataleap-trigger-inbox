package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"triggerinbox/internal/agent"
	"triggerinbox/internal/inbox"
	"triggerinbox/internal/store"
	"triggerinbox/internal/worker"
)

const schema = "loadtest"

type delivery struct {
	at      time.Duration
	agentID string
	eventID string
}

var workerConfig = worker.Config{
	Workers:      200,
	Reservation:  900 * time.Millisecond,
	Heartbeat:    300 * time.Millisecond,
	PollInterval: 20 * time.Millisecond,
}

func main() {
	dbURL := flag.String("db", envOr("DATABASE_URL", "postgres://postgres@localhost:5544/triggerinbox?sslmode=disable"), "Postgres URL")
	only := flag.String("scenarios", "burst,ingest", "comma-separated scenarios to run")
	burst := flag.Int("burst", 10000, "events that arrive at the same moment in the burst scenario")
	rates := flag.String("rates", "1000,5000,10000,20000", "comma-separated rates (events/s) for the ingest scenario")
	duration := flag.Duration("duration", 10*time.Second, "how long the ingest scenario sends at each rate")
	flag.Parse()

	ctx := context.Background()
	r := &runner{dbURL: *dbURL, client: newClient(), log: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))}
	r.header(ctx)

	selected := strings.Split(*only, ",")
	if slices.Contains(selected, "burst") {
		fmt.Printf("\n## burst\n\n%d webinar registrations for one agent arrive at the same moment. 200 senders push them in parallel, like a load balancer in front of the inbox would: a single machine cannot open %d new connections in the same instant.\n\n", *burst, *burst)
		res, err := r.accept(ctx, burstPlan(*burst), 200)
		if err != nil {
			fmt.Fprintln(os.Stderr, "burst run failed:", err)
			os.Exit(1)
		}
		fmt.Println("| events | all answered after | answered 200 | errors | p50 | p95 | p99 | stored | **200 but not stored** |")
		fmt.Println("|---|---|---|---|---|---|---|---|---|")
		fmt.Printf("| %d | %s | %d | %d | %s | %s | %s | %d | **%d** |\n",
			res.sent, seconds(res.elapsed), res.ok, res.errors,
			ms(res.p50), ms(res.p95), ms(res.p99), res.stored, res.notStored)
	}

	if slices.Contains(selected, "ingest") {
		fmt.Printf("\n## ingest\n\nHow fast the inbox accepts webhooks for 1,000 agents while the workers keep running. Each rate runs for %s.\n\n", *duration)
		fmt.Println("| target rate | achieved | sent | answered 200 | errors | p50 | p95 | p99 | stored | **200 but not stored** | still waiting at the end |")
		fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|")
		for _, rate := range strings.Split(*rates, ",") {
			var perSecond int
			fmt.Sscan(rate, &perSecond)
			res, err := r.accept(ctx, ratePlan(perSecond, *duration), 0)
			if err != nil {
				fmt.Fprintln(os.Stderr, "ingest run failed:", err)
				os.Exit(1)
			}
			fmt.Printf("| %d/s | %.0f/s | %d | %d | %d | %s | %s | %s | %d | **%d** | %d |\n",
				perSecond, res.achieved, res.sent, res.ok, res.errors,
				ms(res.p50), ms(res.p95), ms(res.p99), res.stored, res.notStored, res.waiting)
		}
	}
}

type runner struct {
	dbURL  string
	client *http.Client
	log    *slog.Logger
}

func (r *runner) header(ctx context.Context) {
	var version, syncMethod string
	if db, err := pgxpool.New(ctx, r.dbURL); err == nil {
		db.QueryRow(ctx, "SELECT current_setting('server_version'), current_setting('wal_sync_method')").Scan(&version, &syncMethod)
		db.Close()
	}
	fmt.Println("# Load test: Trigger Inbox")
	fmt.Printf("\n%s · %s/%s · %d CPUs · Postgres %s, wal_sync_method=%s · %s\n", time.Now().Format("2006-01-02 15:04"), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), version, syncMethod, runtime.Version())
	fmt.Println("\nHow fast the inbox accepts webhooks while 200 workers keep running simulated agents. The runs are sped up: they take 50–200 ms instead of ~30 seconds.")
}

type ingestResult struct {
	achieved                   float64
	elapsed                    time.Duration
	sent, ok, errors           int
	p50, p95, p99              time.Duration
	stored, notStored, waiting int
}

func ratePlan(rate int, d time.Duration) []delivery {
	total := int(float64(rate) * d.Seconds())
	plan := make([]delivery, total)
	for i := range total {
		plan[i] = delivery{
			at:      time.Duration(float64(i) / float64(rate) * float64(time.Second)),
			agentID: fmt.Sprintf("agent-%04d", i%1000),
			eventID: fmt.Sprintf("evt_%d", i),
		}
	}
	return plan
}

func burstPlan(n int) []delivery {
	plan := make([]delivery, n)
	for i := range n {
		plan[i] = delivery{agentID: "webinar", eventID: fmt.Sprintf("reg_%d", i)}
	}
	return plan
}

func (r *runner) accept(ctx context.Context, plan []delivery, maxInFlight int) (ingestResult, error) {
	ingestDB, workDB, err := r.freshSchema(ctx)
	if err != nil {
		return ingestResult{}, err
	}
	defer ingestDB.Close()
	defer workDB.Close()

	sim := &agent.Sim{MinDuration: 50 * time.Millisecond, MaxDuration: 200 * time.Millisecond, Effects: agent.NewEffects()}
	cfg := workerConfig
	work := store.New(workDB)
	pool := worker.New(work, sim, cfg, r.log)
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { pool.Run(runCtx) })
	in := inbox.New(store.New(ingestDB), r.log)
	in.Start(runCtx)
	srv := httptest.NewServer(in.Handler())

	start := time.Now()
	out := r.send(srv.URL, plan, maxInFlight)
	sendTime := time.Since(start)
	srv.Close()
	st, err := work.Stats(ctx)
	stop()
	wg.Wait()
	if err != nil {
		return ingestResult{}, err
	}
	missing, err := notStored(ctx, workDB, out.acked)
	if err != nil {
		return ingestResult{}, err
	}

	res := ingestResult{
		achieved:  float64(out.sent) / sendTime.Seconds(),
		elapsed:   sendTime,
		sent:      out.sent,
		ok:        out.ok,
		errors:    out.sent - out.ok,
		stored:    st.Waiting + st.Running + st.Done + st.Failed,
		notStored: missing,
		waiting:   st.Waiting,
	}
	res.p50, res.p95, res.p99 = percentile(out.acceptLatency, 50), percentile(out.acceptLatency, 95), percentile(out.acceptLatency, 99)
	return res, nil
}

func notStored(ctx context.Context, db *pgxpool.Pool, acked map[[2]string]bool) (int, error) {
	agents := make([]string, 0, len(acked))
	events := make([]string, 0, len(acked))
	for k := range acked {
		agents = append(agents, k[0])
		events = append(events, k[1])
	}
	var stored int
	err := db.QueryRow(ctx, `
		SELECT count(*) FROM unnest($1::text[], $2::text[]) AS a(agent_id, event_id)
		WHERE EXISTS (SELECT 1 FROM events e WHERE e.agent_id = a.agent_id AND e.event_id = a.event_id)`,
		agents, events).Scan(&stored)
	return len(acked) - stored, err
}

func (r *runner) freshSchema(ctx context.Context) (ingest, work *pgxpool.Pool, err error) {
	admin, err := pgxpool.New(ctx, r.dbURL)
	if err != nil {
		return nil, nil, err
	}
	_, err = admin.Exec(ctx, "SET lock_timeout = '30s'; DROP SCHEMA IF EXISTS "+schema+" CASCADE; CREATE SCHEMA "+schema)
	admin.Close()
	if err != nil {
		return nil, nil, err
	}
	if ingest, err = open(ctx, r.dbURL, 80); err != nil {
		return nil, nil, err
	}
	if work, err = open(ctx, r.dbURL, 100); err != nil {
		ingest.Close()
		return nil, nil, err
	}
	if err := store.New(ingest).Migrate(ctx); err != nil {
		ingest.Close()
		work.Close()
		return nil, nil, err
	}
	return ingest, work, nil
}

func open(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := store.PoolConfig(url, maxConns)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	return pgxpool.NewWithConfig(ctx, cfg)
}

type sent struct {
	sent, ok      int
	acceptLatency []time.Duration
	acked         map[[2]string]bool
}

func (r *runner) send(baseURL string, plan []delivery, maxInFlight int) sent {
	plan = slices.Clone(plan)
	slices.SortStableFunc(plan, func(a, b delivery) int { return int(a.at - b.at) })

	type answer struct {
		ok      bool
		latency time.Duration
	}
	answers := make([]answer, len(plan))
	var slots chan struct{}
	if maxInFlight > 0 {
		slots = make(chan struct{}, maxInFlight)
	}
	start := time.Now()
	var wg sync.WaitGroup
	for i, d := range plan {
		if wait := d.at - time.Since(start); wait > 0 {
			time.Sleep(wait)
		}
		if slots != nil {
			slots <- struct{}{}
		}
		wg.Go(func() {
			t := time.Now()
			ok := r.post(baseURL, d)
			answers[i] = answer{ok: ok, latency: time.Since(t)}
			if slots != nil {
				<-slots
			}
		})
	}
	wg.Wait()

	out := sent{sent: len(plan), acked: map[[2]string]bool{}}
	for i, a := range answers {
		if a.ok {
			out.ok++
			out.acked[[2]string{plan[i].agentID, plan[i].eventID}] = true
		}
		out.acceptLatency = append(out.acceptLatency, a.latency)
	}
	return out
}

func (r *runner) post(baseURL string, d delivery) bool {
	body := fmt.Sprintf(`{"id":%q,"type":"simulated"}`, d.eventID)
	res, err := r.client.Post(baseURL+"/triggers/"+d.agentID, "application/json", strings.NewReader(body))
	if err != nil {
		return false
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	return res.StatusCode == http.StatusOK
}

func newClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 4000
	t.MaxIdleConnsPerHost = 4000
	return &http.Client{Transport: t, Timeout: 30 * time.Second}
}

func percentile(ds []time.Duration, p int) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(len(s)-1, len(s)*p/100)]
}

func ms(d time.Duration) string {
	if d == 0 {
		return "–"
	}
	return fmt.Sprintf("%.1f ms", float64(d.Microseconds())/1000)
}

func seconds(d time.Duration) string {
	return fmt.Sprintf("%.1f s", d.Seconds())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
