# Dataleap Trigger Inbox

Dataleap's docs warn that bursty webhooks can drop agent runs:

> Don't assume every event is queued and processed — runs can be dropped in
> burst scenarios.

Trigger Inbox is a small Go + Postgres service in front of the agent runs. It
saves every event before answering, then runs it as soon as there is room.
Agent runs are simulated, so the whole thing runs on a laptop.

**Words used here, the way Dataleap uses them**

| Word | Meaning |
|---|---|
| Agent | What a Dataleap user built, e.g. "Payment follow-up". Its webhook URL is `/triggers/{agent_id}`. |
| Run | One time an agent works on one event. This is where the AI does the work. |
| Worker | Our code: takes an event from the table, starts a run and records the result. No AI. |

## Run it

Needs Go and Postgres 18 (`brew install postgresql@18`).

```sh
make db-up       # Postgres for this project only: ./.pgdata, port 5544
make test        # unit and integration tests against that Postgres
make loadtest    # how fast the inbox accepts webhooks, results as markdown
make run         # the service on localhost:8787 with simulated agents
make db-down
```

With `make run` going:

```sh
curl -X POST localhost:8787/triggers/stripe -d '{"id":"evt_1","type":"invoice.payment_failed"}'
curl -X POST localhost:8787/triggers/stripe -d '{"id":"evt_1","type":"invoice.payment_failed"}'  # duplicate, still 200
curl localhost:8787/stats
curl 'localhost:8787/agents/stripe/events?status=done'
```

## Requirements

**Functional**

- Accept webhooks from Stripe, Slack, webinar tools and anything else.
- Every event starts one agent run; an event delivered twice runs once.
- A failed run is visible with its error.

**Non-functional**

- No saved event is ever lost.
- Accept peaks of 5,000 events per second, answering in under 100 ms
  ([how the number is calculated](#load-estimate)).
- Never more runs at once than the agent platform can handle. Extra events
  wait instead of being dropped.
- One agent's burst does not starve the others.
- Consistency over availability: answer 503, never a 200 without a saved row.

### Load estimate

Generous guesses, not Dataleap's real numbers:

| | |
|---|---|
| Agents | 20,000: 90% quiet with 10 events a day, 10% busy with 150 |
| Events a day | 18,000 × 10 + 2,000 × 150 = 480,000 |
| Average | 480,000 / 86,400 s ≈ 5.6 events/s |
| Peak (30× the average) | ≈ 170 events/s |
| 20× growth | ≈ 3,300 events/s → goal: 5,000 events/s |

## Design

```
Stripe / Slack / Webinar ──POST──▶ Inbox ──INSERT──▶ events table ◀──claim── Workers ──start──▶ runs
                                  save, then 200                    done or failed
```

**One table.** Every event is one row in `events`
([schema](internal/store/schema.sql)). `UNIQUE (agent_id, event_id)` stores a
duplicate delivery once; the inbox still answers 200.

**Batched saves.** The expensive part of a commit is the flush to disk. The
inbox's writers take whatever webhooks queued up while the previous commit ran
and save them in one `INSERT`, so many webhooks share one flush
([writer](internal/inbox/writer.go)). Under low load a batch is a single
webhook, so nothing waits. Each batch is sorted by agent and event id, so two
batches that share a duplicate never wait on each other in a deadlock. A sender still gets its 200 only after its batch
has committed.

**Runs at once.** A worker starts a run and waits for it, so one worker drives
one run at a time. Waiting costs a worker almost nothing (a few kilobytes, no
AI), which makes the number of workers simply the setting for how many runs
happen at once: set it to what the agent platform can handle. More events than
that wait in the table instead of being dropped.

**API**

| Endpoint | What it does |
|---|---|
| `POST /triggers/{agent_id}` | Saves the event, then answers 200. The event id comes from `X-Event-ID`, Slack's `event_id` or Stripe's `id`. Database down or slow: 503, so the sender retries. |
| `GET /agents/{agent_id}/events?status=failed` | Lists events by status (`waiting`, `running`, `done`, `failed`). |
| `GET /stats` | Counts per status and the age of the oldest waiting event: the numbers that show where a bottleneck is. |

**Worker rules** ([worker](internal/worker/worker.go), [store](internal/store/store.go))

| What can go wrong | What happens |
|---|---|
| A burst of events | The inbox only inserts. The workers start runs as fast as the platform allows; the rest waits. |
| Two workers grab the same event | Claiming is one SQL statement with `FOR UPDATE SKIP LOCKED`: a row being claimed is skipped, never shared, even across processes. |
| One agent floods the table | Workers walk agents in name order and take each agent's oldest event, so every agent gets a turn. |
| A run hangs | Left to the agent platform: only it sees the run's steps, so it stops a stuck run and reports an error, which the worker records as failed. The queue sets no max runtime, so long runs are fine. |
| A worker dies | A worker reserves an event for 30 seconds and renews the reservation every 10 seconds while the run goes on: a heartbeat. Nobody asks the worker whether it is alive; if it dies, nobody renews, the reservation runs out and the event waits again, within about 30 seconds. A renewal is one small `UPDATE`, about 100 per second for 1,000 runs at once. |
| An old worker comes back | Every write checks `attempts` (how often the event was handed out), like a ticket number. A worker whose reservation was taken over notices at its next heartbeat, stops its run and cannot overwrite the new owner. |
| A run fails | It is marked failed with its error and not retried: Dataleap's run system may already do that, see the open questions. |

The worker is plain deterministic Go. The AI only lives inside the agent run.

## Code

| Path | |
|---|---|
| [`internal/store`](internal/store) | The table and every query: insert, claim, renew, complete, fail, release expired reservations. |
| [`internal/inbox`](internal/inbox) | HTTP handlers and the batched writers. |
| [`internal/worker`](internal/worker) | Claim → run with a heartbeat → done or failed; gives the events of dead workers back. |
| [`internal/agent`](internal/agent) | The `Runner` boundary to the agent platform and the simulated agent. |
| [`cmd/inbox`](cmd/inbox) | The service. |
| [`cmd/loadtest`](cmd/loadtest) | Measures how fast the inbox accepts webhooks: a burst and steady rates. |

## Results

Full output: [results/RESULTS.md](results/RESULTS.md). Read the numbers as an
order of magnitude, not as production capacity; see
[test setup and limits](#test-setup-and-limits).

**What the tests prove** (`make test`, against a real Postgres)

| Behaviour | Test |
|---|---|
| A burst with duplicates is stored once per event, and every sender gets its 200 | `TestABurstWithDuplicatesIsStoredExactlyOnce` |
| A duplicate delivery is stored once and still answered 200 | `TestADuplicateIsStoredOnceAndStillAnswered200` |
| Database down: 503, never a 200 without a saved row | `TestADownDatabaseAnswers503NotAFalse200` |
| Two workers, even in two processes, never take the same event | `TestClaimNeverHandsOutAnEventTwice`, `TestTwoProcessesNeverShareAnEvent` |
| Every event runs once | `TestEveryEventRunsOnce` |
| A dead worker's event is taken over by another worker | `TestAnEventOfACrashedWorkerRunsAgain` |
| A long run keeps its event thanks to the heartbeat | `TestALongRunKeepsItsReservation` |
| One busy agent does not starve the others | `TestClaimGivesEveryAgentATurn` |

**Accepting webhooks while the workers keep running**

p95 and p99 are answer times: 95% and 99% of webhooks got their 200 within
that time.

| Traffic | Errors | p95 | p99 | Answered 200 but not stored |
|---|---|---|---|---|
| 10,000 events for one agent at the same moment (200 parallel senders), all answered after 0.8 s | 0 | 24 ms | 27 ms | 0 |
| 1,000/s for 1,000 agents, 10 s | 0 | 9 ms | 11 ms | 0 |
| **5,000/s (the goal)** for 1,000 agents, 10 s | 0 | 14 ms | **17 ms** | 0 |
| 10,000/s for 1,000 agents, 10 s | 0 | 16 ms | 23 ms | 0 |
| 20,000/s for 1,000 agents, 10 s | 0 | 23 ms | 39 ms | 0 |

The numbers move between runs because the machine is shared: across several
runs p99 at 10,000/s ranged from about 20 to 37 ms.

**What the tests taught us**

- One `INSERT` per webhook topped out at about 4,600/s once every commit was
  really flushed to disk. Batched saves made 10,000/s and 20,000/s work.
- Under overload (40,000/s, eight times the goal, from the same machine) the
  inbox answers 503 and never a 200 without a saved row. But half-open connections left claim
  transactions holding row locks, which `SKIP LOCKED` would skip forever.
  Claiming is now a single SQL statement, so no transaction stays open, and
  TCP keepalives clean up dead connections.
- A burst is never lost, but it takes time to work through. Batching events
  into fewer runs is the next lever.

## Test setup and limits

Everything ran on one machine:

| | |
|---|---|
| Machine | Mac mini (Mac16,11), Apple M4 Pro, 12 cores (8 performance, 4 efficiency), 24 GB RAM, internal SSD |
| Software | macOS 27, Go 1.27, Postgres 18.6 from Homebrew with default settings |
| Durability | Every commit flushed to disk (`wal_sync_method=fsync_writethrough`, set by `scripts/db.sh`). macOS's default skips that flush and looks several times faster. |

What that means for the numbers:

- The load generator, the service and Postgres shared the same 12 cores, so
  they competed with each other. Separate machines would have more headroom.
- There was no network: every request went over loopback. In a data center
  each hop adds latency, typically under a few milliseconds.
- Cloud disks often flush more slowly than a local SSD, which makes batched
  saves matter even more.
- Agent runs are simulated and take milliseconds instead of ~30 seconds. The
  load on the inbox is real; the time to work off a backlog is not.

So the results show that the design holds and roughly where its limits are on
modest hardware. Real capacity needs a test on the production setup.

## Not in this prototype

- **Idempotency keys for effects.** Delivery is at-least-once: a worker that
  dies right after an effect has it repeated by the next worker. The fix
  belongs in the agent's tools, and not every API supports it.
- **Retrying failed runs.** A failed run stays visible with its error.
  Dataleap's run system may already retry runs; if not, the queue can put the
  event back until an attempt limit, then keep it in a dead letter list.
- **Stopping hung runs.** The heartbeat shows that the worker is alive, not
  that the run makes progress. If the agent platform has no timeout of its own,
  the worker can add a generous max runtime.
- **One renewal for all of a process's runs** in a single `UPDATE`, instead of
  one per run.
- **Catch-up from the source** for webhooks that never arrive (Stripe's events
  API could backfill; duplicates are ignored by the unique key).
- **Batching** similar events into one run, the next lever for long backlogs
  and cost.
- Cleaning up old done rows, webhook signature checks, several
  customers or databases.

## Open questions for Dataleap

- **Why are runs dropped today?** A limit on runs at once, a full queue, cost
  protection, or something else? The design assumes the first.
- **How does a run report back: done, failed, hung?** Does the run system
  already retry failed runs or stop hung ones? If not, the queue can retry
  (see above).
- **Is there also a limit per agent**, such as at most 2 runs of the same agent
  at once? Then the worker would have to respect it too, so extra events of
  that agent wait instead of being rejected. This prototype only limits the
  total number of runs.
- **How is a run started?** If the worker only hands it off and the run system
  tracks it from there, the reservation only has to cover that call and the
  heartbeat is not needed. If the worker waits for the result, it is.
- **Are webhooks signed and deduplicated today?**
- **What should happen when a trigger fires while the same agent is still
  running?**
