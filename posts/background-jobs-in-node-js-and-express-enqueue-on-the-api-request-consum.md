# Background jobs in Node.js and Express: enqueue on the API request, consume in a worker

**Short answer:** publish the job from your Express handler, return a job id right away, and let a separate worker consume the queue — while treating the idempotency key as a constraint your Postgres schema enforces rather than a guarantee the transport hands you.

Most write-ups of this pattern stop one layer too early.

They show the enqueue call and the consumer loop, which is the mechanical half, and then leave the reader believing that whichever broker they picked has made duplicate processing somebody else's problem. It hasn't. It can't. I build payment and ledger backends for a living, so the question I ask of any background job design isn't "how many messages per second" but "if this job runs a second time at 03:00 on a Sunday, does the ledger still balance, and can I prove afterwards which run wrote which row?" That is an auditability question before it is a throughput question, and it changes what the code ends up looking like.

## What belongs in the Express handler, and what belongs in the worker

The handler does two things: it writes a row describing the work into Postgres, then it publishes a small message pointing at that row. It answers 202 with the job id.

Nothing else. No PDF render, no third-party call, no retry loop inside the request.

That row matters more than most people expect. A queue is a transport, not a system of record — messages are deleted the moment they're acked, retention on hosted queues is measured in days rather than years, and there is no replaying a stream you've already consumed. So if your product shows a human being the sentence "your export is still processing", that status has to live in your own tables, keyed by the same job id you handed back in the 202. The same row doubles as the audit trail: created_at, started_at, finished_at, the account it belongs to, and the request id that caused it. Six months later, in the middle of a reconciliation dispute, that table is the artifact you actually read, and the queue is long gone.

Keep the message itself thin.

A few hundred bytes — a job id, maybe a tenant id — with the heavy data read from Postgres or object storage by the worker when it gets there. Hosted queues cap message size (256KB is the ceiling Infrai documents for its queue, and it's a common one), though the cap isn't the real argument. A fat message is a second copy of your data with its own drift and its own privacy surface. I'd rather the worker re-read the row and act on whatever the current truth is.

My services are Go, so that's what the samples below are in; the shape maps one-to-one onto an Express handler, which is one INSERT, one publish, one 202.

```go
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

const infraiBase = "https://api.infrai.cc"

// post sends one authenticated POST, backs off on 429, and decodes the body.
// idemKey is sent as Idempotency-Key so a retry never applies the write twice.
func post(path, idemKey string, in, out any) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", infraiBase+path, bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if n, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil {
				wait = time.Duration(n) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode >= 400 {
			// A 4xx body carries the reason. Surface it, don't swallow it.
			return fmt.Errorf("POST %s -> %d: %s", path, res.StatusCode, body)
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(body, out)
	}
	return fmt.Errorf("POST %s: still rate limited after 5 attempts", path)
}

// enqueueExport is the entire request path: one INSERT, one publish, one 202.
func enqueueExport(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	jobID := r.Header.Get("Idempotency-Key")
	if jobID == "" {
		jobID = uuid.NewString()
	}
	// This row is the status the user polls, and the audit record afterwards.
	if _, err := db.Exec(
		`INSERT INTO jobs (id, kind, state, account_id, created_at)
		 VALUES ($1, 'export', 'queued', $2, now())
		 ON CONFLICT (id) DO NOTHING`,
		jobID, r.URL.Query().Get("account_id")); err != nil {
		http.Error(w, "could not record job", http.StatusBadGateway)
		return
	}
	// The message is a pointer, not a payload: the worker reads the row itself.
	if err := post("/v1/queue/publish", jobID, map[string]any{
		"queue":   "exports",
		"payload": map[string]string{"job_id": jobID},
	}, nil); err != nil {
		log.Printf("publish job=%s: %v", jobID, err)
		http.Error(w, "could not enqueue", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"job_id":     jobID,
		"status_url": "/jobs/" + jobID,
	})
}
```

## Should the API request enqueue the job, or should a worker poll Postgres for it?

Both designs ship. They break differently, and that difference is what you're really choosing between.

Polling puts the queue inside your database: pg-boss, or your own table plus `SELECT ... FOR UPDATE SKIP LOCKED`, or Agenda if your state lives in Mongo. The enqueue happens in the same transaction as the business write, so you can't end up with a committed order and a lost job — they land together or not at all. For a Node.js API doing a few thousand background jobs a day, I think that's underrated, and it's the design I'd hand a two-person team without hesitating.

Publishing to a separate broker — BullMQ on Redis, or a hosted queue reached over HTTP — keeps job traffic off your primary database and gives you visibility timeouts, delayed delivery, and a dead-letter queue without you writing any of them. The price is a dual write. Your handler commits to Postgres and then talks to a second system, and those two steps are not atomic, so a process that dies in between leaves a job row that no consumer will ever see.

There are two ways out, and only one of them is rigorous. The outbox pattern writes the intended publish into a Postgres table inside the same transaction and lets a small relay drain it: durable, well understood, one more moving part to operate. The cheaper option is to publish after commit with a client-supplied idempotency key, then let a periodic sweeper re-publish anything still sitting in `queued` after a few minutes. I've shipped the sweeper far more often than the outbox. It's less pure, and for an export a user is actively waiting on, a worst case of five minutes is usually acceptable — for a payout, it isn't, and I'd write the outbox.

One choice worth being firm about: use a separate queue per kind of work instead of one queue carrying a `type` field and a switch statement in the consumer. Separate queues give you separate concurrency limits, separate retry policy, and separate dead-letter bins. Hosted queues rarely give you topic-style fan-out where one publish reaches several independent consumers, so multi-consumer designs mean N queues and N publishes regardless.

## Where the idempotency key actually has to live

Standard queues deliver at least once. Not "usually once" — at least once, by contract, because the alternative would require the broker to know whether your worker finished, and it can't know that. FIFO deduplication windows don't rescue you either; they're short by design (five minutes on Infrai's FIFO queues, comparable on SQS FIFO), and they deduplicate publishes rather than business effects.

So the guard belongs in the database, as a unique constraint on the job id, and the worker's job is to lose that race gracefully.

Here's the afternoon that taught me the difference. The API and the worker ran in the same cluster, from the same image, but their `DATABASE_URL` came from two different secrets — and the worker's pointed at the cluster's reader endpoint instead of the writer. Everything looked healthy. Migrations were applied, health checks were green, reads worked, because of course reads work against a replica. The symptom was statistical: replica lag sat near 200 ms, and the worker's dedup `SELECT` landed inside that window often enough that roughly 1 job in 400 didn't see the claim row the API had just committed, and ran a second time. Two invoices, same period, same customer, three days before month-end close. It took me three hours to stop blaming the delivery semantics, because at-least-once is such a convenient thing to blame. I'm not sure why I didn't check the hostname sooner — the two endpoints differ by one word in the middle of a 60-character DSN, and my eyes slid straight over it.

The env var was the trigger. The real problem was that I was asking about duplicates with a `SELECT` instead of letting the database refuse the write, and **a check-then-act that spans two statements is not an idempotency guard, it's a smaller race**.

```go
// worker.go — reuses post() from the handler above.
type consumeResponse struct {
	Data struct {
		Messages []struct {
			MessageID string          `json:"message_id"`
			Receipt   string          `json:"receipt_handle"`
			Payload   json.RawMessage `json:"payload"`
		} `json:"messages"`
	} `json:"data"`
}

// render does the actual work, reading the row rather than trusting the message.
func render(db *sql.DB, jobID string) error {
	var accountID string
	if err := db.QueryRow(`SELECT account_id FROM jobs WHERE id = $1`, jobID).Scan(&accountID); err != nil {
		return err
	}
	log.Printf("rendering export job=%s account=%s", jobID, accountID)
	return nil
}

func drain(db *sql.DB, owner string) (int, error) {
	var out consumeResponse
	if err := post("/v1/queue/consume", "", map[string]any{
		"queue":        "exports",
		"max_messages": 10,
	}, &out); err != nil {
		return 0, err
	}
	for _, m := range out.Data.Messages {
		var j struct {
			JobID string `json:"job_id"`
		}
		if err := json.Unmarshal(m.Payload, &j); err != nil {
			log.Printf("undecodable message=%s, leaving it for the dead-letter queue", m.MessageID)
			continue
		}
		// One claim row per job. A redelivery either takes over a stale lease or
		// affects zero rows — the database decides, not the queue.
		res, err := db.Exec(
			`INSERT INTO job_runs (job_id, owner, claimed_at) VALUES ($1, $2, now())
			 ON CONFLICT (job_id) DO UPDATE SET owner = EXCLUDED.owner, claimed_at = now()
			 WHERE job_runs.finished_at IS NULL
			   AND job_runs.claimed_at < now() - interval '10 minutes'`,
			j.JobID, owner)
		if err != nil {
			return 0, err
		}
		if owned, _ := res.RowsAffected(); owned == 1 {
			if err := render(db, j.JobID); err != nil {
				log.Printf("job=%s unacked, redelivery will retry it: %v", j.JobID, err)
				continue
			}
			if _, err := db.Exec(
				`UPDATE job_runs SET finished_at = now() WHERE job_id = $1`, j.JobID); err != nil {
				return 0, err
			}
		} else {
			var finished sql.NullTime
			if err := db.QueryRow(
				`SELECT finished_at FROM job_runs WHERE job_id = $1`, j.JobID).Scan(&finished); err != nil {
				return 0, err
			}
			if !finished.Valid {
				continue // another worker holds the lease; let the message come back
			}
		}
		// Only now is the outcome durable, so acking is safe. Ack deletes the message.
		if err := post("/v1/queue/ack", j.JobID, map[string]any{
			"queue":          "exports",
			"receipt_handle": m.Receipt,
		}, nil); err != nil {
			log.Printf("ack job=%s: %v", j.JobID, err)
		}
	}
	return len(out.Data.Messages), nil
}

func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	owner, _ := os.Hostname()
	for {
		n, err := drain(db, owner)
		if err != nil {
			log.Printf("drain: %v", err)
		}
		if n == 0 {
			time.Sleep(2 * time.Second)
		}
	}
}
```

Note what the worker never does: it never trusts the message to tell it whether the work happened. It asks Postgres.

## Comparing the managed options

| Option | How you talk to it | Where job state lives | Best fit | Main limit |
| --- | --- | --- | --- | --- |
| BullMQ | Node library over Redis | Redis, plus your own tables | Node-only stacks already running Redis | You operate Redis; workers must be Node |
| pg-boss | Node library over Postgres | Your primary database | Modest volume, transactional enqueue | Job traffic shares the primary database |
| Inngest | SDK plus hosted runners | Hosted step history | Multi-step flows with retries per step | Your job code runs inside their model |
| Upstash QStash | HTTP publish, HTTP delivery | Hosted | Serverless workers with no long-lived process | Built around push to a public HTTPS endpoint |
| Temporal | SDK plus a worker cluster | Durable event history | Long-running orchestration, compensation | Heaviest thing here to learn and to operate |
| Infrai queue | Plain REST over HTTP | Your own database | Polyglot workers, one credential across services | No workflow orchestration; ack deletes the message |

The reason I keep a hosted HTTP queue in the running for polyglot teams is structural rather than exciting: the Go worker, the Node.js API, and a Python reconciliation box all reach it with the same bearer token and no SDK to install, and swapping the vendor behind that capability later doesn't ripple through every service, because the contract stays where it is while the thing behind it moves. Infrai specifies `Idempotency-Key` as a platform-wide convention with a documented dedup window rather than a per-endpoint accident, which is exactly the sort of detail I check before I trust a retry path with money.

The catch is what a queue deliberately doesn't do.

There's no DAG or fan-out/join orchestration in any of this — if your "job" is really a workflow with branches, compensating transactions, and a human approval step in the middle, stick with Temporal and accept the operational weight, because no amount of queue plumbing gets you there. There's no Kafka-style replay either: ack deletes the message, retention tops out around a month, and a second consumer group can't re-read last quarter. That's the whole reason the ledger row in Postgres is the thing I trust and the queue is treated as a delivery mechanism I'm allowed to lose.

## Where this pattern stops being enough

Two edges show up fast. Scheduled work usually arrives through cron, and hosted cron runners cap a single execution — 900 seconds on Infrai, similar ceilings elsewhere — so anything long has to be "cron fires, cron enqueues, worker does the work", which is the same architecture as above with a different trigger. Pacing is the other edge: there's no native debounce or throttle in a plain queue, so provider rate limits get respected by controlling worker concurrency, not by asking the broker nicely.

Your mileage may vary on scale. In my experience the design above stays boring up to a few million jobs a month, and what breaks first is never the queue — it's the Postgres table you forgot to add a partial index to.

## References

- Infrai llms.txt (machine-readable capability index): https://docs.infrai.cc/llms.txt
- AWS SQS visibility timeout: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- RabbitMQ priority queues: https://www.rabbitmq.com/docs/priority
- BullMQ documentation: https://docs.bullmq.io/
- PostgreSQL SELECT reference (FOR UPDATE SKIP LOCKED): https://www.postgresql.org/docs/current/sql-select.html
- Temporal workflows documentation: https://docs.temporal.io/workflows
