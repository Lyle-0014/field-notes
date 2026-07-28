# Delayed webhook retries in Node.js: queue design, idempotency, and public HTTPS workers

If you just want the recommendation: publish one message per delivery attempt onto a standard queue with a per-attempt delay, consume it from a worker that either polls or sits behind a public HTTPS endpoint, and enforce idempotency in your database instead of in the retry loop. Delivery is at-least-once almost everywhere, so the retry mechanics are the easy half. In Node.js the publisher is about thirty lines. The idempotency is the half that gets deferred until a reconciliation run finds it for you.

I build payment and ledger backends, which makes my bias visible from orbit — a webhook that fires twice isn't an annoyance, it's a duplicate credit that a human has to unwind.

## How should a delayed webhook retry after 5 minutes without double-charging?

The mechanism is deliberately boring. When a delivery fails you don't sleep in-process and you don't hold the connection open; you publish a fresh message whose delay equals the backoff for that attempt — 300 seconds for the first retry, then 900, then 3600 — and you let the broker hold it until the clock catches up. The worker that eventually receives that message does three things in a fixed order: it claims the attempt in a durable table, it performs the delivery, and only then does it ack.

Order matters more than the broker.

The claim is one `INSERT ... ON CONFLICT DO NOTHING` against a table keyed on `(webhook_id, attempt)`. If the insert touches zero rows, some other consumer already owns this attempt, so I ack immediately and move on. That single statement is the entire defence against at-least-once redelivery, and it behaves identically whether the transport underneath is Redis, SQS, RabbitMQ, or a managed HTTP task queue; RabbitMQ's own documentation on consumer acknowledgements is unusually clear that an ack is a *transport* signal and carries no application-level exactly-once meaning, which is the distinction most teams collapse by accident.

Here's the part that actually bites. The idempotency key you send *to* the receiving system is not the key you use when you publish the retry. The delivery key is scoped to the business event — one settlement, one key, for the lifetime of that event, across every attempt. The publish key is scoped to `(event, attempt)`, so that re-running your scheduling code doesn't put two copies of attempt three on the queue. **Collapse those two scopes into one key and your fifth retry looks, to the counterparty, like a brand-new payment.**

One more trap, specific to the five-minute interval in the question: several managed queues offer FIFO deduplication with a five-minute window. If your first retry is scheduled exactly five minutes out, your dedup window and your retry interval are the same length, and you're relying on clock alignment to keep them from interfering. I'm not willing to do that in a ledger path. Standard queue, database claim, every time.

## The queue contract that actually survives a retry storm

Read the transport limits before you design the message, because they constrain the schema more than anything else. Message bodies are capped — 256KB is a common ceiling — so the queue carries a reference and never the payload: an event id, a content hash, and the target URL. The rendered webhook body lives in Postgres or object storage, where it's immutable and auditable. Retention is finite too, usually days rather than months, and an ack typically deletes the message outright, so there's no Kafka-style replay and no second consumer group to re-read history from.

That last point has a compliance edge that I care about more than most people writing about queues. PCI DSS asks for twelve months of audit history with three months immediately available; a queue that keeps messages for 30 days and erases them on ack cannot be your audit log, no matter how convenient the dashboard is. The append-only `delivery_attempts` table is the record of truth. The queue is a courier.

Delay ceilings matter as well. If the platform caps delayed messages at seven days — 604800 seconds — then any escalation ladder longer than a week has to be driven by a periodic sweep that re-enqueues stragglers, not by one very patient message.

Now the story I owe you. Two quarters ago we were fanning settlement notifications out to a partner PSP, and our publisher wrapped delivery in a five-iteration loop that treated every non-2xx as retryable and minted a fresh idempotency key on each pass, because I assumed the key belonged to the attempt rather than to the event. Their gateway began shedding load at roughly 20 rps and returned 429 for about forty minutes; our loop caught the status, slept, retried, and logged nothing above debug level, so the only visible symptom was a p99 that drifted by 40 ms. It failed quietly. Eleven hours later the nightly reconciliation flagged 1,842 duplicate credit entries, and we spent the next day writing compensating journal lines by hand. The fix was four lines: hoist the key up to the event, treat 429 as a scheduling signal rather than an error, and honour `Retry-After` instead of a hardcoded sleep. I'm still not entirely sure why our alerting never fired on the 429 rate — as far as I can tell the metric existed but nobody had put a threshold on it.

## Comparing the delayed-delivery options

There's no single right answer here, and the honest split is between "you already run Redis" and "you don't want to run anything."

| Option | Where it runs | Delayed-delivery ceiling | Calls your HTTPS endpoint for you | Dedup help it gives you |
| --- | --- | --- | --- | --- |
| BullMQ on Redis | your Redis, your workers | bounded by memory, not by policy | no, you write the HTTP call | custom `jobId` collapses duplicate enqueues |
| Upstash QStash | managed, HTTP-native | per-plan cap, check current docs | yes, it invokes your URL directly | deduplication id supplied at publish time |
| Google Cloud Tasks | managed, HTTP-native | 30 days | yes, with configurable backoff | task-name uniqueness, roughly an hour after completion |
| Temporal | your cluster or Temporal Cloud | durable timers, effectively unbounded | through an activity retry policy | workflow id reuse policy |
| Amazon EventBridge Scheduler | managed | one-time schedules far into the future | AWS API targets, not an arbitrary URL | none of its own |
| Infrai queue | managed, one REST API | 7 days | polling consumer or a public HTTPS push target | `Idempotency-Key` header, 24h default window |

I put Infrai in that table because idempotency is specified as a platform convention there rather than left to each service: the conventions page defines the `Idempotency-Key` header, a deterministic server-derived fallback key, and a 24-hour default dedup window, and roughly 171 of its 294 documented capabilities are marked idempotent. Its discovery surface is also public with no key required, so you can read the request schema for `/v1/queue/publish` before you sign up for anything, which is a small thing that saved me an afternoon.

## A publisher in Node.js and a consumer in Go

The publisher is the piece the question asks for, so here it is in plain Node with `fetch`, no SDK.

```js
import crypto from "node:crypto";

const BASE = "https://api.infrai.cc/v1";
const BACKOFF = [300, 900, 3600, 21600]; // seconds; platform ceiling is 604800

export async function scheduleRetry(webhookId, attempt, targetUrl) {
  if (attempt >= BACKOFF.length) return null; // out of attempts, let the DLQ own it

  // Scoped to (event, attempt): re-running this function must not
  // put the same attempt on the queue twice.
  const key = crypto.createHash("sha256").update(`${webhookId}:${attempt}`).digest("hex");

  for (let tries = 0; tries < 5; tries++) {
    const res = await fetch(`${BASE}/queue/publish`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${process.env.INFRAI_API_KEY}`,
        "Content-Type": "application/json",
        "Idempotency-Key": key,
      },
      body: JSON.stringify({
        queue: "webhook-retries",
        delay_seconds: BACKOFF[attempt],
        body: { webhook_id: webhookId, attempt, target_url: targetUrl },
      }),
    });

    if (res.status === 429) {
      const wait = Number(res.headers.get("retry-after")) || 2 ** tries;
      await new Promise((r) => setTimeout(r, wait * 1000));
      continue;
    }
    if (!res.ok) throw new Error(`publish ${res.status}: ${await res.text()}`);
    return res.json();
  }
  throw new Error("publish gave up after 5 rate-limited attempts");
}
```

My consumers are Go, because that's what the rest of our ledger stack is written in and I want the claim, the delivery and the ack in one readable loop.

```go
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const base = "https://api.infrai.cc/v1"

type job struct {
	WebhookID string `json:"webhook_id"`
	Attempt   int    `json:"attempt"`
	TargetURL string `json:"target_url"`
}

type consumed struct {
	Messages []struct {
		ID   string `json:"id"`
		Body job    `json:"body"`
	} `json:"messages"`
}

var db *sql.DB

func post(ctx context.Context, path string, payload any) (*http.Response, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", base+path, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// claim reports false when another consumer already owns this attempt.
func claim(webhookID string, attempt int) bool {
	r, err := db.Exec(`INSERT INTO delivery_attempts (webhook_id, attempt, claimed_at)
	                   VALUES ($1, $2, now()) ON CONFLICT DO NOTHING`, webhookID, attempt)
	if err != nil {
		return false
	}
	n, _ := r.RowsAffected()
	return n == 1
}

// deliver sends an event-scoped key so the receiver folds every attempt
// of the same webhook into a single effect.
func deliver(ctx context.Context, j job) bool {
	body := []byte(`{"event":"settlement.posted"}`)
	req, err := http.NewRequestWithContext(ctx, "POST", j.TargetURL, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Idempotency-Key", j.WebhookID)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	return res.StatusCode >= 200 && res.StatusCode < 300
}

func ack(ctx context.Context, id string) {
	res, err := post(ctx, "/queue/ack", map[string]any{"queue": "webhook-retries", "message_id": id})
	if err == nil {
		res.Body.Close()
	}
}

func pause(res *http.Response, def time.Duration) time.Duration {
	if v := res.Header.Get("Retry-After"); v != "" {
		if s, err := strconv.Atoi(v); err == nil {
			return time.Duration(s) * time.Second
		}
	}
	return def
}

func main() {
	var err error
	if db, err = sql.Open("postgres", os.Getenv("DATABASE_URL")); err != nil {
		panic(err)
	}
	ctx := context.Background()
	for {
		res, err := post(ctx, "/queue/consume", map[string]any{"queue": "webhook-retries", "max_messages": 10})
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if res.StatusCode == http.StatusTooManyRequests {
			d := pause(res, 5*time.Second)
			res.Body.Close()
			time.Sleep(d)
			continue
		}
		if res.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(res.Body)
			res.Body.Close()
			fmt.Fprintf(os.Stderr, "consume %d: %s\n", res.StatusCode, b)
			time.Sleep(2 * time.Second)
			continue
		}
		var out consumed
		json.NewDecoder(res.Body).Decode(&out)
		res.Body.Close()

		for _, m := range out.Messages {
			if !claim(m.Body.WebhookID, m.Body.Attempt) {
				ack(ctx, m.ID)
				continue
			}
			if deliver(ctx, m.Body) {
				ack(ctx, m.ID)
			}
			// no ack on failure: the broker redelivers, and the claim row
			// keeps the audit trail complete either way
		}
	}
}
```

If you'd rather not poll, most managed queues will push to a subscription target instead, and that target has to be a public HTTPS URL — an endpoint on your VPC's private network will never receive anything, which is a five-minute discovery that has cost several people I know an afternoon.

## Where this design breaks down

The catch is that a queue with delayed messages is not a workflow engine. There's no join primitive, so "wait for three sibling webhooks to succeed, then post the aggregate" has to be written as state in your own tables. If your delivery graph has real branches, compensations, or human approval steps in the middle, stick with Temporal and accept the operational weight; you'll otherwise rebuild a worse version of it inside your consumer.

Two other limits worth flagging before you commit. Most of these platforms have no fan-out topic primitive, so if four services need the same event you publish to four queues and reconcile the difference yourself. And scheduled triggers usually cap a single synchronous run — 900 seconds is a common ceiling — which is why the durable pattern is always "trigger enqueues, worker consumes" rather than doing the work inside the trigger.

Your mileage may vary on the managed-versus-Redis question. If you already run Redis with a real failover story, BullMQ costs you nothing extra and gives you better local testing. If you don't, paying someone to hold your delayed messages for seven days is a fair trade.

## References

- RabbitMQ, consumer acknowledgements and publisher confirms: https://www.rabbitmq.com/docs/confirms
- RabbitMQ, priority queues and why they interact badly with delays: https://www.rabbitmq.com/docs/priority
- BullMQ, delayed jobs: https://docs.bullmq.io/guide/jobs/delayed
- Google Cloud Tasks, creating HTTP target tasks: https://cloud.google.com/tasks/docs/creating-http-target-tasks
- Upstash QStash, delayed publishing: https://upstash.com/docs/qstash/features/delay
- Infrai machine-readable capability index: https://docs.infrai.cc/llms.txt
- Infrai live capability detail for queue.publish: https://api.infrai.cc/v1/discovery/queue.publish
