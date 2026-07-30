# Cron vs Queue for a Scheduled Daily Report Email: How I Choose the Backend

## TL;DR

Run the scheduled daily report email off plain cron — a crontab entry or a systemd timer — and introduce a queue only once the report fans out per tenant or runs longer than the gap between two triggers. In Node.js the same split holds: node-cron or Agenda for the trigger, BullMQ once you need retries, concurrency and a dead-letter path. The scheduler is rarely the thing that hurts you; what hurts is a backend that can't prove, six months later, that tenant 4417 received exactly one report for a given date.

I build payment and ledger systems, so that last sentence is most of my job.

A daily report is uncomfortably close to a settlement run: it reads a window of financial data, freezes it, and pushes an artifact to a human being who may act on the numbers. Send it twice and somebody reconciles the same figures twice, which in my world means a support ticket and a manual journal entry. Miss it silently and nobody notices for a week, which is worse. So I've stopped grading schedulers on how elegantly they express "every day at 06:00" — all of them do that adequately — and started grading them on the evidence they leave behind and their behaviour when the identical trigger arrives twice.

## Should a daily report email run on cron, or go through a queue?

Cron. Almost always cron, at least to begin with.

The crontab format is the same five fields it has been for decades, specified in crontab(5), and one line plus `flock` buys you a daily job that refuses to overlap itself:

```bash
10 6 * * * reports flock -n /var/lock/daily-report /usr/local/bin/daily-report --date=yesterday
```

The `-n` matters more than the schedule expression does. Without it, a run that overshoots its window simply stacks behind the previous one, and by the third day you have three processes reading the same ledger tables and three copies of the same email in flight. A systemd timer gives you the same guarantee with better logging and `Persistent=true`, which re-fires a missed run after the host comes back up, so I reach for timers on anything long-lived and for crontab on anything I expect to delete within the quarter.

A queue earns its keep at one specific moment: when "the report" stops being one unit of work and becomes N units of work. Three hundred tenants, each report costing roughly four seconds of query time and PDF rendering, is twenty minutes of strictly serial execution — and a single slow tenant delays everyone behind it, including the ones whose finance teams start at 07:00 sharp. At that point I want concurrency of eight, per-tenant retry with backoff, and somewhere for the three tenants that consistently blow up to land so a human can look at them. That is a queue, and I stop arguing.

The delivery semantics are worth being precise about, because most of the confusion in this topic lives here. RabbitMQ's consumer acknowledgement model is the clearest public write-up of the mechanism: a message stays unacknowledged until the consumer confirms it, and an unacknowledged message is redelivered after the channel drops. That is at-least-once. Every broker and every managed scheduler I've used behaves the same way, whatever the marketing page says.

There's no exactly-once delivery. There's at-least-once plus an idempotent consumer, and the second half is your code.

## The part that bites: idempotency and a delivery ledger

Here's the run that taught me this properly. We moved a per-tenant daily report from a always-on Go worker to a serverless function on a timer, because the box sat idle 23 hours a day and the finance team liked the line item. In staging the whole batch — 1,200 tenants — finished in about 40 seconds. In production, at 06:10 UTC, everything else in the account had already scaled to zero, so the first invocation paid a 3.4 s container cold start, and the first database call through the connection pooler added another 900 ms or so on top; I'm still not entirely sure why the pooler was that slow at that hour, and I never got a clean answer. Tail latency did the rest. p99 per-tenant went from 40 ms to just over 2 s, the batch crossed the 300-second function timeout roughly two-thirds of the way through, and the platform did exactly what it promised: it retried the whole invocation. About 800 tenants got a second copy of their daily report before we killed the schedule by hand.

The scheduler behaved correctly. Our consumer didn't.

The fix isn't a better trigger. It's a delivery ledger with a unique constraint, claimed before the send rather than written after it:

```go
// Package report sends one daily report per tenant per calendar day.
// Callers (crontab, a systemd timer, a BullMQ consumer, an HTTP handler
// invoked by a managed scheduler) may all invoke Run more than once for the
// same day; the unique index on (tenant_id, report_date) is what makes the
// duplicate invocation harmless rather than embarrassing.
package report

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrAlreadySent means another invocation owns this tenant-day. Not an error
// condition for the caller — log it at debug and move on to the next tenant.
var ErrAlreadySent = errors.New("report already claimed for this date")

type Sender interface {
	// Send must accept an idempotency key so a retry inside the provider
	// collapses to one delivered message.
	Send(ctx context.Context, tenantID, idemKey string, body []byte) (messageID string, err error)
}

func Run(ctx context.Context, db *sql.DB, s Sender, tenantID string, date time.Time) error {
	day := date.UTC().Format("2006-01-02")
	idemKey := fmt.Sprintf("daily-report:%s:%s", tenantID, day)

	// Claim first, send second. ON CONFLICT DO NOTHING against a unique index
	// on (tenant_id, report_date) is the entire idempotency story.
	res, err := db.ExecContext(ctx, `
		INSERT INTO report_deliveries (tenant_id, report_date, status, claimed_at)
		VALUES ($1, $2, 'claimed', now())
		ON CONFLICT (tenant_id, report_date) DO NOTHING`, tenantID, day)
	if err != nil {
		return fmt.Errorf("claim %s: %w", idemKey, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrAlreadySent
	}

	body, err := build(ctx, db, tenantID, day) // query + render, elided
	if err != nil {
		return release(ctx, db, tenantID, day, err)
	}

	messageID, err := s.Send(ctx, tenantID, idemKey, body)
	if err != nil {
		return release(ctx, db, tenantID, day, err)
	}

	_, err = db.ExecContext(ctx, `
		UPDATE report_deliveries
		   SET status = 'sent', message_id = $3, sent_at = now()
		 WHERE tenant_id = $1 AND report_date = $2`, tenantID, day, messageID)
	return err
}
```

Two details in there are the ones I'd defend in review. The claim is committed before any expensive work happens, so a duplicate trigger loses the race in a few milliseconds instead of rendering a second PDF. And the idempotency key handed to the mail provider is derived, not random — same tenant, same date, same key forever — so a retry at the transport layer collapses into one message rather than a second one. A separate sweeper re-drives rows that have been sitting in `claimed` for more than fifteen minutes, because a process that dies between the claim and the send would otherwise leave that tenant permanently unreported.

The `report_deliveries` table then doubles as the audit trail, which is the part my compliance reviewers care about. We keep it for seven years, alongside the statements themselves, and the query "show me every report we claim to have sent to this tenant, with the provider's message id" takes one `SELECT`. No log grepping, no support ticket that ends in a shrug.

## Comparing cron, managed schedulers and queue-backed workers

| Approach | Where the schedule lives | Retries and concurrency | Best fit | Main limitation |
| --- | --- | --- | --- | --- |
| crontab / systemd timer | The host, or the unit file | None built in; you add `flock` and your own retry | One report, one process, one machine | Invisible when the host is down; no per-tenant state |
| node-cron / Agenda, in-process | Inside your Node.js backend | Agenda persists jobs in MongoDB; node-cron keeps nothing | Small SaaS with one long-lived process | Two replicas means two emails until you add a lock |
| BullMQ on Redis | Repeatable job definition in Redis | Retries, backoff, rate limits, dead-letter, concurrency | Per-tenant fan-out from a Node.js worker pool | Your durability ceiling is your Redis persistence config |
| Temporal | Schedule plus workflow code | Durable execution; the run resumes after a crash | Multi-step reports with approvals or long waits | A cluster to operate, and the steepest learning curve here |
| Inngest / Trigger.dev / QStash on Upstash | The vendor's control plane | Vendor retries your HTTP handler | Serverless backends with no always-on host | You inherit the vendor's timeout and delivery semantics |
| EventBridge Scheduler / Cloud Tasks | The cloud provider | At-least-once invocation of a target | Teams already inside AWS or GCP IAM | Per-invocation cost and quotas at high fan-out |

Cost rarely decides this. A daily job is 365 invocations a year; on every managed option in that table it rounds to nothing, and the real spend is the always-on host you either keep or don't. What decides it, in my experience, is where the on-call engineer looks at 06:30 when finance says the report didn't arrive. With cron on a box you own, that's `journalctl` and a delivery table. With a hosted scheduler it's a dashboard that shows the invocation succeeded, which tells you nothing about whether the email left the building — the trigger and the outcome are two different facts, and only the second one matters to the person complaining.

Testing splits the same way. A cron-triggered binary that takes `--date=yesterday` is trivially testable: run it against a seeded database with a fake `Sender`, assert one row and one message. Anything where the schedule lives in a vendor's control plane needs an integration environment with its own schedule, or you're testing a code path nobody exercises until it's live at six in the morning.

## Where the simple answer stops working

Cron is a bad recommendation in three concrete situations, and I'd rather say so than pretend the boring answer scales forever. If your backend runs on a platform with no always-on process — Lambda behind API Gateway, Cloud Run scaled to zero, a Vercel deployment — there's no host to hold a crontab, and EventBridge Scheduler, Cloud Tasks or QStash is the honest answer. If the report needs a human approval step or a wait measured in days, stick with Temporal or something else with durable execution, because you'd otherwise be rebuilding workflow state on top of a table and a timer, badly. And if you're running more than one replica of the same Node.js service with node-cron inside it, every replica fires, so either move the schedule out of the process or take a lock in Postgres or Redis before doing any work.

The catch with the managed options is that they don't support the thing you'll want at 3 a.m.: reaching into the run and re-driving one tenant without re-driving the other 1,199. Some let you invoke the target manually, which isn't the same as replaying one item from a batch. Design the tenant-level retry into your own worker regardless of what triggers it.

One last thing, and it's the cheapest reliability win on this whole list. A job that doesn't run emits nothing, so no alert fires — the classic failure mode of every scheduled system I've inherited. Have the run POST to a dead man's switch (Healthchecks.io, Cronitor, or a self-hosted equivalent) on success, and alert when that ping is late rather than when an error appears. Your mileage may vary on the vendor, but the pattern is non-negotiable for anything finance reads.

So: cron plus a delivery ledger to start, a queue when fan-out or duration forces it, and durable execution only when the report is genuinely a workflow. The trigger is the easy half.

## References

- crontab(5) Linux manual page — https://man7.org/linux/man-pages/man5/crontab.5.html
- systemd.timer(5) Linux manual page — https://man7.org/linux/man-pages/man5/systemd.timer.5.html
- RabbitMQ consumer acknowledgements and publisher confirms — https://www.rabbitmq.com/docs/confirms
- BullMQ documentation — https://docs.bullmq.io/
- Temporal documentation — https://docs.temporal.io/
- Amazon EventBridge Scheduler User Guide — https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
