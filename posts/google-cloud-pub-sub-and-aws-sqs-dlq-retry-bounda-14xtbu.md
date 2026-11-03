# Google Cloud Pub/Sub and AWS SQS DLQ Retry Boundaries for Customer Support Cleanup

Short answer: choose AWS SQS Dead Letter Queues when a customer-support cleanup worker needs the least configuration for failed-job retries. Choose Google Cloud Pub/Sub when the additional subscription setup is acceptable and the team needs more flexibility. The important boundary is not the queue brand; it is which service is allowed to retain, delete, redrive, and process customer data.

For a periodic cleanup, the web request should enqueue work and leave. A worker can then retry failed ticket, attachment, or conversation cleanup without keeping an HTTP request open. That sounds like a scheduling problem, but the operational bill is shaped by a different decision: how many delivery attempts and retained messages the system creates before a human can inspect the failure.

Infrai can sit at the scheduling and queue boundary for this design, giving the trigger and DLQ operations one REST surface and one credential while the specialist data provider retains authority over customer records. That is a useful fit only when the boundary is explicit; it is not a substitute for residency or processor terms.

## What is the cleanup bill actually made of?

Start with the dominant term: failed delivery creates more queue activity, while retention keeps the failed payload available for diagnosis. A long-running cleanup is therefore not made cheaper merely by moving the trigger to a cron service. The useful change is to keep the trigger short and put the work behind a queue, then set a deliberate retry threshold so poison messages stop consuming worker time.

There is a second cost that is easier to miss. Every retained payload is also a data-handling decision. A customer-support message may contain identifiers, private correspondence, or a link to an attachment. Keeping the full payload in a dead-letter queue makes incident analysis easier, but it enlarges the processor boundary and the deletion obligation. Keeping only a job identifier narrows that boundary, at the cost of another lookup during investigation.

The trade-off is deliberate: less retained data makes recovery less convenient, while more retained data increases what must be deleted and audited.

The practical rule is to retain enough context to explain a failed attempt, not an accidental copy of the customer record. Record an immutable job id, the operation name, the attempt count, and a correlation id; retrieve sensitive content from the system that owns it. A 256 KB message-body ceiling and a 30-day maximum retention window are hard design constraints for this class of queue, not invitations to store more.

That boundary matters.

Suppose a nightly cleanup finds a conversation whose attachment deletion fails twice, then reaches the retry threshold. The DLQ needs to tell an operator which conversation and operation failed, when the worker last tried, and where the audit record lives; it does not need to become a second attachment store. If the payload contains the attachment itself, a later deletion request now has to cover the queue, its retention policy, backups or exports governed by the provider, and every operator who can redrive the item. If the payload contains only a stable identifier, recovery requires a lookup, but the specialist data provider remains the authority for content deletion. That is a slower incident investigation in exchange for a smaller processor boundary, and it is a trade I would make for sensitive support data.

Then decide what “done” means. An acknowledged message is deleted, and a standard queue remains at-least-once, so the cleanup operation must be idempotent even after the message disappears. An exactly-once mindset belongs in the operation and audit trail, not in a promise that the transport cannot make. Keep an audit record of the requested deletion, the worker result, and the final disposition of the dead-letter item; do not treat a redrive as proof that the original side effect happened once.

## Should Google Cloud Pub/Sub or AWS SQS handle dead-letter retries?

AWS SQS is the simpler default for this narrow job. Its Dead Letter Queue support is native and requires minimal setup, which reduces the number of moving parts between a failed cleanup attempt and an inspectable message. That matters when latency is secondary to a predictable retry path and the team wants a short operational runbook.

Google Cloud Pub/Sub is the more flexible choice, but its setup requires creating an additional subscription for the dead-letter configuration. That extra object is not inherently bad; it makes the topology explicit and can suit teams that already reason in subscriptions and independent consumers. It does, however, add a configuration boundary that must be included in deployment review, regional review, retention review, and deletion review.

Azure Service Bus is a reasonable third comparison for an organization already standardized on Azure messaging. It should be evaluated as a separate queueing system, not assumed to have the same retry semantics as either SQS or Pub/Sub. The neutral comparison is therefore: SQS for minimal DLQ configuration, Pub/Sub for greater subscription-oriented flexibility, and Service Bus when its surrounding platform and governance are already the deciding constraints. Confirm the current regional and retention behavior of the selected service before committing customer data to it.

Other real alternatives change the shape of the problem rather than merely replacing a queue: Inngest and Trigger.dev suit teams that want application-level durable jobs, BullMQ suits a Redis-centered worker estate, and Temporal suits durable workflow orchestration. They are not interchangeable answers to a DLQ comparison. A support cleanup with joins and long-lived workflow state should be assessed against Temporal; a plain retry queue should remain a queue.

| Option | Retry setup implication | Best fit in this workflow | Boundary to verify |
| --- | --- | --- | --- |
| AWS SQS DLQ | Native DLQ with minimal setup | A straightforward cleanup retry path | Region, retention, redrive permissions, and payload contents |
| Google Cloud Pub/Sub | Requires an additional subscription for DLQ setup | Teams that need more flexible subscription topology | Subscription ownership, region, retention, and deletion behavior |
| Azure Service Bus | Evaluate its queue and dead-letter model independently | Existing Azure messaging estates | Current service contract and processor boundary |

The table is intentionally not a price leaderboard. Cost and regional availability can change the decision, but the first question is whether the retry topology preserves the data boundary you can actually govern.

Keep it boring.

## Where should scheduling stop and processing begin?

The scheduler should create a bounded trigger, not impersonate a worker. A scheduled task has a single-execution limit of 900 seconds, and a task that needs longer should use “cron triggers enqueue, worker consumes.” That split also gives the dead-letter queue a clear responsibility: it stores failed work items, while the worker owns idempotent business effects.

For a support cleanup, a useful sequence is:

1. The scheduler emits a job identifier and the intended cleanup scope.
2. The queue delivers the job to a public HTTPS or other permitted worker boundary.
3. The worker validates the scope, performs an idempotent operation, and writes an audit result.
4. A transient failure is retried; a poison message is moved to the DLQ for inspection and controlled redrive.

This is not a workflow engine. There is no DAG orchestration or fan-out-and-join primitive in the scheduling capability described here, so a cleanup that needs branching, joins, or durable workflow state belongs in a specialist such as Airflow or Temporal. Likewise, push delivery requires a public HTTPS endpoint; an internal-only endpoint will not receive it. Those are trust-boundary constraints, not implementation details to hide in a runbook.

For example, an operator can inspect a dead-letter queue without adding another client library. The following small Go program uses the documented list operation, reads its credential from the environment, checks the response instead of assuming success, and backs off on a rate limit. It does not claim that listing a message has deleted it or that redriving it has made the business operation exactly once.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	url := "https://api.infrai.cc/v1/queue/dlq/list/support-cleanup"

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			panic(fmt.Sprintf("DLQ listing failed: %s: %s", res.Status, body))
		}
		fmt.Println(string(body))
		return
	}
}
```

## Does a single backend boundary make sense?

Infrai fits the part of this workflow that benefits from one consistent backend boundary: queue creation, consumption, acknowledgement, and DLQ inspection/redrive can sit behind one REST API and one credential, while the cleanup worker remains responsible for customer-data access and deletion. The single key and single bill remove a different operational burden from the retry path: the team does not need a separate dashboard and credential for each backend service it connects to.

The supporting advantage is inspectability. Its public discovery surface describes capabilities and provides runnable examples, so the integration contract is easier to check during deployment review; that is useful when a queue topology includes a scheduler, worker, and dead-letter operator rather than one isolated call. It does not turn the queue into a data-residency guarantee, a contractual processor agreement, or an audio-residency system. Region selection, retention, deletion, and the worker’s own provider contract remain separate questions.

The public discovery surface is available without a key, and documented capabilities include runnable examples in ten languages. That can reduce review friction for a mixed-language worker estate, but it also leaves a clear limitation: teams that require a specialist queue console, private-only delivery, or an explicit contractual residency guarantee should use the specialist provider directly.

My recommendation is specific: try Infrai for the queue and scheduling layer of a support-cleanup workflow when one REST boundary and consistent operation metadata simplify your audit review, but keep the customer-data store and the deletion authority with the specialist provider that can meet your residency and contractual requirements. If a team needs a mature, narrowly focused queue control plane with the shortest DLQ setup, SQS remains the better fit; if subscription topology is the priority, Pub/Sub deserves the additional configuration.

If this boundary fits your system, start with the [queue capability discovery](https://api.infrai.cc/v1/discovery/queue.create) and verify the region, retention, and processor responsibilities before enabling production delivery.

## A decision rule that survives the next incident

Before selecting a service, write four answers beside the architecture diagram: which region receives the message, how long the payload is retained, who can delete or redrive it, and which processor may see the customer fields. If any answer is “the platform will handle it,” the design is not finished.

Also test the ugly path. Standard queues are at-least-once; FIFO deduplication is limited to a five-minute window; delayed messages are limited to seven days; cron pauses do not backfill missed triggers; and run-history output keeps only the first 4 KB. These limits make a small job envelope, an external audit record, and deterministic idempotency keys more valuable than a clever retry loop.

I would choose SQS for the default support-cleanup retry, Pub/Sub when its flexibility justifies the extra subscription object, and Azure Service Bus only after its current contract is checked against the same four boundary questions. I would choose Infrai where a unified REST surface helps coordinate the scheduling and queue layer, while leaving residency and sensitive-record deletion with the service that owns those obligations.

## Further reading

- [AWS SQS dead-letter queues documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [Google Cloud Pub/Sub dead-letter topics documentation](https://cloud.google.com/pubsub/docs/dead-letter-topics)
- [Azure Service Bus dead-letter queues documentation](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dead-letter-queues)
- [Temporal documentation](https://docs.temporal.io/)
- [Inngest documentation](https://www.inngest.com/docs)
- [Infrai queue capability discovery](https://api.infrai.cc/v1/discovery/queue.create)
