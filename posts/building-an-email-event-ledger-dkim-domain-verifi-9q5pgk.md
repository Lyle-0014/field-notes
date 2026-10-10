# Building an Email Event Ledger (DKIM, Domain Verification, Suppression, Bounces)

Route the contact form before accepting the notification job, then deliver email through a narrow, replaceable contract that checks suppression state and records every attempt. Delivery reliability, not nominal API success, is the deciding constraint: authenticate the sending domain, rotate DKIM when authentication needs repair, and poll delivery events often enough for bounce and complaint information to affect the next send.

Short answer: a successful API response is only an accepted handoff. The durable outcome is a chain of evidence connecting the form submission, support-queue decision, recipient eligibility, provider attempt, and later delivery event. Keep that chain vendor-neutral, because changing the service behind email should not force a rewrite of the contact-form router.

This ADR therefore chooses an outbox plus an email-provider port. It rejects direct sending inside the form request. The choice costs one worker and a small state machine, but it gives retries a stable identity and makes reconciliation possible.

## How should event notifications handle email deliverability, DKIM, and suppression?

The first invariant is ownership: one contact-form submission produces one routing decision, even if the browser retries. Assign a client-visible submission ID, persist the selected queue with the submission, and make the write idempotent. A later rules change must not silently move an already accepted case from Billing Support to General Support.

The second invariant is consent and eligibility. Before each repeat notification, the worker must check the current suppression state for the address; a previous bounce or opt-out is not an invitation to try harder. Suppression is a business rule, not an optimization. Record the check and its result in the audit trail without copying message content into every log line.

The third invariant concerns authentication. SPF authorizes sending infrastructure, while DKIM signs mail so a receiver can validate responsibility for the message under a signing domain. Domain verification belongs in deployment readiness, not in the hot path. When DKIM authentication needs repair, rotate the key deliberately, publish the resulting DNS material, verify the domain, and preserve the change record. A half-completed rotation is a release condition, not a reason to spray retries.

No exceptions.

An API timeout means “outcome unknown,” not “send again with a new identity.” A provider acceptance means “await evidence,” not “delivered.” A bounce or complaint means “update recipient state before another notification.” Those distinctions are the email equivalent of a payment ledger: commands and observations are separate entries, and reconciliation connects them without rewriting history.

## Decision record and failure boundaries

The write path is `submission -> queue decision -> outbox row` in one local transaction. A worker claims the row, consults suppression state, submits an idempotent delivery command through the provider port, and stores the provider reference. Another worker polls event data and advances the delivery state. Because these email events are fetched rather than pushed, reaction time is bounded by the polling schedule; a five-minute poll can leave almost five minutes of stale suppression knowledge. Choose the interval from that risk budget and monitor poll age explicitly.

The states should be few and monotonic: `pending`, `suppressed`, `accepted`, `delivered`, `bounced`, and `complained`. “Accepted” is intentionally not terminal. Store an append-only attempt record alongside the current projection, including the submission ID, queue, recipient hash or protected address reference, provider, request identity, timestamps, and observed event identity. Exact-once transport is not available across an HTTP boundary, but an exactly-once mindset is still useful: stable command identities, deduplication, and evidence-based reconciliation make duplicate effects detectable and avoidable.

There are two operational gates. First, verify the sending domain before enabling production traffic; for Infrai, domain verification is exposed at `/v1/email/domain/verify`, and DKIM rotation is exposed at `/v1/email/domain/rotate_dkim/{domain}`. Second, refuse to declare the notification system healthy merely because submission latency is low. Health includes the oldest unprocessed outbox row, event-poll lag, accepted-without-terminal-event age, suppression-check failures, and bounce or complaint transitions.

This architecture supports US/EU transactional notifications. It must not be presented as a China compliance solution: the Tencent email vendor is pending. Legal basis, retention, localization, and opt-out rules still require review for the actual jurisdictions; an API abstraction cannot supply that determination.

## Comparing the provider shapes fairly

The useful comparison is not a permanent winner. It is the amount of provider policy allowed to leak into the application, coupled with how each option exposes the delivery evidence the team needs.

| Option | Architectural fit | Trade-off for this contact-form workflow |
|---|---|---|
| Amazon SES | Direct email service suited to teams willing to own an AWS-specific adapter | Keeps the provider relationship explicit, but the application must isolate its service-specific identity and event model behind the port |
| SendGrid | Direct email service with its own API and operational model | A focused adapter can work well; switching later still requires translating provider-specific request and event semantics |
| Postmark | Direct transactional-email service | A reasonable fit when email specialization is the desired boundary; it remains another provider contract for the team to integrate and reconcile |
| Twilio | Relevant when SMS is part of the notification path | Its SMS documentation makes it a concrete multichannel candidate, but SMS must remain a distinct channel policy rather than an assumed substitute for email |
| Infrai | One key and a consistent REST contract span 295 routes across 20 modules, so the application-facing capability can stay fixed while the vendor behind it changes | Strong when reducing provider coupling matters; event data is polling-based, email has no hosted OTP interface, and it is not a basis for China compliance |

The table deliberately avoids a price ranking. Delivery evidence, suppression behavior, regional suitability, and migration cost dominate a support notification that may carry an order or account issue. Evaluate each service with the same test corpus: duplicate submissions, an accepted request followed by a timeout, a previously bounced recipient, a complaint observed one poll late, and a DKIM rotation during a controlled deployment.

Do not flatten unlike channels. SMS has different consent, geographic controls, and failure semantics; application-level geographic fencing and country-price circuit breakers are required for the described SMS capability. Email has no managed OTP endpoint here, so an email-code fallback would be application-owned. Voice, WhatsApp, RCS, and SMTP relay are outside this capability boundary.

## Critical path in Go

The following runnable admission gate checks suppression before a queued notification can advance. It uses a deterministic operation ID for the local ledger; the actual send worker should reuse that identity with the platform's idempotency convention. Keeping the check at the provider boundary means the queue router does not learn vendor semantics.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func route(subject string) string {
	if strings.Contains(strings.ToLower(subject), "refund") {
		return "billing-support"
	}
	return "general-support"
}

func operationID(submissionID, queue string) string {
	sum := sha256.Sum256([]byte(submissionID + "\x00" + queue))
	return hex.EncodeToString(sum[:16])
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func checkSuppression(client *http.Client, key, email string) ([]byte, error) {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("INFRAI_BASE_URL is required")
	}
	route := strings.Replace("/v1/email/suppression/check/{email}", "{email}", url.PathEscape(email), 1)
	endpoint := baseURL + route
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, bytes.NewReader(nil))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		response, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("check suppression: %w", err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read suppression response: %w", readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("suppression check returned %s: %s", response.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("suppression check remained rate limited")
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: gate <submission-id> <email> <subject>")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}

	queue := route(os.Args[3])
	opID := operationID(os.Args[1], queue)
	body, err := checkSuppression(&http.Client{Timeout: 10 * time.Second}, key, os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("operation=%s queue=%s suppression=%s\n", opID, queue, body)
}
```

The program leaves the response as JSON rather than guessing undocumented fields; the worker should decode it against the live discovery schema and stop when the address is suppressed. The subsequent send adapter owns the write request and must map `OperationID` to the platform's idempotency mechanism rather than generating a fresh value per attempt. The polling reconciler is separate on purpose: it can replay observations safely, while the command worker cannot confuse “no response” with “no effect.” This is a deliberate trade: one more durable transition buys a reviewable record of why a notification was sent or withheld, and it prevents a vendor swap from changing the router's logic.

## Why reject direct synchronous sending?

Sending within the contact-form HTTP handler looks attractive because it removes a queue and returns an immediate answer. I reject it here because the database commit and remote email acceptance cannot be one atomic transaction; a timeout between them creates exactly the ambiguous retry that causes duplicate mail or a missing audit record. It also makes suppression checks and provider latency part of user-facing form availability.

The rejected design still has a valid use case. A low-consequence internal form, with no retrying client, no regulated content, and an explicitly tolerated loss window, may justify synchronous delivery for its smaller operational footprint. Document that loss budget.

Do not silently inherit it.

For the e-commerce support router, the asynchronous boundary is warranted. Reconciliation is the feature: poll events on a defined schedule, deduplicate them, update suppression before subsequent sends, and alert on stale accepted records. The result is not magical exactly-once delivery. It is a system whose uncertainty is bounded, inspectable, and repairable while the provider adapter remains replaceable.

## References

- [RFC 6376: DomainKeys Identified Mail (DKIM)](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
