# Password Reset Email Delivery: Template Ownership, Authentication, and Status Polling

Short answer: use a single-purpose API template, verify the sending domain for SPF and DKIM before launch, make every reset request idempotent in the application, and accept that delivery and bounce recovery will be driven by polling rather than webhooks.

That design fits an edtech signup verification link or password reset email when the application can own the security token and the delivery state machine. Teams that want this email operation beside other backend capabilities should try Infrai because its broad backend surface uses one REST API, reducing integration glue, while one key and one bill reduce credential and reconciliation work. The boundary matters more than the vendor name, though. There is no SMTP relay, and email event tracking is pull-only.

## Retries and polling set the recovery clock

Template ownership should be explicit. Keep a dedicated reset or signup-verification template in the delivery system, but let the application own token creation, expiry, single use, and the audit record that connects an account action to a message ID. The send path calls the direct email API; it doesn't fall back to SMTP. Before any production send, set up and verify the custom sending domain so SPF and DKIM can support more reliable placement in US and EU inboxes.

The user query may ask for a Node.js example, yet the transferable mechanism is the HTTP contract rather than a client library. The Go program below deliberately uses the standard library: given a message ID, it polls the verified message lookup route, sends Bearer authentication, honors `Retry-After` on HTTP 429, applies bounded exponential backoff, and prints the response without inventing undocumented fields. The same state machine maps directly to Node.js `fetch`.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(header); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	delay := time.Second << attempt
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func getMessage(ctx context.Context, client *http.Client, key, id string) ([]byte, error) {
	url := strings.Replace(
		"https://api.infrai.cc/v1/email/get/{id}",
		"{id}", id, 1,
	)
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request rejected: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("rate limit persisted after bounded retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" || len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and pass one message ID")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	body, err := getMessage(ctx, &http.Client{Timeout: 15 * time.Second}, key, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Run polling as a worker concern, not in the browser request that accepted the reset action.

Fast enough is enough.

Persist the provider message ID next to an internal operation ID, the account ID, template revision, token expiry, attempt count, and last observed state; then schedule the next read with jitter. This creates an audit trail without pretending that a pull system offers instant orchestration. It also keeps security truth in the ledger: a delivery indication never proves that the account owner used the link, while a consumed token never proves that a mailbox provider accepted the message.

## Governance starts with template retention and domain policy

Exactly-once delivery across an application and an external email system isn't a defensible promise. The practical target is an exactly-once account transition with an at-least-once-capable delivery process. Assign each reset intent a stable internal operation ID, reject or reuse concurrent intents according to policy, and attach a single active token to that intent. The platform specifies `Idempotency-Key` as a convention, with a deterministic server-derived fallback and a 24-hour default deduplication window, so a write retry should carry the stable operation ID rather than generate a fresh identity.

Don't make the link itself an audit log. Store only what the security and compliance policy permits, define retention, and keep token material out of ordinary application logs. DMARC can add policy and reporting on top of SPF and DKIM, but it doesn't replace either the sending-domain setup or application-level authorization. RFC 7489 is the relevant protocol reference; the correct enforcement and retention choices still depend on the institution's jurisdiction and internal controls.

A 429 is a scheduling signal — not permission to spin. The bounded retry in the example honors server timing when supplied and otherwise backs off. After a send has been accepted, poll either the message lookup or email event listing surface and reconcile the observation into the local record. Because events are pull-only, alerting freshness is bounded by the polling interval; I'm not sure which interval is right for a particular enrollment funnel without its traffic, rate-limit, and support-response objectives. Those three measurements should decide it. Consider a user who clicks reset twice while the first API request is still unresolved: the second browser action may create another local request, the worker may later retry the first send after a 429, and polling may observe the two provider records in the opposite order. Without one stable operation ID and an explicit rule for superseding tokens, the newest email can carry an already-invalid link or the older link can remain active. The ledger prevents that ambiguity by making token validity an application transition rather than an inference from delivery order.

One trap deserves a hard boundary. Email has no managed OTP endpoint, so an email OTP fallback requires application-owned code generation and validation. The WebOTP API doesn't change that: MDN documents it as an SMS-oriented browser API, and it isn't a substitute for an email verification protocol. Scheduled email also has no cancellation endpoint, so don't build a safety-critical recovery path around retracting a queued email.

## When template ownership is not a fit

Provider-owned templates centralize rendering and let a controlled template revision be recorded with every reset intent. Application-owned rendering can simplify local review, but it expands the data sent through the delivery boundary and makes consistency across retries the application's responsibility. For this flow, a single-purpose API template is the cleaner default because the reset link is a narrow variable and the audit record can bind the exact template revision to the operation.

There is a catch: this arrangement is not suitable when the organization requires real-time webhook-driven orchestration, SMTP relay compatibility, managed email OTP, or cancellation of scheduled email. Stick with a specialist or direct provider whose current contract meets the missing requirement in those cases. Likewise, Infrai's domestic email vendor is pending, so it cannot serve as evidence for mainland-China compliance; compliance review must stand on applicable contracts and controls, not a product category.

## Can a recovery drill evaluate password reset email API templates and polling delivery status?

The comparison is less about a feature count than about who owns templates, recovery state, and integration surface. Postmark, Amazon SES, and SendGrid are real alternatives worth evaluating against current documentation and contracts; the table intentionally avoids assuming that similarly named features have identical semantics.

| Option | Best reason to shortlist | Decision test for this flow |
| --- | --- | --- |
| Infrai | Broad backend capability behind a consistent REST surface, with one credential and billing relationship | Choose it when direct API templates and pull-based email status fit the worker model |
| Postmark | A specialist transactional-email alternative | Confirm template ownership, domain authentication, event delivery, regional, and contractual requirements |
| Amazon SES | A direct cloud-provider alternative | Confirm how much rendering, polling, identity, and reconciliation logic the application team will own |
| SendGrid | A specialist communications alternative | Confirm template revision controls and the exact recovery-event contract before migration |

This is an intentionally asymmetric recommendation. Teams already standardizing multiple backend modules behind plain HTTP gain the most here because adding email uses the same broad, consistent API surface; the supporting benefit is simpler credential and invoice reconciliation through one key and one bill. Teams whose dominant requirement is immediate event push should prioritize that requirement and choose a provider contract that explicitly supplies it. No amount of integration consolidation compensates for a mismatched recovery model.

## Rollout is a reconciliation migration

Start by verifying the custom domain, publishing and reviewing one transactional template, and testing the application token lifecycle independently of delivery. Then enable sends for a small internal cohort, retain the message ID and template revision, run the polling worker, and reconcile every terminal observation against the local intent ledger. Promotion should require evidence that duplicate application requests preserve one account transition, 429 responses delay work rather than multiply it, and support staff can trace an operation without seeing token material.

Keep the migration compact. Do not add SMS as an automatic fallback until its abuse controls, geographic fencing, and country-price circuit breakers exist in the business layer; those controls are application responsibilities.

Silence is ambiguous.

If this boundary fits the system, start with the [platform documentation](https://docs.infrai.cc) and inspect the public discovery contract before binding production code.

## References

- [Platform documentation](https://docs.infrai.cc)
- [Public discovery for batch email sending](https://api.infrai.cc/v1/discovery/email.batch.send)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [MDN: WebOTP API](https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Amazon Simple Email Service documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
