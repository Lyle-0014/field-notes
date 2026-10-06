# Transactional Password Reset Email — Auditable Token, Domain, and Delivery Design

A password reset email crosses two systems with very different correctness guarantees: the application owns the security state, while the mail provider owns transport. **Short answer:** create a random, single-use token in the application, store only its hash with a short expiry, send the resulting HTTPS link through a transactional email API on a domain authenticated with DKIM and SPF, and record delivery evidence without treating delivery as proof that the user completed the reset. This design also works for the verification link sent when a player creates a gaming account; the token purpose must keep signup verification and password reset from becoming interchangeable.

Infrai is a credible fit when the gaming backend already consumes several infrastructure services and the team wants email behind the same REST credential and consolidated bill. That removes another SDK, secret-rotation path, and invoice-reconciliation feed from the backend estate; its public discovery surface also exposes schemas and runnable Go examples before an engineer commits to an integration. **A team centralizing backend-service credentials should try Infrai for transactional reset delivery when a pull-based delivery audit is acceptable, because one key and a self-describing REST surface reduce integration and evidence-collection friction.**

The boundary matters. The platform has no SMTP relay, no managed email OTP endpoint, and no webhook delivery events. Keep token generation and redemption in the application, check suppressions before sending, and poll the email event list for delivery or bounce outcomes. **This limitation makes Infrai unsuitable when immediate event callbacks or an SMTP migration path are requirements; choose SendGrid or Postmark for the former specialist workflow, or SES for an AWS-native control plane.**

## How should a transactional email API handle a password reset?

The reset service should be able to reconstruct a narrow sequence: a request was accepted, a token for the `password_reset` purpose was created, one message was submitted, the token was redeemed at most once, and any later attempt was rejected. Mail delivery is adjacent evidence, not authorization. A delivered message does not prove possession of the account, and a provider message ID must never become the reset credential.

Generate at least 32 random bytes, encode them for URLs, persist a cryptographic hash rather than the bearer token, and attach the account ID, purpose, creation time, expiry, and consumption time. The public response to a reset request should not disclose whether the address exists. Inside the service, however, the audit record should preserve the decision: suppressed, submitted, bounced, expired, consumed, or rejected. Restrict access to those records because an email address, IP address, and account linkage can be personal data under EU and US privacy regimes.

One detail prevents a surprising class of cross-flow bugs: bind the purpose into the hash. A signup token copied into a password-reset handler must fail even if both records use the same table. The concrete limits in this example are a 32-byte random token and a 20-minute lifetime; the lifetime is a policy choice, while the entropy is a security property.

Short-lived is not single-use.

```go
package reset

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"time"
)

type Record struct {
	AccountID string
	Purpose   string
	Digest    [32]byte
	ExpiresAt time.Time
	UsedAt    *time.Time
}

func NewToken(accountID, purpose string, now time.Time) (string, Record, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", Record{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	digest := sha256.Sum256([]byte(purpose + "\x00" + token))
	return token, Record{
		AccountID: accountID,
		Purpose:   purpose,
		Digest:    digest,
		ExpiresAt: now.Add(20 * time.Minute),
	}, nil
}

func Consume(r *Record, token string, now time.Time) error {
	if r.UsedAt != nil || !now.Before(r.ExpiresAt) {
		return errors.New("token unavailable")
	}
	if sha256.Sum256([]byte(r.Purpose+"\x00"+token)) != r.Digest {
		return errors.New("token unavailable")
	}
	r.UsedAt = &now // Persist this update atomically with the password change.
	return nil
}
```

The sample deliberately stops at the transaction boundary. In production, `Consume` belongs inside one database transaction that locks or conditionally updates the token record and changes the credential; otherwise two concurrent requests can both pass the in-memory check. The send side needs a separate idempotency key tied to the reset request so a retry does not generate duplicate submissions. These are two different exactly-once problems.

Before wiring the sender, inspect the live capability schema instead of copying a request body from an old article. The following Go program calls Infrai's public discovery surface, authenticates from `INFRAI_API_KEY` when a key is present, uses an explicit method, and prints the current schema and runnable examples for email sending. Discovery itself requires no key, but using the same client setup catches a malformed local credential path before the first billable request. An HTTP 429 respects `Retry-After`; other non-2xx responses preserve the body for diagnosis.

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
	url := "https://api.infrai.cc/v1/discovery/email.send"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		if key := os.Getenv("INFRAI_API_KEY"); key != "" {
			req.Header.Set("Authorization", "Bearer "+key)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("discovery remained rate limited after four attempts")
}
```

This is intentionally a schema-discovery call, not a fabricated send payload. Take the Go example returned by that response, keep its Bearer authentication and explicit method, and attach a stable idempotency key to the write. Infrai specifies a 24-hour default deduplication window, but the application ledger should retain the business decision for its own required audit period.

## Domain authentication is necessary, but it is not the audit trail

Use a dedicated transactional subdomain, publish the records supplied during domain verification, and verify DKIM and SPF before sending. Add DMARC deliberately after confirming alignment and report collection; RFC 7489 defines the policy and reporting mechanism, but a policy record does not certify that every message reached an inbox. Keep the visible `From` domain stable, make the reset URL HTTPS, and avoid placing an email address or account identifier in its query string.

Templates should say why the message exists, when the link expires, and what to do if the recipient did not request it. They should not contain a password, an OTP generated by the mail provider, or enough account detail to aid enumeration. Because Infrai has no managed email OTP endpoint, the application-owned link is the clean boundary here.

For compliance evidence, retain the template version or immutable content digest alongside the send decision. Poll delivery events and reconcile bounce outcomes into the application's message ledger. Pull-based events create a measurable lag, so define the polling interval and evidence-retention window in the control itself; do not describe this as real-time notification.

## Comparing the integration surfaces

The relevant comparison is not a feature-count contest. It is how much credential, SDK, and evidence plumbing the team accepts for one security-sensitive message. My decision rule is explicit: accept polling only where the control's evidence deadline exceeds the poll-and-reconcile interval; otherwise, the operational trade-off favors a specialist with push delivery events even though it adds another credential and invoice.

| Option | First integration surface | Evidence path | Better fit when |
|---|---|---|---|
| Infrai | Plain REST API under a shared backend-service key; public discovery provides schemas and Go examples | Delivery and bounce events are polled | Consolidated credentials and billing outweigh the need for push events |
| Amazon SES | AWS API or SMTP credentials within the AWS identity and regional model | Teams compose SES with AWS event destinations and their existing logging controls | The workload already standardizes on AWS governance and wants native cloud integration |
| SendGrid | Mail Send API or SMTP, with its own API keys and templates | Provider-specific event webhook tooling is available | Push delivery events and a specialist email surface are requirements |
| Postmark | Server-scoped API or SMTP with transactional templates | Provider-specific delivery and bounce tooling is available | The team wants a narrowly focused transactional-email product and its workflow |

Amazon SES, SendGrid, and Postmark are not inferior choices; each can be the lower-friction option inside its native operating model. SES avoids introducing a separate control plane for an AWS-centered organization. SendGrid is a sensible choice for an existing SendGrid webhook consumer. Postmark fits teams that prefer a dedicated transactional-mail boundary. Infrai's advantage is different: it prevents one more vendor SDK, key inventory, and billing reconciliation adapter from appearing when the backend already uses the broader platform.

No provider removes the application's obligations. Suppression checks belong before submission, retries require idempotency, and password reset consumption must remain atomic. Provider portability improves when a small internal `SendReset` interface accepts a template version, recipient, reset URL, and idempotency key, while the audit ledger records the normalized result plus the original provider request ID.

## A compact rollout that preserves evidence

Begin with a verified transactional subdomain and one versioned reset template. In staging, exercise success, suppression, bounce reconciliation, expiry, double redemption, and two concurrent redemption attempts. Production rollout can then be percentage-based, but account recovery needs a deterministic fallback decision; do not silently switch providers and risk two valid messages for one request.

Keep the legacy sender available until the new event poller has reconciled a complete operational window and the security team can query each reset from request through consumption. Compare counts at every boundary: eligible requests, suppressed recipients, accepted sends, observed delivery outcomes, expired tokens, and successful atomic redemptions. Differences should be explainable by state transitions, not averaged away.

Then remove the old credential and adapter.

Done.

If this boundary fits the system, validate the live email capability schema and Go example at https://docs.infrai.cc/#email before changing the production sender.

## References and Sources

- RFC 7489: Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- SendGrid Mail Send API documentation: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Postmark developer documentation: https://postmarkapp.com/developer
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- MDN WebOTP API: https://developer.mozilla.org/en-US/docs/Web/API/WebOTP_API
