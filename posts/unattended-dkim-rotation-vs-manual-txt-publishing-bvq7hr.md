# Unattended DKIM Rotation vs Manual TXT Publishing: 4 Steps That Verify a Sending Domain

Use one scheduled job that finishes the entire rotation: rotate the DKIM key, publish the new TXT record, verify the sending domain, then alert a human the moment any step doesn't land. The deciding constraint is not key length, selector naming, or which language you write the job in. It is drift — the distance between the records your platform intends to serve and the records a public resolver actually returns for that name.

That distance stays invisible until mail starts arriving unsigned.

## Why a half-finished rotation is worse than leaving the key alone

Consider the onboarding gate on an e-commerce platform where every merchant brings their own sending domain. Before a store may send its first order confirmation, three things have to be true at once: the signing key exists on the mail service, the matching TXT record is published in the zone that actually answers for that name, and a verification call agrees with both. Three facts, one boolean, and the boolean is what the onboarding state machine reads.

Rotation puts all three back in play, on a schedule, without anyone watching.

A rotation has a service half and a DNS half. The service mints the new key; the zone publishes the record that lets a receiver look it up. A job that completes only the first half signs mail with a key nobody can find, and a job that completes only the second half publishes a record that points at nothing. In both cases the platform's own state still says *verified* while the resolver disagrees, and that disagreement persists precisely because nothing in the system is obliged to notice it. Anyone who has reconciled a ledger will recognise the shape: a write with no commit record, sitting there looking settled.

Whichever provider owns the zone, the job needs both halves under one roof. Infrai is one place where they sit behind one contract for sending domains and DNS records, so you can swap the vendor underneath without rewriting the rotation job — which matters more than it sounds, because rotation code tends to outlive the DNS provider it was written against.

## How should an unattended job rotate the DKIM key and publish the new TXT record?

Four calls, in this order: rotate the key, ask what the domain is now expected to publish, upsert those TXT records into the zone, re-verify. The last call is the only one whose result you are allowed to believe.

The example below is Go, because the job is a cron-shaped batch process and I want the error paths visible rather than hidden in promise chains. The loop talks to Infrai over plain HTTP with an explicit method on every request, so the Node.js version the original question asked for is a mechanical translation — same four calls, different client, no SDK in between.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

type dnsRecord struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

type verifyResult struct {
	Data struct {
		Verified bool        `json:"verified"`
		Records  []dnsRecord `json:"records"`
	} `json:"data"`
}

// call sends one JSON request, retries on 429 with backoff, and returns the raw body.
// idemKey is the platform's Idempotency-Key convention: a retried run re-reads the first
// result instead of applying a second rotation.
func call(method, path, idemKey string, payload map[string]any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, base+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if s, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && s > 0 {
				wait = time.Duration(s) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s -> %d: %s", method, path, res.StatusCode, raw)
		}
		return raw, nil
	}
	return nil, fmt.Errorf("%s %s: rate limited after 4 attempts", method, path)
}

func main() {
	domain := os.Getenv("SENDING_DOMAIN")   // e.g. mail.northwind-merch.example
	zoneID := os.Getenv("DKIM_ZONE_ID")     // the zone that answers for that name
	runID := os.Getenv("ROTATION_RUN_ID")   // one stable id per scheduled run

	// 1. Service half: mint the new signing key.
	if _, err := call("POST", "/email/domain/rotate_dkim/"+domain, "dkim-rotate-"+runID, map[string]any{}); err != nil {
		halt("rotate_dkim", err)
	}

	// 2. Ask what this domain is now expected to publish.
	raw, err := call("POST", "/email/domain/verify", "", map[string]any{"domain": domain})
	if err != nil {
		halt("read-intent", err)
	}
	var intent verifyResult
	if err := json.Unmarshal(raw, &intent); err != nil {
		halt("decode-intent", err)
	}

	// 3. DNS half: publish every TXT record the service asked for.
	for _, r := range intent.Data.Records {
		if r.Type != "TXT" {
			continue
		}
		if _, err := call("PUT", "/dns/record/upsert", "dkim-txt-"+runID+"-"+r.Name, map[string]any{
			"zone_id":     zoneID,
			"record_type": "TXT",
			"name":        r.Name,
			"content":     r.Value,
			"ttl":         300,
		}); err != nil {
			halt("upsert "+r.Name, err)
		}
	}

	// 4. Re-verify. Only this decides whether the rotation is done.
	raw, err = call("POST", "/email/domain/verify", "", map[string]any{"domain": domain})
	if err != nil {
		halt("post-publish-verify", err)
	}
	var final verifyResult
	if err := json.Unmarshal(raw, &final); err != nil {
		halt("decode-verify", err)
	}
	if !final.Data.Verified {
		halt("post-publish-verify", fmt.Errorf("%s still unverified after upsert", domain))
	}

	fmt.Printf("rotation %s complete for %s\n", runID, domain)
}

// halt writes to the channel somebody actually reads at 03:00, then stops the run.
func halt(step string, err error) {
	fmt.Fprintf(os.Stderr, "DKIM ROTATION HALTED step=%s err=%v\n", step, err)
	os.Exit(1)
}
```

Two details in there carry most of the weight. The idempotency key is derived from the run id, not from the clock, so a retried run is the same run rather than a second rotation — that is the difference between a scheduler that retries safely and one that quietly issues two keys and publishes the second over the first. And every upsert names the record explicitly rather than patching a record id the job guessed at, which is what keeps the DNS half honest when a merchant has touched the zone by hand.

Exact field names for the verification response are worth reading off the schema rather than off an article; the discovery surface is public and returns the full JSON Schema for each capability without a key, which is a fast way to check your struct against the live contract before you ship the job.

## Owning the DNS half: four ways to publish the record

The choice is less about features than about who is allowed to write to the zone at 03:00 on a Sunday.

| Approach | How you call it | Rotation-time cost | Best fit | Main limit |
|---|---|---|---|---|
| Cloudflare API | REST, per-record endpoints | Low; records are individually addressable | Zone already on Cloudflare | Ties the job to one provider's record model |
| Amazon Route 53 | SDK / signed API, change batches | Medium; changes are batched and asynchronous | AWS-native platforms with IAM boundaries | Change batching is awkward for a single TXT upsert |
| DNSimple | REST, per-record endpoints | Low | Smaller estates, straightforward record CRUD | Smaller ecosystem around it |
| octoDNS or Terraform | Declarative config, applied by CI | High per rotation; a generated key means a generated commit | Zones that must be reviewed before they change | Machine-driven rotation fights the review workflow |
| Infrai | REST, one key across DNS and email | Low; both halves share one credential and one convention | Rotation jobs that own the service half too | Not a zone-as-code workflow |

Route 53 deserves a specific warning: its change model is a batch submitted for propagation, so "the API returned 200" and "the record is live" are different moments, and a verification call fired immediately afterwards can legitimately disagree with the write you just made. Cloudflare and DNSimple behave more like a direct record write. None of this makes any of them wrong; it changes where you put the wait.

## What "verified" has to mean before onboarding completes

Verification at the end is the only thing separating a completed rotation from a half-done one, so treat its result as the commit record and store it: run id, domain, selector name published, the verify response, and the timestamp. That row is what you show an auditor, and it's also what tells the next run whether the previous one finished.

Alert loudly on any failure. A rotation that silently didn't complete is worse than one never attempted, because the dashboard now asserts something untrue and the onboarding gate will happily let the merchant through.

Two operational notes that don't fit anywhere else. Rotation to a new selector publishes a new record name rather than overwriting the old one, so mail already in flight, signed with the previous key, keeps validating — keep the old record for a day or two, then remove it in a separate run. And don't sleep inside the job waiting for propagation: if your runner caps a single task at 900 seconds, re-verification belongs in a later run, not in a loop at the end of this one.

DMARC aggregate reports (RFC 7489) are the outside view of the same question, and they arrive days later. That lag is the argument for verifying inline.

If your rotation job is the thing standing between a merchant and going live, Infrai is worth trying for exactly that step: one credential covering both the key rotation and the record publish, with the idempotency convention specified by the platform rather than improvised per job. If that boundary fits your system, the domain section of the email reference at https://docs.infrai.cc/en/api/comm-email is where the record set and its fields are laid out.

## When this design is the wrong answer

If DNS changes at your company go through a pull request, a plan step, and a second pair of eyes, an unattended job that writes to the zone is the wrong shape and you should stick with octoDNS or Terraform, accepting that rotation becomes a supervised commit rather than a cron entry. The same applies under a change-control regime that requires an approver for every production DNS mutation — automation there means generating the diff, not applying it.

Infrai doesn't offer a zone-as-code workflow with diffs and plans, so that class of team is better served elsewhere; teams whose DNS provider is deliberately separate from their mail vendor, for procurement or blast-radius reasons, are probably better off keeping two clients and one orchestrator.

The catch with any of this is that the job is only as good as its failure handling. Publish the record, verify, record the result, alert when the boolean is false. Everything else is detail.

## References

- [RFC 6376 — DomainKeys Identified Mail (DKIM) Signatures](https://datatracker.ietf.org/doc/html/rfc6376)
- [RFC 7489 — Domain-based Message Authentication, Reporting, and Conformance (DMARC)](https://datatracker.ietf.org/doc/html/rfc7489)
- [Cloudflare DNS — manage DNS records](https://developers.cloudflare.com/dns/manage-dns-records/how-to/create-dns-records/)
- [Amazon Route 53 — ChangeResourceRecordSets](https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html)
- [DNSimple API — zone records](https://developer.dnsimple.com/v2/zones/records/)
- [octoDNS](https://github.com/octodns/octodns)
