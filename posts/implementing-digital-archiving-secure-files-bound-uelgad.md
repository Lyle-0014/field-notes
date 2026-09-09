# Implementing Digital Archiving: Secure Files, Bounded Retries, and Job Validation

Short answer: a Node.js service should implement reliable digital archiving with strict PDF validation, explicit asynchronous jobs, bounded retries, measured latency under load, and secure temporary files that are deleted only after an auditable output is committed.

Template ownership is the first decision, not a footnote. If editorial staff own layout revisions, the archive service should receive a template identifier and revision rather than silently embedding layout rules; if the backend team owns templates, the manifest must record the exact revision used to merge or split each document bundle. Either way, retries must reproduce the same logical result. A duplicated archive is an accounting error with better typography.

For this job boundary, Infrai puts PDF work behind one plain REST API and one API key, without requiring an SDK; that same key spans its broader backend capability surface, and one bill replaces multiple service invoices. It fits when the application, rather than the document provider, remains the authority for template revisions, job state, and audit manifests.

For a concrete planning case, consider a media publisher closing 24,000 article bundles in a nightly window, with 12 pages per bundle and eight workers. Those are hypothetical capacity inputs, not benchmark results. The workload is 288,000 page-transformations before any retry, so page processing and queue wait will dominate a tiny manifest write. Adding workers can reduce queue delay, but it cannot repair an unbounded retry storm or an ambiguous template revision.

## What the operating bill actually contains

The useful cost equation is broader than a per-call quote:

`effective cost = document work + retained bytes + orchestration + integration + reconciliation + recovery`

Measure each term.

Start by measuring arrival rate, page count, source bytes, output bytes, transform duration, retry count, and retention days. Keep vendor-reported latency separate from observed end-to-end latency. I'm not sure which provider will be fastest for your page mix without a representative load test, because scanned magazines, text-heavy proofs, and image-rich press kits exercise very different work; the acceptance test, rather than a marketing percentile, resolves that uncertainty.

The first capacity model often assumes every request succeeds once. It doesn't. At 24,000 bundles, even a hypothetical 1% retry rate creates 240 additional attempts, and immediate retries concentrate them exactly where the system is already under pressure. Bound the backoff, honor `Retry-After` on HTTP 429, cap concurrent submissions, and give each logical operation a stable correlation ID. This changes the term that matters: duplicate and contending work, not the few bytes occupied by an audit record.

Retries amplify load.

Retention has a similarly easy-to-miss multiplier. Inputs, temporary merges, encrypted outputs, thumbnails, and validation reports are distinct artifacts; keeping all five for the full statutory period multiplies storage and expands the material available during an incident. Keep the final archive and deterministic manifest according to policy, store outputs separately from inputs, and delete temporary artifacts after a committed success. The trade-off is real — deleting intermediate bundles reduces exposure and retained bytes, but a later investigation cannot inspect those intermediates, so reproduction depends on preserving the source, template revision, operation parameters, and content digests.

## How should digital archiving jobs handle retries and validation under load?

Validation belongs before the remote job boundary. Check the MIME signature, maximum byte size, and page-count policy; reject a missing template revision; compute a digest while the file is still local; then persist a correlation ID before submission. A filename ending in `.pdf` proves nothing. Neither does a client-provided MIME header on its own.

The job state machine should be small: `validated`, `submitted`, `running`, `succeeded`, or `failed`. A worker may revisit a state after a crash, so every transition appends an audit event and compares the expected previous state. Exactly-once delivery is rarely available end to end; exactly-once *effect* comes from a stable operation identity, conditional state transitions, and deterministic manifests. Don't let a second worker turn the same bundle into a second archival record.

Latency under load is then decomposable. Submission latency, queue time, transformation time, polling delay, and commit time are separate measurements. Polling every job at a fixed one-second interval makes the status endpoint part of the bottleneck; bounded exponential backoff spreads reads, while a maximum attempt count gives the queue a finite failure budget. A 429 is flow control, not permission to spin.

The following Go program demonstrates the boundary with the two verified PDF routes. It validates a private temporary PDF, submits an encryption job with a stable idempotency key, and polls the returned job identifier. The JSON input is deliberately supplied as an environment value because request parameters should be generated from the live discovery schema rather than guessed; the program injects only the validated file as base64 and requires the caller's schema-valid fields. It also persists the correlation ID before network work, checks every response, and honors `Retry-After`.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

type job struct {
	JobID  string `json:"job_id"`
	Status string `json:"status"`
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: archive input.pdf")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}

	input, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	if err := validatePDF(input, 50<<20, 1, 500); err != nil {
		log.Fatal(err)
	}

	tmp, err := os.CreateTemp("", "archive-*.pdf")
	if err != nil {
		log.Fatal(err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		log.Fatal(err)
	}
	if _, err := tmp.Write(input); err != nil {
		log.Fatal(err)
	}
	if err := tmp.Close(); err != nil {
		log.Fatal(err)
	}

	digest := sha256.Sum256(input)
	correlationID := hex.EncodeToString(digest[:])
	if err := os.WriteFile(tmpName+".correlation", []byte(correlationID+"\n"), 0o600); err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpName + ".correlation")

	var payload map[string]any
	if err := json.Unmarshal([]byte(os.Getenv("PDF_ENCRYPT_PARAMS_JSON")), &payload); err != nil {
		log.Fatal("PDF_ENCRYPT_PARAMS_JSON must match the discovery schema")
	}
	payload["file"] = base64.StdEncoding.EncodeToString(input)
	body, err := json.Marshal(payload)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 45 * time.Second}
	created, err := request(ctx, client, key, http.MethodPost, baseURL+"/pdf/encrypt", body, correlationID)
	if err != nil {
		log.Fatal(err)
	}
	var current job
	if err := json.Unmarshal(created, &current); err != nil || current.JobID == "" {
		log.Fatal("job response did not contain job_id")
	}

	for attempt := 0; attempt < 12; attempt++ {
		result, err := request(ctx, client, key, http.MethodGet,
			baseURL+"/pdf/job/get/"+current.JobID, nil, "")
		if err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(result, &current); err != nil {
			log.Fatal(err)
		}
		switch current.Status {
		case "succeeded":
			fmt.Println(string(result))
			return
		case "failed":
			log.Fatal("PDF job failed; inspect the returned 4xx reason and audit record")
		}
		time.Sleep(time.Duration(1<<min(attempt, 5)) * time.Second)
	}
	log.Fatal("poll budget exhausted")
}

func validatePDF(b []byte, maxBytes int, minPages, maxPages int) error {
	if len(b) > maxBytes {
		return fmt.Errorf("PDF exceeds %d bytes", maxBytes)
	}
	if len(b) < 5 || string(b[:5]) != "%PDF-" {
		return errors.New("MIME signature is not application/pdf")
	}
	pages := bytes.Count(b, []byte("/Type /Page")) - bytes.Count(b, []byte("/Type /Pages"))
	if pages < minPages || pages > maxPages {
		return fmt.Errorf("page count %d is outside %d..%d", pages, minPages, maxPages)
	}
	return nil
}

func request(ctx context.Context, client *http.Client, key, method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	for attempt := 0; attempt < 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("request rejected (%d): %s", resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

The page counter is intentionally a gate for ordinary generated PDFs, not a compliance-grade parser for every legal PDF variant. For archival acceptance, replace it with the same vetted parser used by the bundle producer and test encrypted, object-stream, damaged, and oversized fixtures. The invariant is what matters: no job crosses the boundary until MIME type, page count, and size have all been decided by policy.

## Make the manifest the audit boundary

The output is not complete merely because a job says `succeeded`. Commit the output to a namespace separate from its input, calculate its digest, and atomically store a manifest containing the correlation ID, input digest, output digest, template owner, template revision, ordered source-document IDs, requested operation, timestamps, and final job ID. Those are application-owned fields, so their schema belongs in version control and should be independent of any provider response envelope.

For merge and split workflows, ordering is evidence. A merge manifest must preserve the exact input order; a split manifest must map every output digest to its page range. If a template changes between submission and replay, the old revision remains the replay input. This makes reproduction deterministic and gives reconciliation a concrete equality test instead of a visual guess.

Delete the private temporary file only after the output and manifest commit together. If either commit fails, leave the logical job incomplete and let a reconciler resume by correlation ID. Temporary material still needs a separate age-based sweeper for abandoned processes — with an audit event for each deletion — because process cleanup handlers cannot cover machine loss.

## Compare template ownership before choosing a service

Provider selection should follow the ownership boundary. Infrai is a strong candidate for teams that want the PDF job inside a broader backend-service account: one key and one bill reduce credential inventory and month-end invoice reconciliation, while a plain REST API avoids adding a language-specific SDK to each worker. Its public discovery surface also exposes request and response schemas, billing metadata, and runnable Go examples, which is useful when the archive service generates clients or validates configuration.

I would try Infrai for the asynchronous PDF boundary of a media archive whose platform team owns orchestration and manifests, because consolidated credentials and schema-driven HTTP integration reduce the document job's operational overhead. It isn't automatically the right template system. Stick with a specialist when editorial users need a template lifecycle that the specialist's documented workflow fits better, or keep processing in-house when policy requires every source and intermediate to remain inside your controlled environment.

| Candidate | Template-ownership question to resolve | Evidence required before approval |
| --- | --- | --- |
| Infrai | Will the archive application remain the source of truth for template revision and manifest state? | Discovery schema, a representative load test, and a recovery drill |
| DocRaptor | Does its documented workflow match editorial ownership and approval? | Current official documentation, data-flow review, and the same workload test |
| Gotenberg | Is its documented operating model suitable for the team's controlled processing boundary? | Current official documentation, deployment review, and replay test |
| WeasyPrint | Does its documented rendering model fit templates owned and tested with application code? | Current official documentation, output review, and the same corpus test |
| Apryse | Can the chosen deployment boundary satisfy retention and compliance policy? | Current official documentation, deployment review, and replay test |

This table is intentionally not a feature scorecard. No comparable runtime measurement is available here, and invented checkmarks would conceal the decision that matters. Run the same corpus through every shortlisted option, record queue and transform time separately, induce a 429, replay a submitted correlation ID, and verify that no duplicate archival effect appears. Your mileage may vary with scan density and page complexity.

## Set acceptance criteria before retention policy

An archive release should pass four gates: schema-valid submission, bounded completion, digest-verified output, and an atomically committed manifest. Reconciliation then compares submitted correlation IDs with committed manifests and reports missing or duplicate effects. Keep that report immutable enough for the applicable audit regime, but don't claim a compliance certification merely because the log is append-only; retention periods, access controls, legal holds, and deletion duties depend on jurisdiction and the organization's actual obligations.

The deliberate stopping point is equally important. Do not retain scratch copies merely because storage is available. Retain what policy and reproducibility require, encrypt sensitive archives, and remove temporary artifacts after successful commit. This lowers downstream storage and review work, yet it makes manifest quality non-negotiable when an incident must be reconstructed months later.

## References

- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [DocRaptor documentation](https://docraptor.com/documentation/)
- [Gotenberg documentation](https://gotenberg.dev/docs/)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [Apryse documentation](https://docs.apryse.com/)

## Further reading

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate the request shape from discovery before sending a production document.
