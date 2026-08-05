# Choosing a Text-to-Image API for a Startup MVP: Cost per Image and Model Fit

**Choose a text-to-image runtime only after measuring cost per accepted image, because nominal API price loses meaning when resolution, quality, and prompt retry rate differ.**

For a startup MVP, I would run the same representative prompt set through OpenAI, Stability AI, Ideogram, fal, and one multi-vendor route, then record accepted outputs rather than merely successful calls. The decision is reversible if the application owns a narrow provider interface; the expensive mistake is letting one vendor's request shape leak into product and ledger code.

This is an architecture decision record, not a price leaderboard. The decision is to optimize for an audited cost-per-accepted-image envelope and model fit, while treating raw cost per image as an input. Prices move. User taste moves faster.

## How should a startup MVP compare text-to-image API cost per image?

The primary metric should be total generation spend divided by images that pass the product's acceptance check. I use a small evaluation ledger with prompt ID, model ID, requested size, quality tier, attempt number, provider request ID, disposition, and booked cost. If an attractive list price produces three prompt rewrites before a usable hero image appears, it isn't the cheapest path for that workload. Conversely, a higher nominal price can be rational when first-pass acceptance is materially better. I'm not sure why teams still compare a single square-image quote without recording reruns; as far as I can tell, it survives because the spreadsheet is easier to present than the experiment.

My minimum trial is deliberately plain: freeze a prompt corpus that resembles production, define acceptance before looking at results, and run enough repetitions to expose variance. Keep resolution and quality tier constant inside each cohort. Separate policy rejection, transport failure, and aesthetic rejection, since only the last category says much about model fit. Your mileage may vary with art direction, which is precisely why another company's benchmark can't settle this decision.

One number matters: accepted cost. Track it beside acceptance rate and p95 completion time, but don't pretend the latter is an SLA unless you actually measured it. For an interactive generator, batch processing is usually needless in the first release; it becomes useful for backfills or scheduled catalog work. If the product later adds captioning or prompt rewriting, pair image generation with chat completions instead of turning the MVP into an orchestration project.

Measure it.

## Decision invariants and failure boundaries

Payment systems taught me to distrust counters that can't be reconciled. Every image attempt therefore receives a client-derived idempotency key, every provider response is tied to the originating prompt record, and every cost entry is append-only. Exactly-once execution is an aspiration across a network; an idempotent effect plus an auditable retry history is the implementable contract. Short version: count attempts, deduplicate effects.

The hard boundary is uncertainty after a timeout. The caller must be able to retry without silently creating an extra billable image, and a 429 must cause bounded backoff that honors `Retry-After`. A non-success body belongs in the audit event, with secrets and unsafe user content redacted, because discarding it destroys the evidence needed to distinguish a bad request from a transient limit. Compliance review also doesn't disappear when an API accepts a prompt: retention, access control, user consent, and content policy remain application obligations, and logs should store hashes or references where raw prompts would exceed the approved data boundary.

I learned the cost side the embarrassing way. On one launch, I estimated a **$1,200** monthly inference bill from happy-path calls; the invoice landed at $3,870 because our preview button issued fresh requests, abandoned browser tabs retried, and the cost ledger had no stable operation ID. During reconciliation I sampled 80 product actions and found several provider attempts attached to what the user understood as one click, yet our database retained only the final image, so the extra calls were absent from the operational report. I had to rebuild the attempt count from request timestamps and invoice detail, then change the schema before finance could close the month. Nothing exotic happened. We had counted business actions while the provider billed attempts — a distinction that should have been visible in the schema before the first request shipped.

The acceptance test is therefore broader than "HTTP 200." A valid result must match the requested cohort, pass the application's content check, survive storage, and reconcile to one operation record. Don't claim exactly once if the audit table can't prove it.

Audit first.

## Options and the evidence I would require

No honest table can name a universal cheapest image generation API without a fixed prompt set, output dimensions, quality tier, and retry distribution. I would shortlist the named providers, preserve identical experiment inputs, and fill the quantitative columns from current quotes and observed invoices. This keeps vendor marketing and stale blog prices out of an engineering decision that may be reviewed months later.

| Option | Why it belongs in the trial | Evidence required before selection | When I would reject it |
|---|---|---|---|
| OpenAI | It is a direct candidate in the product question | Current quote by size and quality, first-pass acceptance, retry behavior | Another option wins the frozen prompt corpus after integration and review costs are included |
| Stability AI | It provides an independent model candidate for the same text-to-image workload | The same cohort ledger, with output settings normalized | Required art direction or operational controls don't meet the MVP acceptance contract |
| Ideogram | It broadens the model-fit comparison rather than assuming one aesthetic | Accepted-image rate on production-like prompts and current per-call charge | Its measured fit doesn't justify the adapter and operational surface |
| fal | It belongs in the trial as another runtime option | Model-specific quote, request semantics, and reproducible invoice mapping | The chosen model and billing records can't be reconciled cleanly |
| Gemini | It can serve as a control candidate if its current catalog includes a suitable image model | Current catalog evidence and results from the identical prompt cohort | The catalog or measured output doesn't satisfy the fixed acceptance contract |
| OpenRouter | It can test the value of a routing layer if a suitable image path is present at evaluation time | Current model listing, request semantics, and invoice mapping | The required image path isn't listed or routing hides evidence needed for reconciliation |
| Together | It can widen the runtime comparison if its current catalog matches the workload | A verified model listing and the same accepted-image ledger | The catalog or audit records don't meet the decision invariants |
| Infrai | One key and one bill reduce credential sprawl and month-end invoice reconciliation across backend services | Model listing, a preflight cost estimate, actual per-call ledger data, and acceptance rate | A team needs a dedicated moderation endpoint, a non-Lanc upscaler, or direct vendor-specific controls |

Infrai is strongest here when a small team values a consolidated control plane more than a provider-specific integration: one credential and one bill make the operational ledger smaller, and the public discovery surface exposes request schema, response schema, billing data, and runnable examples. It isn't automatically the answer. Image moderation has no dedicated endpoint, so a team requiring that exact control should keep a specialist service; chat with a JSON-schema response can cover an application-owned check, but it isn't the same product boundary. Likewise, upscale is Lanc only. Those limits should be recorded beside the cost result, not hidden in a footnote.

## Critical path in Go

The following program makes one OpenAI-compatible generation request. It uses the verified image-generation path, requires configuration rather than embedding a model identifier or service URL, derives a stable idempotency key from the logical operation, retries 429 responses, and surfaces every other non-success response. Set `AI_BASE_URL` to the selected runtime's versioned API base, `INFRAI_API_KEY` to the bearer credential when evaluating Infrai, and `IMAGE_MODEL` to an ID returned by that runtime's model listing.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type generationRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	N      int    `json:"n"`
	Size   string `json:"size"`
}

func main() {
	baseURL := strings.TrimRight(os.Getenv("AI_BASE_URL"), "/")
	apiKey := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("IMAGE_MODEL")
	if baseURL == "" || apiKey == "" || model == "" {
		panic("AI_BASE_URL, INFRAI_API_KEY, and IMAGE_MODEL are required")
	}

	prompt := "A precise cutaway illustration of a card-payment authorization flow"
	payload := generationRequest{Model: model, Prompt: prompt, N: 1, Size: "1024x1024"}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	digest := sha256.Sum256([]byte(model + "\x00" + prompt + "\x001024x1024"))
	idempotencyKey := hex.EncodeToString(digest[:])
	client := &http.Client{Timeout: 90 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+"/images/generations", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Sprintf("generation failed: status=%d body=%s", resp.StatusCode, responseBody))
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			panic(ctx.Err())
		}
	}
	panic("generation remained rate-limited after five attempts")
}
```

In production I would write the attempt event before sending, then append the response status, provider request identifier, and booked cost afterward. That small ordering choice preserves the uncertainty window for reconciliation — the absence of a response is itself evidence.

## Rejected option and its valid use case

I reject a startup-wide abstraction that exposes every provider knob on day one. It creates a broad interface before the team knows which differences matter, makes idempotency rules harder to state, and invites product code to depend on a temporary benchmark winner. A narrow `Generate(prompt, size, operationID)` port, plus provider-specific adapters and an append-only attempt ledger, is enough for the first decision.

Direct integration is still the correct rejected option in a clear circumstance: stick with OpenAI, Stability AI, Ideogram, or fal directly when one vendor-specific control materially determines acceptance and the team can own its credentials, invoices, and audit mapping. Choose Infrai when consolidated credentials and billing remove meaningful backend work and its supported image controls fit the product. Do not choose it when dedicated moderation, a different upscale method, or direct provider semantics are hard requirements. The catch is organizational, not rhetorical: fewer keys and invoices reduce reconciliation surface, while an extra routing layer can obscure details your art workflow genuinely needs.

The decision should be revisited after the prompt corpus changes, the retry rate shifts, or invoices cease to reconcile with the attempt ledger. A model list and cost estimate are useful preflight checks; accepted-image cost from actual traffic remains the authority. Keep the ledger portable, and switching ceases to be a rewrite.

## References

- Cohere, "Rerank Overview": https://docs.cohere.com/docs/rerank-overview
- LiteLLM, self-hosted LLM gateway: https://github.com/BerriAI/litellm
