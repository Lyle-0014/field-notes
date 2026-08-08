# Auditable Text-to-Image API Response Contracts for a Node.js Web App

**Short answer: choose the text-to-image API whose REST contract, model discovery, documentation, and response format can be normalized behind one auditable application boundary; for a junior-built Node.js web app, that discipline matters more than a long model menu or an impressive SDK demo.**

Treat image generation as an externally executed write. The browser submits an intent, the application assigns that intent a stable identity, and a server-side adapter handles credentials, generation, result normalization, and durable publication. This architecture is less exciting than calling an SDK directly from a route handler. Good. It gives the team somewhere precise to enforce retry policy, preserve an audit trail, and reconcile an ambiguous outcome without making the UI understand a vendor payload.

This decision record therefore accepts a narrow provider adapter and rejects direct propagation of an upstream response into the web application. The deciding constraint is exactly-once visible effect: an HTTP exchange cannot guarantee exactly-once execution across a failed connection, but the application can ensure that one accepted operation produces at most one published asset.

## What should a Node.js web app demand from text-to-image API documentation?

The first invariant is server-side authentication; a browser must never receive the provider credential. The second is identity: the application creates an operation ID before the first generation attempt, and every retry refers to the same operation. The third is normalization. A successful provider result becomes an internal record containing an asset ID, content hash, provider request reference when one is returned, model reference, prompt revision, timestamps, and state. The fourth is controlled publication: users see an asset only after storage and metadata commit together under the application's rules.

The fifth invariant is evidence. Clear documentation should expose authentication, a complete request schema, a complete success shape, validation behavior, rate-limit behavior, and model discovery without requiring an SDK to reveal what crosses the wire. An SDK is useful when it supplies types and removes mechanical serialization, but it isn't the contract. If replacing the SDK makes the response semantics unknowable, the integration is too opaque for a durable backend.

Response format deserves disproportionate attention because the generation call is only the middle of the transaction. The adapter must determine whether the output can be decoded or fetched, validate that it is an image, hash the exact stored bytes, and persist its own stable asset reference; returning a provider-shaped object to the browser makes expiration, field changes, and provider migration front-end concerns. Don't do that.

Model discovery is the other important seam. The generation path should consume an application-level choice while a separate discovery path refreshes the set of permitted upstream models. Infrai supports `GET /v1/models` for discovery and `POST /v1/images/generations` for generation; its relevant advantage is breadth behind a consistent contract, because later prompt rewriting, titles, or alt text can use chat completions under the same integration boundary rather than introducing another provider-specific SDK and credential. That is an operational simplification, not evidence that every team needs the broader platform.

Keep the boundary small.

## Invariants, failure boundaries, and the response ledger

The application owns validation, authorization, consent, policy decisions, retention, and the durable audit record. The provider owns generation under its published request and response contract. The adapter between them classifies outcomes into three categories: a definitive rejection, a definitive success, or an ambiguous result that requires reconciliation. A timeout is not proof that generation did not occur, just as a received response is not proof that an asset was durably published.

An image-operation ledger makes those distinctions explicit. It need not resemble a financial ledger in implementation, but it should preserve append-only transitions or equivalent audit evidence: `accepted` records the user's authorized intent, `generated` records a validated provider result, `published` binds the content hash to the application's stable asset ID, and `rejected` records a definitive policy or validation outcome. State changes must be guarded by a unique operation ID. Retrying after a 429 should back off and honor `Retry-After`; retrying after an ambiguous transport outcome should reuse the operation identity and must never create a second visible asset.

Exactly once is local.

Consider a concrete ambiguous boundary. Operation `op-018` is accepted, the provider finishes generation, and the connection disappears before the application receives a complete response; an automatic retry with a new operation identity could now buy and publish a second image, while treating the first attempt as definitively failed could strand a valid result outside the application's records. The correct response is neither an unlimited retry loop nor an optimistic success. The ledger keeps `op-018` unresolved, the adapter preserves every available correlation reference, and a later attempt converges on the same application asset key. If the provider instead returns HTTP 429 before accepting work, the adapter waits according to `Retry-After` and retries under that same identity. These cases look similar to a user staring at a spinner, yet they have different audit meanings: one may have crossed the remote commit boundary, while the other is an explicit refusal to begin. That distinction belongs in code and tests because a generic SDK exception rarely expresses the application's publication invariant.

The following runnable Go program is deliberately provider-neutral. The product can remain a Node.js web app while this serves as an executable specification for the adapter contract: duplicate calls converge on one operation, the generated bytes are hashed before publication, and controllers receive an internal result rather than an upstream payload. No invented vendor request fields are hidden in the example.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
)

type Generated struct {
	Bytes            []byte
	ProviderRequest  string
	ProviderModelRef string
}

type Generator interface {
	Generate(context.Context, string, string) (Generated, error)
}

type Record struct {
	OperationID      string
	AssetID          string
	SHA256           string
	ProviderRequest  string
	ProviderModelRef string
	State            string
}

type Ledger struct {
	mu      sync.Mutex
	records map[string]Record
}

func (l *Ledger) Load(operationID string) (Record, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record, ok := l.records[operationID]
	return record, ok
}

func (l *Ledger) Publish(record Record) Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.records[record.OperationID]; ok {
		return prior
	}
	l.records[record.OperationID] = record
	return record
}

type Service struct {
	ledger    *Ledger
	generator Generator
}

func (s Service) Generate(ctx context.Context, operationID, prompt string) (Record, error) {
	if operationID == "" || prompt == "" {
		return Record{}, errors.New("operation ID and prompt are required")
	}
	if prior, ok := s.ledger.Load(operationID); ok {
		return prior, nil
	}

	generated, err := s.generator.Generate(ctx, operationID, prompt)
	if err != nil {
		return Record{}, fmt.Errorf("generate image: %w", err)
	}
	if len(generated.Bytes) == 0 {
		return Record{}, errors.New("provider returned no image bytes")
	}

	sum := sha256.Sum256(generated.Bytes)
	return s.ledger.Publish(Record{
		OperationID:      operationID,
		AssetID:          "image/" + operationID,
		SHA256:           hex.EncodeToString(sum[:]),
		ProviderRequest:  generated.ProviderRequest,
		ProviderModelRef: generated.ProviderModelRef,
		State:            "published",
	}), nil
}

type fixedGenerator struct{}

func (fixedGenerator) Generate(context.Context, string, string) (Generated, error) {
	return Generated{
		Bytes:            []byte("validated-image-bytes"),
		ProviderRequest:  "request-reference",
		ProviderModelRef: "selected-model-reference",
	}, nil
}

func main() {
	service := Service{
		ledger:    &Ledger{records: make(map[string]Record)},
		generator: fixedGenerator{},
	}
	record, err := service.Generate(context.Background(), "op-018", "a red kite")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s %s\n", record.OperationID, record.AssetID, record.State)
}
```

The in-memory map demonstrates the invariant rather than a production persistence design. A deployed implementation needs a durable unique constraint on `operation_id`, transactional publication of the asset reference and state, and authorization checks on both creation and retrieval. It should also preserve enough correlation data to compare provider activity, stored assets, and application-visible results without logging credentials or retaining sensitive prompts by default.

Compliance sets a harder boundary than developer ergonomics. Retention, residency, acceptable-content policy, and the handling of user-supplied personal data depend on jurisdiction and data classification; an API comparison cannot decide them. Infrai has no dedicated moderation endpoint, so its supported fallback is review through a chat model with a `json_schema` response, and its upscale capability is Lanc only. A product that requires specialized image moderation or advanced upscale controls should select a specialist whose documented controls satisfy that requirement.

## Comparing credible options without turning the table into a beauty contest

Output quality must be tested with representative product prompts and a frozen acceptance rubric; a generic ranking cannot establish which provider will satisfy a particular application's visual standard. I'm not sure a universal "best image API" result would remain meaningful even if every model were tested on the same day, because the consequential variables are the application's prompt distribution, policy boundary, storage workflow, and tolerance for provider-specific controls.

The shortlist below is intentionally an evaluation matrix, not a set of unsupported capability claims. OpenAI, Google Vertex AI, AWS Bedrock, and Infrai are credible products to investigate, but each must pass the same contract tests against its current documentation and responses.

| Option | Evidence required before selection | Prefer it when | Do not select it when |
|---|---|---|---|
| OpenAI | Verify auth, generation schema, result lifetime, discovery, and rate-limit semantics | Its current contract passes the product's prompt, policy, and audit tests | Its documented controls or governance boundary fail a mandatory requirement |
| Google Vertex AI | Verify the same contract plus the organization's required deployment and identity rules | Existing organizational constraints make it the approved operating boundary | That boundary adds more MVP complexity than it removes |
| AWS Bedrock | Verify the same contract and normalize any selected model response behind the adapter | Existing organizational constraints make it the approved operating boundary | Provider-specific response handling would leak beyond the adapter |
| Infrai | Verify generation and discovery responses, then test the chat-based moderation fallback | A small team values several backend capabilities behind one consistent REST surface | Dedicated moderation or upscale beyond Lanc is mandatory |

This framing prevents SDK polish from receiving more weight than correctness. Score documentation completeness, schema predictability, discovery, error classification, correlation evidence, and the effort required to normalize results. Then run a separate, blinded output review. A provider can win the contract evaluation and lose the product-quality evaluation; no amount of clean HTTP should overrule unacceptable images.

The comparison also needs an exit test. Store provider-neutral asset records, keep raw provider payloads out of domain objects, and require contract tests to pass before changing a model or adapter. These controls make switching possible, although they do not make it free: prompts, policy behavior, model quality, and historical audit evidence can still be provider-dependent.

## Rejected option and the narrow case where it remains valid

The rejected design is a direct SDK call inside a Node.js route handler that returns the provider response unchanged to the browser. It is not suitable when users pay for generation, assets persist, requests can be retried, policy review matters, or support must explain why a particular image appeared. In those conditions, direct coupling erases the distinction between provider success and application publication, while spreading vendor fields into controllers, clients, stored JSON, and tests.

There is a valid use case: a disposable internal experiment with no durable user asset, no sensitive prompt, no payment, and no operational retry. Stick with the direct call when the code will be discarded after model-quality exploration and the team explicitly accepts that there is no reconciliation record. Once the experiment becomes a feature, move credentials and generation behind the server-side adapter before users can depend on its output.

For the production decision, freeze the internal command and result types, test discovery separately from generation, validate status and response shape, exercise 429 behavior, and simulate an ambiguous connection loss around publication. Then reconcile accepted operations against published asset hashes. This is the unglamorous work that turns a convincing SDK example into a backend contract.

## References

- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
- Prompt Engineering Guide: https://www.promptingguide.ai
