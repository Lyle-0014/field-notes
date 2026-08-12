# Audit-First Chatbot Controls: LLM API Verdicts Under JSON Schema

Short answer: for basic moderation in an in-app chatbot, choose an LLM API only after proving that it can return a JSON Schema-constrained verdict, then enforce that verdict in an application-owned, fail-closed state machine with versioned policy, replay protection, and a review path. A dedicated moderation endpoint is optional for low-impact screening; an auditable control boundary is not.

The least complex credible design uses two model calls: one produces a narrow moderation decision and, only after an `allow` decision is committed, another produces the assistant response. Screen the response before delivery as a separate decision. This costs latency, but it prevents a fluent generation from outrunning the control that is supposed to govern it.

Schema conformance solves one small problem: it makes the verdict machine-readable. It does not prove that the verdict is correct, neutralize prompt injection, provide exactly-once processing, or satisfy a retention obligation. OWASP identifies prompt injection and insecure output handling as distinct risks; treating valid JSON as trusted content collapses those risks into one and leaves both poorly controlled.

Fail closed.

## How should an API use LLM JSON Schema for in-app chatbot moderation?

The API needs to support a constrained object whose possible outcomes are closed and operationally meaningful. A useful contract has an enumerated `action` such as `allow`, `block`, or `review`; a bounded set of policy labels; a policy version; and a short operator-facing reason code. Free-form explanations should not determine program flow. The application must validate the entire object locally, reject unknown properties, and refuse to advance when a required value is absent or outside its declared range.

Three outcomes are better than a Boolean. `review` preserves uncertainty instead of laundering it into `allow`, which matters whenever a later reconciliation must explain why the system acted. I'm not sure a universal confidence threshold can be defended across products, languages, and abuse categories; the evidence needed to set one is a labeled evaluation corpus drawn from the product's own traffic distribution, with separate false-negative and false-positive measurements for each policy category.

The model also needs a fixed separation between policy instructions and untrusted conversation text. User content is data — quoted instructions inside it cannot select the policy, alter the schema, or declare itself exempt. The same rule applies to retrieved documents and tool output. A schema limits the shape of the model's answer, while prompt-injection defenses govern which instructions have authority; those are complementary controls.

Finally, inspect the API as an operational dependency rather than a feature checklist. Require explicit timeouts, stable model identifiers, documented data handling, enough request metadata to correlate a decision, and a way to pin the model configuration used in an evaluation. If the provider does not offer an idempotency primitive, the application can still enforce an exactly-once effect inside its own boundary: derive a decision key from the normalized message hash, policy version, schema version, and model configuration, then place a uniqueness constraint on that key. Retries read the committed result instead of silently creating a second verdict.

Exactly once is scoped.

## The safety boundary is a small ledger

Moderation should be modeled as a sequence of durable state transitions, not as a prompt placed somewhere in a request handler. For an incoming message, the useful states are `received`, `input_screened`, `generated`, `output_screened`, and `delivered`. Each transition records the expected prior state, decision key, policy and schema versions, model configuration, timestamp, and normalized disposition. Conversation text should be stored separately, encrypted where appropriate, and retained only for a documented period; an audit trail does not justify indefinite storage of personal or regulated data.

This design resolves the ugly cases. If a client disconnects after input screening, a retry resumes from the committed verdict. If generation completes but the worker loses its lease, the next worker screens the stored candidate instead of generating a new one. If output screening returns `review`, delivery cannot occur merely because the input was allowed. No database transaction can remain open safely across remote inference, so an outbox, leased worker, and compare-and-swap transition provide a more honest boundary than pretending the entire exchange is atomic.

Consider one concrete replay sequence. Message `m-1042` is accepted, its normalized content and release versions produce decision key `d-88`, and the input verdict commits as `allow`; generation then produces candidate `c-19`, but the mobile connection closes before output screening begins. The client retries `m-1042` while a worker lease is still present. A naive handler can launch another input check and another generation, producing two candidates whose wording and later verdicts may differ, after which whichever request finishes first becomes the accidental system of record. The ledger-shaped handler instead finds `d-88`, verifies that its policy, schema, and model configuration match the retry, and resumes the stored candidate `c-19` from `generated`. A new worker acquires the next transition with compare-and-swap, screens that candidate, commits exactly one output verdict, and writes one delivery intent; if the first worker wakes later, its stale expected state prevents a second transition. This is not a claim of universal exactly-once execution, because the network can still duplicate delivery and an external model call cannot join the local transaction. It is a narrower, defensible guarantee: one committed decision and one authorized delivery intent for the decision key inside the application boundary. The remaining edge is handled at the channel adapter with a stable delivery identifier and reconciliation, much as a payment system separates an authorized ledger entry from the uncertain observation of a remote side effect. That distinction is fussy. It is also the difference between an audit record that merely lists events and one that can establish which event had authority.

Authority must be singular.

The following Go core deliberately excludes any vendor route or request shape. The transport adapter is responsible for obtaining a verdict; this layer is responsible for rejecting malformed decisions and preventing an invalid transition from acquiring the authority of a committed record.

```go
package safety

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"time"
)

type Action string

const (
	Allow  Action = "allow"
	Block  Action = "block"
	Review Action = "review"
)

type Verdict struct {
	Action        Action   `json:"action"`
	Labels        []string `json:"labels"`
	PolicyVersion string   `json:"policy_version"`
	ReasonCode    string   `json:"reason_code"`
}

type Decision struct {
	Key           string
	MessageHash   string
	SchemaVersion string
	ModelConfig   string
	Verdict       Verdict
	CommittedAt   time.Time
}

var allowedLabels = []string{"abuse", "self_harm", "sexual", "violence"}

func Validate(v Verdict) error {
	if v.Action != Allow && v.Action != Block && v.Action != Review {
		return errors.New("MOD_ACTION_INVALID")
	}
	if v.PolicyVersion == "" || v.ReasonCode == "" {
		return errors.New("MOD_SCHEMA_INVALID")
	}
	for _, label := range v.Labels {
		if !slices.Contains(allowedLabels, label) {
			return errors.New("MOD_LABEL_UNKNOWN")
		}
	}
	return nil
}

func NewDecision(message []byte, schemaVersion, modelConfig string, v Verdict) (Decision, error) {
	if err := Validate(v); err != nil {
		return Decision{}, err
	}
	messageSum := sha256.Sum256(message)
	messageHash := hex.EncodeToString(messageSum[:])
	keySum := sha256.Sum256([]byte(messageHash + "\x00" + v.PolicyVersion + "\x00" + schemaVersion + "\x00" + modelConfig))

	return Decision{
		Key:           hex.EncodeToString(keySum[:]),
		MessageHash:   messageHash,
		SchemaVersion: schemaVersion,
		ModelConfig:   modelConfig,
		Verdict:       v,
		CommittedAt:   time.Now().UTC(),
	}, nil
}
```

The allowlist is illustrative policy structure, not a universal taxonomy. A production schema should bound array sizes and string lengths, disallow additional properties, and be tested with missing, duplicated, unknown, and wrong-type fields. The database insert should be unique on `Decision.Key`; the transition to generation should occur only after that insert commits. Don't send raw conversation text to ordinary application logs when a hash, correlation identifier, and restricted evidence store can support the audit requirement with less exposure.

Compliance constraints remain product-specific. Payment, health, employment, and children's services may have different legal bases, residency restrictions, deletion duties, review requirements, and prohibitions on automated decisions. Engineering can expose the control points, evidence, and retention settings, but counsel and the accountable business owner must decide which obligations apply.

## Compare architectures after defining the loss function

There is no universally best API because “basic moderation” does not define the consequence of a miss. Before comparing implementations, write down the prohibited categories, false-negative cost, false-positive cost, maximum added latency, languages, appeal process, review capacity, data-location constraints, and what happens when the screening dependency times out. Only then can an API feature have decision value.

| Control pattern | Useful property | Material limitation | Appropriate use |
|---|---|---|---|
| Schema-constrained general LLM call | Policy can express contextual distinctions in a typed contract | Probabilistic judgment adds latency and requires product-specific evaluation | Basic conversational triage with a review path |
| Dedicated moderation classifier | Purpose-built taxonomy and a narrow integration boundary | Published categories may not match the product policy | Common abuse categories with an acceptable taxonomy fit |
| Self-hosted classifier | Direct deployment and data-location control | Requires labeled data, calibration, model operations, and drift monitoring | Residency or scale justifies an ML operating function |
| Deterministic rules | Replayable, explainable, and fast | Context, obfuscation, and language variation make rules brittle | Exact identifiers and hard compliance boundaries |
| Human review | Handles ambiguity and creates adjudicated labels | Queue delay, reviewer consistency, privacy, and staffing constrain throughput | High-consequence or uncertain decisions |

The catch is that a second general LLM call is not suitable when one miss can authorize an irreversible financial or safety action, when an applicable rule requires a validated deterministic control, or when the latency budget cannot tolerate sequential inference. Use deterministic rules for exact prohibited instructions, a purpose-built classifier when its documented taxonomy matches the policy, self-hosting when control of processing location outweighs operational burden, and human review when the consequence exceeds the evidence available to automation. A layered system often has the cleanest audit argument, but more layers also create more versions, failure modes, and reconciliation work.

Cost belongs in capacity planning, not in the headline. Evaluate inference calls, retries, peak concurrency, evidence storage, appeals, and review labor against a representative transcript set. A cheaper call that produces more reviews can cost more to operate; a fast classifier that misses the one regulated category can be unacceptable regardless of throughput. Your mileage may vary because traffic mix and reviewer economics are local facts.

## Test failures, not just happy-path JSON

A credible evaluation freezes the policy, schema, model configuration, and labeled corpus as one release artifact. The corpus should include multilingual phrasing, misspellings, obfuscation, quoted harmful material in benign discussion, multi-turn context, prompt-injection attempts, empty input, oversized input, and borderline cases that reviewers genuinely dispute. Report per-category confusion matrices and review rates. One aggregate accuracy score can hide the category with the highest loss.

Transport and workflow tests matter just as much. Exercise timeouts before and after a verdict is committed, duplicated deliveries, reordered jobs, a worker lease expiring after generation, schema-invalid output, unknown labels, policy-version mismatch, and a client disconnect immediately before delivery. Assert that none of these paths bypasses screening or emits two externally visible answers. A local `MOD_SCHEMA_INVALID` result should be terminal for that attempt and observable by code, while the user receives a neutral retry message rather than internal diagnostic detail.

Logging needs discipline. Record correlation IDs, hashes, versions, dispositions, latency, transition failures, queue age, reviewer disagreement, overrides, and appeals. Keep the raw message out of broad telemetry. Access to any evidence store should itself be audited, and deletion must cover replicas and derived datasets under the applicable retention policy. It's easy to collect evidence; it is harder to prove why each field is necessary and when it disappears.

## Roll out with reversible policy versions

Begin in shadow mode, persisting verdict metadata without changing user-visible behavior, and have qualified reviewers label a stratified sample. Resolve disagreements before treating the labels as ground truth. Enforcement can then proceed from deterministic hard boundaries to reviewed model decisions and, finally, a limited traffic canary whose promotion criteria include timeouts, schema failures, queue age, appeals, per-category misses, and reviewer disagreement.

Rollback must be semantic. Pin the policy, JSON Schema, model configuration, and decision logic together; retain the prior signed artifact; and record the artifact version on every decision. Rolling application code back while leaving an incompatible policy or schema active creates an audit trail that can be replayed but not explained. Short version: deploy the contract as a unit, reconcile in-flight decisions by version, and never turn an unavailable or ambiguous verdict into permission.

## References

- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- OpenRouter documentation: https://openrouter.ai/docs

## Further reading

- https://owasp.org/www-project-top-10-for-large-language-model-applications/
- https://openrouter.ai/docs
