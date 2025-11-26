# Strata Task Chapter Metadata & Determinism Spec

Problem: Strata chapters currently persist only raw JSON `TaskData` bodies. When `DoTask` sees an existing chapter for an ordinal, it blindly returns that chapter instead of re-running the task, even if the input data for this invocation differs. We need workflow-level metadata that proves a cached chapter corresponds to the same inputs, or we fail with a deterministic error.

## Goals
- Persist task payloads alongside workflow metadata (step, capability, worker) in a single JSON envelope.
- Attach an input hash to every `TaskData` so cache hits are validated before reuse.
- On `DoTask`, reject cached chapters whose stored input hash is missing or does not match the current input, surfacing deterministic errors (`workflow was not deterministic`).
- Keep artifacts stored as Strata artifacts, not inlined into the envelope.
- Support future result shapes (e.g., errors) without changing the envelope contract.

## Data Model (Chapter Envelope)
- Chapter body is JSON for easy inspection:
  ```json
  {
    "meta": {
      "version": 1,
      "ordinal": <int>,
      "task_type": "<capability name>",
      "worker_id": "<swf worker id>",
      "created_at": "<RFC3339>",
      "input_hash": "<hex sha256>"
    },
    "payload_kind": "App", // discriminator matches struct/type name
    "payload": <payload object (JSON)>
  }
  ```
- Artifacts stay on the chapter (as today); they are not embedded in the envelope. The input hash must incorporate artifact references (see hashing).
- `version` allows future format changes; `source` tags who wrote it.
- Extensibility:
  - `payload_kind` is a required discriminator; value matches the concrete payload struct/type name to keep schema discoverable (`App`, `AppError`, `SystemError`).
  - Each payload kind is its own struct so we can add fields over time without breaking others. `payload` is the JSON-serialized struct for that kind. There is only one payload slot; errors are expressed as typed payloads (no separate `error` field).

### Payload Structs (v1 scope)
- `App`: current task output, same semantics as today (`map[string]interface{}` JSON). Artifacts remain on the chapter (never embedded); optional lightweight references may be included if helpful.
- `AppError`: structured task/user-land error (slog-friendly), fields like `{ message, level, attrs?, stacktrace? }`.
- `SystemError`: infrastructure/transport failure (e.g., saving to Strata, updating pgwf), fields like `{ message, component, code?, retryable?, stacktrace? }`.

## Hashing
- Compute `input_hash` as `sha256` over:
  - Canonical JSON bytes for the task input payload (`TaskData.GetData().ToBytes()` is JSON).
  - Deterministic encoding of artifacts: concatenate each artifact’s `(uri, hash, filename)` in sorted order by URI. Use the artifact hash provided by Strata; do not fetch artifact bodies. Strata guarantees artifact body/hash consistency.
- Represent the hash as lowercase hex. Use a shared helper for write + validation.
- The hash is computed on the task input to this ordinal (the data handed into `DoTask`), not on the task output.

## Write Path
- When `DoTask`, `TaskHandle.Finish`, or the final job completion write is about to persist a chapter:
  - Compute `input_hash` from the input `TaskData`.
  - Wrap the output `TaskData` JSON payload in the envelope above, filling metadata from the current runner context (`job_id`, `ordinal`, `task_type`, `worker_id`, timestamp).
  - Save the chapter with `WithBytes(envelopeJSON)` and attach artifacts as today.
- Initial job creation (`StartJob`) and restarts (`RestartJob`) also envelope their initial chapter so every ordinal participates in deterministic validation.

## Read Path / Cache Validation
- At the start of `DoTask`, compute `input_hash` for the provided input.
- Fetch the chapter for the target ordinal:
  - On cache hit: decode the envelope. If `meta.input_hash` is missing, return a deterministic error for missing hash. If present but does not match the computed hash, return `workflow was not deterministic`. Do not return the cached payload in either error case.
  - On cache hit with matching hash: unwrap `payload` back into `TaskData` and return without executing the task worker.
  - On miss: run the task worker and persist output as above.
- If envelope decoding fails, treat it as a deterministic error (same error family) to avoid silently accepting malformed data.

## Errors
- Add well-known errors, e.g., `ErrWorkflowNotDeterministic` (message includes `workflow was not deterministic`) and `ErrMissingInputHash` for absent hashes; include ordinal/task type in context.
- `DoTask` should propagate these errors directly.

## Testing
- Cache hit with same input: `DoTask` returns without executing and does not error.
- Cache hit with different input: `DoTask` errors with `workflow was not deterministic`.
- Cache hit with same data but different artifacts (hash differs): deterministic error is raised.
- Cache hit with missing hash: deterministic missing-hash error is raised.
- Envelope round-trip: writing then reading preserves payload JSON and artifacts unchanged aside from added metadata.
