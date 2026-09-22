# TreeDB Template Compression — Sprint Plan (Autonomous Implementation Runbook)
Updated: 2026-01-24

This is an executable implementation plan for a **schema-blind template compression engine** for TreeDB’s value-log. It is written so a future agent can implement it autonomously with minimal ambiguity.

Git workflow (MUST):
- Do all work on feature/sprint branches (never commit to `main`).
- Use one branch per sprint or per PR unit (e.g. `feature/template-t0-codec`, `feature/template-t1-routing`).
- Keep PR-sized changesets small and reviewable; do not merge until final review post-implementation.
- Before starting: `git fetch --all` and branch from the current `origin/main`.
- When the worktree is already dirty, create the branch first (so changes are captured on the branch), then commit.

Primary goals:
- Workload-agnostic (byte-level, schema-blind).
- Shared model persists in a **TreeDB-backed side store** (like dictdb): the DB lifetime acts as the “block/window”.
- Use **TreeDB public APIs** for templatedb operations (no custom cache layer in the compressor).
- Keep template compression **separate from dict/zstd** for this sprint (TemplateOnly).
- Deterministic, bounded, safe under adversarial inputs.

Non-goals (this sprint):
- Template GC / reachability tracking (append-only templatedb is acceptable).
- Autotuning / integrating with dict/zstd tuner.
- “Best possible” compression ratio.

---

## A) Scope + invariants (MUST)

### A1) Compression mode (explicit)

Add an explicit tri-state option for value-log template compression:

- `TemplateOff`
- `TemplateOnly` (forces dictID=0; template is the only compression engine used)
- `TemplatePrepass` (template first, then existing dict/zstd path; NOT implemented this sprint, but defined now to remove ambiguity)

For this sprint: implement `TemplateOnly` end-to-end.

### A2) Format stability policy (for the sprint)

TemplateValue payload format is **frozen for this sprint**.

Any future format change MUST:
- bump a header `version` byte, and
- either require DB wipe or ship an explicit migration tool.

### A3) Read strictness (explicit)

Add `TemplateReadStrict bool`:
- strict (default for tests/CI): missing template bytes is an error (`ErrMissingTemplate`)
- lenient (optional): if payload claims “template encoded” but template missing/corrupt, return an explicit error (do NOT treat as raw by default; treating as raw can be silent corruption)

For this sprint: implement strict by default; allow an option for lenient recovery if desired.

### A4) Concurrency model (explicit)

For this sprint:
- No asynchronous/background template training goroutines.
- Training (sampling, synthesis attempts, publish decisions) runs inline on the write path under strict CPU/ops budgets.
- Publishing templates and routing index updates are done inline via a single `WriteSync` batch.

Rationale: deterministic behavior and simplicity first. If this is slow, the optimization target is TreeDB and/or a future async design, not a bespoke cache in the compressor.

---

## B) Data model + encoding format (TemplateValue)

We need schema-blind multi-hole templating: anchors + gaps.

### B1) Template definition

A template is an ordered backbone of anchors:

- anchors: `A0..A(n-1)` (byte strings, exact match)
- decoded value is reconstructed as `gap0 + A0 + gap1 + ... + A(n-1) + gapN`

Hard caps (configurable):
- `MaxAnchorsPerTemplate` (e.g. 16–64)
- `MinAnchorLen` (e.g. 16) to avoid punctuation anchors
- `MaxAnchorBytesTotal` (e.g. 512–4096)
- `MaxAnchorLen` (e.g. 64)

Anchor length strategy (MUST):
- Training starts from fixed-length k-grams (`K=16`) for counting.
- When constructing a template backbone, anchors MUST be deterministically **extended** beyond K
  (up to `MaxAnchorLen`) to reduce ambiguity and matching cost:
  - pick a deterministic reference sample (e.g. the lexicographically smallest value bytes or the
    smallest xxh3_64(value))
  - for each selected anchor’s median position, extend left/right while bytes match in
    >= `MinPresenceRatio` samples
  - cap to `MaxAnchorLen` and ensure final length >= `MinAnchorLen`

### B2) TemplateValue payload encoding (versioned header)

Binary layout:

```
magic[2]   = 'T','M'
ver[1]     = 1
flags[1]   = bit0: template_encoded
templateID = uvarint (uint64)
gapCount   = uvarint (bounded, <= MaxGaps)
gaps       = repeated gapCount times:
  gapLen   = uvarint (bounded)
  gapBytes = gapLen bytes
```

Semantics:
- `gapCount` MUST equal `len(anchors)+1` for the referenced template.

Safety invariants (MUST):
- Cap `gapCount`.
- Cap each `gapLen`.
- Cap total decoded size *before* allocation:
  - `decodedLen = sum(gapLens) + sum(anchorLens)`
  - reject if overflow or exceeds a max (e.g. `limits.MaxRecordSize` or a separate cap).
- Any parse error => `ErrCorrupt`.
- Missing template => `ErrMissingTemplate` (strict mode).

---

## C) `templatedb` store (dictdb-style, public API only)

### C1) Public API only (concrete requirement)

`templatedb` MUST use TreeDB’s public DB wrapper for storage access (not internal `*db.DB`):
- backend-only open (`treedb.OpenBackend`) is acceptable.
- All operations use `(*treedb.DB).Get`, `GetAppend`, `SetSync`, `DeleteSync`, `NewBatch`, iterators.

Acceptance test (MUST):
- `TreeDB/internal/templatedb` must not import `TreeDB/db` or other internal DB packages directly.

### C2) Key schema + schema versioning

Prefix all keys with a store schema version byte to prevent silent drift:

- Template definitions:
  - `v1:t:<templateID_be64>` -> `TemplateDefBytes`
- Fingerprint routing index:
  - `v1:f:<fp_be64>` -> `CandidateListBytes`
- Optional meta keys (if needed):
  - `v1:m:...`

### C3) Template ID assignment (MUST pick one)

Use **content-hash IDs** for the prototype:

- `templateID = xxh3_64(canonicalTemplateBytes)` (or SHA256->u64, but xxh3 is fine)
- collision rule:
  - if key exists with different bytes, rehash with a deterministic salt (e.g. append `0x01`, `0x02` ...) for a small fixed number of attempts; if still colliding, reject publish

CanonicalTemplateBytes must be deterministic:
- anchors encoded in order with length prefixes (varint) and raw bytes.

### C3.1) TemplateDefBytes binary format (MUST)

TemplateDefBytes is a strict binary format used for:
- decoding templates,
- canonical hashing (templateID), and
- determining `templateSize` for candidate ranking/eviction.

Format:

```
tmpl_ver u8 = 1
anchorCount uvarint (bounded by MaxAnchorsPerTemplate)
anchors repeated anchorCount:
  anchorLen uvarint (bounded by MaxAnchorLen and >= MinAnchorLen)
  anchorBytes[anchorLen]
crc32c u32 (optional but RECOMMENDED; over all bytes preceding the crc)
```

Rules:
- Anchors are stored in backbone order (A0..A(n-1)).
- Any decode error or bounds violation => corrupt template.
- `templateSize` is `len(TemplateDefBytes)` (including CRC if present).

### C4) Candidate list encoding + deterministic eviction

CandidateListBytes MUST store size metadata to avoid extra template reads during eviction/scoring.

CandidateListBytes format:
- repeated tuples `(templateID, templateSize)`:
  - `templateID uvarint`
  - `templateSize uvarint`

Encoding rules:
- Dedup by templateID (if duplicates appear, keep the smallest templateSize; ties by ID).
- The on-disk tuple order MUST be deterministic. For simplicity:
  - store tuples sorted by `(templateSize asc, templateID asc)`.

Caps (MUST):
- `MaxCandidatesPerFP` (e.g. 32)
- `MaxCandidateListBytes` (optional redundant cap)

Eviction policy (MUST be deterministic):
- When list exceeds caps, keep the **best** candidates by:
  1) smaller template byte-size (requires `templatedb` to know template sizes), then
  2) templateID ascending

Implementation note:
- easiest: when inserting, load existing tuples, insert/update one tuple, sort by (size,id), keep first K, re-encode.

### C5) Write amplification mitigation (MUST)

Publishing a template must NOT update unbounded numbers of routing keys.

Define per-template routing key set:
- `RouteFPCount = 8..16` (fixed)
- RouteFPs are chosen deterministically, e.g.:
  - compute fingerprints over `concat(anchors)` and take the smallest `RouteFPCount` hashes, or
  - compute fingerprints over a representative sample that matched the template and take smallest `RouteFPCount`

Hard cap:
- max index updates per published template = `RouteFPCount`.

### C6) Atomic publish semantics

Template publish must be all-or-nothing:
- write `t:<id>` and all `f:<fp>` updates in a single `WriteSync` batch.

If partial write ever occurs (should not), strict read mode treats missing template as error; lenient mode may recover by treating as raw or error (choose one).

### C7) Template activation fence (MUST)

To prevent low-quality or early templates from polluting routing lists:

- A template MUST NOT be added to any `v1:f:<fp>` candidate lists until it is “activated”.
- Activation policy (minimal, in-memory; no new on-disk schema required):
  - during training, after building a candidate template, simulate matching/encoding on reservoir samples
  - only add routing index entries if the template achieves `MinActivateHits` matches among the M samples
    (or `MinActivateSavedBytes` total savings)
- Even after activation, enforce per-bucket cooldown (see training section).

---

## D) Candidate retrieval: “quickly find the right template”

This is mandatory for viability once template count grows.

### D1) Fingerprinting algorithm (fully specified)

We need determinism + implementability without research-grade rolling hashes.

Fingerprinting v1 (simple, stable):
- Parameters:
  - `K = 16` (k-gram length)
  - `W = 64` (winnowing window in k-grams)
  - `MaxFP = 64` (cap)
- Compute `h[i] = xxh3_64(value[i : i+K])` for all k-grams (no rolling hash).
- Winnowing:
  - For each window `[i, i+W)` choose the minimum hash.
  - Tie-break: if equal minima, choose the **leftmost** position.
- Dedup:
  - dedup by hash value only (ignore position) to keep small sets.
- Take the smallest `MaxFP` hashes if more produced (deterministic truncation).

Edge cases:
- if `len(value) < K`: fingerprints are empty.
- if fingerprints empty: no candidates; store raw.

### D2) Candidate scoring + tie-breaking (deterministic)

Candidate collection:
- For each fingerprint `fp`:
  - load candidates from `templatedb.GetCandidates(fp)`
  - per templateID increment `score[id]++` and retain templateSize from the candidate tuple

Pick candidates to verify:
- primary: higher score
- secondary: smaller template byte-size
- tertiary: templateID ascending

Hard caps:
- `MaxFPReads = min(len(fps), MaxFP)`
- `MaxTemplateFetch = N` (e.g. 16 or 32)
- `MaxDBOpsPerValue = MaxFPReads + MaxTemplateFetch` (enforce)

If caps exceeded:
- stop early and proceed with best candidates collected so far.

---

## E) Matching algorithm guardrails (MUST)

### E1) Anchor quality constraints (enforced now)

Reject anchors that:
- are shorter than `MinAnchorLen` (e.g. 16)
- exceed max bytes total or count
- are “ambiguous”:
  - if `bytes.Count(value, anchor) != 1` for more than `AmbiguityPct` of sampled values (e.g. 10%), don’t include it in templates

### E2) Per-match work bounds

Bound matching work to avoid quadratic surprises:
- `MaxAnchorSearchOps` per match (e.g. 128)
- each `bytes.Index` call increments ops; if exceeded, treat match as failed

### E3) Cheap “expected savings” pre-check

Before extracting gaps, compute lower bound:
- `minEncoded = headerOverhead + varintOverhead(gap lens) + (rawLen - sum(anchorLens))`
- if `minEncoded >= rawLen - MinSavingsBytes`, skip extraction and treat as no match.

---

## F) Training/publishing (online, bounded, deterministic)

We need a trainer that is feasible online and avoids LCS/suffix arrays.

### F1) Bucket key construction (deterministic)

Define bucket key as:
- compute fingerprints for value
- take the smallest 8 fp hashes (or fewer if not available)
- bucketKey = xxh3_64(concat(those 8 hashes in ascending order))

This is stable across restarts and independent of input ordering.

### F2) Reservoir / sampling bounds

Per-bucket reservoir:
- `MaxBuckets`
- `MaxValuesPerBucket`
- `MaxBytesPerBucket`

Sampling policy:
- only add values with `len(value) >= K` and non-empty fingerprints.

### F3) Anchor candidate counts with collision defense

When counting k-gram hashes for anchor candidates:
- store representative bytes for each hash (first seen)
- on subsequent occurrences of that hash:
  - verify bytes equal representative before incrementing (hash collision defense)

### F4) Template synthesis (backbone selection)

For a bucket:
1) Promote anchor candidates:
   - count >= `MinAnchorFreq`
   - `len(anchor)==K`
   - ambiguity guard based on sample scan (`bytes.Count` mostly == 1)
2) For each sample value:
   - locate promoted anchors and record (anchorID, position)
3) Select backbone:
   - keep anchors present in >= `MinPresenceRatio` (e.g. 0.95)
   - order by median position
   - drop overlaps
   - enforce `MaxAnchorsPerTemplate` and total bytes caps

### F5) Publish quality gate (MUST)

Before publishing:
- simulate encoding on M samples from reservoir (e.g. 16)
- require mean savings >= `MinPublishSavingsBytes` OR mean ratio <= `MinPublishRatio`
- if not met: do not publish

### F6) Cooldown / stop-spam rules (MUST)

Per bucket:
- after publishing a template, enforce cooldown:
  - do not publish another for `CooldownValues` new samples (or `CooldownTime`)
- if recent published templates have low hit-rate (if tracked), back off further (optional for sprint)

### F6.1) Training CPU budgets (MUST)

Training must be bounded so it cannot dominate ingest:
- Only admit 1 in N values into training (`TrainSampleStride`), per bucket and/or globally.
- Only attempt synthesis every `S` new samples per bucket (`SynthesizeEverySamples`).
- Cap per-synthesis scan work:
  - `MaxAnchorScanPerSynthesis`
  - `MaxValuesScannedPerSynthesis`

### F7) Routing keys for published template (deterministic)

RouteFPs for template are:
- compute fingerprints over `concat(anchors)` and take smallest `RouteFPCount`

Do not update more than `RouteFPCount` index keys.

---

## G) Integration (template-only) + operational constraints

### G1) Template-only mode forces dict off

TemplateOnly must force dict compression off for value-log writes:
- dictID=0, dict=nil for value-log append path in this mode
- (later, TemplatePrepass can compose; not now)

### G2) templatedb growth and benchmark contamination

Even with GC deferred, enforce:
- `MaxTemplatesPerRun` (bench-only)
- per-bucket max published templates
- optional bench flag to reset templatedb between modes to keep comparisons fair

Benchmark hygiene rule (MUST pick one per benchmark suite):
- Reset: delete templatedb between mode runs; OR
- Warm-up then measure: run a fixed warm-up phase that allows templates to publish, then measure a steady-state phase; OR
- Snapshot templatedb after warm-up and reuse it across modes.

Bench tooling MUST explicitly state which policy it uses to avoid misleading comparisons.

---

## H) Observability (MUST)

Keep template stats separate from dict/zstd stats.

Define `TemplateStats` and report at least:
- attempted (values where candidate lookup performed)
- matched (values that matched at least one template)
- kept (values stored template-encoded)
- bytes_saved_total
- candidate_fp_reads_total
- candidate_templates_considered_total
- template_fetches_total
- templates_published_total

Expose via:
- bench output (deterministic suite)
- `Stats()` map keys with `treedb.cache.vlog_template.*` namespace (cached mode)

### H1) Failure reason taxonomy (MUST)

To make autonomous debugging tractable, encode MUST increment reason counters for non-kept outcomes.
Define a fixed set of reason codes (strings) and report counters per code, e.g.:

- `tmpl_skip_small` (len(value) < K)
- `tmpl_skip_no_fps` (fingerprints empty)
- `tmpl_fp_lookup_err`
- `tmpl_no_candidates`
- `tmpl_template_fetch_err`
- `tmpl_match_fail_expected_savings`
- `tmpl_match_fail_ops_cap`
- `tmpl_match_fail_missing_anchor`
- `tmpl_match_fail_overlap`
- `tmpl_keep_fail_no_savings`
- `tmpl_keep_fail_bounds`

These reason codes MUST be stable and used in tests/benches to avoid “kept=0” mysteries.

---

## I) Tests (MUST)

### I1) Unit tests
- TemplateValue parse/encode roundtrip.
- Corrupt payload: bad varints, bad gapCount, overflow lengths, missing template.
- Candidate list encoding/decoding + eviction determinism.
- Fingerprinting determinism (same input => same fps, tie-break fixed).
- Matching guardrails (ambiguity, ops bounds, expected savings skip).

### I2) Integration tests (restart determinism required)
- Write template-encoded values, close DB, reopen, read back, bytes identical.
- TemplateOnly ensures dictID is 0 and dict path not used.

### I3) Fuzz tests
- Fuzz TemplateValue decoding: must not panic; must cap allocations.

### I4) Golden corpus (deterministic fixtures + expected ranges) (MUST)

Add a small deterministic “golden corpus” used by tests and the deterministic bench harness:

- Workloads:
  - `template_friendly_mid` (fixed prefix/suffix, variable middle)
  - `highly_compressible_tail64` (existing)
  - `incompressible`
- Value size: 1024 bytes
- Seed: fixed (e.g. 1000)
- Records per workload: 10,000

Expected outcomes (ranges, after a warm-up window):
- `template_friendly_mid`:
  - `templates_published_total` in [1, 8]
  - `kept_frac` >= 0.30
  - `observed_ratio` <= 0.98
- `incompressible`:
  - `templates_published_total` in [0, 2]
  - `kept_frac` <= 0.02
- `highly_compressible_tail64`:
  - no strict requirement for template (it may or may not help depending on anchors), but MUST not publish unbounded templates and MUST remain within CPU/ops caps.

The point of the corpus is autonomy: it gives agents a stable target to validate routing/training and avoid regressions.

---

## J) Benchmarks (MUST)

Deterministic suite (CI-friendly):
- Compare: off vs template_fixed (TemplateOnly) vs dict_fixed (separate baseline).
- Include workloads:
  - incompressible
  - template-friendly (fixed prefix/suffix, variable middle)
  - protobuf-ish synthetic (existing)

Report:
- raw/stored ratio
- throughput (simulated)
- template stats (attempted/matched/kept, templates published)

---

## K) Sequential sprints (deliverables + acceptance)

### Sprint T0 — Freeze format + templatedb store + strict decode
- Implement TemplateValue encoding/decoding with magic+ver+flags and safety caps.
- Implement templatedb using TreeDB public DB API only.
- Implement templateID hash assignment and collision rule.
- Implement candidate list storage with deterministic eviction policy.
- Tests: unit + fuzz for decode and candidate list eviction.

Acceptance:
- `go test ./TreeDB/template ./TreeDB/internal/templatedb ...` passes.
- templatedb does not import internal db packages.

PR-sized breakdown (recommended):
- PR-T0a: TemplateValue payload codec + strict decode + fuzz target.
- PR-T0b: TemplateDefBytes serialization + templateID assignment + collision handling.
- PR-T0c: CandidateListBytes tuple format + deterministic eviction + unit tests.
- PR-T0d: templatedb backed by public TreeDB API only + import constraint test.

### Sprint T1 — Fingerprinting + routing + matching (end-to-end)
- Implement fingerprinting and candidate retrieval from templatedb.
- Implement bounded matching with guardrails and keep policy.
- Implement TemplateOnly integration on write + read.
- Add TemplateStats counters and basic reporting.

Acceptance:
- Integration test: write/read (including restart determinism).
- Deterministic bench shows non-zero template kept and savings on template-friendly workload.

PR-sized breakdown (recommended):
- PR-T1a: Fingerprinting implementation + determinism unit tests.
- PR-T1b: Candidate retrieval from templatedb + scoring/tie-break + DB op caps.
- PR-T1c: Matching implementation + guardrails + failure reason counters.
- PR-T1d: TemplateOnly integration seam + restart determinism integration test.

### Sprint T2 — Online training + publish with quality gate
- Add bounded reservoirs + deterministic bucketing.
- Add anchor counting with collision defense and ambiguity checks.
- Add template synthesis + publish quality gate + cooldown.
- Publish templates via one WriteSync batch (def + routing updates).

Acceptance:
- Under template-friendly workload, templates are published and hit rate rises.
- Under incompressible workload, templates published is bounded and kept ~ 0.

PR-sized breakdown (recommended):
- PR-T2a: Reservoir/bucketing + training budgets + unit tests.
- PR-T2b: Anchor counting + collision defense + ambiguity checks.
- PR-T2c: Backbone selection + anchor extension + publish quality gate.
- PR-T2d: Activation fence + atomic publish (template + routing updates in one batch).

### Sprint T3 — Hardening + operational knobs
- Add strict/lenient read mode option handling.
- Add growth caps and bench reset knob.
- Improve observability and debugging hooks (reason codes).

Acceptance:
- Bench output includes all required template stats.
- Fuzz targets stable; no panics; allocations capped.

PR-sized breakdown (recommended):
- PR-T3a: Strict/lenient read option semantics (lenient is error-only, no recover-as-raw by default).
- PR-T3b: Benchmark hygiene (reset/warmup/snapshot) wired and documented in tooling.
- PR-T3c: Expanded observability and reason taxonomy enforcement in benches/tests.

---

## L) Code map (intended files)

- Template codec + fingerprinting + matching:
  - `TreeDB/template/*`
- TreeDB-backed store (public API only):
  - `TreeDB/internal/templatedb/*`
- Cached integration (template-only):
  - `TreeDB/caching/db.go`
- Value-log read path decode:
  - `TreeDB/internal/valuelog/reader.go`
  - `TreeDB/internal/valuelog/manager.go`
- Bench harness:
  - `TreeDB/cmd/unified_bench/*`
  - `TreeDB/caching/vlog_autotune_bench.go`

---

## M) Integration seam map (exact call sites) (MUST)

This section exists to prevent an autonomous agent from integrating at the wrong layer.

Write path seams (cached mode):
- `TreeDB/caching/db.go`:
  - `(*DB).appendValueLog(...)`
  - `(*DB).appendValueLogOne(...)`
  - TemplateOnly must force `dictID=0` here and run template encode on the value bytes before calling the value-log writer.

Template store wiring:
- `TreeDB/public.go`:
  - Open `<root>/templatedb` in cached mode when template is enabled, and call `cached.SetTemplateStore(...)`.

Read path seams:
- `TreeDB/internal/valuelog/manager.go`:
  - `(*Manager).SetTemplateLookup(...)` must propagate to open files.
- `TreeDB/internal/valuelog/reader.go`:
  - `ReadAtWithDict(...)` / `decodeRecord(...)` and streaming `(*Reader).ReadNext()` must detect TemplateValue magic and decode via TemplateLookup.

templatedb seams:
- `TreeDB/internal/templatedb/*`:
  - must use TreeDB public DB methods only and provide: GetTemplateDef, PutTemplateDef, GetCandidates, PutCandidates/update.
