# Old Compression Implementation Review (PR50 / `feature/slab-optimizations`)

This doc is a **planning and extraction runbook**: it reviews the discarded historical compression work from:

- PR50: `feature/slab-optimizations` → `backup/main-pre-safe-candidate-20260116-154607`
- Branch: `feature/slab-optimizations` (87 commits)

…and distills the **key runtime + benchmarking ideas** that are likely useful to **speed up / harden our current ValueLog dict compression implementation** (the PR19+ / PR30+ / PR31+ chain).

Important: we should **not cherry-pick** PR50; the goal here is to **re-implement** the best ideas cleanly in the current architecture.

---

## 0) What PR50 actually implemented (high-level)

PR50’s compression work is primarily **Slab-centric** (slab v2), but it includes several **general-purpose dict-compression ideas** that apply directly to our current ValueLog dict work:

### A) Adaptive compression gating: Pause + Probe + Paused sampling
In `feature/slab-optimizations:TreeDB/slab/manager.go`:

- `shouldAttemptCompression(rawLen)` implements:
  - **Pause** compression for some number of bytes after detecting “not worth it”.
  - While paused, periodically allow a small **probe window** where compression is attempted again (so we don’t stay paused forever if the data stream changes).
  - A separate `shouldCollectPaused()` allows **sampling even while paused** at a stride (so training can restart on a changed stream).

This is materially better than “pause for N bytes and do nothing else”, because it converges fast on incompressible data but also **recovers quickly** when compressibility returns.

### B) Dictionary training: history buffer, validation, fixed-size dict, dedup, anti-thrash
In `feature:TreeDB/internal/compression/trainer.go`:

- Builds a “history” buffer (first `dictBytes` bytes) to avoid degenerate dicts on repetitive inputs.
- Validates dicts (`validateDict`) and retries reduced dict if needed.
- Enforces a fixed dict size invariant (pad/truncate to 40960).
- Maintains dedup + anti-thrash logic:
  - sample-hash based dedup
  - rolling ratio drift checks before accepting new dict/K

We already reimplemented most of this in our current `TreeDB/internal/compression` package.

### C) K-selection and evaluation tooling (offline)
PR50 includes several **analysis tools** under `feature:TreeDB/cmd/`:

- `kv_choosek_bench`: prints a K table using real dataset values + a dict; scores K by bytes saved vs decode cost.
- `kv_dict_batch_bench`: measures encode ns/row and two decode regimes:
  - D1: decode a group then touch 1 row (random-access style)
  - D2: decode a group then touch all rows (scan style)
- `kv_dict_opt_phase1`: explores dict sizes (32KiB → 1MiB) via zstd CLI training, plus “multi-dict routing” experiments and optional template-xor transforms.
- `kv_slab_layout_poc`: explores layout overhead tradeoffs (per-row dict vs block).

These tools are not runtime code, but they encode **useful measurement models**:
- K is a throughput knob that interacts strongly with the **read pattern** (random vs scan).
- Dict size selection can be explored with real data and a cost model (ratio vs decode cost).

### D) CI perf baseline checker (bench gate)
PR50 includes:

- `.github/scripts/check_slab_adaptive_bench.go`
- `.github/perf_baselines/slab_adaptive_defaults.json`

This demonstrates a pragmatic approach:
- run a benchmark suite in CI
- parse results
- warn/error if MB/s or observed_ratio leaves expected bounds

We want an analogous guardrail for our **ValueLog dict benches** to prevent regressions.

### E) Metadata WAL compression (non-dict)
PR50 adds a `WALCompression` option for the metadata WAL.
We don’t currently have an equivalent for the new journal/commitlog path; this is optional, but may be worth revisiting later.

---

## 1) What we already have today (current PR chain)

Already reimplemented (or surpassed) from PR50:

- Dictionary training and acceptance logic (`TreeDB/internal/compression/*`) including:
  - history buffer
  - validate + padding invariant
  - dedup
  - anti-thrash gating
- Dict codec pools and caching:
  - global cache + last-dict fast path (`TreeDB/internal/valuelog/dict_codec_cache.go`)
  - per-writer caching (PR29)
- “Free on incompressible” behavior:
  - dict pause gating (attempted_frac ~ 0 in steady-state)
  - writer-level “no benefit” backoff (skip frames)
- Mode4 ingest throughput improvements:
  - larger frame grouping (PR31: `MaxFrameK=32`)
- Read-path allocation reduction for grouped+compressed random access:
  - pooled decode scratch + copy-out only requested value (PR30)

---

## 2) Key gaps vs PR50 ideas (things we should consider importing)

### Gap 1 — Pause is currently “blind”; add Probe + Paused sampling
Current ValueLog dict pause logic effectively disables compression until pause bytes are consumed.
PR50’s slab implementation is better:
- while paused, periodically probe compression (cheap signal)
- while paused, still sample occasionally (so training can restart early)

Why this matters:
- For mixed streams (incompressible → compressible), we should **recover quickly** without waiting for an entire pause window.

### Gap 2 — K-selection model should include encode cost and K>8
PR50’s tooling makes it explicit:
- encode cost can dominate for compressible data
- decode cost differs by read regime (touch 1 vs touch all)

Our current `ChooseKForDict`:
- evaluates only `k ∈ [1..8]` (historically)
- uses a simplified decode-cost estimate, and does not explicitly model encode cost

Now that `MaxFrameK=32`, we should upgrade K-selection to:
- consider K>8
- incorporate (approximate) encode cost and the expected read regime (mode3 vs mode4)

### Gap 3 — CI “perf gate” for ValueLog dict is still missing
PR50 had a concrete baseline+checker.
We should add a similar guardrail for:
- `unified-bench -suite vlog_dict`
- and/or `go test -bench ValueLogDict*` microbenches

This reduces the chance we regress “dict-on incompressible must be free” or “compressible MB/s must not crater”.

### Gap 4 — Dataset-driven “real values” benchmark harness
PR50 includes several dataset-driven tools that operate on `tmp/treedb_kv_full.jsonl` and dict files.
We should keep our primary CI benches synthetic/deterministic, but add an **opt-in local harness**:
- real structured values (if available)
- report ratio and throughput across:
  - dict sizes (32KiB/64KiB/128KiB)
  - K values (8/16/32)

This gives us confidence our tuning is not overfitting to “repeat tail” patterns.

### Gap 5 — Optional: journal/commitlog compression (metadata)
PR50 had `WALCompression` (zstd) for metadata WAL.
For mode3, we could consider compressing commit records (metadata) if it reduces IO without hurting throughput.
This should be a later PR once dict compression is stable.

---

## 3) Concrete implementation plan (PR sequencing)

This is intentionally written as a **waterfall PR plan** (small PRs, 1–2 changes each), mirroring our current process.

### PR32 — ValueLog pause + probe + paused sampling
Goal: import the best part of PR50’s adaptive gating into the ValueLog dict path.

Deliverables:
- Add `valueLogDictProbeBytes` + `valueLogDictProbeRemaining` (atomics) in cached DB.
- Implement `shouldAttemptValueLogDictCompression(rawBytes)` that mirrors:
  - “paused → consume pause bytes”
  - “while paused → allow periodic probe attempts”
- Modify `valueLogDictCollectSamples` to:
  - still sample occasionally while paused (stride gated)
  - avoid expensive heuristics on every record
- Add a test analogous to `TestCompressionPauseAndProbeResume`:
  - write incompressible payloads until pause triggers
  - then switch to ultra-compressible payloads
  - assert we resume compression (or at least resume *attempts*) without waiting for full pause window

Acceptance:
- `go test ./... -count=1`
- `unified-bench -suite vlog_dict` shows:
  - incompressible: attempted_frac stays ~0 after pause, but probes occur rarely
  - compressible after incompressible: we recover quickly (no long “stuck paused”)

### PR33 — Upgrade `ChooseKForDict`: evaluate K>8 and include encode-cost term
Goal: fix the K-selection model so mode3 picks a reasonable K automatically, and mode4 can keep using MaxFrameK.

Deliverables:
- Update `TreeDB/internal/compression/profile.go`:
  - evaluate candidate Ks up to `valuelog.MaxFrameK` (or a capped subset like {1,2,4,8,16,32})
  - incorporate an encode-cost estimate (even a coarse one) so we don’t pick K that is “ratio good” but “encode too slow”
- Update `applyValueLogDictProfile` policy:
  - mode3 should prefer smaller K when random reads dominate
  - mode4 (disableJournal ingest) can keep using MaxFrameK for throughput

Acceptance:
- microbench: `BenchmarkValueLogDictCompressibilitySweep` improves MB/s at larger K while keeping ratios sane
- microbench: `BenchmarkValueLogDictReadCPU_NoIO` shows the expected read penalty for K=32, and mode3 selection should avoid that if needed

### PR34 — Add an explicit “K cost model” bench command (optional, dev-only)
Goal: codify PR50’s `kv_dict_batch_bench` idea in our current repo but aligned to ValueLog.

Deliverables:
- New cmd (dev-only): `cmd/vlog_dict_kmodel`:
  - load synthetic + optional structured dataset
  - print encode ns/row and decode D1/D2 ns/row for K candidates
  - output markdown table for PR comments

Acceptance:
- tool runs locally and helps justify defaults

### PR35 — CI perf baseline checker for `vlog_dict`
Goal: prevent perf drift.

Deliverables:
- Add `.github/perf_baselines/vlog_dict_defaults.json`
- Add `.github/scripts/check_vlog_dict_bench.go` (adapt PR50’s checker)
- Add/extend GH workflow step that:
  - runs `./bin/unified-bench -suite vlog_dict -dbs treedb ...`
  - saves output
  - runs checker and emits warnings/errors on bounds violations

Acceptance:
- CI catches obvious regressions (e.g., attempted_frac spikes on incompressible; MB/s collapses on compressible)

### PR36 — Dataset-driven dict size exploration (opt-in)
Goal: validate our approach on real values.

Deliverables:
- Add a small script or cmd that:
  - uses `tmp/treedb_kv_full.jsonl` if present
  - benchmarks dict sizes + K values
  - prints ratio and decode cost summary

Acceptance:
- gives us a repeatable, human-run harness for “real data tuning”

### PR37 (optional, later) — Journal/commitlog compression
Goal: evaluate metadata compression for mode3 durability writes.

Deliverables:
- Add a flag/options knob for commitlog/journal compression
- Bench: confirm wall-time and MB/s impact is acceptable

Acceptance:
- only land if it does not regress mode3 throughput materially

### Not planned (yet): multi-dict routing / template transforms
PR50 explored multi-dict routing and template XOR transforms.
These are interesting but high-complexity. Treat as research only until:
- we’ve stabilized single-dict performance + K-selection + guardrails

---

## 4) Summary of what to port first (priority)

1) **Pause+probe+paused sampling** (direct runtime improvement; reduces “stuck paused” risk).
2) **Better K-selection model** (encode+decode aware; now that MaxFrameK=32 exists).
3) **CI perf gate** (prevents drift).
4) **Real-data harness** (confidence and tuning guidance).

