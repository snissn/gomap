# AGENTS: Slab RC Addendum (Work Log + Status)

This document tracks the RC addendum work for slab dictionary compression.
Focus: correctness + memory safety + RC-grade runtime behavior, aligned with
adaptive training + K selection.

Format:
- Each sprint has: goal, rationale, scope, tests, and status log.
- Update status as work progresses.

---

## Sprint 1: Read-Path Safety + Dict Length Correctness

**Goal**
Ensure V2 slabs remain readable even when no global dict exists, and avoid
implicit assumptions about dictionary length.

**Rationale**
- Current read path can error for Zone 1+ if global dict is missing.
- DictLength is written but ignored; this can break CRC validation or future
  behavior if dicts are not always 32KB.

**Scope**
- Add a regression test that reproduces the Zone 1+ failure when no global dict
  exists (Zone 0 fallback behavior vs Zone 1 behavior).
- Fix read path to fall back to raw ZSTD when global dict is absent.
- Align DictLength usage: either honor DictLength on read or enforce a fixed
  padded length consistently (document which path we choose).

**Tests**
- New test: "V2 read fallback when no global dict" (Zone 1+).
- New/updated test: "Zone header DictLength respected" (if length becomes
  variable) or "DictLength always padded to GlobalDictSize".

**Status Log**
- [x] completed: added regression tests for zone1 fallback and short local dicts; implemented fallback + dict padding fixes.

---

## Sprint 2: USE_REF + Dictionary Dedup Wiring

**Goal**
Wire dict dedup results into zone header emission and implement USE_REF read
path so repeated dictionaries don't cost 32KB per zone.

**Rationale**
- The trainer tracks dedup, but the write path does not emit USE_REF/USE_GLOBAL.
- This wastes disk and write I/O on homogeneous datasets.

**Scope**
- Extend zone header emission to choose among USE_GLOBAL / USE_REF / USE_LOCAL
  based on dedup results.
- Implement USE_REF read path (load referenced dict by zone ID or dict ID
  mapping).
- Define/implement the "ref index" semantics in the zone header (zone ID or
  dictionary index).

**Tests**
- New test: "Dedup emits USE_REF".
- New test: "USE_REF read reuses dict and decodes correctly".
- Update integrity tests for CRC validation on referenced dicts.

**Status Log**
- [x] completed: wired USE_REF emission + read path; added ref header and dedup tests.

---

## Sprint 3: Bounded Decoder Cache (LRU)

**Goal**
Prevent unbounded decoder pool growth for local/ref dicts.

**Rationale**
- Per-zone pools can grow without bound in long-lived processes.
- An LRU cache preserves hot zones while bounding memory.

**Scope**
- Implement a global LRU cache for local/ref dict decoders (per slab + zone or
  per dict hash).
- Keep the per-slab global decoder pool as-is.
- Define cache size tuning knobs (default conservative).

**Tests**
- New test: "LRU eviction frees older zone decoders".
- New test: "LRU reuse keeps hot zones fast".

**Status Log**
- [x] completed: added global LRU for local/ref decoders; tests cover eviction behavior.

---

## Sprint 4: Zero-Copy Dict Slices (mmap-backed)

**Goal**
Avoid heap copies when loading dictionaries for local/ref zones.

**Rationale**
- Dict copies create GC pressure and waste memory bandwidth.
- The plan explicitly wants mmap-backed slices with slab refcounting for safety.

**Scope**
- Load dicts as sub-slices of slab mmap data (or a stable read-only mapping).
- Ensure slab refcounting keeps mappings alive while any decoder uses them.
- Validate no use-after-free during slab close or remap.

**Tests**
- New test: "Zero-copy dict slice uses mmap backing" (sanity check).
- Existing read-path tests should still pass.

**Status Log**
- [x] completed: zero-copy dict slices via mmap when available; added mmap-backed dict test and cache purge on slab close.

---

## Sprint 5: Minimal Plan Alignment (Doc)

**Goal**
Reconcile the spec with the current adaptive training + K selection approach.

**Rationale**
- The current optimization plan does not describe adaptive profile selection.
- This can lead to rework or regressions if the plan is treated as canonical.

**Scope**
- Add a short section to the plan describing adaptive training + K selection as
  the active policy for dict selection.
- Clarify that "global dict trained at slab start" is superseded by the
  "active profile" model.

**Tests**
- None (doc-only).

**Status Log**
- [x] completed: documented adaptive profile + K selection policy in `TreeDB/local_dictionary_compression.md`.

---

## Sprint 6: Documentation + DX / Public API Review (Stub)

**Goal**
Plan and execute a documentation + DX overhaul for slab compression and
public-facing configuration.

**Scope (initial)**
- Gather user-facing docs to update (README, config docs, env var reference).
- Review API ergonomics for slab compression options.
- Define a concise "how to use" + troubleshooting guide.

**Status Log**
- [x] in_progress: drafted value-size limitation + remediation notes; identified docs to update (README, TREEDB_TUNING, TREEDB_BENCHMARKING).
- [x] in_progress: audited public envs/options; updated tuning + benchmarking docs with env overrides, legacy/deprecation notes, and bench-only knobs; updated TreeDB README note.
- [x] in_progress: added value-log and durability/integrity option guidance to tuning docs.
- [x] in_progress: clarified `treedb.Open` env override behavior in API docs and getting-started guide.
- [x] in_progress: added compressed profile variants (durable/fast) and updated profile docs.

### Sprint 6 Plan (DX / Public API)

**Track 1: User-facing docs cleanup**
- Create a single "TreeDB configuration quickstart" section (README or `docs/README.md`).
- Consolidate slab compression docs into one canonical page (link from README/TUNING).
- Add a "known limits" section (V2 value cap, format stability).

**Track 2: Public API audit**
- Review `treedb.Options` defaults for consistency and clarity.
- Document env var overrides (which flags affect which options).
- Identify confusing flags or obsolete fields; propose renames/aliases if needed.
- Audit compression modes and legacy toggles (e.g., non-dictionary compression).
- Audit all public configs/envs for legacy/deprecated options (not just compression).
- Propose deprecations/removals with migration guidance.

**Track 3: Error messaging / troubleshooting**
- Add a compact "common errors" section with cause + remediation.
- Ensure errors like `ErrRecordTooLarge` mention the V2 cap.
- Document recovery actions for WAL/vlog/slab errors (link to existing docs).

**Track 4: Example configs / recipes**
- Provide 3–4 copy/paste configs: cached default, backend-only, slab-heavy (force pointers + compression), and large-batch ingest.
- Include benchmark-friendly env presets (trace replay settings).
- Link to Celestia run notes if available.

---

## Regression: Trace Replay "record too large" (V2 batch boundary)

**Goal**
Prevent batch flushes from constructing buffers larger than a V2 zone, which
causes `record too large` during trace replay with slab compression enabled.

**Status Log**
- [x] completed: guard AppendMany flushes against V2 zone boundaries; added regression test; trace replay now passes with slab compression enabled.
