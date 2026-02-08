# HashDB Port Plan (Rust-first, no backward compatibility)

This agent file outlines a practical, end-to-end implementation plan for a clean Rust port of HashDB.
It is written as guidance for contributors and automation agents; it does not need to be buildable
as-is. The focus is on sequencing, artifacts, and validation checkpoints.

## Goals

- Rust-first implementation with a simplified, explicit on-disk format (v2).
- Keep scope to HashDB (hash index + slab value log + recovery + batch atomicity).
- Use Go as a reference when useful, but do not require binary compatibility.
- Provide a clear, test-driven, milestone-based path to parity.

## Non-Goals

- TreeDB changes.
- Performance tuning before correctness.
- Backward compatibility with existing on-disk data.
- Shipping a production-ready WAL/GC system in the first milestone.

---

## Phase 0: Format Spec + Fixtures

### 0.1 Draft the v2 On-Disk Format

Define a clear, implementation-friendly spec. Recommended primitives:

- **Segment header** (fixed-size, aligned):
  - magic, version, flags
  - segment id
  - record count (optional, if cheap)
  - checksum of header

- **Record format** (append-only):
  - record header: key_len, val_len, flags, checksum
  - payload: key bytes, value bytes

- **Control record** (batch begin/commit):
  - distinct record type
  - batch id

- **Metadata file** (fixed-size block):
  - capacity, key count
  - active segment id
  - active segment size
  - clean/dirty marker

**Notes:**
- Use explicit little-endian encoding everywhere.
- Avoid “raw struct via mmap”; always encode/decode.
- Decide whether checksums are per record or per block; per record is simpler.

### 0.2 Create Test Vectors

- Generate a set of “golden” files by writing a tiny Go program or a rust script.
- Include edge cases: empty DB, tombstones, batch begin/commit, torn tails.
- Check these vectors into `HashDB/artifacts/` or `artifacts/` with a short README.

### 0.3 Document Recovery Rules

- Define exact behavior for torn records and partial batches.
- Decide “truncate on first corrupt record” vs “scan and skip”.
- Write this explicitly in the spec.

---

## Phase 1: Rust Read-Only Engine

### 1.1 Layout the Rust Crate

- `crates/hashdb/` (or `rust/hashdb/`)
- Modules: `format`, `segment`, `index`, `db`, `recovery`, `batch`, `mmap`.

### 1.2 Implement Read Path

- Implement `open()` to:
  - open metadata
  - open active segment
  - open index (if present) or rebuild index from log
- Implement `get(key)` with:
  - hashing
  - SwissTable-like probe (or another hash index if v2 allows)
  - value fetch from slab

### 1.3 Recovery Scanner

- Implement a streaming log scanner that:
  - validates headers and checksums
  - applies put/delete into the in-memory index
  - respects batch begin/commit semantics
  - truncates torn tails

**Checkpoint:**
- Read-only open + get works against test vectors.

---

## Phase 2: Rust Write Path + Batch Atomicity

### 2.1 Write-Ahead Log (slab) Append

- Implement `put(key, value)` and `delete(key)` to append to slab.
- Update in-memory index and metadata counters.
- Ensure active segment rotation works.

### 2.2 Batch Begin/Commit

- Implement batch control records in the log.
- Recovery must replay only fully-committed batches.

**Checkpoint:**
- Functional tests: put/get/delete/batch round trips, crash recovery.

---

## Phase 3: Index Persistence (Optional)

Decide if the index is rebuild-only or persisted for faster open.

- Option A: Keep index as a cache-only artifact; rebuild on open.
- Option B: Persist index mmap with explicit encoding and versioned header.

If choosing B:
- Define encoding for index entries.
- Validate alignment/padding explicitly.
- Never rely on raw struct overlays.

---

## Phase 4: Cache + WAL (Optional)

If you need the HashDB cache semantics (write-back cache + optional WAL):

- Port `CacheKV` behavior.
- Implement cache WAL with simple record format.
- Add tests for crash recovery.

---

## Phase 5: Parity Tests + Benchmarks

### 5.1 Conformance Tests

- Define a shared workload spec (ops + seed + dataset).
- Run on both Go and Rust and compare:
  - final key/value map
  - log integrity
  - recovery behavior

### 5.2 Bench Suite

- Wire into existing `unified_bench` or add a Rust bench harness.
- Track regressions but do not block functional milestones.

---

## Implementation Notes (Rust)

- Expect some localized `unsafe` for mmap and unaligned loads.
- Keep `unsafe` in dedicated modules (`mmap.rs`, `layout.rs`).
- Prefer `bytemuck`-style explicit packing over raw struct overlays.
- Use `memmap2` or equivalent; gate OS-specific advice behind feature flags.

---

## Suggested Milestones

1. **Spec + vectors** (v2 format doc + golden files)
2. **Rust read-only** (open + get + recovery)
3. **Rust write path** (put/delete + batch)
4. **Optional persisted index** (if needed)
5. **Optional cache WAL** (if needed)
6. **Parity + benchmarks**

---

## Quick Checklist for Agents

- [ ] Spec is written and versioned
- [ ] Recovery rules are explicit and tested
- [ ] No raw struct overlays in persistent storage
- [ ] Crash tests cover torn records and partial batches
- [ ] Benchmarks run but do not gate correctness

## Benchmark Profiling Tooling (Go Harness)

For HashDB performance work in this repo, use the shared Go benchmark harness
and profile analyzer:

- Capture with `unified-bench` using `-profile-dir`:
  - `OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)`
  - `./bin/unified-bench ... -profile-dir "$OUT"`
- Analyze with `benchprof`:
  - `./bin/benchprof -profiles-dir "$OUT"`

If benchmark test names, profile filenames, or `-profile-dir` defaults change,
update parser/tests/docs in the same PR:

- `cmd/benchprof/main.go` and `cmd/benchprof/main_test.go`
- `cmd/unified_bench/profile_artifact_dir_test.go`
- `cmd/unified_bench/README.md` and `cmd/benchprof/README.md`
