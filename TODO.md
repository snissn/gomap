# TODO / Roadmap (High Priority)

This repo currently exposes two TreeDB entrypoints:

- **Backend DB**: `treedb.OpenBackend(opts)` → the uncached engine (`TreeDB/db`)
- **Cached DB**: `treedb.Open(opts)` / `treedb.OpenCached(opts)` → write-back caching wrapper (`TreeDB/caching`) on top of the backend

The cached layer is **not** a read-cache; it is an LSM-style **write-back** layer:
`Memtable + WAL + background flush → backend`.

Historically, the caching WAL was not replayed on open and there was no cross-process lock (both are now fixed).
This document outlines a coherent future plan to:

1. Provide a single public DB type/API with a runtime mode switch (cached vs backend).
2. Enforce **exclusive open** (single-writer, cross-process).
3. Implement **coherent crash recovery** so the same on-disk state is recovered whether the next opener chooses cached or backend mode.

## Status (2025-12-14)

This roadmap is now largely implemented:

- Public API: `treedb.Open(opts)` returns a single `*treedb.DB`; choose backend-only via `opts.Mode = treedb.ModeBackend` (or `treedb.OpenBackend(opts)`).
- Exclusive open: `treedb.Open*` enforces a cross-process directory lock (`treedb.ErrLocked`).
- Coherent crash recovery: backend open replays any cached WAL segments in `Dir/wal/` into the backend (sync commit) and removes them.
- WAL cleanup: cached flush deletes WAL segments after successful flush so WAL does not grow unbounded.

Remaining future work is mostly optional polish (e.g. persistent checkpoints in meta, “ignore WAL” unsafe mode, and more corruption diagnostics).

## 1) Unify the Public API (One `treedb.DB`)

### Goal
Make backend-vs-cached a user option, not a separate concrete type, while keeping the API ergonomic and obvious.

### Proposed Public Shape
- `treedb.Open(opts)` returns a single public type: `*treedb.DB`.
- `opts` includes a mode flag:
  - `opts.EnableCaching bool` (existing field in `TreeDB/db.Options`) OR
  - `opts.Mode treedb.Mode` where `ModeCached` / `ModeBackend`.

Keep explicit helpers for power users:
- `treedb.OpenCached(opts)`
- `treedb.OpenBackend(opts)`

### Implementation Options
Pick one (both are valid):

1) **Concrete wrapper type (recommended for ergonomics)**
```go
type DB struct {
    cached  *caching.DB
    backend *db.DB
    mode    Mode
}
```
Forward methods to the chosen implementation. This preserves a concrete type for users.

2) **Interface return**
Expose a `treedb.DB` interface and return either backend or cached implementation.
This is flexible, but users lose a concrete type and discoverability suffers.

### Key API Compatibility Considerations
- Ensure iterator and batch APIs feel uniform:
  - `Iterator(start,end)` and `ReverseIterator(start,end)` should behave the same.
  - `NewBatch()` should return a single public batch type (wrapper around cached/backend batch).
- Preserve current semantics where possible, but it’s OK to make modest breaking changes if tests/features are maintained.

## 2) Exclusive Cross-Process Locking

### Goal
Prevent accidental concurrent opens across processes (cached and backend) and eliminate “open races”.

### Requirements
- Lock is acquired at the beginning of `treedb.Open*`.
- Lock is exclusive (writer lock); future read-only/shared modes can be considered later.
- Lock is released on `Close()` and on abnormal process termination (OS releases).

### Suggested Mechanism
Create a dedicated lock file, e.g.:
- `Dir/LOCK` or `Dir/index.db.lock`

Implement cross-platform locking via `golang.org/x/sys`:
- Unix: `flock` on the lock file fd
- Windows: `LockFileEx` / `UnlockFileEx`

### Behavior
- If already locked, `Open` returns a clear error (e.g. `ErrLocked`) including PID info if available.
- Lock should be held for the full lifetime of the DB handle (including cached wrapper).

## 3) Coherent Crash Recovery (Backend + Cached WAL Replay)

### Previous Problem
Cached TreeDB writes go to:
- caching WAL (buffered) + memtable (RAM)
- flushed later to backend

If the process crashes:
- backend contains only the last flushed state,
- WAL files may exist, but were previously not replayed,
- so reopening as cached vs backend yields the **same** (backend-only) recovered state, losing unflushed updates.

### Goal
Make crash recovery deterministic and consistent:
After a crash, *any* opener (cached or backend) should recover the same state.

### High-Level Model
On-disk durability becomes:
- **Backend data** (meta + pages + slabs)
- **Cache WAL segments** (authoritative for writes not yet applied to backend)

Recovery pipeline (for both modes):
1. Acquire exclusive lock.
2. Recover backend meta/pages/slabs (existing `db.recover()`).
3. Discover cache WAL segments.
4. Replay WAL records into the backend in a bounded, idempotent way.
5. Persist a “WAL checkpoint” to backend meta.
6. Retire/delete WAL segments that are fully checkpointed.
7. Start normal operation (cached or backend mode).

### WAL Checkpointing (Avoid Replaying Forever)
Replaying WAL on every open is logically correct, but can cause disk growth and repeated work.
We need a durable checkpoint in backend meta, such as:
- `AppliedWALSegment uint64`
- `AppliedWALOffset  uint64` (optional; per-segment offset)

Rules:
- Only delete/rotate WAL segments once the backend commit that includes them is durable.
- Recovery replays only segments after the checkpoint.
- Replay must be **idempotent** (safe if run twice).

### Replay Granularity
Two reasonable approaches:

1) **Segment = memtable generation**
- Cached layer rotates WAL when rotating memtable.
- Recovery replays complete segments into backend, then checkpoints at segment boundaries.
- Simpler bookkeeping.

2) **Segment + offset**
- Supports mid-segment checkpointing (more complex).
- Needed if segments can become huge or if you want frequent checkpoints without rotation.

### Record Durability Semantics
Align with the existing contract:
- `Set` / `Batch.Write` (non-sync): may not survive crash; recovery replays only what made it to disk.
- `SetSync` / `Batch.WriteSync`: must survive crash; implementation must `fsync` the WAL (and/or backend commit) accordingly.

That means:
- Cached `SetSync` must ensure WAL buffers are flushed + fsynced.
- Cached `Batch.WriteSync` must ensure WAL durability for operations that are only in cache.

### Corruption / Partial Records
WAL records have CRCs. Recovery should:
- Stop at first truncated/invalid record at end-of-file (treat as clean truncation).
- For mid-file corruption, fail open with a clear error unless an explicit “ignore WAL” option is set.

### Safety Rule (Avoid Mixed Opens)
If cache WAL segments exist past the checkpoint:
- backend-only open should still run the same replay, or
- backend-only open should refuse unless `opts.IgnoreWAL` is set.

Recommendation: **always replay** in the shared recovery path so backend/cached open are consistent.

## 4) Spec Tests (North Star)

Add tests that lock in the contract and prevent regressions:

### A) “Crash then reopen” consistency test
Simulate:
1. Open cached.
2. Perform a set of `SetSync` operations (ensure WAL is durable).
3. Do **not** flush to backend (force them to remain only in WAL/memtable).
4. Simulate crash by closing without flushing memtables (test hook) or by directly writing WAL files and skipping cached state.
5. Reopen backend (or unified open in backend mode) and assert keys exist.
6. Reopen cached and assert the same state.

Notes:
- Tests can’t actually SIGKILL the process, but can simulate by:
  - writing WAL files directly using the WAL writer,
  - or adding an internal test-only hook that closes without flushing.

### B) Idempotent replay test
- Run recovery twice on the same WAL segments; results must be identical and not duplicate data or corrupt meta.

### C) Truncated record test
- Write a WAL file with a valid prefix then truncate the last record; recovery must succeed and replay the valid prefix only.

### D) Locking test
- Attempt to open the same dir twice; second must fail with `ErrLocked`.

## 5) Implementation Milestones (Suggested Order)

1. **Locking** (done)
   - Add lock file type + acquire/release in `treedb.Open*`.
2. **Refactor Open path** (done)
   - Route all opens through a shared recovery pipeline.
3. **WAL segment discovery** (done)
   - Enumerate `Dir/wal/` files, parse sequence numbers, sort.
4. **Replay engine + cleanup** (done)
   - Apply WAL ops to backend via backend batches (`WriteSync`) and remove WAL segments on success.
   - Retire/delete old WAL segments after successful flush (removes the old “delete old WAL file” TODO in the cached writer path).
5. **Mode selection** (done)
   - Return `*treedb.DB` wrapper that chooses cached vs backend at runtime.
6. **Spec tests** (done)
   - Crash recovery + truncated WAL handling are covered by tests.
7. **Optional: checkpoint persistence**
   - If we ever need to keep WAL segments around for debugging/retention, extend meta format (versioned) to store an applied checkpoint.

## 6) Open Questions (Worth Deciding Early)

- Where to store WAL checkpoint in meta (meta versioning / backward compatibility)?
- Should backend-only mode create/use a WAL at all, or only replay and then operate without WAL?
- Should “ignore WAL” be an explicit unsafe option?
- Do we want a “read-only open” mode (shared lock) later?

## 7) CI & Tooling (Quality Gates)

- Add `go vet` to CI in all three contexts: `./`, `TreeDB/`, and `cmd/unified_bench/`.
- Run CI on an OS matrix (Linux/macOS/Windows) since mmap/locking/memlock paths differ.
- Optional: add a `gofmt` check step (fail if `gofmt` would change tracked files).
- Optional: add `go test -race` on Linux (at least for `TreeDB/...` and `HashDB/...`) to catch iterator/flush concurrency issues.

## 8) Benchmark Reproducibility

- Add `-seed` to `cmd/unified_bench`:
  - Deterministic key generation when set.
  - Print the seed in the output header so results are reproducible.
  - Document how it interacts with `-tests` and keyspace offsets.

## 9) HashDB Follow-ups (from `docs/DEV_NOTES.md`)

- Explore low/zero-copy slab reads for read-heavy workloads (mmap slab or shard-local reusable buffers; be careful with slab growth/rotation and caller lifetimes).
- Deepen `GetMany` by batching slab reads per shard (Linux experiments: `preadv` / io_uring).
- Compaction correctness + performance for segmented slab design (make it correct first, then tune).
- Compression policy tuning (thresholds, codecs, defaults; document recommendations).
- Optional stricter durability: a small WAL with configurable fsync policies (keep slab log as primary recovery).

---

# V1 Milestone: “Wow” Documentation

Goal: ship documentation that makes a new developer productive in ~15 minutes, and explains
the major design tradeoffs (esp. TreeDB cached vs backend) with enough clarity to choose
the right engine and tuning knobs.

## A) Top-Level Project Docs

### 1) Root `README.md` (the “front door”)
Must answer (with short examples):
- What is this repo? (HashDB + TreeDB + BTreeOnHashDB + benchmark tooling)
- What’s stable vs experimental?
- Quickstart: `make test`, `make build`, run `cmd/unified_bench`
- “Choose your engine” table: HashDB vs TreeDB vs TreeDBBackend vs BTreeOnHashDB
- Links to deeper docs (architecture, tuning, benchmarks, troubleshooting)

### 2) “Getting Started” guide
Include:
- Requirements: Go version, OS notes (mmap/mlock behavior), ulimit hints
- Local dev workflow (Makefile targets, CI expectations)
- Minimal code examples for:
  - HashDB: open/put/get/delete/close
  - TreeDB: open/set/get/iterator/batch

### 3) “Repo Map” / architecture overview
One page explaining:
- Directory layout and major packages
- How `cmd/unified_bench` is structured and what each test means
- Where specs live (e.g. `TreeDB/specs/`)

## B) TreeDB Docs (Highest Priority)

### 1) “TreeDB Concepts” (backend engine)
Explain clearly:
- Page/Node model, meta pages, slabs, zipper writes (high-level)
- Iterator semantics (`Iterator` domain is `[start,end)`; `UnsafeKey/UnsafeValue` caveats)
- Durability semantics:
  - `Set` vs `SetSync`, `Batch.Write` vs `Batch.WriteSync`
- Concurrency model (single-writer, multi-reader; future cross-process lock work)
- On-disk layout:
  - `index.db`, slab files, and what data lives where

### 2) “Cached vs Backend: Which to use?” (decision doc)
This must be explicit and benchmark-informed.

Include a simple decision table and *why*:
- Use **cached TreeDB** when:
  - You want fast small writes (`Set`) and strong random read throughput (`Get`)
  - You can tolerate write-back behavior (data may be in WAL/memtable until flush)
  - You’re doing mixed workloads (read/write) and want predictable latency
- Use **backend TreeDB** when:
  - You mostly write in large batches and/or care about max scan throughput
  - You want the simplest on-disk read path (no merge across in-memory layers)
  - You want a lower-overhead iterator for heavy full scans / “prefix scans”

Call out the observed benchmark patterns (typical, not guaranteed):
- Backend is **much slower** for per-key sequential writes (because each `Set` is a full commit).
- Backend is often **faster** for:
  - large `Batch.Write*` runs (more efficient amortization),
  - full scans,
  - prefix/range scans (key-only iteration).

Also include:
- How to force “scan speed mode” in cached TreeDB (e.g. flush before scan) if applicable.
- Correctness caveats today:
  - caching WAL is replayed on open (coherent recovery across cached/backend),
  - multiple processes are unsafe without exclusive locking.

### 3) “Performance + Tuning” guide
Document the knobs and practical guidance:
- `ChunkSize` tradeoffs (mmap granularity vs address space / page faults)
- `FlushThreshold` tradeoffs (write buffering vs scan overhead)
- Inline threshold / slab behavior
- Iterator patterns (key-only scans should avoid calling `Value()` unless needed)

### 4) “Operations / Troubleshooting”
Include:
- Common failure modes (checksum errors, slab read errors)
- How to run verifier (`TreeDB/cmd/verify`)
- How to interpret `Stats()`
- Crash safety notes (current vs planned behavior)

## C) HashDB Docs

### 1) “HashDB Concepts”
- What HashDB is (mmap’d hash table with SwissHash control bytes)
- On-disk layout (control bytes file vs key/index file; cleanup/resizing behavior)
- Memory policy:
  - what `IndexMemoryPolicy` does (mlock best-effort, madvise best-effort)
  - OS constraints (`ulimit -l`, Windows lock limits)
- “Redis-equivalent server” tooling:
  - what `HashDB/redisserver` is for (performance testing + future development)
  - how it uses HashDB + Badger (what data is in which layer)

### 2) “Performance + Tuning”
- Sharding defaults and tradeoffs
- Capacity/load factor guidance
- When to pick HashDB vs TreeDB

## D) Benchmarking Docs (“Make numbers trustworthy”)

### 1) Unified bench reference
Document every benchmark:
- `Sequential Write`, `Random Write`, `Batch Write`, `Random Read`, `Random Delete`
- `Full Scan` (iterate all keys/values)
- `Prefix Scan` (range scan over contiguous keyspace; key-only iteration)

Clarify:
- What keys each test writes (some tests write to an offset keyspace)
- How results are computed (items/sec), and which tests are key-only vs key+value
- How to run: flags, examples, what “-” means in the table

### 2) Reproducibility / methodology
- Fixed seed option (add one if needed)
- Warmup guidance
- Notes about OS page cache, SSD vs RAM, and how it affects scans

## E) API Reference + Examples (“GoDoc-level polish”)

Requirements:
- Public packages have GoDoc comments on:
  - exported types, options, and key methods
- Provide runnable examples (Go `Example*` tests) for:
  - `hashdb.Open`, `Put`, `Get`, `Close`
  - `treedb.Open`, `SetSync`, `Iterator`, `NewBatch`

## F) Documentation Packaging

Deliverables:
- A `docs/` index (`docs/README.md`) that links everything
- Simple diagrams (ASCII or embedded images) for:
  - TreeDB write path (cached vs backend)
  - TreeDB read/scan path
  - HashDB file layout

Quality bar:
- Every doc page has “Who is this for?” and a short “TL;DR”.
- Every major component has at least one minimal copy-pastable code example.

## G) Documentation Debt / Legacy Cleanup

- Audit and reconcile older docs that still use legacy naming (“gomap”, “Gomap”, old benchmark names):
  - Update to current naming (`HashDB`, `TreeDB`, `BTreeOnHashDB`, “Full Scan” / “Prefix Scan”) where accurate, or
  - Move to a clearly marked `docs/legacy/` folder if they’re historical.
- Identify Gemini-related helper scripts/plans and either:
  - move to `docs/legacy/`, or
  - add to `TENTATIVE_DELETIONS.md` if genuinely obsolete (do not delete until confirmed).

---

# Milestone: Raft-backed Database (“RaftDB”)

Goal: build a consensus-based “database” on top of `TreeDB` and/or `HashDB` (Raft log + replicated state machine),
with a stable surface that future projects can depend on confidently.

## A) In-repo Prerequisites (Storage Contracts)

These are required before a Raft layer can safely rely on this repo:

- **Coherent durability + recovery**
  - TreeDB must have a single recovery story (see sections 1–5 above) so crash recovery does not depend on whether the next opener chooses cached vs backend mode.
- **Exclusive cross-process locking**
  - `treedb.Open*` must enforce exclusive open; the Raft layer should never “accidentally” open the same dir from multiple processes.
- **Clear durability API contract**
  - Document exactly what is durable after each call (`Set`, `SetSync`, `Batch.Write`, `Batch.WriteSync`) and what recovery guarantees exist.
  - Avoid “silent best-effort durability” for anything the Raft layer would treat as committed.
- **Atomic batch apply**
  - A Raft state machine apply needs an efficient `ApplyBatch` style path that is atomic and durable when requested.
- **Snapshot support (consistent read view)**
  - Need a way to create a consistent snapshot for:
    - state machine snapshotting (iterate all keys deterministically), and
    - restoring from snapshot.
  - This can be: point-in-time iterator semantics, a `View`/read-only transaction, or explicit “flush then iterate” with documented tradeoffs.
- **Deterministic iteration**
  - Snapshotting requires stable ordering and clearly defined iterator bounds/semantics.
- **Concurrency model documented**
  - Raft will have concurrent goroutines (apply loop, reads, snapshot creation, compaction); DB types must clearly state what’s safe concurrently.

## B) “Stable Surface” Requirements (API + Versioning)

- Decide what packages are “stable” for dependents:
  - likely `github.com/snissn/gomap/TreeDB` (public `treedb` API) and `github.com/snissn/gomap/HashDB` (`hashdb`).
- Keep internals internal:
  - move implementation details behind `internal/` where possible; avoid exporting types that aren’t part of the intended stable API.
- Add typed errors and invariants where helpful (e.g. `ErrLocked`, `ErrClosed`, `ErrCorrupt`).
- Consider a “compat/stability” doc:
  - what can change freely vs what is intended to be stable.
  - later: SemVer tags once the surface stabilizes.

## C) Raft Storage Requirements (Design Targets)

At minimum, a Raft-backed DB needs:
- **Log store**: append entries, read by index, truncate suffix/prefix, and compact.
- **Stable store**: persist current term, voted-for, and cluster config.
- **State machine store**: apply committed commands (KV mutations) atomically and snapshot/restore.

Open questions to decide early:
- Which Raft implementation to build around (`hashicorp/raft`, `etcd/raft`, or a minimal custom layer)?
- Which engine stores the Raft log vs the user KV state (TreeDB backend vs cached vs HashDB), and why?
- How to encode commands and schema/version them.

## D) Test Plan (North Star)

- **Crash tests**: simulate power-loss at key phases; assert committed entries survive and uncommitted do not “appear”.
- **Replay idempotence**: WAL/log replay can run twice without corruption.
- **Linearizability / ordering**: state machine applies in commit order; reads are consistent with the chosen read mode (leader lease vs read-index vs local).
- **Snapshot/restore correctness**: snapshot then restore yields identical keyspace.
- **Race tests**: run `-race` in CI for core packages.

## E) Documentation Requirements (Go Best Practices)

Goal: make it hard to misuse the storage engines when building Raft on top.

- **GoDoc-first API docs**
  - Exported packages/types/funcs/methods should have doc comments.
  - Not every private helper needs comments; prefer clear naming + comments for non-obvious invariants.
- **Package docs (`doc.go`)**
  - Each public package should have a top-level comment describing purpose, safety/durability caveats, and a minimal example.
- **Runnable examples**
  - Add `Example*` tests that appear in `go doc`/pkgsite and are executed by `go test`.
- **Design docs**
  - Add a `docs/raft/` section later: log format, snapshot model, recovery pipeline, and operational guidance.

## F) Handoff Checklist (“Future Projects Can Adopt This”)

Goal: a new engineer (or a downstream repo) can depend on this repo without tribal knowledge.

- **Stable API surface explicitly defined**
  - Create `docs/API_STABILITY.md`:
    - list the supported public packages (e.g. `treedb`, `hashdb`) and the expected stability level,
    - state what can change (internals, benchmark tooling) vs what should be compatible.
  - Prefer small, purpose-built exported APIs; move everything else behind `internal/`.
- **GoDoc coverage**
  - Every exported identifier that is part of the stable surface has a GoDoc comment explaining semantics (durability, concurrency, error behavior).
  - Package-level docs (`doc.go`) explain the “happy path” plus caveats.
  - Add runnable examples (`Example*`) for the primary entrypoints and common patterns.
- **Specs + invariants live with code**
  - Key behavioral contracts are pinned by tests (crash recovery, iterator semantics, batch atomicity).
  - Add “spec tests” that read like documentation and prevent regressions.
- **Developer onboarding**
  - Add `docs/README.md` as an index (“start here”).
  - Add a “repo map” page + architecture overview (where to look for what).
  - Add `CONTRIBUTING.md` with: style, testing commands, benchmarks methodology, and how to change on-disk formats safely.
- **Release / change communication**
  - Add `CHANGELOG.md` and document how breaking changes are handled.
  - Once the stable surface is ready, tag releases (SemVer) so downstream repos can pin.
