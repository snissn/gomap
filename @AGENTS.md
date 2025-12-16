# Repo Agent Log (Go Cleanup)

## Goals

- [x] Make `go test ./...` pass in `./`, `TreeDB/`, and `cmd/unified_bench/`.
- [x] Rename `GeminiTreeDB` → `TreeDB` and remove `gemini` references from Go code.
- [x] Fix `cmd/unified_bench` to match current DB(s) (legacy DB already removed).
- [x] Move `gomap` → `HashDB/` directory and rename to HashDB.
- [x] Rename `btree` → `BTreeOnHashDB`.
- [x] Consolidate to a single root `go.mod` and keep CI/Makefile aligned.
- [x] Apply Go best practices (gofmt, clearer naming, simpler APIs) without breaking tests/features.
- [ ] Track any “safe to delete” candidates in `TENTATIVE_DELETIONS.md` (do not delete unless clearly unneeded).
- [x] TreeDB: unify open + coherent crash recovery (cached WAL replay + cleanup; spec tests added).
- [ ] V1 milestone: “Wow” documentation (see `TODO.md`).
- [ ] Milestone: downstream-ready storage primitives (stable surface + contracts; see `TODO.md`).
- [x] HashDB: add exclusive open lock + tests.

## Progress Log

- 2025-12-14: Baseline tests
  - `go test ./...` (root): PASS
  - `go test ./...` (`GeminiTreeDB`): FAIL (memtable test compile error)
  - `go test ./...` (`cmd/unified_bench`): FAIL (expects `../../TreeDB` module path; directory missing)
- 2025-12-14: Housekeeping
  - Added `TENTATIVE_DELETIONS.md`
  - Stopped tracking `.DS_Store` and added it to `.gitignore`
- 2025-12-14: TreeDB rename + tests
  - Renamed `GeminiTreeDB/` → `TreeDB/` and updated module/import paths
  - `go test ./...` (`TreeDB`): PASS
- 2025-12-14: unified_bench cleanup
  - Removed legacy/gemini DB references; now benchmarks `treedb` and `treedbcached`
  - `go test ./...` (`cmd/unified_bench`): PASS
- 2025-12-14: Repo formatting + artifacts
  - Ran `gofmt` across the repo
  - Removed checked-in build artifact `cmd/unified_bench/benchmarker` (now gitignored)
- 2025-12-14: CI
  - Added GitHub Actions workflow to run `go test ./...` in `./`, `TreeDB/`, and `cmd/unified_bench/`
- 2025-12-14: HashDB rename (in progress)
  - Renamed root module/package `gomap` → `HashDB` (`github.com/snissn/gomap/HashDB`, package `hashdb`)
  - Updated internal imports and `cmd/unified_bench` to build against the new module path
  - `go test ./...` (`HashDB/`): PASS
- 2025-12-14: BTreeOnHashDB rename
  - Renamed `btree/` → `BTreeOnHashDB/` (package `btreeonhashdb`)
  - Updated `cmd/unified_bench` import to `github.com/snissn/gomap/HashDB/BTreeOnHashDB`
- 2025-12-14: HashDB directory move
  - Moved the HashDB module into `HashDB/` (including `benchmark/`, `redisserver/`, `stress/`, and cmd tools)
  - Updated CI workflow + `cmd/unified_bench` replace paths for the new layout
  - `go test ./...` passes in `HashDB/`, `TreeDB/`, and `cmd/unified_bench/`
- 2025-12-14: Build artifacts cleanup
  - Removed checked-in binaries/profiles in `HashDB/BTreeOnHashDB/` and `HashDB/redisserver/` and added ignores
- 2025-12-14: Repo layout cleanup
  - Moved top-level specs/notes into `docs/`
  - Moved loose benchmark outputs into `artifacts/`
  - Moved one-off scripts into `scripts/`
- 2025-12-14: Multi-module ergonomics
  - Added `go.work` for local multi-module development
  - Rewrote `Makefile` with `help`, `fmt`, `test`, `vet`, `tidy`, `deps`, and `build` targets across modules
- 2025-12-14: Single-module workspace
  - Removed `go.work` and per-directory `go.mod` files; added root `go.mod`
  - Updated CI + `Makefile` to reflect the single-module layout
- 2025-12-14: HashDB refactor
  - Renamed core types: `Hashmap` → `DB`, `HashmapDistributed` → `ShardedDB`, `CachedHashmap` → `CachedDB`
  - Introduced `Open`/`Put` APIs (kept compatibility wrappers), added proper `Close`, and unexported DB internals
  - Renamed HashDB Redis backend `gomapredis` → `hashdbredis`; stress tests now run `redisserver` in `hashdb` mode
  - Updated `cmd/unified_bench` engines to prefer `hashdb` (keeps `gomap` as an alias)
- 2025-12-14: HashDB index split
  - Split SwissHash control bytes into separate mmap file (`hashctl-<capacity>`) from keys (`hashkeys-<capacity>`)
  - Updated incremental rehash cleanup and `TestResizeLeak` to assert both file families are cleaned up
  - `go test ./...` (root): PASS
- 2025-12-14: HashDB index memory policy
  - Added `IndexMemoryPolicy` (default: mlock controls best-effort, madvise key map WILLNEED+RANDOM best-effort)
  - Implemented cross-platform memory pinning via `x/sys` (`unix.Mlock` / `windows.VirtualLock`) and best-effort unlock on close
  - `go test ./...` (root): PASS
- 2025-12-14: HashDB legacy cleanup
  - Removed unused syscall-based `mlock` implementation (replaced by `x/sys` helpers)
  - Fixed Linux `applyFadvise` to use a single advice (`FADV_RANDOM`)
  - `go test ./...` (root): PASS
- 2025-12-14: Public API polish (HashDB)
  - Promoted the distributed/sharded engine (formerly `gomap_distributed`) as `hashdb.HashDB` (kept `ShardedDB`/`HashmapDistributed` aliases)
  - Switched package-level `hashdb.Open(...)` to open the sharded `HashDB`; added `hashdb.OpenSingle(...)` for the single-shard DB
  - Updated internal callsites (BTreeOnHashDB, redisserver, unified_bench) to prefer `HashDB`
  - `go test ./...` (root): PASS
- 2025-12-14: Public API polish (TreeDB)
  - Added root `treedb.Open(...)` / `treedb.OpenCached(...)` returning the cached wrapper (`TreeDB/public.go`)
  - Kept the uncached engine available via `treedb.OpenBackend(...)` and `TreeDB/db.Open(...)`
  - Updated `cmd/unified_bench` so `treedb` uses the cached DB by default (added `treedbbackend` for uncached comparisons)
  - `go test ./...` (root): PASS
- 2025-12-14: TreeDB scan iterator perf
  - Switched disk iterator to zero-copy pages (`pager.Get`) + verified-checksum cache
  - Avoided per-entry allocations via `GetLeafEntryView`; slab values now load lazily in `UnsafeValue`
  - `go test ./...` (`TreeDB`): PASS
- 2025-12-14: TreeDB merge iterator perf
  - Made `TreeDB/internal/merging` value-lazy (don’t call `UnsafeValue()` during `Next()`/selection)
  - Early-close exhausted sources to avoid iterator leaks
  - `go test ./...` (`TreeDB/internal/merging`, `TreeDB/caching`): PASS
- 2025-12-14: TreeDB benchmarks cleanup
  - Reduced benchmark noise by precomputing keys (avoid per-iteration `fmt.Sprintf` allocations)
  - `go test ./...` (`TreeDB/db`): PASS
- 2025-12-14: TreeDB scan allocs cleanup
  - Added `node.NewNodeView` and stored iterator nodes by value to avoid per-page heap allocs
  - `BenchmarkScan`: ~314 allocs/op → ~7 allocs/op on local run
  - `go test ./...` (root/TreeDB/cmd/unified_bench): PASS
- 2025-12-14: TreeDB cached Iterator correctness
  - Fixed cached `Batch.Write` streaming path to update backend key-range tracking so `Iterator(nil,nil)` includes backend data
  - Added regression test `TestCachingDB_IteratorIncludesBackendAfterStreamingBatch`
  - `go test ./...` (`TreeDB/caching`): PASS
- 2025-12-14: unified_bench scan clarity
  - Renamed benchmark rows to `Full Scan` and `Prefix Scan` (aliases: `scan`, `range_scan`)
  - `Prefix Scan` now reports items/sec and targets the active keyspace (base keys vs batch_write offset keys)
  - `-` is now only for unsupported/not-run (uses NaN); real zero results print `0`
- 2025-12-14: CI (optional)
  - Added `gofmt` pre-commit hook under `.githooks/` and a `make hooks` installer target.
- 2025-12-14: unified_bench reproducibility
  - Added `-seed` and made randomized tests use per-test PRNGs so all DBs see the same random key/query sequences.
- 2025-12-14: CI quality gates
  - Added `gofmt` check and `go vet` steps to `.github/workflows/go-tests.yml`.
  - Expanded CI to run on Linux + macOS.
  - Renamed workflow/job names for clarity in the GitHub Actions UI.
  - Restored `pull_request` trigger for CI.
  - Quoted job names containing `:` to fix GitHub Actions YAML parsing.
- 2025-12-14: CI workflows
  - Split CI into separate workflow files (`format.yml`, `root-tests.yml`, `treedb-tests.yml`, `unified-bench-tests.yml`) so check names don’t all share the same workflow prefix in GitHub UI.
- 2025-12-14: Docs
  - Documented TreeDB exclusive open (`treedb.ErrLocked`) and added `cmd/unified_bench/README.md`.
- 2025-12-14: HashDB exclusive open
  - Added directory locking (`hashdb.ErrLocked`) for both `hashdb.Open*` (sharded) and `hashdb.OpenSingle` (single).
  - Updated HashDB crash-recovery test to simulate crashes via a helper subprocess so OS releases locks.
  - Fixed BTreeOnHashDB persistence test to close HashDB before reopening.
  - Added in-process and cross-process lock regression tests in `HashDB/lock_test.go`.
  - Documented the new lock behavior in `HashDB/doc.go`.
- 2025-12-14: HashDB stress tests
  - Made `HashDB/stress/compaction_test.go` tolerate transient `ENOENT` during size walks while compaction creates/removes `*-compact` dirs (fixes flaky macOS CI failure).
- 2025-12-15: Windows CI fix
  - Fixed HashDB `stress` tests on Windows by building/execing the redis server binary with a `.exe` suffix and using `localhost:6380` for readiness checks.
- 2025-12-15: Downstream readiness (HashDB + contracts)
  - Removed “raft” terminology from docs/roadmap and renamed `docs/raft` to `docs/downstream`.
  - Added HashDB `PutSync`/`DeleteSync` durability APIs and rebuild-on-open crash recovery (slab log scan + torn-tail truncation).
  - Added cross-engine durability contract tests (`internal/contracttest`) validating durable writes survive simulated crashes.
- 2025-12-15: Downstream readiness (batch + snapshot contracts)
  - Added HashDB `ApplyBatch`/`ApplyBatchSync` with crash-atomic commit markers and recovery handling.
  - Added `hashdb.ForEach` iteration API (arbitrary order) with a sharded snapshot implementation that flushes caches before scanning.
  - Added contract tests for snapshot/restore round-trips and basic concurrency/iterator bounds (`internal/contracttest/*`).
  - Added HashDB crash tests validating `ApplyBatchSync` durability and uncommitted batch truncation (`HashDB/applybatch_crash_test.go`).
- 2025-12-15: HashDB snapshot helpers + GetMany/IO hardening
  - Added streaming snapshot helpers: `hashdb.(*DB).Export/Restore` and `hashdb.(*HashDB).Export/Restore` (`HashDB/snapshot.go`).
  - Made slab writes robust to short writes by using `writeAll` in all slab write paths; added a short-write unit test (`HashDB/writeall_test.go`).
  - Improved `HashDB.GetMany` by coalescing slab `ReadAt` calls per segment (best-effort locality win) via a backend `getManyWithHashes` fast-path (`HashDB/getmany.go`).
  - Added HashDB sharded contract tests (per-shard batch atomicity on error, `PutSync` overrides cached `Put` on crash, `ForEach` blocks writers) (`internal/contracttest/hashdb_sharded_semantics_test.go`).
- 2025-12-15: Compression policy + benches
  - Centralized the compression threshold (`minValueBytesForCompression`) and reused it in all write paths.
  - Added a small benchmark matrix for compression inputs/sizes (`HashDB/compression_bench_test.go`).
- 2025-12-15: Docs (TreeDB)
  - Added TreeDB docs: concepts, recovery, and tuning (`docs/TREEDB_CONCEPTS.md`, `docs/TREEDB_RECOVERY.md`, `docs/TREEDB_TUNING.md`).
  - Updated docs index and the handoff checklist (`docs/README.md`, `docs/HANDOFF_CHECKLIST.md`).
- 2025-12-15: HashDB perf + optional durability
  - Reduced read syscalls via sealed-segment slab mmaps and GetMany chunk buffering (`HashDB/slab_ro_mmap.go`, `HashDB/getmany.go`).
  - Improved GetMany fallback by issuing exact record reads when chunks are incomplete (`HashDB/getmany.go`).
  - Added optional per-shard cache WAL with configurable fsync policy (default off) and tests (`HashDB/cache_wal.go`, `HashDB/cachekv_wal_test.go`).
  - Added Linux `pread`-based `readAt` helper (`HashDB/readat_linux.go`).

## Notes / Conventions

- Prefer small, reviewable commits; run relevant tests before/after each.
- Keep renames “modest”: mostly package/module names and imports; avoid large API churn unless tests demand it.

## Open Things to Investigate in TreeDB

### 1. Adaptive Inline Threshold (High Priority)
*   **The Issue:** The `InlineThreshold` is statically set to **256 bytes**. This creates a sharp "cliff" in performance behavior.
*   **Pathological Case:** A workload writing **257-byte values** will force *every* value into the Slab file (random I/O, extra pointer overhead), whereas **255-byte values** stay inline in the B-Tree (sequential I/O, better cache locality).
*   **Solution:** Implement the **Adaptive Controller** (mentioned in `specs/spec.md` but missing in code). It should dynamically adjust the threshold based on "Slab Pressure" vs. "Index Pressure" to smooth out this cliff.

### 2. Memtable Memory Layout (Arena Allocation)
*   **The Issue:** Increasing `FlushThreshold` to 64MB degraded random write performance due to increased CPU cache thrashing and Garbage Collection pressure (managing 64MB of B-Tree nodes).
*   **Opportunity:** Replace the pointer-heavy B-Tree (`google/btree`) with an **Arena-backed SkipList** or a flat array-based structure.
*   **Benefit:** This would allow much larger `FlushThresholds` (e.g., 64MB–128MB) for superior disk batching *without* suffering the CPU/GC penalty.

### 3. "Zero-Copy" Write Path
*   **The Issue:** Profiling showed `runtime.memmove` consuming **~30% of CPU**. Data is copied multiple times: `User Buffer` -> `Batch Slice` -> `WAL Buffer` -> `Memtable Node`.
*   **Opportunity:** Investigate a "zero-copy" flow where the Memtable points directly to slices within the `Batch` or `WAL` buffer (pinned until flush), reducing memory bandwidth usage.

### 4. Comparison Micro-Optimizations
*   **The Issue:** `bytes.Compare` (`cmpbody`) consumed **~6% of CPU** in initial profiling.
*   **Opportunity:** Implement **Prefix Compression** in B-Tree nodes to reduce storage size and comparison time for keys with common prefixes. Alternatively, explore SIMD-optimized comparisons for fixed-length keys.

### 5. Heuristic Cleanup
*   **The Issue:** The system uses a mix of hardcoded and dynamic heuristics (`streamSwitchThreshold`, `InlineThreshold`, `FlushThreshold`).
*   **Action:** Consolidate these into a unified `WritePolicy` struct. Re-evaluate `streamSwitchThreshold` (32) as it might be redundant or could be derived from other configurable thresholds.

### 6. Slab Space Reclamation
*   **The Issue:** While `slabManager` logic exists, its efficiency for reclaiming fragmented space (e.g., from updates/deletes of large values) was not deeply analyzed.
*   **Risk:** Heavy workloads with many large value updates/deletes could lead to `*.slab` file fragmentation and inefficient space reuse if the "Graveyard" or "Compaction" logic isn't perfectly aggressive. Ensure efficient reuse of slab holes.