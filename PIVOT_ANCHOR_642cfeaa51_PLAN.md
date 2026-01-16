# Plan: Anchor on `642cfeaa51` and Move Forward Cleanly (No ValueIndex/GC, No Slab-V2 Dict/Zone Work)

## Goal

Adopt `642cfeaa51` (PR #44) as the **performance anchor**, then selectively forward-port only the **general bugfixes** from later history that do *not* depend on:

- Value Index / ValueID indirection
- GC (which exists to support Value Index)
- Slab V2 zonal dictionary / 2MB-zone choreography
- Large tracing/tooling bundles

This is pre-alpha; backwards compatibility is not required. The target is a **clean, stable base** plus a short TODO list of optimizations to evaluate (e.g. `KeepRecent`, pruning throughput).

## Anchor Choice

- Anchor commit: `642cfeaa51` (merge of PR #44).
- Rationale: observed throughput is significantly better at this point than earlier commits, and it predates the ValueIndex/GC and “Slab opt rc” bundles.

## What We Are Explicitly Dropping (Hard Exclusions)

These are “dead ends” for this pivot:

1) **Value Index / ValueID**:
   - Any code introducing/depending on ValueIDs, ValueIndexPrefix, System-tree value mapping, NextValueID, etc.
   - Any commits touching:
     - `TreeDB/db/value_index.go`
     - `TreeDB/db/gc.go`
     - their tests

2) **GC (refcounted segment GC in PR #45)**:
   - This GC’s reachability scan uses ValueIDs + Value Index mappings; if we drop Value Index, this GC is irrelevant and should not be imported.

3) **Slab V2 “2MB zone” dictionary work (PR #49)**:
   - Anything that changes zonal headers, boundary rules, dict-in-zone handling, or slab manager choreography to make zonal dicts work.
   - Anything that changes:
     - `TreeDB/slab/slab.go`
     - `TreeDB/slab/slab_v2.go`
     - `TreeDB/slab/manager.go`

4) **Trace capture / replay tooling** (PR #47 inside #49):
   - Not part of the engine “clean base”.

## Commit Review: What Changed After `642cfeaa51`

Range reviewed: `642cfeaa51..main` (**392 commits**).

Machine-generated inventory file:
- `PIVOT_ANCHOR_642cfeaa51_COMMITS.md`

Exhaustive (but compact) core-engine commit index:
- `PIVOT_ANCHOR_642cfeaa51_KEY_COMMITS.md`

That inventory annotates each commit with coarse tags and a default action:
- `DROP(ValueIndex/GC)` when ValueIndex/GC is detected
- `DEFER(slab format/choreography)` for slab-format / slab-manager changes tied to dict/zone work
- `DROP(tracing/tooling)` for trace PR content
- `CANDIDATE_FIX` for commits whose subject starts with `fix` (still requires review)

## Execution Strategy (Preferred): “Forward-port fixes onto the anchor”

Instead of reverting `main`, create a new “clean mainline” branch from the anchor and cherry-pick only the fixes we want.

### Step 0: Create the clean base branch

```bash
git checkout -b clean/anchor-642cfeaa51 642cfeaa51
```

### Step 1: Enforce guardrails before/after every cherry-pick batch

Hard bans (must remain true):

```bash
# No ValueIndex/GC files
test ! -f TreeDB/db/value_index.go
test ! -f TreeDB/db/gc.go

# No slab format/choreography changes in this pivot branch
git diff --exit-code 642cfeaa51 -- TreeDB/slab/slab.go TreeDB/slab/slab_v2.go TreeDB/slab/manager.go

# No new ValueID/ValueIndex identifiers (defense-in-depth)
rg -n "ValueID|ValueIndex|ValueIndexPrefix|NextValueID|EnableValueIndex|\\bGC\\(" TreeDB/db TreeDB/caching TreeDB/internal TreeDB/node TreeDB/page TreeDB/tree && exit 1 || true
```

### Step 2: Cherry-pick “general bugfix candidates” (excluding ValueIndex/GC)

Important: merges mean many commits reachable from `main` are not strictly “linear after” `642cfeaa51`.
For scope, treat the lists below as an allowlist and use `PIVOT_ANCHOR_642cfeaa51_KEY_COMMITS.md` as the exhaustive index.

These are good candidates because they address crashes/corruption/liveness and do **not** touch `TreeDB/slab/*` or `TreeDB/db/value_index.go` / `TreeDB/db/gc.go`.

#### Must-review / likely-keep (general correctness)

Iterator + safety:
- `13643e2960` fix: prevent panic in DBIterator.Next when invalid
- `b41dff7502` fix: robust iterator error handling
- `15a25d27dc` fix(treedb): explicit error check in DBIterator.Value/ValueCopy
- `68c1ec670d` fix(treedb): refine DBIterator error resetting
- `f39ce1b34e` fix: resolve use-after-free crash in GetAppend and iterator views
- `e576f782f7` fix(treedb): address review comments and add iterator regression test

Value-log / mmap correctness:
- `75086eea22` fix: resolve VLog mmap corruption and DBIterator panic

Close/shutdown correctness:
- `f49b2ca96d` fix: ensure both cached and backend layers are closed

Windows file removal robustness:
- `18f715d216` fix(treedb/caching): implement reliable file removal retry on Windows for WAL/vlog cleanup
- `c59db1e442` fix(treedb/caching): ensure value-log segments are closed before removal for Windows compatibility
- `8591004dd3` fix(treedb/caching): suppress redundant error reporting in dropValueLogSegment
- `13944027e4` fix(treedb/caching): suppress WAL removal errors to prevent fatal failures on Windows file locks

Memtable/vlog pointer normalization:
- `6057d9dfc0` fix: robustly normalize memtable value-log pointers when value log is disabled

Concurrency / sequencing correctness:
- `2703144cc0` fix(treedb): resolve concurrency race and sequence 0 pinning bug
- `114413ab08` Fix backpressure stall and add stress test plan

Copy-on-Flush (durability/corruption hardening; **only if it does not drag in ValueIndex code**):
- `12d628f7aa` fix(treedb/caching): enforce copy-on-flush to decouple WAL from backend storage

Vacuum robustness:
- `fa3714c542` fix(treedb): skip corrupt value entries during vacuum

“Review before taking” (still non-slab/non-ValueIndex, but broad or unclear):
- `9125ed31cd` ✦ I have resolved the crash, the index.db ballooning issue, and the slab stats corruption.
- `be4a377259` attempt to fix invalid id lngth
- `3e6180c859` go fmt

#### Optional (policy knobs; treat as TODO experiments rather than unconditional picks)

These are simple default changes; they can be reintroduced as small commits after benchmarking:
- `c6ab8b38bb` fix: lower default KeepRecent to 20 to prevent index bloat
- `966b28f339` fix: increase default prune throughput to prevent index ballooning

#### Cherry-pick procedure

Pick in small batches; after each batch, run guardrails + tests:

```bash
git cherry-pick <hash1> <hash2> ...
go test ./... -count=1
```

If a cherry-pick conflicts because the commit assumes ValueIndex code exists:
- Abort the cherry-pick and manually port only the non-ValueIndex portion.

### Step 3: Explicitly ignore PR #49 in this pivot branch

PR #49 (`43215d419c`) contains extensive slab-format/slab-manager changes and compression/dict work tied to 2MB zones.
This pivot branch must not take any of it.

If later you want *some* of the “bugfixes” from that PR:
- port them manually, one at a time, while keeping `TreeDB/slab/slab.go`, `TreeDB/slab/slab_v2.go`, and `TreeDB/slab/manager.go` identical to `642cfeaa51`.

#### Step 3A: Allowlisted non-slab fixes/perf tweaks inside the PR #49 range

These commits are “PR49-era” and touch **no** `TreeDB/slab/*` files and **no** ValueIndex/GC files.
They are the only “PR49-era” changes that are candidates to port onto the clean anchor without pulling in the 2MB-zone slab work.

Node/prefix-path performance (independent of slab V2 dict/zones):
- `d0051d31f0` Speed up prefix leaf key scans
- `6eeedb9c25` Reuse leaf prefix sizes in merge
- `2874d3d44d` Reduce prefix key copying
- `35dbb1e709` Pool zipper child work slices
- `b2ffd6bce5` Shorten leaf split separators
- `662d97684b` node: inline uint16 writes in builder
- `638e76687a` node: avoid key copy for zero-prefix leaf entries
- `e31dff3de5` Inline uint16 reads in node leaf paths
- `dcba8ce042` Inline uint32 writes in leaf builder

Caching correctness (memtable corruption mitigation):
- `e1747a02a8` Disable memtable pooling to fix corruption
- `9204b8482f` Remove memtable pooling and reader tracking
- Note: `e1747a02a8`/`9204b8482f` only make sense if you also introduced pooling; if the clean anchor does not have pooling, skip these entirely.

Compaction robustness (panic guard):
- `4fe010faac` compaction: guard zstd BuildDict panics

Optional profile/docs convenience (not required for a “clean base”):
- `3a12d93d08` profiles: add compressed variants and fix docs
- `f75ad53334` treedb adapter: ignore ErrClosed on reads
- `040fba37fe` treedbtrace: move tracing into optional wrapper (if you still want `treedbtrace`, otherwise skip)

Procedure: cherry-pick these individually and run `go test ./... -count=1` after each (or after small batches).

#### Step 3B: Explicitly excluded “fix-ish” commits from PR #49 (slab V2 / zone coupling)

These commits may look like bugfixes, but they directly touch slab V2 / zone-boundary behavior and are out of scope for this pivot.
Do **not** cherry-pick them into the clean anchor branch:

- `fdeea8e677` slab: fix V2 lazy-load detection and refine zone boundary logic
- `042237675c` slab: guard zstd training against empty samples
- `4f6890f36a` Fix V2 zone fallback and dict padding
- `3f85535717` Fix V2 batch flush boundary handling
- `0f7ca9543c` TreeDB: Robustness fixes for trainer and write throughput benchmark (touches slab trainer/zonal logic)

More generally: **any** commit that touches `TreeDB/slab/` is excluded in this pivot, even if it is labeled as a “fix”.

#### Step 3C: PR49-era commits that must be **manually split** (never cherry-pick wholesale)

These are “mixed” commits that include banned scope (slab / ValueIndex / trace harness) but also include some potentially valuable non-slab changes.
If you want anything from them, port only the safe paths and treat everything else as out-of-scope:

- `ff597262bf` feat: Phase 17.3 Value Index, Unified Seq, Refcounted GC
  - Hard rule: **never** cherry-pick wholesale; if there is a specific fix you want, port it via a smaller later commit or re-implement it directly.
- `14f73e3d6d` fix(treedb): fix in-place compaction for ValueID entries and improve overall system stability
  - Candidate safe areas inside: `TreeDB/internal/wal/*`, `TreeDB/internal/vlog/*`, and any `TreeDB/caching/*` correctness changes
  - Hard rule: exclude any ValueID/ValueIndex paths.
- `8d257fab74` fix(treedb): comprehensive stability and integrity fixes for Celestia sync
  - Candidate safe areas inside: `TreeDB/internal/wal/*` and compaction correctness fixes
  - Hard rule: exclude `TreeDB/slab/*` and any ValueIndex/GC.
- `de6d63a991` caching: optional iterator rotation reduction
  - Candidate safe areas inside: `TreeDB/caching/db.go` and `TreeDB/db/db.go`
  - Hard rule: exclude trace benchmark harness changes.

## TODO List (Post-clean-anchor, explicitly separate from this pivot)

### Disk/perf trade knobs to experiment with (low effort)
- Evaluate `KeepRecent=20` on top of the `642cfeaa51` anchor.
- Evaluate prune defaults:
  - `PruneInterval` (250ms → 100ms)
  - `PruneMaxPages` (4096 → 40960)
- Evaluate `PreferAppendAlloc` (throughput vs disk growth trade).

### WAL compression/hardening
- Re-land WAL compression/hardening cleanly (independent of slab zones).
- Ensure length caps + CRC verification in WAL reader.

### Dictionary compression refactor (future work; not part of this pivot)
- Implement dictionary compression without:
  - per-value-only compression limitations
  - 2MB slab zone coupling
  - slab V2 boundary choreography

## Deliverables

1) A new branch `clean/anchor-642cfeaa51` that:
   - is based on `642cfeaa51`
   - includes only the selected general bugfixes
   - contains **no ValueIndex/GC**
   - contains **no slab-format/slab-manager changes**

2) A short TODO list (above) to iterate on with benchmarks.
