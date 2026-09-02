# Random Read Throughput Review (2026-02-12)

## Scope

Review read-path throughput and profiling for TreeDB while preserving current public semantics:

- `Get(key)` returns a safe copy.
- no behavior changes to missing keys/tombstones.
- no weakening of snapshot consistency.

## Commands Run

### Branch under test

```bash
make unified-bench
/usr/bin/time -p ./bin/unified-bench \
  -dbs leveldb,treedb \
  -profile fast \
  -test sequential_write,random_read,random_read_batch \
  -keys 500000 \
  -batchsize 256 \
  -checkpoint-between-tests \
  -treedb-force-value-pointers=false \
  -valsize 85 \
  -format markdown
```

3 non-profile runs:

- `/tmp/gomap_cur_runs_RxlP2E/run_1.md`
- `/tmp/gomap_cur_runs_RxlP2E/run_2.md`
- `/tmp/gomap_cur_runs_RxlP2E/run_3.md`

Profile run:

```bash
/usr/bin/time -p ./bin/unified-bench ... -profile-dir /tmp/gomap_cur_prof_tUbqfx
./bin/benchprof -profiles-dir /tmp/gomap_cur_prof_tUbqfx
```

### Main baseline worktree

```bash
git worktree add /tmp/gomap_main_wt origin/main
cd /tmp/gomap_main_wt
make unified-bench benchprof
/usr/bin/time -p ./bin/unified-bench \
  -dbs leveldb,treedb \
  -profile fast \
  -test sequential_write,random_read \
  -keys 500000 \
  -batchsize 256 \
  -checkpoint-between-tests \
  -treedb-force-value-pointers=false \
  -valsize 85 \
  -format markdown
```

3 non-profile runs:

- `/tmp/gomap_main_runs_C3HVEU/run_1.md`
- `/tmp/gomap_main_runs_C3HVEU/run_2.md`
- `/tmp/gomap_main_runs_C3HVEU/run_3.md`

Profile run:

```bash
/usr/bin/time -p ./bin/unified-bench ... -profile-dir /tmp/gomap_main_prof_XOtWla
./bin/benchprof -profiles-dir /tmp/gomap_main_prof_XOtWla
```

## Throughput Summary

All values are ops/s, median of 3 non-profile runs.

| Test | Main TreeDB | Branch TreeDB | Delta |
|---|---:|---:|---:|
| Sequential Write | 9,918,593 | 10,317,566 | +4.02% |
| Random Read | 1,766,942 | 1,850,854 | +4.75% |
| Random Read (Batch=256) | n/a on main | 2,050,065 | n/a |

LevelDB medians (sanity):

- Sequential Write: 571,965 -> 566,771 (-0.91%)
- Random Read: 720,773 -> 758,371 (+5.22%, noisy but directionally stable in these runs)

## Profile Findings

### Branch profile (`/tmp/gomap_cur_prof_tUbqfx`)

`random_read` (TreeDB):

- CPU is copy-heavy: `runtime.memmove` is dominant (`65.71%` flat).
- Alloc objects are concentrated in:
  - `db.(*DB).AcquireSnapshot` (`51.72%`)
  - `db.(*Snapshot).Get` (`45.13%`)
- Alloc space is concentrated in:
  - `db.(*Snapshot).Get` (`78.04%`)
  - `db.(*DB).AcquireSnapshot` (`14.91%`)

`random_read_batch` (TreeDB):

- Throughput is higher than single-key reads.
- Alloc objects are already very low (`total=4,081` for whole run).
- Alloc space is mostly copy arena in `db.(*DB).GetMany` (`99.14%`).
  - This is expected with safe-copy semantics.

### Main profile (`/tmp/gomap_main_prof_XOtWla`)

`random_read` (TreeDB):

- Same fundamental shape as branch:
  - high copy cost in `runtime.memmove`
  - alloc split between `AcquireSnapshot` and `Snapshot.Get`.

## Top Semantics-Preserving Throughput Opportunities

Ordered by expected impact for `random_read`.

1. Add a reusable read handle API to amortize snapshot setup.

- Problem: `AcquireSnapshot` is a top allocator for single-key reads.
- Direction: introduce a long-lived read context (for example `ReadHandle`) that pins one snapshot for many `Get` calls.
- Semantics: each `Get` still returns a safe copy; consistency is explicit per handle lifetime.
- Expected impact: high for point-read loops.

2. Add `GetManyInto`/`GetInto` APIs with caller-owned buffers.

- Problem: safe-copy semantics force copy cost every call; default APIs must allocate.
- Direction: add optional APIs that append into caller-provided buffers/slices while preserving value bytes as safe copies.
- Semantics: existing `Get` behavior unchanged; new API is opt-in.
- Expected impact: high for benchmark-style and service hot loops that do not retain values long-term.

3. Reduce snapshot registration overhead in backend internals.

- Problem: snapshot acquire/release bookkeeping allocates per read.
- Direction: optimize reader registry path (fewer heap allocations for register/unregister, cheaper pin tracking).
- Semantics: no API change, same snapshot safety.
- Expected impact: medium to high for single-key random read.

4. Add an explicit bulk point-read path on the Tree layer.

- Problem: `GetMany` currently loops over per-key tree traversal.
- Direction: add internal batch traversal primitive (reuse page/key decode state across neighboring keys, especially on prefix-compressed leaves).
- Semantics: return array of safe copies in input order.
- Expected impact: medium, strongest for batch reads.

5. Keep optimizing copy path details without changing semantics.

- Problem: `memmove` dominates point reads.
- Direction: minimize avoidable intermediate copies and memclr in hot loops, keep one-copy-to-caller as the required semantic boundary.
- Semantics: unchanged safe-copy contract.
- Expected impact: medium.

## Recommendation

Next implementation PR should focus on **(1) reusable read handle + (3) snapshot registry cost**, because current profiles show these costs clearly even in inline-value mode (where value-log decode noise is minimal).
