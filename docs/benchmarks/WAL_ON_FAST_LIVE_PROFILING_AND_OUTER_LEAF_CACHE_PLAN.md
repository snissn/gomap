# Wal On Fast Live Profiling And Outer-Leaf Cache Plan

## Sprint Goal

Make `run_celestia + dwell` on `wal_on_fast` produce enough live evidence to
decide whether a real outer-leaf cache is worth building next.

This sprint does not ship the cache itself. It ships the measurement surface we
need first.

## What This Sprint Adds

1. Process-wide outer-leaf read telemetry
   - successful `leaf_vlog` page loads
   - point-read vs iterator load counts
   - bytes loaded
   - sampled recent-ref locality windows at capacities `64`, `256`, `1024`,
     and `4096`
2. A live `run_celestia` profiling wrapper
   - periodic `/debug/vars` snapshots
   - periodic CPU profiles from `/debug/pprof/profile`
   - final heap/goroutine capture
   - compact summary focused on outer-leaf locality and maintenance state

## Why This Is The Right First Sprint

- Celestia already gives a stable pprof/debug-vars endpoint on port `6062`.
- TreeDB already exposes process-wide read-path and value-log stats.
- We do not yet know whether live reads are reusing the same outer leaves often
  enough for a page cache to pay for itself.

Without locality evidence, an outer-leaf cache would be guesswork.

## Decision Rules For The Next Sprint

Use the live `summary.md` / `summary.json` artifacts from this sprint.

- If `outer_leaf.cache_potential.capacity_256_hit_ratio` or
  `capacity_1024_hit_ratio` is already materially high on Celestia dwell,
  ship a bounded decoded-page cache next.
- If locality is weak but CPU profiles still point at outer-leaf decode, prefer
  decode-path specialization before a cache.
- If locality is weak and decode is not dominant, do not build the cache.

## If We Build The Cache Later

The first acceptable cache shape is narrow:

1. Key by physical outer-leaf identity
   - `ValuePtr` / leaf-log record identity, not by logical tree path
2. Store validated decoded leaf pages
   - cache the post-decode page bytes, not just compressed frames
3. Admit on repeated point-read reuse
   - do not blindly admit one-touch pages
   - use a tiny probationary policy or second-touch admission
4. Exclude iterator-driven scans from admission
   - iterators naturally stream many leaves and will churn a naive page cache
5. Bound by bytes and entries
   - start with a small cap and make it fully optional

That design is only justified if live reuse is materially higher than what this
sprint observed.

## First Wal On Fast Run Findings

Live wrapper output:
- run output dir: `/tmp/celive_wal_on_fast_20260416192003`
- run home: `/home/mikers/.celestia-app-mainnet-treedb-20260416192034`

Final run results:
- sync duration: `298s`
- total duration: `1199s`
- dwell elapsed: `901s`
- max RSS: `12,340,620 kB`
- final `application.db`: `2,887,529,866 B`
- final split:
  - `maindb/index.db`: `65,536,000 B`
  - `maindb/leaf_vlog`: `2,663,355,302 B`
  - `maindb/value_vlog`: `158,411,129 B`
  - `maindb/wal`: `4,096 B`

Outer-leaf telemetry:
- total outer-leaf loads: `33,412,653`
- point loads: `30,077,051`
- iterator loads: `3,335,602`
- sampled loads: `522,072`
- final sampled cache-potential hit ratios:
  - `64`: `2.8634%`
  - `256`: `3.0659%`
  - `1024`: `3.4495%`
  - `4096`: `5.0370%`
- peak sampled cache-potential hit ratios over the full run:
  - `64`: `4.9782%`
  - `256`: `5.2757%`
  - `1024`: `5.9231%`
  - `4096`: `7.3080%`

Leaf-pack maintenance:
- attempts: `952`
- admitted: `952`
- runs: `0`
- last skip reason: `plan_admission:no_candidates`
- expected reclaim bytes stayed `0`

CPU profile interpretation:
- restore/apply phase is dominated by write/import work
- dwell-phase manual `30s` sample was dominated by:
  - `scanValueLogFileMaxRID`
  - `pread` / `Syscall6`
  - `LeafGenerationPack` planning
- later dwell samples also show iterator-driven scanning and
  `collectValueLogRefCounts`, not a strong hot-page outer-leaf reuse pattern

## Conclusion

This run does not justify shipping an outer-leaf decoded-page cache next.

The evidence says:
- outer-leaf traffic is heavy
- sampled short-window reuse is weak
- dwell CPU is more obviously consumed by maintenance scans than by repeated
  hot outer-leaf point loads

The better next sprint is to attack leaf-pack planning/scanning cost and
candidate effectiveness before investing in an outer-leaf page cache.

## Validation Workflow

```bash
scripts/profile_run_celestia_live.sh
```

Defaults:
- local `gomap` checkout as `LOCAL_GOMAP_DIR`
- `TREEDB_OPEN_PROFILE=wal_on_fast`
- `POST_SYNC_DWELL_SECONDS=900`

Primary review artifacts:
- `summary.md`
- `summary.json`
- `vars/latest.json`
- `pprof/cpu_*.pb.gz`
