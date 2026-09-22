# TreeDB #2960 apply allocation churn census

Parent tracker: #2960. Child issues covered: #2964-#2972.

This note records the source-level ownership map used for the allocation cleanup in the span-native prepared apply / leaf-log append path.

## Hot allocation ownership map

| Area | Pre-cleanup owner | Allocation class | Resolution |
| --- | --- | --- | --- |
| `caching.(*DB).appendValueLogOneInternal` | single prepared block leaf-log append | `[1]valuelog.Record` and `[1]page.ValuePtr` scratch escaped through generic prepared-frame and interface append helpers | Added `prepareAppendFrameOne` and `valuelog.Writer.AppendEncodedFrameOne`; the single prepared append benchmark is now `0 allocs/op` after warmup. |
| `caching.putVlogPreparedFrames` | prepared frame metadata pool | `sync.Pool.Put([]preparedDictFrame)` boxed slice headers, one object per prepared append/batch return | Replaced the hot slice return path with bounded typed lease stacks for prepared-frame slices. |
| `caching.putValueLogRecordsNoClear` | prepared leaf batch record scratch | `sync.Pool.Put([]valuelog.Record)` boxed slice headers, one object per prepared batch | Replaced the hot slice return path with bounded typed lease stacks for value-log record slices. |
| `caching.putValueLogPtrsNoClear` | value pointer scratch | `sync.Pool.Put([]page.ValuePtr)` boxed slice headers, one object per append/batch return | Replaced the hot slice return path with bounded typed lease stacks for value-log pointer slices. |
| `valuelog.FramePreparer.PrepareFrameInto` | frame body construction | Body allocation only when caller destination capacity is insufficient | Added allocation-budget tests proving reused destinations stay at `0 allocs/op`. |
| `valuelog.Writer.AppendEncodedFrameInto` | prepared frame append | Pointer-slice allocation avoided only when caller supplies `dst`; single-frame callers still needed slice scratch | Added `AppendEncodedFrameOne` for one-frame prepared appends without caller slice scratch. |
| `zipper` prepared payload staging | span-native worker payload production | Payload arena/slice ownership is already wrapper-based and reused; no per-page slice boxing found in the selected path | No change needed. |

## Remaining allocations and scope boundaries

- `AppendPreparedLeafPages` still allocates one returned `[]page.LeafLogPtr` per call. This is API-owned output, not per-frame metadata churn. The span-native path uses `AppendPreparedLeafPageChildRefs` with caller-owned refs and is now `0 allocs/op` after warmup.
- Encoded frame body storage is still required until the writer has copied/appended it. Reuse is through `vlogPreparedFrameBody` leases; allocation-budget tests warm this path before measuring steady state.
- Error formatting, segment-rotation retain path slices, and dictionary codec initialization are outside the steady-state prepared apply hot path measured here.

## Focused evidence

Local focused before/after on the same host (`11th Gen Intel(R) Core(TM) i5-11400F`, Go 1.25.7 toolchain):

| Benchmark | Before | After |
| --- | ---: | ---: |
| `BenchmarkAppendValueLogAllocs` | ~25-26 B/op, `1 allocs/op` | ~0-1 B/op, `0 allocs/op` |
| `BenchmarkAppendPreparedLeafPageAllocs` | ~73 B/op, `3 allocs/op` | `0 B/op`, `0 allocs/op` |
| `BenchmarkAppendPreparedLeafPagesAllocs` | ~3195-3205 B/op, `4 allocs/op` | ~3110 B/op, `1 allocs/op` (returned `[]LeafLogPtr`) |
| `BenchmarkAppendPreparedLeafPageChildRefsAllocs` | not present | `0-2 B/op`, `0 allocs/op` |

10M random-write profile runs on the same host/base (`origin/main`/`dfe3f6d83` versus this branch) with `-treedb-flush-apply-span-native`, backlog coalescing, `-batchsize 8000`, `-valsize 128`, and `-profile-dir`:

| Apply concurrency | Baseline ops/s | After ops/s | Baseline alloc objects | After alloc objects |
| ---: | ---: | ---: | ---: | ---: |
| 4 | 617,723 | 627,029 | 7,704,858 | 1,175,184 |
| 8 | 805,575 | 843,368 | 5,767,481 | 1,090,480 |
| 16 | 829,229 | 830,128 | 6,893,958 | 1,116,675 |

After the cleanup, the former top allocation sites (`appendValueLogOneInternal`, `putVlogPreparedFrames`, `putValueLogRecordsNoClear`, `putValueLogPtrsNoClear`) are no longer material in the 10M c8/c16 allocation profiles. The c16 run reported only ~0.10s total leaf-log append-lock wait (`leaf_log_lanes.append_lock_wait_ns_total=100,596,414` baseline, `104,354,400` after), so no follow-on lane-contention implementation was required for #2972.

Representative validation commands:

```sh
GOWORK=off go test ./TreeDB/caching -run 'TestAppendPreparedLeafPage(AllocsBudget|ChildRefsAllocsBudget)$' -count=1
GOWORK=off go test ./TreeDB/internal/valuelog -run 'TestAppendEncodedFrameOne_RoundTripAndAllocsBudget|TestFramePreparerAndAppendEncodedFrameIntoAllocsBudget' -count=1
GOWORK=off go test ./TreeDB/caching -run '^$' -bench 'BenchmarkAppendValueLogAllocs|BenchmarkAppendPreparedLeaf(Page|PageChildRefs|Pages)Allocs' -benchmem -count=3
GOWORK=off go test ./TreeDB/...
```
