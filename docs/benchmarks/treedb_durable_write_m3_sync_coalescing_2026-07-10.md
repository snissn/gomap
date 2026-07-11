# TreeDB Durable-Write M3 Sync Coalescing (2026-07-10)

Issue: [#3657](https://github.com/snissn/gomap/issues/3657)

Parent: [#3652](https://github.com/snissn/gomap/issues/3652)

Base commit: `851d8e89c1e38fd1af6edc682ebe1722852d4215`

Candidate mechanism commit: `b8f9625fc4965fad9a912437176334f3c7632ef6`

This candidate removes one proven duplicate value-log file sync without
weakening the durable return boundary. It is a partial mechanism win and a
latency-gate no-go: #3657 remains open because the required 20% focused
improvement and 16 ms state target are not demonstrated.

## Result

- A forced-pointer dirty `WriteSync` on an unchanged active value-log segment
  now performs one materialization file sync instead of a materialization sync
  followed by a duplicate external-reference sync.
- The later command-WAL file sync is unchanged. The irreducible ordered floor
  remains one value-log file sync followed by one command-WAL file sync.
- The canonical alternating comparison changes physical value-log syncs from
  2 to 1 and logical external-reference syncs from 1 to 0 in every sample.
- Wall time does not move reliably: the two focused rows are statistically
  unchanged and their geomean is 3.0% slower. This is far short of the issue's
  required 20% improvement.
- Allocations are unchanged. Crash/reopen, failure fallback, race, focused
  package, and full repository tests pass.

M2 measured the removable second-sync ceiling at only 1.35-1.46 ms per affected
batch. A 20% reduction from a roughly 19.5 ms focused geomean would require
about 3.9 ms. The measured candidate confirms that this lane cannot by itself
meet the M3 gate on this host.

## Safety proof

The materialization file sync runs while holding the lane's `vlogMu`. After a
successful sync, the lane records a certificate containing the active value-log
file ID, lane sequence, and writer size. The durable batch retains its
`lane.syncing` reservation from value materialization through command-WAL
external-reference ordering.

At external-reference ordering, reuse is allowed only while holding the same
`vlogMu` and only when all of these remain true:

1. the durable-write reservation is still held;
2. the materialization sync succeeded;
3. the active file ID and sequence match the certificate;
4. the append-only writer size is unchanged; and
5. the command actually references that active file ID.

Because the active value log is append-only, the successful sync covers every
referenced record at or below the certified size. Intent construction validates
each pointer and reads its RID before the external-reference barrier may append
or sync the command frame. A malformed or unreadable pointer therefore fails
before command-WAL durability.

All uncertain cases retain the conservative path:

| Case | Result |
| --- | --- |
| exact certified active file, sequence, size, and reservation | reuse materialization sync |
| active writer grew | external-reference sync |
| reservation released | external-reference sync |
| segment rotated or file ID differs | active fallback plus direct sync of each referenced old segment |
| no active writer | direct sync of the referenced segment |
| failed materialization sync | error; no certificate |
| fallback sync failure | error propagated before command-WAL durability |
| empty barrier | unchanged pending-lane sweep, then command-WAL barrier |

The normative ordering contract is in
`TreeDB/docs/spec/command-wal-durable-write-contract.md`.

## Focused benchmark

Artifacts are under:

```text
/mnt/fast4tb/gomap-3657-m3-evidence-20260710
```

The accepted focused comparison used separately compiled base and candidate
binaries, one CPU (`taskset -c 5`), warmups excluded, and the alternating
schedule `A B B A A B B A A B B A A B B A`. Each reported sample ran exactly
20 operations; there are eight samples per side.

```sh
taskset -c 5 ./TreeDB.test -test.run='^$' \
  -test.bench='^BenchmarkPublicCommandWALDurableTinyBatchWriteSync$/^placement=forced_pointer$/^shape=(dirty_batch|state_point_point_sync_batch_sync)$/^ops=1$' \
  -test.benchtime=20x -test.count=1
```

| Shape | M2 base | M3 candidate | Comparison | Required |
| --- | ---: | ---: | --- | ---: |
| dirty batch | 16.93 ms/op +/-38% | 16.44 ms/op +/-86% | -2.9%, `p=0.721`, n=8 | at least -20% |
| state-shaped | 22.36 ms/op +/-68% | 24.44 ms/op +/-64% | +9.3%, `p=0.878`, n=8 | at least -20% and <=16 ms |
| geomean | 19.46 ms/op | 20.04 ms/op | +3.0% | at least -20% |

The host's durable syscall latency is highly variable, but the deterministic
mechanism counters are exact:

| Counter | M2 base | M3 candidate |
| --- | ---: | ---: |
| value-log file sync calls/op | 2 | 1 |
| logical value-log sync calls/op | 2 | 1 |
| materialization sync calls/op | 1 | 1 |
| external-reference sync calls/op | 1 | 0 |

Allocation results are neutral: dirty-batch is 36 allocs/op and 6.151 KiB/op
on both sides; state-shaped is 39 allocs/op with 6.972 KiB base and 6.969 KiB
candidate. The candidate did not select allocation as an optimization lane.

An adjacent all-shape five-sample pair is retained as
`comparison_adjacent.txt` for counter and allocation reconciliation, but is
excluded from wall-time conclusions: unrelated inline rows moved by +50% to
+70%, while pointer rows ranged from about -40% to +91%. The pinned alternating
comparison above is the canonical latency evidence.

## Profiles and Linux syscall validation

The 100-operation CPU profile contains only 60 ms of samples over 3.03 s
(1.98%), confirming an I/O-bound path. Allocation, block, mutex, and trace
profiles do not identify a local M3 target: the allocation profile includes
open/close setup, blocking is dominated by background channel waits, and total
mutex delay is under 1 ms.

The five-operation Linux trace uses the production `os.File.Sync` path. Its
measured active window contains exactly five active value-log `fsync` calls and
five later command-WAL `fsync` calls, alternating once per operation. There is
no duplicate external-reference value-log `fsync`. The in-process row agrees:
one materialization sync, zero external-reference syncs, one aggregate
value-log file sync, and one command-WAL file sync per operation.

Primary artifacts:

| Artifact | Purpose |
| --- | --- |
| `focused_abba8_core5_comparison.txt` | canonical alternating comparison |
| `baseline/focused_abba8_core5.txt` | eight M2 base samples |
| `candidate/focused_abba8_core5.txt` | eight M3 samples |
| `comparison_adjacent.txt` | excluded noisy all-shape pair; counters only |
| `candidate/profile/{cpu,allocs,block,mutex}.pprof` | focused profiles |
| `candidate/profile/trace.out` | runtime trace |
| `candidate/profile/strace.log` | Linux syscall trace |
| `candidate/profile/strace_benchmark.txt` | in-process syscall ledger |

## Verification

The following completed successfully with `GOWORK=off`:

```sh
go test ./TreeDB/caching ./TreeDB/db ./TreeDB \
  -run 'TestCachingValueLogExternalRefSyncCoalescingGuards|TestCachingValueLogExternalRefFlusherSyncsRotatedSegments|TestFlushCommandWALBarrierOrdersExternalRefsBeforeCommandWAL|TestPublicCommandWALBatchWriteSyncExternalRefOrderingPhaseStats' -count=1

go test ./TreeDB \
  -run '^TestCrashRecovery_CommandWALDurableSyncedUncheckpointedFramesReplay$' -count=1

go test -race ./TreeDB/caching ./TreeDB \
  -run 'TestCachingValueLogExternalRefSyncCoalescingGuards|TestCachingValueLogExternalRefFlusherSyncsRotatedSegments|TestPublicCommandWALBatchWriteSyncExternalRefOrderingPhaseStats' -count=1

go test ./TreeDB/db ./TreeDB/caching ./TreeDB -count=1
go test ./... -count=1

go test ./TreeDB/db \
  -run '^TestAppendRawKVCommandWALOrderedEntriesRejectsMalformedValueLogPointerBeforeDurability$' \
  -count=1
```

The full repository run passed; notable package durations were 406.987 s for
`TreeDB/db` and 190.534 s for `TreeDB/collections`.

## Ironbird diagnostic

The required candidate-pinned production-shape diagnostic completed without a
runner or result error. The scenario dependency manifest resolves the exact
candidate source:

```text
github.com/snissn/gomap@v0.6.2-0.20260711055505-b8f9625fc496
=> b8f9625fc4965fad9a912437176334f3c7632ef6
```

The backend verifier observed TreeDB for both app and node databases and the
`kv` transaction indexer, matching the requested configuration. The run used
one validator, no non-validator nodes, 100,000 preseeded accounts, 5,000 active
wallets, 500 requested transactions per block, and 450 requested blocks:

```sh
/mnt/fast4tb/bin/ironbird-local-report-runner-3657 \
  -scenario simapp-treedb-all \
  -validators 1 -nodes 0 -wallets 5000 \
  -preseed-profile accounts -preseed-accounts 100000 \
  -cosmos-txs 500 -cosmos-blocks 450 \
  -tx-indexer kv \
  -load-window-min-duration 5m \
  -load-window-target-fraction 0.995 \
  -stop-catalyst-after-load-window \
  -app-debug-vars -raw-tx-audit=false \
  -out /mnt/fast4tb/ironbird-gomap-3657-m3-20260710T2005HST/accepted/result.json
```

| Signal | Result |
| --- | ---: |
| intended transactions | 225,000 |
| accepted target at 99.5% | 223,875 |
| included and successful | 223,997 (99.55422% of intended) |
| accepted load window | 331.580154797 s |
| accepted throughput | 675.544047976 tx/s |
| backend verification | valid |
| runner/result errors | 0 |

Whole-scenario TreeDB stat deltas give these public batch `WriteSync` rows:

| Store | Calls | Total | Mean | Checkpoint barrier wait |
| --- | ---: | ---: | ---: | ---: |
| `application.db` | 1,313 | 18.973057 s | 14.450157 ms | 900.581200 ms |
| `state.db` | 1,313 | 18.511351 s | 14.098515 ms | 0.002928 ms |
| `blockstore.db` | 2,626 | 25.622918 s | 9.757395 ms | not selected |

The application and state means are below the issue's absolute production-row
ceilings of 17 ms and 16 ms, respectively. This is not evidence that the M3
mechanism caused those results: every production store records zero public
preflight-materialization time and zero public external-reference-ordering
calls. The optimized forced-pointer branch was therefore inactive in this row;
the relevant public `WriteSync` values stayed inline.

This single candidate row is diagnostic only. It is not a paired baseline and
does not establish a throughput delta or the required 20% focused latency
reduction. The canonical forced-pointer comparison remains the decision
evidence: it proves the physical sync reduction but fails the latency gate and
the focused 16 ms state target. The accepted Ironbird artifacts are under
`/mnt/fast4tb/ironbird-gomap-3657-m3-20260710T2005HST/accepted`.
