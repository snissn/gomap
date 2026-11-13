# Cold indexed load: reference-delta reuse

Issue: [#5409](https://github.com/snissn/gomap/issues/5409). This packet leaves the performance change unmerged.

On the local 100,000-document “Bulk load: 2 displayed indexes” workload, median settled drain falls **31.33%**, from **397.306 to 272.834 ms**. Every one of five alternating pairs improves drain, by **29.22–33.30%**. Median total throughput increases **39.56%**, from **175,328 to 244,682 documents/s**; median total time falls 28.34%, from 570.360 to 408.693 ms. All samples are retained.

## Change and correctness boundary

The active buffered indexed flush calls `publishOrderedRootDeltaBatchGroupWithSystemDeltaBuilderSerialized`. Its command-WAL path previously disabled existing reference-delta construction. The shared descriptor proof also rejected a successfully enumerated empty descriptor base, forcing a candidate dependency scan after building the roots.

The change enables the maintained deltas on that path and admits a pure cold attachment only when all predecessors are zero and inline descriptors consume every distinct nonzero produced root, with no foreign/unused output or implicit user/system alias. Descriptor aliases count a produced root once. All four shared proof callers preserve census errors, so failed enumeration cannot be mistaken for an empty base. The warm proof, producer registration, persistent segment capture, activation ordering, sync and reclamation authority remain in the existing implementation. No disk format, compression, read packing, profile default or durability boundary changes.

Per load, the existing finalize counter falls from median **129.164417 to 0.172875 ms**. Leaf CRC checks and fallback ReadAt attempts fall from **18,901 to 2** in every primary sample. Two separately collected CPU-profile pairs attribute **120 ms** each to `scanCandidateExternalReferences` before the change and **no sampled CPU** to that stack afterward. This removes repeated dependency discovery; it does not disable checksum verification on reads.

## Frozen workload and environment

- Base: `2590d1e9e443c3f968303ce91b3c82933a714f12`.
- Runtime candidate: `45b33e8e202340a6543ba7ca563668fa711ed151`; production blob `0f5589c5b344f952efe50c3d9aed718299465b1a`, SHA-256 `0d34b75c64be7a7555f5b7a5ce6d680ccf6bae091f3a18c033dc8e7393e91884`.
- Only `TreeDB/db/ordered_root_publish.go` differs among non-test Go sources. Baseline binaries use an exact-base overlay for that file; the build receipt proves the overlay bytes and otherwise identical runtime sources. Product binaries have no probes. Artifact-only descendants retain these runtime bytes.
- Apple M3 / Mac15,13, eight logical CPUs, 16 GiB RAM, macOS 26.2 (25C56); Go 1.26.10 darwin/arm64, CGO enabled. Runtime Go tuning variables and GOFLAGS are cleared; default GOMAXPROCS is eight.
- Shared local machine: `INFRASTRUCTURE_UNAVAILABLE` for an exclusive dedicated runner. Existing persistent Go build cache and durable local Documents artifacts were available. Owned tests/builds were stopped during measurements; this is not a claim of exclusive machine ownership.
- Gateway BSON, prebuilt documents, official MongoDB driver `driver-command-raw`, loopback server, 100,000 documents, 1,000 documents per batch, 100 driver calls, 16 producers, two displayed secondary indexes plus the age range index. Declared indexes are `_id_`, `email_1`, `city_1`, `age_1`; four roots are published per load.
- `command_wal_relaxed`, compressed data/index-state/index roots, settled read state, checkpoint maintenance. Load time includes foreground relaxed ACKs **and settled drain**, excludes prebuild/setup and the later checkpoint/Close. It is not durable-per-request latency or a MongoDB comparison.
- Predeclared five-pair order: B/A, A/B, B/A, A/B, B/A. CPU/alloc profiles use two separate alternating pairs; reads use three pairs; the original warm guard uses three pairs. Local harness provenance review and freeze preceded collection. The user requires no merge, so neither harness nor product work was merged as a measurement prerequisite.

## Every primary sample

| Pair | Variant | Total ms | Documents/s | Foreground ms | Drain ms | Finalize ms |
|---|---|---:|---:|---:|---:|---:|
| 1 | before | 513.805 | 194,626 | 117.213 | 396.592 | 129.088792 |
| 1 | candidate | 408.693 | 244,682 | 139.316 | 269.377 | 0.194500 |
| 2 | before | 523.956 | 190,855 | 118.489 | 405.467 | 129.164417 |
| 2 | candidate | 388.975 | 257,086 | 118.519 | 270.456 | 0.172875 |
| 3 | before | 573.319 | 174,423 | 170.383 | 402.936 | 129.234041 |
| 3 | candidate | 726.191 | 137,705 | 440.999 | 285.192 | 0.176750 |
| 4 | before | 570.360 | 175,328 | 174.249 | 396.111 | 129.411791 |
| 4 | candidate | 396.608 | 252,138 | 118.769 | 277.839 | 0.156500 |
| 5 | before | 618.888 | 161,580 | 221.582 | 397.306 | 128.973042 |
| 5 | candidate | 579.700 | 172,503 | 306.866 | 272.834 | 0.162541 |

Pair 3's candidate foreground spikes to 440.999 ms; its total throughput is 21.05% worse than its paired baseline even though drain improves 29.22%. That sample and pair 5's noisier foreground remain included. The >=15% objective passes through consistent **drain reduction**; total throughput is not positive in every pair. These are descriptive medians, not a confidence interval or a universal throughput guarantee.

The median throughput comparison is `(median(candidate docs/s) / median(before docs/s) - 1) * 100`. Time reductions use `1 - median(candidate ms) / median(before ms)`. Paired percentages use each pair's actual measurements. See [all observations and derived values](results.json).

## Guardrails and follow-up

| Guard | Before median | Candidate median | Interpretation |
|---|---:|---:|---|
| ID reads, 10,000 per sample | 24,049/s | 24,181/s | +0.55%; p95 stays 55 us |
| Email reads, 10,000 per sample | 20,426/s | 21,084/s | +3.22%; p95 60 to 57 us |
| Age ranges, 1,000 per sample | 23,044/s | 23,508/s | +2.02%; p95 57 to 52 us |
| Whole-macro peak RSS | 626,655,232 B | 630,521,856 B | +0.62%; includes setup/prebuild/checkpoint/Close |
| After-load logical bytes | 73,761,742 | 73,761,742 | Equal |
| After-checkpoint logical bytes | 77,961,166 | 77,961,166 | Equal |
| WAL-excluded logical bytes, both boundaries | 10,770,372 | 10,770,372 | Equal; file length, not allocated blocks |
| Direct load allocation B/document | 8,008.10 | 8,002.27 | -0.073%, effectively unchanged |
| Direct load allocations/document | 3.67888 | 3.67846 | -0.011%, effectively unchanged |

Allocation snapshots from pprof are cumulative process samples including setup/prebuild and subject to GC/sampling lag. Exact sampled totals are 717,304,854 / 469,572,121 bytes and 2,375,248 / 1,628,102 objects before, versus 686,110,301 / 693,299,032 bytes and 1,833,830 / 2,496,443 objects after. Dividing each by 100,000 load documents gives before 7,173.05 / 4,695.72 B/document and 23.75248 / 16.28102 objects/document, versus candidate 6,861.10 / 6,932.99 B/document and 18.33830 / 24.96443 objects/document. These normalized samples still include setup/prebuild and do not establish a load allocation improvement. The archive's `analysis/profile-allocation-normalization.json` and `pprof-total-alloc-bytes.out` preserve exact values and commands. A separately predeclared three-pair diagnostic instead uses observation-only `runtime.ReadMemStats` snapshots around foreground plus settled drain, before post-phase Stats, with no forced GC. The direct numbers above count all process allocations in that bracket, including client/server and concurrent background work, excluding prebuild/checkpoint. Those instrumented timings are excluded from primary performance claims.

The maintained public indexed-update benchmark initially used 1,000 operations per sample. Its three-pair medians were 4.50% faster at one writer (76,952 to 73,492 ns/op; 12,995 to 13,607 ACK/s) and **6.24% slower at eight writers** (28,364 to 30,134 ns/op; 35,257 to 33,185 ACK/s). This concern was retained and investigated with a new bounded five-pair, 10,000-operation control, fixed before collection, using the same opener, official driver, profile and workload. Observation-only counters are read after `StopTimer`.

| Longer warm update guard | Before ns/op | Candidate ns/op | Change | Before/candidate ACK/s | Before/candidate B/op | Allocs/op |
|---|---:|---:|---:|---:|---:|---:|
| One writer | 91,951 | 91,457 | -0.54% | 10,875 / 10,934 | 77,836 / 77,819 | 405 / 405 |
| Eight writers | 38,748 | 38,414 | -0.86% | 25,808 / 26,032 | 57,003 / 56,996 | 285 / 285 |

The longer guard passes the predeclared <=5% regression threshold separately for each writer count; the short-run slowdown does not repeat. Every actual long sample reports zero completed grouped publications in its request-to-ACK bracket. This aggregate counter does not establish an exact grouped route or describe later Close/checkpoint work. Go's non-verbose BENCH log truncation hides actual long-sample finalize counters; only calibration finalize counters are retained, and no claim relies on their unobserved values. See [all follow-up observations](control-results.json). No warm speedup is claimed.

## Validation and review

- All 26 macro samples (primary, CPU, read and allocation controls) reopen a fresh process and pass a complete 100,000-document typed BSON census with the same canonical digest, all eight result queries, declared index metadata, cursor/client cleanup and graceful reopened Close. Load-only validation rejects both paired and unpaired update fields; validator self-test passes 14 positive/negative controls. Queries are unhinted and metadata validation does not prove physical index activation.
- New public command-WAL batch regression is RED on base (`candidate scans=1 want0`) and GREEN on candidate, covers two external persistent ValuePtrs, outer leaves and a descriptor alias, exact tracker/full-scan parity, checked original Close without manual leaf sync, reopen reads and checked reopened Close. The direct fixture is not compressed; the macro is compressed.
- New warm retarget/abort test preserves exact counts and visible roots across failure, then requires `ErrRecoveryRequired` after post-append failure. Cold proof tables cover alias, foreign, duplicate, unused and implicit roots; malformed/pointer/delete/range deltas retain fallback. Existing invalid-tracker repair is still exercised explicitly.
- Full local `TreeDB/db` passes (433.682 s), and `TreeDB/collections` (531.555 s) plus `TreeDB` (605.056 s) pass with GOMAXPROCS=2/GOMEMLIMIT=4GiB and a 15-minute package timeout. The earlier combined 600-second wrapper timeout is retained as an unsuccessful attempt. Focused ordered-root/durable-reference tests, publication race checks, affected vet, CI impact and workflow contract checks pass.
- Independent read-only source review accepts the runtime/test blobs. Review prompted removal of manual leaf sync and tighter required-evidence wording. Real injected census-error coverage through all four unrestricted callers remains a nonblocking test gap; their error guards were inspected directly. Independent read-only review also accepts the observation-only controls. Hosted review and exact-head CI status are recorded on the PR, separately from these local results.

The canonical macro ignores its original handle's deferred Close return; successful macro runs alone do not certify that return or crash/power-loss safety. The fresh reopened handle's cleanup is checked, and the separate direct regression checks original Close/reopen. Existing recovery/GC suites remain applicable. No MongoDB-relative or new README benchmark result is asserted here.

## Raw evidence and reproduction

[raw-evidence.tar.gz](raw-evidence.tar.gz) contains every retained run, raw benchmark JSON, storage snapshots, fresh reopen census/cleanup receipts, CPU/heap/alloc/goroutine pprof files, pprof reports, command receipts, build/source/overlay hashes, frozen plans, reviewed recipes and local test outputs. [SHA256SUMS](SHA256SUMS) seals the published packet; the archive contains `artifact-inventory.json` with every member's SHA-256. Binaries and removed benchmark DB directories are excluded. Full local artifact root: `/Users/michaelseiler/Documents/gomap-bulk-speedup-20261113T003038Z`.

After extraction, profiles can be inspected directly:

```sh
go tool pprof -top runs/final-cpu-p1-before/profiles/load_insert_many.cpu.pprof
go tool pprof -top -focus=scanCandidateExternalReferences runs/final-cpu-p1-before/profiles/load_insert_many.cpu.pprof
go tool pprof -top -sample_index=alloc_space runs/final-cpu-p1-candidate/profiles/load_insert_many.allocs.pprof
```

To reproduce on this machine, use a clean worktree at the frozen runtime candidate, copy the archive's `recipe/`, `analysis/final-plan.json` and `analysis/control-plan.json` into a **new empty artifact root** containing `bin/`, `setup/`, `runs/`, `analysis/`, and set `GOMAP_BULK_REPO` and `GOMAP_BULK_OUT` to those new paths. The scripts pin the toolchain; `recipe/build_final.py` builds the exact-base overlay and candidate plus validator and runs validator self-tests. After confirming no owned tests/builds run, create `setup/timing-window.json` with `{"owned_builds_tests_running":false}` and execute `recipe/campaign.py`. `build_controls.py` and `controls.py` reproduce the separately reviewed allocation and longer warm controls; the latter similarly requires `setup/control-timing-window.json`. Measurement outputs are never overwritten. `analyze.py` and `analyze_controls.py` regenerate the tables from all observations. Individual exact command lines and environment deltas are in each `*.command.json`; tests/builds and timing windows remain separate.
