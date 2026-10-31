# Mongo durable-B point-read attribution — 2026-10-31

Inquiry [#5357](https://github.com/snissn/gomap/issues/5357), scoped under [#5225](https://github.com/snissn/gomap/issues/5225).

The native held-sync control **passes**: an independent `Collection.Get` of an unchanged physical-baseline row completes while another row's actual command-WAL file sync is held and its `UpdateBSONSet` remains unacknowledged. This rejects unavoidable direct sync blocking for this fixture. The cause of the final12 durable-B read penalty remains **inconclusive**; no production optimization is selected.

## What the retained benchmark establishes

The [completed final12 report](../ycsb_mongodb_gomap_final_2026-10-30.md) measured source `8bea490ad5a2d89a14f02f428b4283579a6dd864`, published at `95c6ff3de473786bd98b692375cfdb9cef951668`. The analysis here retains all twelve valid samples. The original rejected attempt and raw evidence remain in the published final12 packet; nothing is retried or replaced.

Medians across three samples per arm/class (latencies are medians of printed per-run means):

| Class | Arm | Mixed ops/s | READ mean us | UPDATE mean us | Load ops/s |
| --- | --- | ---: | ---: | ---: | ---: |
| relaxed | GoMap | 59,245.2 | 251 | 457 | 17,457.9 |
| relaxed | MongoDB | 42,753.6 | 359 | 374 | 39,516.3 |
| durable-B | GoMap | 11,766.6 | 944 | 7,624 | 2,185.0 |
| durable-B | MongoDB | 19,879.2 | 197 | 9,602 | 40,152.8 |

The workload was 100k records, ten 100-byte BSON binary fields, hashed IDs, uniform 95% all-fields READ / 5% one-field UPDATE, 10k operations and 16 clients. Full typed censuses warmed caches before timing. GoMap observed GOMAXPROCS 12 and affinity 0–11 with a supervised 24 GiB RSS cap; MongoDB used its distinct 12 GiB container / 5.6 GiB cache envelope. Preserve the original report's host-noise caveats. These short cells are not sustained or quiet-host measurements.

Durable-B intentionally preserves different timed write requests: GoMap `w:1` with `j` absent under `command_wal_durable`; MongoDB `w:1,j:true`. This is not strict identical `j:true` parity. The original explicit-`j:true` load failure and #5349's separate inconclusive prefix-materialization question remain unchanged.

GoMap's READs account for 69–71% of count-weighted **summed concurrent request latency** in durable-B, not CPU share or additive wall time. Whole-owner counters show maximum observed durable group size one and about 40.7–41.0 seconds aggregate file-sync time, but include setup, load, censuses and untimed work. They cannot attribute the timed read delay or justify group-policy changes. The retained final12 archive contains no pprof/profile/trace members. The older relaxed CPU profile cannot explain this durable penalty.

Derived rows, arithmetic prototypes, input/archive hashes and source-blob bindings are in [prior-analysis/](prior-analysis/). The 28 selected policy/source/harness blobs were identical at measured and inquiry base revisions. This is not equivalence with future main.

## Discriminating control

Base: `95c6ff3de473786bd98b692375cfdb9cef951668`. [source.diff](held-sync/source.diff) adds only the diagnostic test and its reflect import to `TreeDB/native_command_wal_group_test.go`. It makes no production change and keeps default group caps, delay, CRC and durability.

Two no-index native BSON rows each contain ten 100-byte binary fields. After seeding and checkpoint, existing immutable capture metadata proves a `*caching.NativeSemanticCapture`, physical base root 10, no captured/typed sources, and no logical source for the unchanged ID. During the update, the physical root stays 10 and pending contains only the other ID. Thus the unchanged row must take physical-baseline lookup; checkpoint success alone is not the proof.

The changed row enters `Collection.UpdateBSONSet` with one changed same-width binary field. The hook holds nonzero-LSN `ResourceCommandWAL` at `BeforeDependencyFileSync` (observed LSN 3), emitted by the actual captured file sync. It does not hold `group.testBeforeSync`, which would run under capture gates and test a different premise.

While sync is held, unchanged-row `Collection.Get` returns exact bytes within a fixed two-second progress bound, and update completion has not returned. Release permits matched/modified success and nil error. Exact rows pass read-back, checkpoint, checked manager/DB close and fresh reopen. Returned bytes remain unchanged through subsequent work and close; mutation after close does not alter reopened rows. The `cap==len` assertion is only a shape check, not an ownership proof. Release and goroutine joins precede hook removal on failure paths.

The first normal optimized build and coherent batch passed: new control 0.60 s; six named controls 5.05 s total, including independent native updates, failure poisoning, singleton/maintenance, crash recovery and raw held-sync coverage. These are scoped checks, not general crash certification. Commands, logs, resource samples, source/diff/ELF hashes and toolchain receipts are in [held-sync/](held-sync/).

Race check **incomplete**: the single compile attempt was terminated by the unchanged filesystem guard after 42.92 s (child SIGTERM, guard exit 143). No race ELF or test execution exists. Minimum sampled free was 8,643,624,960 bytes, above the hard 8 GiB floor but inside its 64 MiB stop reserve. Packet peak was 591,060,992 bytes, below 2 GiB. Filesystem usage also grew outside this packet; its owner/cause is not attributed. No retry or product correction occurred. [RACE_REPORT.md](held-sync/RACE_REPORT.md) and the raw receipts retain this infrastructure failure; the normal PASS is not promoted to race qualification. Released race cache/tmp were retired under [race-cache-retirement.json](race-cache-retirement.json).

Read-only reflection deliberately binds this prototype to private immutable fixture shape and fails closed if that shape changes. Existing maintained group/fusion tests already cover the general completion mechanisms. Keeping the reproducible diagnostic patch in this evidence packet is simpler than adding this source-shape-dependent inquiry fixture to permanent CI. No production regression or RED bug is claimed.

## Source interpretation and remaining question

The primary gateway read reaches `Collection.Get` → native GetInto → manager Capture/Get. Native logical hits can bypass physical lookup. Physical traversal/decode shares basis `readMu`; pointed-value materialization has physical-value synchronization. Deferred capture close/retirement, scheduling/GC, storage pressure and client/connection behavior remain competing explanations.

The native BSON `$set` path bypasses the legacy update combiner. Publication/writer admission is released before durable completion, and the completion group releases capture/publication gates before `target.Sync()`. Source inspection and the control agree that removing a presumed fsync-held read lock is unjustified. The control does not exclude intermittent contention or bound a public-workload cost fraction.

## Public diagnostic disposition and next priority

The optional one uninstrumented plus one instrumented fresh GoMap durable-B cell was **not launched**. The preferred `/mnt/fast4tb` mount is full. Even after verified released-output cleanup, root free space remains below the original public probe's 10 GiB floor; the normal control retained its separate 8 GiB floor. No resource, query, cache, ACK or durability limit was weakened.

The actual CLI entry is `TreeDB/mongo_gateway/server.go` (`go build ./TreeDB/mongo_gateway/server.go`). It already retains original owner Close via `internal/ycsbevidence.Observer.Close` and original manager FlushAll/CheckpointStorage through the private checkpoint control. `cmd/mongo_gateway_bench` profiling flags do not apply to this CLI. The older external HTTP-marker prototype is a reusable pattern, but does not itself satisfy the current producer/finalization/lifecycle contract. No new concrete, reviewed phase producer or its negative finalization/close controls was completed. Those remain launch gates, alongside storage and host custody; there is no claimed new public profile or speedup.

The next useful question remains phase-scoped public attribution of the durable read penalty on a capacity-qualified host, with separate CPU, mutex/block, scheduling/GC and I/O evidence and original checked-close/checkpoint/reopen receipts. #5357's ordered optional diagnostic recipe is the bounded specification; do not restart the completed Mongo comparison or bypass its maintained launcher's MongoDB refusal.

The #1136 native cache residual inquiry remains **unselected**, because no current public classifier chose physical decode/cache work. Prior local native controls establish cache mechanics and lifetime, not public fraction or an optimization opportunity. #5238 retains history/capacity correctness ownership; #5259/#5162 retain their Dgraph/cache ownership. #5333/#5334/#5335 remain completed qualifications. This accepted inquiry decision does not pass #5225's unmet product performance gates.

## Evidence and reproduction

The normal source SHA256 is `e388a86d51df6339af6ce55e1170d7a44970ca125dfa75ed007b9fa558ac40bf`, diff `3b1cd3715022ea23871700450d650dcd47dc935d023b92e8d3bbdbfbb323e35d`, ELF `9a5ffeb24c6dbc21522461aa4641985fa2bdcf14e7f35f15a89cb8e5bcb1e731`. The unprofiled ELF and cold build cache were retired after source/hash/log retention and no-consumer audit; [normal-cache-retirement.json](normal-cache-retirement.json) records the intentional binary absence from the original unchanged manifest. No profile needs this retired ELF. The original `source.go` is archived byte-for-byte as `held-sync/source.go.txt` so Go package discovery treats it as evidence; the original worker manifests retain their original filenames. The packet-wide manifest covers the archive names.

For the control, create a fresh worktree at the base, apply `held-sync/source.diff` with `git apply`, and use the exact pinned environment/build/test commands in its command receipts. Bind GOROOT explicitly: preflight found an inherited Go 1.25 GOROOT even when selecting the Go 1.26.10 executable. The correction occurred before compilation, not after a failing test. Reproduction must reserve the documented storage budget and requires the offline dependencies or their authenticated equivalents.

For derived latency, stage the original published `report.json` as `prior-analysis/context/report.json` in a temporary copy of `prior-analysis`, then run `python3 prototypes/analyze_final_latency.py`. Authenticate its SHA256 against `context/input-manifest.json`. For owner analysis, authenticate and selectively extract the members named by `context/selected-evidence.json` from the published `raw-phase-evidence.tar.gz` into `evidence/selected`, then run `python3 prototypes/analyze_owner_observations.py`. Raw archive SHA256 is `21081ee3d89a8ef2babb327ca338badbcd6d1e013d214b4f28cc674f6c31d0c3`. Outputs must match the retained JSON/CSV; these scripts analyze original samples and do not rerun them.

The prior reader's checkpoint-envelope KeyError and source-export path error are preserved in prior-analysis evidence and explained in its execution notes. No sample was discarded. Current normal build/test passed on its first attempt. The separate race build was stopped by resource pressure before any race test; its incomplete state is preserved. Sparse-worktree setup initially left source unpopulated; it was materialized and clean before any worker edit or build. Cleanup receipts preserve original manifests and list intentional retired working outputs; raw results, typed readbacks, source bindings, failure records and profile-associated binaries were preserved.
