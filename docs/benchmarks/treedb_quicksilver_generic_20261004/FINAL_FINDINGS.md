# TreeDB generic Quicksilver sprint

**Complete evidence: all 43 predeclared unprofiled cells pass source-qualified validation.**

This sprint made `fast` use authoritative `no_wal_fast`, replaced the fixed
random-4KiB main workload with generic variable keys and values, and landed
three measured optimizations. The final product source is
`17712b9cfcef2b90516419a34ccf3d464984d473`; the before source is
`6137db44b66e0323daba05ae891db44a8055c0fd`, which already includes the new
durability contract. These are before/after performance optimizations, not a
comparison between unsafe and authoritative modes.

The final snapshot also incorporates intervening main changes. The aggregate
comparison measures those two complete sources; individual matched studies and
profiles support the optimization mechanisms rather than an additive attribution
of every change in the final table.

The workload is synthetic and informed by Quicksilver's role. It is not a
Cloudflare production trace or a verified production histogram. Keys mix
namespaces, hostnames and opaque identities with variable lengths and scrambled
interior identities. Primary values use 80% 32–256B, 18% 257–2048B and 2%
2049–32768B, with independently selected structured/opaque content. The held-out
mixture changes key families, size weights and content to 75% opaque values.
It also changes the miss rate from 90% to 70% and the working set from uniform
to 20%. The compression-off, 1% working-set and historical random-4KiB cases
remain controls.

## Results

The final tables use all 43 source-qualified unprofiled
records in `RESULTS.json`. The primary comparison uses three different fixture
seeds at 3M keys, four readers and GOMAXPROCS=12. Seed ranges describe observed
variation; they are not repeat-based confidence intervals. The 10M and
4/8/16/32/64-reader results are after-only capacity/scaling observations.

**AFTER: current 3M primary comparison.** Million lookups/s; higher is better. Mixed and concurrent requests use 90% misses. Each static phase performs 6M requests; concurrent readers run about 8s. Medians over seeds 24/91/2027.

| Engine / ACK | Hits | Misses | Mixed | Concurrent reads + writes |
|---|---|---|---|---|
| TreeDB durable | 0.985 | 1.895 | 1.716 | 0.654 |
| LMDB durable | 2.519 | 3.708 | 3.444 | 3.442 |
| RocksDB sync | 0.376 | 1.848 | 1.225 | 0.953 |
| TreeDB fast (volatile ACK) | 0.979 | 1.848 | 1.690 | 0.704 |

**TreeDB before → after**, same primary workload and ACK policy.

| Mode | Hits | Misses | Mixed | Concurrent reads + writes |
|---|---|---|---|---|
| TreeDB durable | 0.279 → 0.985 | 1.878 → 1.895 | 1.121 → 1.716 | 0.370 → 0.654 |
| TreeDB fast | 0.281 → 0.979 | 1.802 → 1.848 | 1.162 → 1.690 | 0.385 → 0.704 |

**AFTER-only 10M capacity:** held-out seed 173, 70% requested misses, 20% working set, four readers. One observation per configuration; no 10M before baseline exists.

| Engine / ACK | Hits | Misses | Mixed | Concurrent reads + writes |
|---|---|---|---|---|
| TreeDB durable | 0.865 | 1.463 | 1.183 | 0.479 |
| LMDB durable | 2.569 | 3.409 | 3.042 | 2.998 |
| RocksDB sync | 0.086 | 0.219 | 0.136 | 0.137 |
| TreeDB fast (volatile ACK) | 0.915 | 1.550 | 1.226 | 0.489 |

| Engine / ACK | Load seconds | Concurrent CP total seconds | Concurrent composition seconds | Final inventory GB |
|---|---|---|---|---|
| TreeDB durable | 137.328 | 160.492 | 166.218 | 12.656 |
| LMDB durable | 165.258 | 0.005 | 8.000 | 4.521 |
| RocksDB sync | 74.873 | 0.157 | 8.008 | 2.570 |
| TreeDB fast (volatile ACK) | 49.580 | 135.715 | 147.475 | 12.328 |

Concurrent CP calls are timed separately; their sum and writer-inclusive composition overlap and must not be added. An engine completing writes early spends more of the reader window on a quieter state; equal sustained writer overlap is not established. Inventory is apparent file bytes, including retained physical history and logs, not allocated disk blocks or exact live-data size.

**AFTER-only reader scaling:** 3M-key held-out seed173, 70% requested misses, 20% working set, GOMAXPROCS=12. Concurrent million reads/s; one observation per configuration.

| Readers | TreeDB durable | LMDB durable | RocksDB sync | TreeDB fast (volatile ACK) |
|---|---|---|---|---|
| 4 | 0.629 | 3.260 | 0.785 | 0.712 |
| 8 | 1.062 | 5.623 | 1.284 | 1.025 |
| 16 | 1.272 | 6.865 | 1.514 | 1.216 |
| 32 | 1.984 | 6.801 | 1.507 | 1.315 |
| 64 | 1.347 | 6.917 | 1.543 | 1.237 |

TreeDB peaks at32 readers in this series and declines at64. These observed
points do not establish a production-optimal reader count or linear scaling.
Concurrent rates use the reader interval; writer tails and checkpoint work
are retained separately in `RESULTS.json`.


`REPORT.md` retains the full primary before/after tables, sampled p99 and Go
allocation measurements. `RESULTS.json` retains every required configuration,
phase, checkpoint, file inventory, verification count and source/receipt hash.
The four-reader scaling point reuses the matching held-out run; it is not an
extra independent repeat.

## What changed and why

- [O1, PR #4989](https://github.com/snissn/gomap/pull/4989) removes a materializing
  entry pre-read from cached snapshot `Get`. It reuses the existing append-read
  route once, then detaches the result through existing ownership helpers.
  Returned bytes still survive later reads, caller mutation and snapshot close.
  This removes repeated leaf/frame work and most concurrent Go allocation.
- [O2, PR #4990](https://github.com/snissn/gomap/pull/4990) bounds eligible fresh
  auto/balanced compressed frames by 32KiB of actual decoded value bytes, with
  the selected record count as an upper bound. A lookup decompresses fewer
  unrelated values. Existing writer/preparation paths and pooled metadata are
  reused. Oversized singletons, dictionary/template/retained/outer-leaf policies,
  explicit size/throughput overrides and rewrite grouping preserve their
  existing policy. No new format, cache, filter or integrity shortcut is added.
- [O3, PR #4992](https://github.com/snissn/gomap/pull/4992) reuses producer segment
  inventory for additive publication instead of repeatedly scanning the index,
  and uses the existing Linux retained-file barrier policy to avoid redundant
  mapped flushes before `fdatasync`. Destructive/unknown projections, retained
  dependencies and dependency → index → alternate-meta ordering remain intact.
  Flush-only and non-Linux paths retain mapped flushes.

Four matched diagnostic captures compare the O1 source with O1+O2, separately
from unprofiled throughput. For the same 6M successful lookups, sampled flat
Snappy+LZ4 decode CPU falls from 48.79 to 4.98s in durable mode and 49.48 to
5.14s in fast mode. CRC512 samples fall from 11.57 to 2.65s and 11.55 to 2.31s.
These are sampled attribution, not exact per-operation CPU or a new timing
comparison. Other CRC symbols are recorded separately. The original profiles,
benchprof/insight outputs and trace locations remain retained.

O3's six matched 200k-key explicit-sync captures reduce median load time from
67.119 to 12.578s, 5.336×. The required 3M explicit-sync held-out case now
completes full checkpoint/reopen verification in 1350.747s. Its old baseline
was censored at 30 minutes with empty phase JSON, so it provides no finite
throughput or speedup denominator. The final explicit-sync writer composition
still takes 669.423s: destructive overwrite publication remains expensive.

## Durability and benchmark dispatch

`fast` now selects production `no_wal_fast`; `bench_unsafe` is an explicit
benchmark ceiling. Ordinary no-WAL acknowledgments are volatile. Successful
explicit Sync, Checkpoint and clean Close cover completed registered writes and
publish an atomic, dependency-closed recovered root. This coverage concerns
writes completed before the boundary begins. Checksums stay enabled and
the value log remains persistent storage. Independently buffered collection
domains do not promise a global ordinary ACK-order prefix.

Public boundaries drain registered collection managers, including no-op/empty
sync forms, and coordinate active asynchronous preparation/publication. Failure
handling distinguishes retryable pre-meta failure from post-meta poisoning.
Unsupported no-WAL physical column/column_graph mutations fail before admitting
rows. Document, secondary/text index, template/dictionary, value-pointer and
outer-leaf resource cases are covered; full physical column recovery is not
claimed for no-WAL.

The final composed source passes six exact non-skipped public no-WAL recovery
certifications, both authoritative-resource profile certifications, focused
root/GC/ownership tests and current-head required CI. These use a stable-image,
selective-writeback model and public reopen, not a physical hardware power-loss
test.

The realistic suite defaults to ordinary `Commit`, with explicit-sync controls
retained; the historical workload retains its sync default. TreeDB durable
ordinary Commit synchronizes its command journal. No-WAL ordinary Commit is
volatile; explicit CommitSync seals it. LMDB uses durable transaction Commit and
RocksDB enables synchronous writes. Identical method names do not establish
identical native crash contracts or identical checkpoint work.

## Costs and remaining targets

Smaller frames trade read amplification for more frame/header metadata and less
cross-value compression. O2's matched primary value-log inventory increases
about 20%; total final storage changes by −1.3% to +8.8% over primary seeds and
by +10.8% in the fast 1% locality guard. These are file inventories, not allocated
blocks or exact live-data sizes. No free space win is claimed.

Overall before → after primary medians (time: lower is better), distinct from the O2-only costs above:

| Mode | Load s | Initial CP s | Concurrent CP sum s | Final inventory GB excluding WAL |
|---|---|---|---|---|
| TreeDB durable | 41.992 → 39.474 | 4.650 → 2.042 | 24.414 → 21.147 | 2.722 → 2.910 |
| TreeDB fast | 10.472 → 3.813 | 6.127 → 3.851 | 19.817 → 14.605 | 2.470 → 2.418 |

Some barriers get slower. In the matched O2 guards, fast held-out initial
Checkpoint adds 3.580s while load plus initial Checkpoint falls from 7.563 to
7.397s; fast 1% locality adds 0.997s and load plus initial Checkpoint rises 7.11%.
Durable seed2027 adds 0.342s to the initial barrier with greater captured flush
debt, while its concurrent checkpoint sum improves. A durable held-out final
Checkpoint adds 40.827ms despite a faster no-work cached checkpoint; the public
residual's exact cause remains unproven. These observations are accepted scoped
costs, not waived as noise.

Original-scale sync misses add 2.289 Go bytes/read in the O2 comparison, with
zero scratch-pool allocations and more empty maintenance plans. More physical
frame metadata is a plausible contribution; the wider Stats window does not
prove exact allocation attribution. A separate fast seed2027 miss increase is
mostly accounted for by the shared scratch pool. Pool aliases are not summed
as independent allocations.

The full-snapshot compression-off control also retains slower static hits
(1.512 → 1.408M/s, −6.85%) and misses (2.409 → 2.175M/s, −9.71%).
Mixed reads rise 0.64% and concurrent reads rise 37.75%; these do not erase
the static losses. Miss Go allocation rises 9.396 → 15.426B/read. In the
O2-only matched off pair it falls 16.317 → 15.426B/read, while both wider
Stats windows include a GC run and an empty rewrite plan. The new compressed
frame cap excludes explicit compression-off; the retained O1 CPU diagnostic
identifies the unchanged backend receiver for static reads. These observations
limit causal attribution; they do not prove a noise explanation or uniform
static parity. The investigated O1/O2 guard assessments remain linked below.

The 10M durable run spends 166.218s in concurrent writer composition, including
160.492s in public checkpoint calls. Its final inventory is 12.656GB; 10.837GB
is under the outer-leaf value-log directory. This is an after-only capacity
limitation, not a measured regression against a 10M baseline. Coarse publication
timers overlap and must not be added. The exact-source audit identifies up to
three user-index value-pointer projections and two outer-leaf walks in the
destructive fallback as a possible next algorithmic target. Direct scan-count
and function-level profiling should establish their actual cost before changing
it. Reusing exact projected counts is a narrower first option than adding a new
global maintenance cache; preserve registration, cross-root deduplication,
snapshots, GC and recovery closure. Simply bypassing the destructive fallback
would weaken the correctness guard.

## Limits and retained evidence

The runner is a shared i5-11400F host with 12 logical CPUs, 31GiB RAM and NVMe.
Dedicated infrastructure was unavailable. Native runs are strictly serial;
foreign workloads were not stopped. Results include key generation, request
tracking and full-value checks. The initial full oracle/checkpoint/reopen warms
caches, so this is not a cold-cache test.

Concurrent reader windows last about eight seconds, while writer completion can
take substantially longer. Go MemStats stop when readers join, before the
writer-only tail; overlapping writer/harness work is included. Raw data does not
expose completed mutation/checkpoint counts inside that window. Sampled p99 uses
a capped deterministic prefix, not an unbiased whole-duration sample.

Memory controls differ: TreeDB's value-log mapping budget is 1GiB and Go's
GOMEMLIMIT is 2GiB, RocksDB's block cache/write buffer are each 64MiB, and LMDB's
64GiB map is virtual capacity. Native allocations, mmap and OS cache are outside
Go allocation counts; GOMEMLIMIT is not an RSS cap. RocksDB has its native
10-bit Bloom policy; TreeDB's global miss filter is disabled. Native versions,
integrity controls and dynamically loaded library hashes are retained. This is
a comparison of those declared configurations, not equally tuned memory budgets.

Raw captures, diagnostic profiles, source/build/native/loader receipts and the
censored baseline database remain under
`/mnt/fast4tb/quicksilver-authoritative-20261003` on the Linux runner. Completed
raw receipts are also copied under the same-named `tmp/` run root in the local
repository. Four diagnostic traces remain native with verified hashes; they
were not replaced by empty local files. The measured source remains `87eb3445`;
landed binding verifies the full TreeDB tree, all 2131 compiled project inputs
and the protected production harness. Only two pinned README repairs and a
standalone microprofile helper differ. Published compact evidence preserves
both identities.

Compact reviewed evidence is retained unchanged under [evidence/](evidence/):
[landed source binding](evidence/O2-final-landed-source-binding.json),
[composed native correctness](evidence/O2-O1-composed-native-qualification.json),
[O1 guard assessment](evidence/O1-coordinator-performance-assessment.md),
[O1 independent static review](evidence/review-O1-final-static-evidence.md),
[O2 tradeoff assessment](evidence/O2-coordinator-performance-assessment.md),
[matched profile attribution](evidence/O2-O1-matched-profile-summary.json),
[profile native attestation](evidence/O2-O1-four-profile-native-attestation.json),
[capacity source audit](evidence/review-E-10m-publication-audit.md), and
[first25 numeric review](evidence/review-E-first25-numeric.md). Original reviews
retain their examined hashes and outstanding gates at the time of review; final
acceptance records subsequent gate completion separately.
