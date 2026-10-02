# Public snapshot-rotation coalescing decision (#4916)

Retain existing coalescing defaults. The widened existing budgets show repeatable
physical-work and interval benefits for inline values with one checkpoint, but
sampled read p99.9 remains an unresolved guard. This evidence supports neither a
broad wide-budget selection nor a background-coalescing win. The accepted
independent decision review validates the packet integrity and this disposition.
The [published disposition](https://github.com/snissn/gomap/issues/4916#issuecomment-5955237522)
and [parent reconciliation](https://github.com/snissn/gomap/issues/4893#issuecomment-5955237939)
record the same limits. There is no runtime, policy, default, public API or storage-format change.

Refs [#4916](https://github.com/snissn/gomap/issues/4916),
[#4893](https://github.com/snissn/gomap/issues/4893). The parent physical-work,
persistent-delta, shared-traversal, integration and capacity decisions retain
their separate gates; this disposition can stand independently.

## Scope and controls

Runtime source `e3e912be5501083b9ccb6ae32761bc789cfb21c0`, whole tree
`baf0638c8fa48c62ce27f4085ade7cd981149269`, TreeDB tree
`cce8112f79ac0a765d4612e8382ce68cc2d8ab84`. The [public workflow](README.md#public-snapshot-rotation-supplement-4916)
loads 250,000 keys, updates 40,000 distinct permuted keys (multiplier 7919),
uses 32-byte keys and 256-byte full generation-checked values, and acknowledges
1,000 updates per `WriteSync`. Each checked batch close is followed by a public
snapshot acquisition, owned full-value last-key read and checked snapshot close
before any scheduled checkpoint: 40 boundaries, one snapshot at a time.
Full source oracles, unaffected values/misses, replay/LSN and checked final
closure remain required. This does not establish power-loss durability.

Eight eligibility cells cross pointer threshold 16384 (inline) / 1 (persistent
vlog pointers), checkpoints 1 / 4, and default / wide budgets. Default budgets
are 64 tables / 2,097,152 ops / 512 MiB; wide is 128 / 4,194,304 / 1 GiB.
Flush threshold stays 64 MiB. Actual c2, shard/lane topology, age/pressure,
EWMA/backpressure, checksums, vlog lifetime and CommandWAL closure are unchanged.
The eligibility process is excluded from the three matched fresh-process rounds;
each process runs all eight cells in the same fixed order with a fresh DB per
cell. No counter-only overlay is used for timing.

## What the measurements decide

Only inline/CP1 admitted beyond baseline, both in eligibility and each matched
round: default admits 32 extra tables / 8,000 ops, wide 56 / 14,000. Total minus
checkpoint admitted runs is zero in every cell. Queue max 120 is a cumulative
observation, not an interval queue distribution. The physical deltas are
identical in all three eligible pairs: approximately 10.6% fewer old node loads
and bytes, 10.4% fewer leaf pages/bytes, and 19.8% fewer internal page bytes,
with all 40,000 operations applied. These are logical engine work counters,
not total device-I/O savings.

The eligible interval improved in all pairs (9.96–15.39%); acknowledgment and
checkpoint medians also improve. Its p99.9 median rises 18.28%, with paired
changes −22.03%, +24.98%, +20.94%. Only 4,262–5,784 sampled reads support each
eligible tail (stride 16; about four to six observations at the extreme).
Snapshot cost is included in the interval and separately recorded; its paired
changes are mixed. Neither tail precision nor a read-latency SLO is established.
The allocated-byte/object reduction is a whole-workflow observation, including
reader and public snapshot costs; direct post-reopen one-GC heap is distinct.
Go B/op and allocs/op have a different boundary from process allocation deltas.

Keep the failed controls: inline/CP4 has no extra admission, +4.88% acknowledgment
median and mixed timing/tails; pointer/CP1 has no extra admission, +3.16% heap,
p99 worse in all pairs and +74.81% p99.9 median; pointer/CP4 has no extra admission
and +3.06% heap with mixed read/ack guards. Pointer/CP1's p99.9 spread is extreme
(r3 default 8.861 ms versus wide 4.936 ms). These controls cannot attribute their
mixed timing or allocation changes to extra coalescing. Failed guards remain
in the tables; noisy timing is not a general coalescer rejection.

## Canonical arithmetic

All matched medians use n=3 per budget; paired columns preserve r1/r2/r3 order.
Change is `(wide median/default median − 1) × 100`; paired change uses each
matched pair; spread is `(max−min)/abs(median) × 100` per budget. MiB is `2^20` B.
Interval includes stop/join after the final checkpoint; ack/checkpoint/snapshot
are separately measured. Read max uses every completed read; p99/p99.9 use
stride-16 samples. Physical rows show interval after−before counter deltas.

<!-- canonical-tables:start -->
| Eligibility cell | Admitted runs Δ | Checkpoint Δ | Background Δ | Extra tables Δ | Extra ops Δ | Cumulative queue max |
| --- | --- | --- | --- | --- | --- | --- |
| pointer=1/checkpoints=1/wide=false | 0 | 0 | 0 | 0 | 0 | 20 |
| pointer=1/checkpoints=1/wide=true | 0 | 0 | 0 | 0 | 0 | 16 |
| pointer=1/checkpoints=4/wide=false | 0 | 0 | 0 | 0 | 0 | 20 |
| pointer=1/checkpoints=4/wide=true | 0 | 0 | 0 | 0 | 0 | 16 |
| pointer=16384/checkpoints=1/wide=false | 1 | 1 | 0 | 32 | 8000 | 120 |
| pointer=16384/checkpoints=1/wide=true | 1 | 1 | 0 | 56 | 14000 | 120 |
| pointer=16384/checkpoints=4/wide=false | 0 | 0 | 0 | 0 | 0 | 32 |
| pointer=16384/checkpoints=4/wide=true | 0 | 0 | 0 | 0 | 0 | 32 |

| Matched cohort | Metric | Default median | Wide median | Change % | Paired r1/r2/r3 % | Spread default/wide % |
| --- | --- | --- | --- | --- | --- | --- |
| pointer=1/checkpoints=1 | Interval ms | 1418.303 | 1370.779 | -3.351 | -0.13/-7.93/-3.35 | 5.69/2.54 |
| pointer=1/checkpoints=1 | Ack ms | 1210.591 | 1163.737 | -3.870 | +0.30/-2.91/-13.41 | 6.99/8.36 |
| pointer=1/checkpoints=1 | Checkpoint ms | 190.552 | 188.879 | -0.878 | -4.43/-5.19/+40.34 | 27.03/12.93 |
| pointer=1/checkpoints=1 | Snapshot ms | 37.225 | 40.336 | +8.355 | +8.36/-96.55/+252.98 | 126.28/203.33 |
| pointer=1/checkpoints=1 | Allocated MiB | 107.172 | 103.760 | -3.184 | -6.55/-2.53/-2.87 | 1.83/2.69 |
| pointer=1/checkpoints=1 | Allocations | 314850.000 | 284991.000 | -9.484 | -15.10/-11.12/-0.93 | 2.92/16.67 |
| pointer=1/checkpoints=1 | Reopen GC heap MiB | 237.716 | 245.219 | +3.156 | +1.70/+3.59/+2.63 | 4.26/2.89 |
| pointer=1/checkpoints=1 | Read p99 µs | 18.922 | 20.261 | +7.076 | +22.21/+4.05/+6.14 | 3.60/14.23 |
| pointer=1/checkpoints=1 | Read p999 µs | 28.063 | 49.057 | +74.810 | +43.04/+74.81/-44.30 | 31480.70/9983.33 |
| pointer=1/checkpoints=1 | Read max ms | 40.617 | 30.325 | -25.339 | -25.34/-31.85/+28.77 | 36.60/20.52 |
| pointer=1/checkpoints=1 | Reads | 91643.000 | 81466.000 | -11.105 | -16.83/-12.66/-1.10 | 3.03/18.87 |
| pointer=1/checkpoints=1 | Samples | 5727.000 | 5091.000 | -11.105 | -16.83/-12.66/-1.10 | 3.02/18.86 |
| pointer=1/checkpoints=4 | Interval ms | 1797.683 | 1739.353 | -3.245 | -3.25/-3.24/-6.12 | 12.53/15.43 |
| pointer=1/checkpoints=4 | Ack ms | 1093.510 | 1040.140 | -4.881 | -2.33/+2.97/-8.76 | 18.41/17.84 |
| pointer=1/checkpoints=4 | Checkpoint ms | 711.621 | 652.789 | -8.267 | -6.27/-8.27/-1.21 | 16.27/11.73 |
| pointer=1/checkpoints=4 | Snapshot ms | 34.381 | 36.273 | +5.504 | +40.46/-13.71/-12.83 | 69.89/35.98 |
| pointer=1/checkpoints=4 | Allocated MiB | 139.827 | 137.515 | -1.654 | -5.70/+2.36/-6.36 | 10.93/7.39 |
| pointer=1/checkpoints=4 | Allocations | 522481.000 | 534165.000 | +2.236 | -2.67/+6.23/-4.40 | 18.01/15.26 |
| pointer=1/checkpoints=4 | Reopen GC heap MiB | 236.009 | 243.227 | +3.058 | +5.53/+4.01/+2.95 | 1.54/2.92 |
| pointer=1/checkpoints=4 | Read p99 µs | 16.421 | 17.147 | +4.421 | +2.22/-11.67/+15.46 | 20.85/15.25 |
| pointer=1/checkpoints=4 | Read p999 µs | 37.399 | 29.536 | -21.025 | -28.38/-39.84/+1.63 | 69.85/21.73 |
| pointer=1/checkpoints=4 | Read max ms | 30.779 | 30.204 | -1.867 | -17.42/+29.56/+30.93 | 36.30/39.14 |
| pointer=1/checkpoints=4 | Reads | 159579.000 | 164030.000 | +2.789 | -2.74/+6.88/-4.32 | 19.31/16.19 |
| pointer=1/checkpoints=4 | Samples | 9973.000 | 10251.000 | +2.788 | -2.74/+6.87/-4.32 | 19.30/16.19 |
| pointer=16384/checkpoints=1 | Interval ms | 1296.819 | 1147.486 | -11.515 | -14.94/-15.39/-9.96 | 4.24/5.92 |
| pointer=16384/checkpoints=1 | Ack ms | 458.606 | 417.884 | -8.879 | -2.04/-8.88/-1.03 | 20.42/21.04 |
| pointer=16384/checkpoints=1 | Checkpoint ms | 839.917 | 648.364 | -22.806 | -24.66/-18.79/-15.35 | 11.35/19.08 |
| pointer=16384/checkpoints=1 | Snapshot ms | 28.142 | 26.452 | -6.007 | +53.91/-24.77/+88.40 | 76.18/66.00 |
| pointer=16384/checkpoints=1 | Allocated MiB | 87.956 | 77.549 | -11.832 | -13.60/-11.51/-11.82 | 3.15/0.76 |
| pointer=16384/checkpoints=1 | Allocations | 245089.000 | 194349.000 | -20.703 | -26.39/-20.65/-20.70 | 10.39/2.55 |
| pointer=16384/checkpoints=1 | Reopen GC heap MiB | 153.652 | 153.662 | +0.006 | -0.02/+0.01/+0.01 | 1.59/1.57 |
| pointer=16384/checkpoints=1 | Read p99 µs | 57.538 | 58.576 | +1.804 | -3.07/+1.80/+9.61 | 5.82/6.77 |
| pointer=16384/checkpoints=1 | Read p999 µs | 65.068 | 76.964 | +18.282 | -22.03/+24.98/+20.94 | 40.74/14.34 |
| pointer=16384/checkpoints=1 | Read max ms | 20.234 | 20.259 | +0.121 | +43.38/+0.12/-41.44 | 70.72/43.23 |
| pointer=16384/checkpoints=1 | Reads | 87229.000 | 71095.000 | -18.496 | -23.18/-17.71/-18.45 | 11.08/4.12 |
| pointer=16384/checkpoints=1 | Samples | 5451.000 | 4443.000 | -18.492 | -23.18/-17.72/-18.44 | 11.08/4.14 |
| pointer=16384/checkpoints=4 | Interval ms | 1881.472 | 1855.286 | -1.392 | +6.82/-2.18/-2.96 | 2.53/8.04 |
| pointer=16384/checkpoints=4 | Ack ms | 389.419 | 408.420 | +4.879 | +6.63/+3.14/+8.71 | 8.83/5.43 |
| pointer=16384/checkpoints=4 | Checkpoint ms | 1468.644 | 1425.973 | -2.905 | +6.75/-2.91/-4.54 | 4.05/7.26 |
| pointer=16384/checkpoints=4 | Snapshot ms | 34.953 | 16.500 | -52.792 | +10.25/-39.84/-54.93 | 28.54/136.43 |
| pointer=16384/checkpoints=4 | Allocated MiB | 122.896 | 114.952 | -6.464 | -5.34/-14.01/-6.46 | 4.75/4.89 |
| pointer=16384/checkpoints=4 | Allocations | 489511.000 | 496332.000 | +1.393 | +2.09/-14.54/+1.82 | 11.03/7.41 |
| pointer=16384/checkpoints=4 | Reopen GC heap MiB | 150.929 | 149.352 | -1.045 | -1.11/+0.00/-1.04 | 0.00/1.13 |
| pointer=16384/checkpoints=4 | Read p99 µs | 24.734 | 25.775 | +4.209 | +12.13/+10.81/-10.35 | 18.29/10.96 |
| pointer=16384/checkpoints=4 | Read p999 µs | 34.779 | 33.781 | -2.870 | +19.92/-4.10/-9.39 | 12.90/30.37 |
| pointer=16384/checkpoints=4 | Read max ms | 20.185 | 20.151 | -0.170 | -0.12/-12.35/-0.17 | 0.14/12.38 |
| pointer=16384/checkpoints=4 | Reads | 154681.000 | 155788.000 | +0.716 | +1.60/-16.17/+1.67 | 11.91/8.53 |
| pointer=16384/checkpoints=4 | Samples | 9667.000 | 9736.000 | +0.714 | +1.60/-16.17/+1.67 | 11.91/8.53 |

| Inline/CP1 physical work (each of three pairs) | Default | Wide | Change % |
| --- | --- | --- | --- |
| Applied ops | 40000 | 40000 | +0.000 |
| Old node loads | 41214 | 36831 | -10.635 |
| Old node bytes | 168812544 | 150859776 | -10.635 |
| Leaf pages | 40000 | 35857 | -10.357 |
| Leaf bytes | 163840000 | 146870272 | -10.357 |
| Internal bytes | 4972544 | 3989504 | -19.769 |
<!-- canonical-tables:end -->

## Identity and retained evidence

Artifacts are relative to the retained `treedb-quicksilver-graph-20261001` packet
root. Reproduction requires those exact artifacts, not source commits alone.
The freeze binds 2,402 actual compile inputs, 17 modules, normalized build/base
environment, compiler tools and the normal binary. The independent review
rehashed 825 reviewed source files. Remote compiler/dependency bytes are bound
by the preparation recorder; this local document check does not reread them.
System C headers and live filesystem/device state are outside the Go input
inventory. Binary SHA-256:
`1f25c6e9a73ecc719716607fb812dd3a77d30318529d5b7a0aa473c2dd8e6281`.
Harness SHA-256:
`b7128ff9520d802fa99713187cd6ad0a4a34c9e34aa42bf862f3642c668dd7d9`;
capture SHA-256:
`1d050cc24481611b7ba7e4d2d36d841183226b9ae19f78a951876bf21d8e78ab`.
The separate diagnostic freeze
`1220e80289bf11a691f91e83893831ddaed36773164d275eb1f1af4242f7c0db`
is not the timing freeze.

Go 1.26.3 linux/amd64, GOAMD64=v1, CGO=1, CC=/usr/bin/gcc,
CXX=/usr/bin/g++; Go executable/compiler/linker/assembler SHA-256 respectively:
`d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`,
`3bb421d581f60b9ab48c2c73adf5c018557e89af32ad018d9c4417afa4bf90c1`,
`dac2c3179a6efe737181d79cda5e09b03c3df74df83bebd8864b5f0c61ef9cf3`,
`dda91b31b0d42e710d70e714a50e80fef9f0be59f68996c52cce2a079ea3b29f`.
Build command: `/home/mikers/.gvm/gos/go1.26.3/bin/go test -c -o
<normal-prepared>/algorithm-work.test ./TreeDB`. Run command: that binary with
`-test.run=^$ -test.bench=^BenchmarkAlgorithmSparseUpdatesSnapshotRotations$
-test.benchtime=1x -test.count=1 -test.benchmem -test.v -test.timeout=30m`.
Each execution record binds its exact argv, environment, source before/after,
normal freeze, binary, successful exit and independent raw stream hashes.

Frozen environment: GOMAXPROCS=2, GOMEMLIMIT=2GiB, GOGC=100, GOWORK=off,
GOTOOLCHAIN=local, GOENV=off, GOTRACEBACK=single; GOFLAGS/GODEBUG/GORACE empty.
GOROOT=/home/mikers/.gvm/gos/go1.26.3, HOME=/home/mikers,
PATH=/home/mikers/.gvm/gos/go1.26.3/bin:/usr/bin:/bin. Build-cache, GOPATH and
both temporary-directory paths are under `/mnt/fast4tb/gomap-quicksilver-full-20261002`
as recorded in the hash-pinned freeze; all paths and compiler settings must
match preparation. Eligibility plan also binds runner/environment/grant/source
proof hashes. All three matched plans were persisted before the first matched
launch (Unix ns 1790951966639050000); host manifests remain with each packet.

| Retained artifact | SHA-256 |
| --- | --- |
| `4916-e3-independent-matched-decision-review.md` | `5fbd05c848d07ef8875a316a73d67852899d291941e2409eac6887f4ca43cd17` |
| `4916-e3-three-repeat-analysis.json` | `e9d8658b70843133d88e16d25fd2942c6efcd2d6aa067ab55ffc6e4e3c1233cb` |
| `4916-e3-matched-pre-timer-plans.json` | `1cbbec8c8090dd33a0c4a02d1e0419588440226c0af92c8ce8b7e9b53aae7662` |
| `4916-e3-eligibility-plan.json` | `af0d26d95ae77830762bf32f8a41a4e9d073b238f422d5c43fd32601cfe996e1` |
| `4916-e3-matched-r1-plan.json` | `578a10e57068063e4cb1f651d9d8eb5242b1fbe56374405d5d7b7fd4fa296f4a` |
| `4916-e3-matched-r2-plan.json` | `92dc022aa697c77f08a947e0cb36f0424bf7d5fcc39e67e02c5d9a13f148fda3` |
| `4916-e3-matched-r3-plan.json` | `5e35ee6672626f49f4c3db1486749031793ec016414904f650f321b4d9cb6f5f` |
| `4916-e3-full-packet/normal-prepared/freeze.json` | `67b1a3b58251d24a8f801e90800164f18d98a6d0135f94baebead83a0c103248` |
| `4916-e3-full-packet/preparation-anchor.json` | `f9c813e2ceebbd8b2317f4495d598150340b44b1e5247302e1d9abc4e5a3f80d` |
| `4916-e3-full-packet/snapshot-rotations-eligibility-v1/validated.json` | `abcbd0eeb488843e7d57c838750a61155ddde034aba4fac0e1870ca0bdf68d6c` |
| `4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/execution.json` | `d8dc40157c1a1227b7b5b7abe351281bdf4e1ff51c7826f1f7e85c4393884e44` |
| `4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/stdout.log` | `6b7ba4a6c1377d9449d3f208a73cc293b3e28587c443a978b288c5ae88c4d32d` |
| `4916-e3-full-packet/snapshot-rotations-eligibility-v1/capture/stderr.log` | `ec421cd9209c6e5ba29ccc62fbaa72358b53b901176f44fdd5b66986f98ad3d3` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r1/validated.json` | `3f076ffe81924207d982ee1a3f54fa8cd2dd9d9f678aacf8f355837d036c8fc3` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/execution.json` | `7875c6bfe2a9da9045d49e90f5a0233cda4b5082b9a25becd2c112661e068a6f` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/stdout.log` | `312b7fb80a0706c970d67ad17f06cf7f0577ee417f737d40aa972f0a3bd7adaa` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r1/capture/stderr.log` | `410bf3325d1f9a0dcaa9e571c9438abdc1427fe9db784cb8b7c7aab65878c6b5` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r2/validated.json` | `f77228d064aea128869d5f1d0613745755ac3f257cdf774d58494fb14e77bf25` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/execution.json` | `3ffe24529a4bddd61f8c47b059f1568d6b92b2668d66be0abf72137bb7f33be9` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/stdout.log` | `a986e74124c9493f2a2f3c81d90ad2531f84e48e54ca4e3246dd30fb8e6fd74e` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r2/capture/stderr.log` | `2c9693a4637419ad6b32998ae83cba86fdd4ce2019d0b877e9c85213d07dad4f` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r3/validated.json` | `069a45fc85d55464e4bc0bf40077d0697b45156caadd931fffc324476b8f98ba` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/execution.json` | `f245b93df178ab5fcb9fafb4ccea151a9ce280be5226adb85fd69d272df515c9` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/stdout.log` | `0cd28877caa1febe847ff559f18d6f331d2c1013f6e9b346a65b282b9cd3fac2` |
| `4916-e3-matched-packet/snapshot-rotations-matched-r3/capture/stderr.log` | `9b79a8ae216ae2bfa501bf75e2ee38f0b1f4f3e92bdb9a93086af6a0caa1f5b1` |

## Limitations and revisit conditions

Linux host `mikers-B560-DS3H-AC-Y1`, kernel 6.8.0-138-generic x86_64, affinity
0–11, ext4 `/dev/nvme0n1p1`; same boot/mounts across phases. The coordinator's
exclusive runner grant serialized graph-native processes (concurrency 1), but
the shared host was not quiet. Matched load averages rose 1.07→2.06,
2.06→2.37, 2.37→3.00. Cell order was fixed, not balanced; fixtures were warm,
OS-cache coldness was not asserted. Three pairs are descriptive, not a
confidence interval. This is neither larger-than-memory capacity, concurrency
scaling, a general background admission result nor a default-change approval.

Revisit on this full inline/CP1 public workflow with actual lane-eligible backlog
beyond the 32-table baseline, positive extra frontier operations and unchanged
correctness/LSN/source identities. Retain the repeatable roughly 10% old-node/
leaf-work and 10–15% interval reduction as the measured target. Before timing,
persist repetition/order and acceptable tail, allocation, memory and noise
rules; balance budget order on a quiet granted host where possible and obtain
more independent tail support. Require a passing read guard or measured
cause/cost disposition for the +20–25% paired p99.9 increases. A background
claim additionally requires positive total-minus-checkpoint admission under
real age/pressure. The broader algorithm and capacity gates remain outstanding.

Check only retained bytes, source identities and arithmetic (stdlib; no Go or
benchmark execution):

```sh
python3 docs/benchmarks/treedb_algorithm_work_20261001/check_snapshot_decision.py \
  --evidence-root /path/to/treedb-quicksilver-graph-20261001
```

The checker rejects a changed canonical table or any pinned input/raw hash.
