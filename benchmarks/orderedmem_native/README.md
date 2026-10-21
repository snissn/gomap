# Public ordered-memory comparison

This package measures the maintained TreeDB `mvcc.Store` API and full managed
Badger, rather than the previous standalone memtable prototypes. The coordinator
owns builds and execution slots. Ordinary `go test` only runs fixture checks;
the database workload requires `ORDEREDMEM_NATIVE_CONFIG` and
`ORDEREDMEM_NATIVE_OUT`.

The first baseline is exact production `b03c73fa010987b8ce4c904ff6dca2d2497be393`
with this benchmark package copied into a separate detached worktree. Its
`adaptive` default and explicit `cow_btree` selector are separate fixed
comparators. They use that baseline binary, because changed production wrapper
sizes in the candidate also affect the old selector. The candidate selects
`cow_ordered` and must report both that route and `SupportsLogicalMVCC() == true`.
The optional capability assertion lets the same harness compile on the original
baseline. Full Badger uses the root module's v4.8.0 dependency; this is not the
old raw skiplist control.

Immediately after the first small seed commit, every arm saves its stats. The
candidate must expose positive ordered logical-key and physical-version counts,
before later flushing or checkpoints can drain the generation. Counts sum roots
across generations and are activation evidence, not unique logical-key totals.

| Initial anchor | Foreground operations | Useful work per operation |
| --- | ---: | --- |
| commit1hot251 | 5,003 | One acknowledged mutation among 251 shared keys |
| batch16wide4096 | 5,003 | One acknowledged batch of 16 mutations among 4,096 keys |
| history256x64 | 200,003 | Owned historical point read over 256 keys with 64 retained versions |
| range32wide4096depth4 | 20,003 | Owned historical logical range returning up to 32 visible rows |
| lifecycle5003batch16 | 5,003 | Batch of 16, 1 MiB flush threshold, periodic persistence every 1,000 commits, pinned initial history |
| shared4hot251 | 5,003 total | Acknowledged singleton commit, split across four writers |
| mixed20003 | 20,003 | Fixed 60% reads / 20% writes / 20% deletes; integer remainder follows the saved trace |

Default values are 128 B, keys are nonempty 24 B, and seed chunks are 256
mutations regardless of foreground batch size. Every version has a distinct
key/timestamp payload. Historical seed timestamps arrive in a deterministic
permutation. Shared writers have increasing timestamps per writer and may finish
out of order. Total work is fixed, rather than multiplied by the worker count.
Historical read cutoffs shift after each key cycle, so the primary point-read
trace exercises every key at every retained cutoff rather than repeating one
cutoff per key.
Future value-size, hotness, batch-size and offered-load expansions require a
separately saved plan. These bounded counts calibrate the first public path;
they do not establish an architectural floor or default-replacement acceptance.

Three TreeDB profiles remain separate. `no_wal_fast` uses relaxed MVCC commits
and volatile ordinary acknowledgments; it has no Badger comparison.
`command_wal_relaxed` uses relaxed commits and Badger `SyncWrites=false`.
`command_wal_durable` uses durable commits and Badger `SyncWrites=true`.
Badger conflict detection is disabled and retained-version count is maximal,
matching the maintained Dgraph-shaped managed API adapter. Actual routes,
options, dependency build information and runtime settings are saved per cell.

`run.py` prints a plan by default. `--execute` requires prebuilt binary and full
source SHA mappings plus a fresh evidence directory. It copies and hashes the
actual executables, saves the exact config/commands/environment whitelist,
stdout, stderr, and JSON for every fresh process, and never retries or drops a
cell. `cow-baseline` inherits the baseline binary/source mapping. Pass
`--source-dir` for source inventories; an inventory is not compiler-input
attestation. The coordinator must additionally bind build commands and the
actual source contents. The minimum final performance comparison is five
matched process samples, with arm order rotated across repeats. A missing or
failed cell leaves the campaign incomplete; descriptive complete-pair results
do not qualify it.

The measurement reports open, seed, foreground, final persistence, held-cut
verification/release, close, reopen, complete retained-history verification, and
final close separately. `cycle_ns` includes that complete elapsed cycle,
including validation and inspection overhead. It is not a pure write-service
measurement. Foreground excludes fixture construction, open, seed, final
persistence and reopen. Lifecycle foreground includes periodic persistence.
TreeDB uses public `Checkpoint`; public `Flush` is unavailable. Badger uses
`Sync`, which is a persistence barrier but not the same sealed-root algorithm.
Whole-cycle and phase results must both be inspected so deferred work is visible.

The post-reopen oracle checks every expected retained key, timestamp, deletion
state, and value. Logical ranges select the newest allowed version *before*
filtering deletions. Point absence and tombstone are normalized to the public
Badger point contract, while history validation retains their distinction.
The range adapter borrows TreeDB entries during selection, copies each live
output, and reuses one key-seek scratch buffer. It seeks past each selected
key's older history instead of decoding every skipped version. Badger streams
its selected versions with the default value-prefetch iterator. These public
adapter costs are explicit; this is not a bare iterator-core comparison.
The held-history comparison admits only later timestamps above the read
timestamp: Badger's managed timestamp does not promise a frozen publication cut
against subsequent backfills. Stronger TreeDB publication/backfill semantics
belong in separate correctness tests. No discard/compaction floor is advanced.

Per-operation p50/p95/p99/max are closed-loop service times, including public
call and output checking, not offered-load response times or progress proofs.
Periodic persistence is included in foreground elapsed and recorded separately,
but excluded from individual commit latency samples. Sample allocation and
quantile sorting are outside foreground. Natural-GC allocation counters include
engine background work during each phase. HeapAlloc is not retained memory or
RSS; optional forced-GC retained checkpoints are separate diagnostics and keep
the fixture/oracle live in every arm. Process CPU and Linux I/O deltas also
include background work. Kernel `read_bytes`/`write_bytes` are not proof of
durable device traffic. Resource snapshots also bracket allocation samplers
outside elapsed-phase timing, so their boundaries are broader and short-phase
CPU ratios need particular care. Directory sizes are logical file lengths, not physical
disk allocation. Profiling and forced-GC timings must not be pooled with normal
qualification. `summarize.py` preserves all samples and reports medians/ranges of
matched per-repeat ratios, with foreground and cycle boundaries separate.

For initial correctness smoke, generate a separate plan with one repeat and a
small operation scale. That short smoke does not establish lifecycle rotation
or final performance activation; inspect the full-count lifecycle stats and
profile selected losses only after correctness. Final timing uses at least five
samples per predeclared case and distinct profile, with natural GC and exclusive
coordinator scheduling. The existing meaningful-regression review policy still
applies; this package introduces no universal throughput or allocation target.

`operational_plan.py` preserves the primary plan and produces a separate
40-process lifecycle expansion: five repeats of four arms under each WAL
profile. TreeDB's flush threshold and Badger's memtable size are both 1 MiB;
Badger's explicit value threshold is 256 B. This matches a configured source
budget, rather than total heap, arena reservation, immutable backlog, or block
cache capacity. Badger retains its declared 2 MiB base-table size and five
memtable slots. The default Badger value threshold cannot be used with this
small memtable because it exceeds Badger's derived maximum batch size.

The operational trace requires at least 8 MiB of foreground value payload and
four observed lifecycle events. COW checks foreground rollovers, including
checkpoint seals, and completed handoffs through final checkpoint. Legacy
TreeDB checks completed flush batches/units and checkpoint activity. Badger
checks foreground numeric `.mem` file-ID advancement plus a completed SST
before close. These are actual-work activation checks, rather than throughput
thresholds or proof that each engine's maintenance algorithm is identical.
Raw stats are saved before/after foreground, after final persistence, and after
releasing the held cut. Directory inspection records transient disappearances
caused by concurrent cleanup; file lengths remain an observed snapshot.
