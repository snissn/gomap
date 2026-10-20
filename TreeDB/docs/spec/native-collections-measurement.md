# Native collection MVCC bounded cost protocol

Frozen before P3 implementation, 2026-10-17. This is a measurement protocol,
not execution evidence or an acceptance waiver. The complete #5142 packet and
the #5139 qualification contract remain controlling.

The landed comparison baseline is `fb1228aed5a4705248fa26a3d276a2169bd20256`
(P1). P3 construction starts on the compiled provisional P2 contract
`93df9ad6c30e084250709a397fd95fff3c4e9fa6`. Freeze the exact final product,
harness and schema commits before retained qualification; construction timings
cannot qualify that final candidate.

Use Go 1.26.3, normal GC, `GOWORK=off GOTOOLCHAIN=local GOMAXPROCS=2` on
the shared current host. Put temporary files, DB directories, profiles and
logs under `/mnt/fast4tb/gomap-5139-resume-20261017`, with separate baseline
and candidate directories. There is no exclusive-host timing claim. A dedicated
runner is unavailable; the fallback is bounded alternating fresh processes on
this shared host. Record runner, cache and durable-storage availability in the
final evidence packet rather than assuming an external service exists.

Run three alternating baseline/candidate process pairs at exact counts. Keep
every repetition and first failure; report individual ratios and host noise,
not just the best result. Profile in separate diagnostic processes. Do not
force GC inside timing. Record resolved options and actual route counters;
configured COW or DATA options alone do not prove activation.

Equivalent existing public paths use the same supplied-ID JSON fixtures,
10,000 rows of 256 bytes, batch size 100, zero and two scalar indexes, across
durable command WAL, relaxed command WAL and production NoWAL. Time ordinary
ACK separately from a final checkpoint/clean drain. Existing collection
benchmarks are useful controls, but backend-only runs do not prove selected
public COW activation. Use the same public fixture harness on both revisions;
an unsupported baseline cell is an enabled-cost result, never a speedup.

The bounded selected public raw control is unified-bench `write_seq,batch_write,
random_read_parallel_acquire_snapshot,full_scan,prefix_scan`, 10,000 keys,
256-byte values, two read workers, `-read-require-hit`, explicit
`-treedb-memtable-mode cow_btree`. Select `treedb_public_command_wal` with
`durable` and `wal_on_fast`, and `treedb` with `fast` for NoWAL. Reverify
resolved profile definitions at the final source. Unprofiled timing and
`-profile-dir` diagnostics are separate runs.

New collection transaction, catalog and typed behavior reports enabled cost.
Use 64 and 128 collections; independently grow manifest/asset entries by 2x
while fixing touched domains, row payload and source count. Measure capture,
retain and final release, one-domain updates, cross-collection updates and
checkpoint handoff with zero pins and one old view spanning three cuts, then
release and drain. Typed writes use only durable WAL and the existing supported
string/float32-vector layout; relaxed/NoWAL refusal is a correctness gate.

Report B/op and allocs/op, copied bytes per batch, live/retained/peak heap and
RSS, filesystem allocated bytes, accepted queue/debt bytes, successful commits,
conflict reasons, ACK and full-drain throughput/latency, p99 latency, commit
lease hold/wait, checkpoint pause, WAL bytes/syncs and native route/materializer
counters. Descriptor path sharing must be demonstrated as catalog size grows.
Record memory after the old view closes and full cleanup finishes. Separate
setup, warmup, timed operations and cleanup.

No numerical improvement target is invented. Material unexplained regressions
require profiling and repair or explicit acceptance of minimized residual cost.
Focused correctness and bounded diagnostics precede expensive retained runs;
reviewed/landed harness and exact source bindings precede retained qualification.
