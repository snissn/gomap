# Native Collection Workload Bench

`collection_workload_bench` measures TreeDB collection operations without the
MongoDB compatibility stack. It is intended to answer whether an operation is
slow before Mongo gateway, wire protocol, cursor, driver, and BSON `_id`
primary-key encoding overhead.

The harness uses native `collections.Collection` calls:

- `InsertBatch`
- `GetInto`
- `FindByIndexValueLimit`
- `FindByIndexRange`
- `Update`
- `Delete`

Document IDs are external native `[]byte` keys such as `doc-000000000001`.
Stored documents still include a normal `id` payload field, but the primary key
is not derived from Mongo `_id` encoding.

## Storage Formats

Use `-formats` to run one or more collection storage formats:

```bash
go run ./cmd/collection_workload_bench \
  -formats json,template-v1,bson \
  -indexes 0,1,2 \
  -read-states buffered,flushed,checkpointed \
  -documents 16000 \
  -batch-size 16000
```

`collections-v1` is accepted as an alias for `template-v1`.

## Index Shapes

The index count is deliberately native and small:

- `indexes_0`: primary root only
- `indexes_1`: unique `email` index
- `indexes_2`: `email` plus non-unique `age`
- `indexes_3`: `email`, `age`, and non-unique `city`

`email_find_one` is skipped for `indexes_0`. `age_range_indexed_limit_10` is
skipped until `indexes_2`. `age_range_scan_limit_10` always runs and measures a
native primary-key scan floor using deterministic IDs and `GetInto`.

## Read State

Each matrix row creates a fresh TreeDB directory, loads the fixture, then applies
one read state:

- `buffered`: read before an explicit collection flush
- `flushed`: run `CollectionManager.FlushAll()` before reads
- `checkpointed`: run `FlushAll()` and `DB.Checkpoint()` before reads

This separates memtable/overlay reads from settled backend reads.

## Output

Text output includes both latency and throughput for every non-skipped phase:

```text
id_find_one        1200.0 ns/op    833333 ops/sec
```

JSON output is available with `-format json` and includes selected TreeDB stats
deltas for each phase.

## Profiling

For now, capture pprof around this command with normal Go tooling, or use the
existing `unified-bench -profile-dir` flow for end-to-end report artifacts. If
this command grows a built-in `-profile-dir`, keep the artifact names compatible
with `cmd/benchprof`.

## R1 complete local row comparison

The optional `r1` subcommand compares the same complete owned rows across JSON,
template-v1, BSON, typed-row plus residual JSON, SQLite JSON, and SQLite native
string columns plus residual JSON. Read the canonical
[R1 indexed row contract](../../TreeDB/docs/spec/r1-indexed-row-contract.md)
for the capability matrix, atomicity, unsupported cells, fixture, and timers.
Historical modes above keep their existing ID-only index measurements.

Run a **nonqualifying rehearsal** in a fresh artifact directory:

```bash
R1_OUT=/tmp/gomap-r1-rehearsal-001 scripts/r1_collection_capture.sh \
  -documents 32 -batch-size 4 -operations 8 -repetitions 2 \
  -qualification rehearsal -durability durable -read-state buffered
```

`durable` verifies SQLite WAL `synchronous=FULL` against TreeDB
`command_wal_durable`; `relaxed` verifies SQLite NORMAL separately. Typed relaxed
admission is currently rejected and retained as an unsupported cell. Atomic
upsert is skipped for ordinary retained-document TreeDB cells. No ID-only cell
is relabeled full-row output. The prepared range decomposition is restricted to
a quiescent single-writer workload; it is not a concurrent snapshot API.
`range_public_complete` measures whole ordinary bounded owned rows (one complete
SQL range SELECT); starting typed output preserves an unsupported residual-only
skip. The repaired runtime enables the same phase.
Storage and engine statistics are observed at the common checkpoint before the
separate trailing upsert phase; unsupported upsert cannot change common live rows.

The producer preserves `source.json`, `source-after.json`, `host.json`, `args.json`, `go-version.txt`,
`go-env.txt`, `cc-version.txt`, `buildinfo.txt`, `binary.sha256`, `build.stderr`, `run.stderr`,
`packet.json`, `validation.txt`, and `summary.json`. `R1_GO` selects the Go binary;
normal `GOROOT`, `GOCACHE`, `GOMODCACHE`, and `GOTOOLCHAIN` overrides apply.
The capture uses persistent files on the selected host and removes its own fresh
benchmark databases only after each cell closes; it never resets a caller DB.

```bash
/path/to/collection_workload_bench r1-validate \
  -source-manifest /path/to/source.json /path/to/packet.json
```

The validator requires the independent expected source manifest and verifies
committed runtime source-blob digest, fixture, cell coverage, full-row denominators, typed reconstruction counters,
acknowledgement class, setup, and ordered complete phases. Raw malformed or
rejected files remain available; do not treat summaries as a replacement for
validation. Summaries retain every repetition and label spread above 15%
inconclusive. All R1 command captures are dedicated artifacts, **not benchprof
inputs**; no built-in `-profile-dir` or unified-bench artifact names change.

Retained baseline example, **only after review, merge, and source freeze**:

```bash
R1_OUT=/retained/gomap-r1-baseline-001 scripts/r1_collection_capture.sh \
  -documents 4096 -batch-size 32 -operations 1000 -repetitions 5 \
  -qualification retained -durability durable -read-state flushed
```

A clean source manifest and five repetitions are necessary but do not certify
review or landing. The coordinator records those external gates. Rehearsal tests
run genuine public routes and deliberately reject invalid packets; they do not
establish a performance target or R1 completion.
