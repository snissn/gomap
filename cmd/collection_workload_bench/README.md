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
The packet separates persistent payload, redo WAL, and transient SQLite `-shm`
WAL-index bytes; transient bytes are excluded from durable payload size.
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

## R1 mutation width and request-size sweep

`r1-mutation-sweep` is a separate `gomap-r1-mutation-sweep-v1` packet, reusing
the R1 four-string fixture and public generic update route. Its 16 cells vary
bio width 96/4,096 bytes, **actual UpdateBatch request rows** 1/32, indexed versus
unindexed schemas, and bio versus email/city changes. Retained configuration is
4,096 live rows, 100 requests per cell and five fresh database repetitions in
one serial process. Larger populations and concurrency remain deferred.

After this mode is reviewed and landed, with the selected source frozen:

```sh
R1_MODE=r1-mutation-sweep R1_OUT=/retained/r1-mutation-sweep-001 \
  scripts/r1_collection_capture.sh \
  -documents 4096 -operations 100 -repetitions 5 -qualification retained
```

A pre-review diagnostic uses `-documents 32 -operations 2 -repetitions 1
-qualification rehearsal`. Keep it explicitly nonqualifying. Both modes retain
the source manifest, executable/hash/build information, args, host/toolchain,
original packet and producer-local validator result. The helper runs
`r1-mutation-sweep-validate -semantic-only`, which always prints `UNQUALIFIED`,
even when the packet labels its configuration `retained`. It verifies structure
and measured bounds; producer artifacts cannot certify their own provenance.
This sweep does not emit A's `summary.json`.

The acceptance owner must independently freeze the reviewed landing and selected
source/runtime/harness identities before capture, observe the exact build and
successful run, and freeze the executable and original completed packet byte
hashes in a separate receipt. Download/restore the original files and replay with
those externally supplied values (never derive them from the packet being checked):

```sh
./collection_workload_bench r1-mutation-sweep-validate \
  -source-manifest independent-expected-source.json \
  -expected-commit "$SOURCE_COMMIT" \
  -expected-runtime "$RUNTIME_SHA256" \
  -expected-harness "$HARNESS_SHA256" \
  -expected-landed-tooling-commit "$LANDED_SOURCE_COMMIT" \
  -expected-binary-sha256 "$OBSERVED_BINARY_SHA256" \
  -expected-packet-sha256 "$OBSERVED_PACKET_SHA256" packet.json
```

All six pins are required unless `-semantic-only` is explicitly selected; the
two modes cannot be combined. This selected route requires source commit to equal
the independently verified landed tooling commit. The validator hashes its actual
`os.Executable()` and the same original packet bytes it decodes, so byte changes
or a rebuilt executable reject the receipt. Receipt verification preserves the
`UNQUALIFIED` label for rehearsals; retained success requires both receipt and
semantic checks. These checks bind the acceptance owner's recorded observations;
they are not cryptographic host attestation or independent proof of landing.

Each acknowledgement observation times request encoding through public durable
ACK. Caller fixture construction, seeding, stats sampling, final `Flush`, complete
row/index verification and close/reopen are outside that timer. All current and
historical email postings and complete city postings are checked after flush and
reopen. The public cached-leaf durable opener and its default background maintenance
are preserved; detailed UpdateBatch statistics are enabled in every cell.
The callback receives the complete current row and returns an encoded complete
replacement with only the selected fields changed. This measures generic row
reconstruction/replacement, rather than a native partial-column setter.

The packet reports ns/request, latency percentiles, B/request, allocations/request,
heap after requests and a separate explicit-flush timer. Required existing WAL,
file-sync, sync and collection publication counters are recorded before requests,
after all ACKs and after `Flush`, with checked deltas. They measure aggregate work
and may include background/asynchronous activity; no individual-request attribution
or invented materialization counts is claimed. A measured zero stays zero; a
missing counter rejects the packet. Go allocations include background activity.
Each serial request changes a field and appends new command bytes; recorded ACK
physical file-sync calls must cover at least the request count and written WAL
bytes must be positive. No field-width-derived write-size formula is asserted.
Heap is neither RSS, peak nor collection-owned retained memory. Restricted `meta.*`
reference preservation remains separately qualified. Original A noisy observations
remain historical, with no current numerical reuse claim. These packets are not
unified-bench profiles or benchprof inputs.
