# R1 local indexed row contract

Selected for [R1.1 #5057](https://github.com/snissn/gomap/issues/5057), under
[parent R1 #5056](https://github.com/snissn/gomap/issues/5056). This is a bounded
workload and evidence contract, not a claim that R1 is qualified or that a new
physical row format exists. Current API guarantees remain in [contracts.md](contracts.md)
and [collections-write-domain.md](collections-write-domain.md).

## Selected application

One process owns one collection on a local persistent filesystem. Native explicit
IDs identify user rows. Each row has non-null UTF-8 `email`, `city`, `name`, and
96-byte `bio` strings; the residual document has `id`, integer `age`, floating
`score`, boolean `active`, integer `revision`, and `optional:null` on even IDs. Odd IDs omit `optional`. This preserves null versus
missing without requiring a nullable typed carrier. IDs are `doc-%012d`; emails
are unique; eight city buckets produce nonunique index fanout. No Mongo gateway,
wire encoding, vector, text, arrays, joins, or multikey indexes participate.

The default decision baseline is 4,096 rows, 32-row atomic load and read batches,
1,000 calls per phase, five independent fresh database repetitions, one worker,
warm OS cache, and the `flushed` initial read state. The deterministic fixture is
hashed as its complete JSON row array. Its exact bytes, dimensions, identities,
and timer boundaries accompany every packet; different dimensions are separate
comparisons. Larger fixtures or concurrency are successor evidence decisions,
not implied by this baseline. A small rehearsal uses 16 or 32 rows and is never
performance acceptance evidence.

## Capability matrix at the starting source

| Capability | Retained JSON / template-v1 / BSON | Typed row plus residual JSON | SQLite comparator |
|---|---|---|---|
| Authoritative stored fields | Complete retained document | Required non-null strings in `typed_row_asset`; remaining fields in residual JSON | Complete JSON document, or native string columns plus residual JSON |
| Complete owned point output | `GetInto`; decode stored template/BSON to ordinary JSON | `GetInto` reconstructs declared fields | SQL select owns complete result bytes |
| Complete prepared batch output | `CollectionReadView.FetchDocumentsByID`; template/BSON conversion included | Same API reconstructs owned complete JSON and reports locator/materialization work | Prepared `IN` select; native fields reconstruct complete JSON |
| Indexes selected here | Unique string email; nonunique string city | Same declared string indexes maintained immediately | Same unique email / city indexes; JSON comparator uses stored generated columns |
| Bounded index range | Ascending, positive limit; full retained document API exists | Ordinary `FindDocumentsByIndexRange` currently returns residual only; R1.2 owns parity repair | Ascending `city,id`, limit 10 |
| Baseline range fallback | Quiescent ID selection then prepared full fetch | Same quiescent decomposition; not a concurrent same-snapshot range guarantee | Same decomposition |
| Insert | `InsertBatch`, atomic batch | `InsertTypedBatchWithStats`, atomic batch | One SQL transaction per same batch |
| Update / replace | Generic callback `UpdateBatch` / `Replace`, single explicit existing ID | Generic `UpdateBatch` reconstructs complete JSON then projects changed authoritative fields; replace uses `ReplaceTypedBatch` | One SQL `UPDATE` transaction |
| Atomic mixed missing/existing upsert | No equivalent ordinary public retained-document API: skipped, not emulated | `UpsertTypedBatch`, one command for an existing and a new row | One `INSERT ... ON CONFLICT` transaction over both rows |
| Delete | `DeleteBatch` | Same | Same IDs in one SQL transaction |
| Schema change | Existing index/schema barriers, outside timed fixture | Existing metadata validation; typed carrier must match exact declared schema | Outside timed fixture |

Typed admission is narrower than all possible `ColumnStoreConfig` declarations:
`TypedColumnBatch` accepts required strings and FP32 vectors. This fixture selects
strings only. Nullable/missing typed fields, numeric/bool typed carriers, typed
column string ownership, compound/multikey typed indexes, schema migration,
and general-purpose typed layouts are unsupported here. Numeric/bool/null/missing
residual fields are supported and verified. No #1186 residual subset or new
universal carrier is needed for the selected workload.

## Visibility, ownership, durability, and errors

All selected collection mutations are collection-local atomic commands. Duplicate
IDs and unique conflicts reject before visible publication; a recoverable-frame
failure can be commit-ambiguous for the whole command. Inputs can be reused after
ordinary typed admission returns. Complete results own bytes, including after the
next fetch and read-view close. A prepared read view pins its open-time state;
opening it first drains pending collection writes. It must not be described as a
buffered snapshot that avoids flush cost. Pending ordinary point/index reads keep
their existing collection visibility contract.

`durable` uses TreeDB `command_wal_durable` and verified SQLite WAL with
`synchronous=FULL` (PRAGMA value 2): each successful mutation acknowledgement
covers local crash/reopen recovery, not replica commit. `relaxed` uses
`command_wal_relaxed` for retained-document collections and verified SQLite WAL `synchronous=NORMAL` (value 1). Relaxed packets promise
process visibility without per-ack fsync and are separate comparisons. Typed
column-store writes currently reject relaxed command-WAL admission even with
`benchmark-relaxed` metadata; the whole cell is retained as unsupported, with the
actual rejection. It must not be silently measured under durable settings. The older
SQLite package benchmarks' NORMAL default is not relabeled durable.

There are no automatic ambiguous retries. Duplicate/unique errors after an
uncertain acknowledgement do not establish exactly-once behavior. Source-ID
replacement has its own atomic public typed API, `ReplaceTypedSourceByID`; it is
not emulated by two commands. Mixed churn here intentionally uses separate
acknowledged delete/reinsert commands for every fourth call and a complete
replacement for other calls. Its `rows` denominator records both mutations in a
delete/reinsert call. Standalone delete removes distinct IDs and is checked for
absence; later phases operate on the remaining live set. After every mutation
phase, outside timers, the oracle checks all live unique email-to-ID mappings
and absence of every historical email removed by updates, replacement, or
deletes, including intermediate emails superseded within a phase. It uses
public `FindByIndex("email", ...)` or an exact indexed SQL SELECT, alongside
city range membership/order and complete-document verification.

The benchmark has no concurrent writers. Its ID-range-plus-prepared-fetch route
therefore measures complete rows safely in a quiescent application, but cannot
qualify same-snapshot ranges under concurrent publication. `VisitIndexValueIDs`
is a same-view exact-value visit; a future bounded range capability must be
explicitly implemented and tested rather than assumed. The packet records the
ordinary typed range's residual-only rejection separately from the complete
fallback. R1.2 must remove that rejection through a production parity fix.

## Timer and evidence boundaries

`cmd/collection_workload_bench r1` extends the existing native harness; historical
modes retain their original meanings. Its complete output is ordinary owned JSON
for all engines. BSON conversion preserves ordinary JSON scalars and null/missing membership
without Extended JSON numeric wrappers. Template lookup/conversion and native SQLite row
reconstruction are inside fetch timers. Fixture-to-storage encoding and typed
carrier/residual preparation are inside mutation timers in every cell. BSON
encodes directly without an unused JSON pass. The preallocated oracle ledger
records generated email references; all index lookups and assertions are outside
mutation timers.

- `load`: rows and transactions/batches reported separately.
- `read_state_transition`: selected explicit flush/checkpoint, before ordinary
  public point reads. Buffered ordinary reads therefore precede view-open flush.
- `read_view_open_flush_setup`: view acquisition (including its implicit pending
  write drain) and document materializer / SQL statement setup.
- `first_complete_batch`: first locator/cache/statement population in this view,
  separately timed; earlier public calls may already have warmed shared assets.
- `point_get_into_complete`: ordinary public point acquisition and full output
  conversion; it includes template materializer open/close. SQLite opens its
  complete-row statement for the same one-shot boundary.
- `point_complete`, `batch_complete`: prepared full output; setup and first fetch
  remain visible in the same packet. Outside-timer exhaustive verification warms
  the fixture intentionally; these are component costs, not request totals.
- `range_public_complete`: whole ordinary `FindDocumentsByIndexRange` call plus
  template/BSON conversion, or one direct bounded complete-row SQL SELECT plus
  native reconstruction. Every bounded row is verified outside the timer. The
  starting typed runtime records an explicit residual-only unsupported skip; the
  repaired runtime enables this same phase without a harness rewrite.
- `range_complete`: limit-10 exact city bounds through the existing range API,
  followed by complete prepared output; index selection is timed.
- `update_nonindexed`, `update_indexed`, `replace`, `upsert`, `delete`, `mixed_churn`:
  caller encoding through acknowledgement. Both indexed city/email and residual
  score change. Generic updates return a preencoded complete replacement from the
  callback; this measures the public generic update path, not a metadata-only setter.
  Upsert inserts a fresh ID as well as replacing an existing row, in a separate
  trailing phase after the common workload and storage observation.
- `checkpoint`: public checkpoint / SQLite TRUNCATE checkpoint after common churn,
  before upsert. All cells have the same complete live rows and operation history
  at the storage observation. Unsupported upsert therefore cannot contaminate
  common delete/churn denominators or the compared storage footprint.

Each phase retains call latency samples summarized at p50/p95/p99, throughput,
rows, B/call and allocations/call, heap after the phase, materialization counters,
and public engine statistics at the common checkpoint. `AssetActiveHandles`
is the maximum observed gauge in a phase; work/duration counters are summed. Allocations are process-wide Go runtime deltas
in a serialized harness; they include background Go activity and exclude SQLite C
allocator bytes. Heap is not RSS or retained-memory attribution. Persistent file
bytes and WAL bytes are separate after checkpoint; persistent TreeDB value-log
and typed asset files are included, WAL is never counted as durable payload size.
SQLite's transient `-shm` WAL-index bytes are reported separately as
`transient_bytes` and excluded from persistent payload and redo WAL. Storage
uses logical file lengths, not allocated disk blocks.

Before expensive retained collection, the harness/schema must receive independent
review and land. Freeze the committed runtime source-blob manifest/digest, harness file hashes, source commit,
binary hash, Go/SQLite/CGO compiler versions and flags, host/load, args, and fixture hash.
The runtime manifest includes non-test compiled source under `TreeDB` and
`cmd/internal/treedbstats`, plus `go.mod` and `go.sum`. External dependencies bind
through the module lockfiles and binary build information. Docs, tests, and
retained artifacts are excluded, allowing artifact-only successors to reuse
evidence only when runtime/harness/fixture identities still match; the measured
commit remains recorded. A new compiled or embedded input must extend this
manifest before retained collection. The capture
script emits raw JSON, source/runtime metadata, build/run stderr and summaries.
A source-bound packet validator rejects malformed identities, dimensions,
incomplete cells/phases, runtime digest/blob mismatch, acknowledgement mismatch, ID-only denominators,
unproven typed reconstruction, missing setup, and oracle failures. Successful
small public-path runs plus deliberate packet rejection are this instrumentation
node's test-first exception; production repairs require their owning red tests.

Retained collection uses at least five fresh repetitions. Summarize median and
minimum/maximum; label throughput spread `(max-min)/median > 15%` inconclusive.
Preserve every raw cell and noisy result. Numeric improvement targets and allowed
regressions are selected only after baseline/noise characterization. The primary
metrics are complete public point/batch/range costs and durable indexed mutation
costs; storage, allocation, setup, checkpoint, and unsupported routes are explicit
guardrails. This contract does not mandate beating SQLite or activate layout,
alignment, postings, or compression changes from old profiles.

The implementation successors own their focused contracts in
[R1.2 row reads](https://github.com/snissn/gomap/issues/5058) and
[R1.3 indexed mutations](https://github.com/snissn/gomap/issues/5059). Their
focused canonical specs join the index when those successor packets land.
