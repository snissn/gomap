# R1 indexed row mutations

Status: selected contract and qualification tests for [#5059](https://github.com/snissn/gomap/issues/5059), under [#5056](https://github.com/snissn/gomap/issues/5056). Final qualification depends on the accepted [#5057](https://github.com/snissn/gomap/issues/5057) fixture, baseline and retained cost matrix. This page does not record a completed performance gate.

The [selected R1.1 decision v1](https://github.com/snissn/gomap/issues/5057#issuecomment-6007333084)
fixture uses caller-supplied external IDs and authoritative non-null
string columns `email`, `city`, `name`, and `bio`, owned by typed row assets.
Retained JSON contains the matching `id`, numeric `age`, `score`, and `revision`,
boolean `active`, and `optional` (present null for even initial IDs, missing for
odd initial IDs). The indexed case adds unique `email` and nonunique `city`
with eight initial buckets. The unindexed case uses the same rows and storage.
The durability profile is `command_wal_durable`.

## Public operation matrix

| Operation | Public API | Result and rejection semantics |
| --- | --- | --- |
| Insert | `InsertTypedBatchWithStats` | Returns IDs in input order. Existing/duplicate IDs and unique conflicts reject the whole batch. |
| Replace | `ReplaceTypedBatch` | Per-input `Matched`/`Modified`; missing IDs are unmatched. Equal typed values and retained bytes are unmodified. |
| Upsert | `UpsertTypedBatch` | Atomically inserts missing IDs and replaces existing IDs. The integer counts existing IDs, including unchanged rows. An entirely unchanged request creates no command frame or new version. |
| Partial row update | `UpdateBatch` | Callback receives the complete current document and returns a complete replacement or a no-op. Per-input `Matched`/`Modified`; missing IDs do not invoke the callback. |
| Delete | `DeleteBatch` | Counts previously present deleted IDs; missing IDs do not contribute. Duplicate IDs reject before publication. |
| Explicit source replacement | `ReplaceTypedSourceByID` | Deletes explicit old IDs and inserts complete typed rows atomically. Counts previously present deleted IDs; insertion wins when sets overlap. |

The typed APIs require every declared column, exactly one non-null string value
per ID for this schema, and retained JSON with no declared paths. Call inputs
may be reused after return. Unique checks use the batch's final owners, allowing
handoffs and swaps. Rejected unique/duplicate batches publish no primary,
secondary, typed row or command-WAL change. Collection-local atomicity does not
extend to multiple collections or distributed transactions.

`TypedColumnBatch` has string and FP32-vector carriers. It does not provide a
numeric, boolean, null or missing native carrier. Those R1 values use retained
JSON and are preserved through typed replacement/upsert. The supported generic
update path can change either declared strings or retained fields, including
transitions between missing, null and non-null retained values. Its complete
document callback incurs reconstruction and replacement work; it is not a
native partial-column mutation.

`UpdateTypedMetadataByID` preserves eligible unchanged typed rows by reference,
but its public scope is `meta.*` paths. R1's selected top-level fields are not
eligible for that API. Existing metadata tests and benchmarks qualify that
separate scope. Retained JSON atomic public upsert remains an unsupported
comparison cell; two calls to delete/insert do not simulate it.

## Acknowledgement, ambiguity and retry

Use the canonical [write path and durability](write-path-and-durability.md),
[write domain](collections-write-domain.md), and [recovery](recovery.md)
contracts. The mutation reuses the existing command WAL and atomic root/asset
publisher. It adds no transaction layer or durable format.

In `command_wal_durable`, success requires a recoverable complete command before
ACK. Root publication and applied-LSN advancement may occur later through the
write domain. Primary/secondary reads and mutation planning use current,
queued, in-flight, then persisted visibility, with newest tombstones suppressing
older rows. `Flush` drains pending publication; it is not a replacement for the
profile's ordinary durable ACK.

An admission/validation or injected pre-append rejection has not accepted the
request. A post-sync error wraps `ErrCommitAmbiguous`: recovery can apply the
complete request even though the caller received an error. Do not blindly retry
it or treat it as rollback. Reopen the poisoned handle, inspect recovered state,
and reconcile the application request before deciding whether another mutation
is necessary. The collection's ordinary conflict retry mechanism explicitly
excludes commit-ambiguous errors. External IDs alone do not turn an arbitrary
mutation retry into an idempotent command.

## Runnable fixture and invariant qualification

`TreeDB/collections/r1_mutation_5059_test.go` is the executable selected fixture.
Run with Go 1.26 or later:

```sh
GOWORK=off go test ./TreeDB/collections -run '^TestR1Mutation' -count=1 -v
GOWORK=off go test -race ./TreeDB/collections \
  -run '^(TestR1Mutation|TestTypedMinimaInFlightPublicationMutation|TestTypedUpsertOverlappingBatches)' \
  -count=1 -timeout=5m
```

The matrix compares complete primary documents and all current/historical
email/city postings against an independent expected-row map before explicit
flush, after flush and after reopen. It checks result order/counts,
unchanged/missing rows, swaps, conflicts, duplicate input rejection, mixed
upsert, input reuse, delete/upsert tombstones, and retained null/missing parity.
Native insert dispatch is checked with typed/indexed-JSON work counters.
Public invalid carrier and retained declared-field tests require no WAL or
visible row change.

The crash fixture runs six real public operations (insert, replace, upsert,
generic update, source replacement and delete), each at three boundaries:
before command append, after command file sync before ACK, and successful ACK.
Each helper exits without `Close`/`Flush`. Reopen must recover exactly the old
state for rejection or the complete new state for ambiguous/acknowledged calls,
with primary and secondary agreement. Applied-frame counters confirm replay for
the ambiguous post-sync cuts; successful ACKs may complete publication before
the process exits and need no remaining frame replay.
This is process-crash evidence, not a physical power-loss simulation.

Reuse existing suites for broader machinery rather than duplicating it:

- `TestTypedMinimaInFlightPublicationMutation` and `TestTypedUpsertOverlappingBatches`: publication overlap and shared admission.
- `TestTypedSourceAdmissionAndAtomicity`, `TestTypedUpsertNoopAndMixedAtomicity`, and `TestTypedSourceMixedUpsertContract`: source semantics and one-frame publication.
- `TestTypedMetadataInvalidBatchRollback4769`, `TestTypedMetadataWALRecovery4769`, and `TestTypedMetadataUpdateNoVectorRewrite4769`: restricted metadata rejection/recovery/reference preservation.
- `TestTypedMinimaReplaySemanticValidation` and `TestTypedSourceNativeAuthority`: replay schema validation and typed source authority.

## Cost evidence boundary

The accepted #5057 comparator owns its fixed-shape fixture and indexed/nonindexed
mutation observations. Its 32-row flag controls load/read batches, not mutation
request size; it does not establish field-width or mutation-batch scaling.
Historical noisy observations remain inconclusive under their original source
identities. Larger populations and concurrent writers are explicit successor
evidence decisions, outside this selected serial qualification.

The sibling `r1-mutation-sweep` mode in the
[collection workload harness](../../../cmd/collection_workload_bench/README.md#r1-mutation-width-and-request-size-sweep)
owns the remaining selected field/row-size and actual request-size measurements:
4,096 live rows, bio width 96/4,096 bytes, request rows 1/32, indexed/unindexed
schemas, and changes to bio or email/city through public generic `UpdateBatch`.
It preserves the four required strings and residual fields, the supported cached
`command_wal_durable` opener, default maintenance, and serial execution. Five
fresh database repetitions contain 100 requests per cell. These complete-row
callbacks measure reconstruction/replacement; they are not native partial-column
setters or restricted metadata-reference updates.

The sweep separately reports request encoding through durable ACK, process-wide
Go allocation deltas, and explicit final `Flush` time. Required existing WAL,
sync and collection publication counters are sampled before requests, after ACKs
and after `Flush`; their deltas are aggregate work, including asynchronous or
background work, not isolated per-call causality. No fabricated materialization
counters are emitted. Complete rows and all current/historical email and city
postings are verified after flush and reopen outside timing. Heap observations
are neither RSS, peak memory nor retained ownership attribution.

Review and land the harness/schema before retained collection, then freeze exact
runtime/harness identities and keep source-bound original packets, binaries and
logs. Restricted `meta.*` reference preservation retains its existing separate
tests and dimension/batch benchmarks. Unsupported cells stay explicit. These
measurements neither establish a speedup nor waive unexplained regressions.
