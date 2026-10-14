# Original ordinary PRIMARY construction custody (#5111)

This snapshot repairs original COW constructor custody and production PRIMARY
failure-cut instrumentation on published base
`54893f48cb2a7a195b1b89198a39c87a9a1919c4`. The SAME DurableRootTransaction,
registry, resource-owner cell and primaryArenaOwnerV5 remain authoritative.
No new transaction scheduler, WAL, publication lifecycle or accounting discount
is introduced. Public command-WAL backend selection is a subsequent packet.

## Source-bound defect

Immutable original inquiry operands at
`a468b6be387f03ea5393311196f82af91df4f972` under
`docs/agent-execution/mvcc-4871-20261008/evidence/cow-constructor-identities-c014`
were recovered byte-for-byte; all 19 manifest entries verified. Rebinding only
absolute overlay paths reproduces three superseded original transactions with
PRIMARY Pending=7680 after public Close. The same inquiry against this snapshot
drains PRIMARY, registry and public accounting to zero. This is instrumented
original-object diagnostic evidence, not uninstrumented performance acceptance.
Its original transaction overlay replaces the new atomic alias-detach method;
product tests separately prove that method using actual production objects.

## Custody and failure contract

* Borrowed basis roots never enter the two private custody cells. The private
  zipper clone records the SAME configured factory output in those cells;
  it allocates no per-Apply factory closure or replacement transaction.
* A predecessor is disposed only after the successful successor seals its
  genuine immutable component closure. Shared components retain actual edges.
* Bind/Detach changes the existing transaction phase and transfers authority.
  Private group cleanup cannot abort a bound, accepted or released transaction.
  Existing acceptance remains irreversible and observable after Close.
* Original Abort atomically removes the mutable construction alias and its
  root edge. Surviving immutable readers retain the bank/component closure.
  Refusal changes neither alias nor edge; physical release drives refunds.
* Apply owns its supplied original; Copy/Build own their local original. Failed
  disposal is attempted once and its actual error propagates. The caller retains
  the exact output and error, rather than retrying a partially completed Abort.
* After failed-next disposal, group cleanup stops before disposing its current
  predecessor. The SAME physical owner retains the group, both originals,
  remaining DATA tracker debt, registry and necessary pins. It releases the
  captured basis read and serialization/builder/teardown locks; repeated Close
  returns the retained receipt and shutdown cannot refund failed physical debt.
* Sibling producer and activation consumer failures retain the exact original
  transaction on a preborn intrusive edge in the SAME physical owner. The
  existing runtime closes DB write admission. An explicit transfer receipt stops
  outer preparation defers from releasing bound candidate custody after failure.
  Completed cleanup steps stay completed; remaining original resources and debt
  stay retained, with no replacement owner or deferred second cleanup lifecycle.

Owner and transaction size-based admission includes their enlarged containing
objects. Disposal lock order is group -> transaction -> arena. Failure linking
separately takes physical-owner -> transaction failure-edge mutex; it never
acquires the transaction lifecycle mutex or calls resource cleanup. Physical
owner shutdown drops its mutex before arena close, never takes the group mutex,
and reports retained cleanup errors. Ordinary release work is not claimed as
bounded native reclamation.

## Actual authority cuts

The existing PRIMARY authority callback writes a fixed three-page capsule into
slot 2 or 5 of `index.db.primary`. BeforeMetaWrite runs before that actual write
and returns mutated=false on refusal. Existing beforeFence runs AfterMetaWrite
and BeforeMetaSync after the complete install. AfterMetaSync runs only after
successful actual fence. All later failures remain mutated=true/ambiguous.
ResourceMeta denotes the authority role and carries the pinned PRIMARY file
identity and actual byte range. No fictional DATA metadata write is emitted.
The disabled observer exits before constructing the event path/closure.

## Snapshot validation

Raw operands, source bindings and receipts are in the issue-owned sibling
`evidence/ordinary-5111` directory. Final focused commands use Go 1.26.4,
GOWORK=off, GOMAXPROCS=2 and task-owned TMPDIR.

| Receipt prefix | Evidence |
| --- | --- |
| original-red | Original public-close diagnostic RED: three originals, Pending7680 |
| product-red | Product supersession/abandonment/retained-reader REDs |
| custody-original-diagnostic | Rebound immutable inquiry normal, final Pending0 |
| custody-original-diagnostic-race | Same original-object inquiry race |
| custody-cut-normal | Actual original group/sibling/bound-failure/authority tests and all 36 existing public cut rows |
| custody-cut-race | Same focused tests and all 36 public cut rows under race |
| baseline-known-public-cuts | Exact548 original four NoWAL meta cut REDs; repaired by actual authority hooks |
| baseline-leaf-race | Exact548 no-rotated-leaf RED at cow_public_review_regression_test.go:308 |
| final-affected-race | Broader race passes internal/db seams, retains that leaf RED |
| final-affected-normal | Full normal internal suites pass; db package 240s timeout late in existing standalone leaf test |
| baseline-standalone-normal / candidate-standalone-normal | Isolated late-timeout test passes on exact baseline and candidate |
| dispatch-diagnostic | Merged command-WAL harness has zero original constructor dispatch; diagnostic, not cost acceptance |

The source-bound baseline leaf RED is not a waiver of the required caller/leaf
lifetime gate. Broader qualification and existing numeric/platform/matrix/fault
and performance gates remain required after the coherent selection packet.

The merged public cost harness leaves IndexPrimaryDirectory unset. Foreground
C1/C3 composite cuts deliberately need no backend PRIMARY constructor;
DirtyCheckpoint zero dispatch identifies the actual selection gap. The public
Options type already aliases db.Options, so the option is publicly settable.
No new foreground publisher, forced checkpoint on ACK, prepared WaitThrough
removal or performance claim is part of this snapshot.

## Unavailable source and remaining integration

The unpublished 31-file `8f20fd204` packet is unavailable and has no verified
path inventory here. These independently repaired group/zipper/transaction/
arena-owner/activation seams may overlap it; exact-byte reconciliation is
required if recovered. No missing source was reconstructed, credited or
replaced from prose. Value/leaf cleanup code remains untouched. Native recovered
bytes, native bounded pruning acceptance, production command-WAL selection,
successor basis/path proof, mature public lifetime and retained cost gates are
separate remaining obligations. This snapshot is not PR5127 acceptance.
