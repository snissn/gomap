# Quiesced whole mutable ANN domain move: initial contract

Status: dormant destination admission checkpoint; full P6 move remains proposed.
This does not complete #4812.
Base: merged P4 ef22ce55b85524de2707453f22c5f8cd6ba89eaa.

The first executable prerequisite is
`TestQuiescedANNDomainMoveEmptyDestinationPublicRuntimeV1`. The corrected base
fixture ran and failed at `open fixed-peer vector collection: collection not
found`. This checkpoint admits an authenticated catalog voter hosting one
configured empty group-c, with fixed canonical source group-a and current ANN
owner group-b. It exposes public NOTREADY health (`dormant_destination`),
refuses search and all public/native/control mutations, and creates no
canonical collection or carrier. Transfer control is not implemented.

The explicit node-local configuration is
`FixedPeerTCPConfigV1.QuiescedANNMoveDestination`, a
`FixedPeerQuiescedANNMoveDestinationV1` with the complete existing lifecycle
`Identity`, `SourceGroup`, `ANNGroup`, `DestinationGroup`, and `MoveID`.
The bounded nonempty move ID follows the existing fixed-peer identity rules
(128 UTF-8 bytes maximum). The role is cloned, persisted and hashed into the
exact local root identity. It is omitted from the shared peer digest so other
configured voters keep identical transport identity.

Admission accepts only the V1 inline schema6 manifest, one disjoint domain,
a fixed canonical collection placement, one distinct ANN owner, and one
distinct locally hosted destination data group with exactly one voter: the
local credentialed node. It requires local catalog voting authority. Paged
schema7, immutable generations, multiple domains, overlap, multiple local
data groups and multiple destination voters refuse. The canonical/current
owner, unknown/nonlocal group, wrong lifecycle/source/index/generation and
uncredentialed node refuse before any listener opens. Missing collection is
never implicit admission; ordinary source/ANN owner startup remains strict.

Existing paired-root admission refuses nonempty roots without their exact
persisted identity. Before starting the first Raft listener, destination Open
opens the normal command-WAL backend once and rejects any applied command-WAL
coverage or key in either user or system root. It retains this DB for the
ordinary FSM/Raft open and Close paths. Reopen rechecks emptiness under the same
exact local configuration; changing the move admission refuses identity reuse.

Dormant public serving has no collection, generation source or backend; it
does not forward search to the existing owner. Generic native metadata/storage
mutations have no attached collection manager/backend. Authenticated HTTP
control admits status, diagnostics, readiness, catalog reads/routing/validation
and group read proof only; publication, submit/forward, vector lifecycle and
replacement mutations refuse before dispatch. Catalog consensus replication
continues normally. The single-voter destination data transport admits only
the configured local identity, and no local ordinary mutation is submitted.
This checkpoint grants no transfer/install/fence/cutover authority.

Checks:
- `TestQuiescedANNDomainMoveEmptyDestinationPublicRuntimeV1`: public status,
  search/insert/native metadata refusal, real authenticated HTTP control
  refusal, public lifecycle refusal, empty reopen and changed local admission.
- `TestQuiescedANNDomainMoveOrdinaryOwnerStartupStillStrictV1`: ordinary
  canonical/ANN owner startup without a source/carrier still fails.
- `TestQuiescedANNDomainMoveAdmissionRejectsInvalidScopeV1`: negative admission
  scope, clone ownership, shared digest stability and absent-role refusal.
- `TestQuiescedANNDomainMoveNonemptyStorageRefusesBeforeListenersV1`: unmarked
  data, unrelated user keys and collection/system state reject before occupied
  Raft/control/public listeners.

`BenchmarkQuiescedANNMoveDestinationOpenCloseV1` measures the enabled fresh
authenticated admission's whole startup/Close with allocations, excluding
fixture genesis and temporary-root setup. Run with `-benchtime=10x`.
`BenchmarkFixedPeerTCPRemoteOwnerCreateV1` measures ordinary legacy HTTP
control dispatch, excluding setup; it covers the nil-role branch, without TLS
or vector search. Neither benchmark measures a domain move, whose transfer
cost, temporary memory/storage and hot-path guardrails remain open.

## Boundaries and capacity

Initial operator scope: one whole domain in one schema6 mutable ANN generation,
one fixed canonical source, one current ANN owner, one distinct already
configured empty destination group. All physical chunks belonging to that
domain move together. Inserts are externally quiesced AND durably fenced;
pending source projection obligations must be settled before capture.

Initial finite proposal: one active move per index; one retained committed move
lineage; total deterministic transfer command payload <= 1 MiB including
logical graph state, assets, history and manifest. Reject excess, unsupported
multi-domain state or an inconsistent frontier before fencing/publication.
Use existing entry and 8 MiB RPC bounds too, checking actual encoded envelopes.
No unbounded packet accumulation, temporary copies or speculative chunking.
This byte ceiling is deliberately small and may refuse practically large
domains; lift only with separately reviewed bounded transfer infrastructure.
Do not evict historical P4 state to fit.

Explicit trusted destination admission is a role bound to expected catalog,
collection/index/generation, source/ANN/destination group and operator move ID.
It opens only existing transport, Raft/FSM and public NOTREADY surfaces.
No serving authority, canonical duplication, Raft-snapshot relabeling or
automatic graph rebuild follows from this admission.

## Deterministic transfer and cutover

Add an ANN-only destination install command to the existing deterministic
command matrix/FSM/WAL/root publication. Its canonical bounded packet preserves
immutable domain chunks, graph adjacency/vectors, stable IDs and deleted-owner
records, live revision and all domain/owner epochs, source identity, exact
logical coverage, and historical target receipts. Rebind destination-local
physical roots without changing logical source identity. Install graph,
coverage and transfer receipt atomically under destination Raft apply.
Identical move ID/payload retries return the same receipt; conflicting retries
refuse before effects.

A separate dedicated catalog move transition validates durable source fence
and destination receipt/frontier, publishes one owner and coherently rebinds
lifecycle/READY authority. Generic catalog publication keeps its current guard.
The runtime adopts committed authority rather than editing configured catalog
bytes or accepting caller proof as ownership. Before cutover, destination
cannot serve. After cutover, the old owner refuses new serving/writes.

Keep canonical rows/source IDs fixed. Preserve all original P4 identity keys
and receipts, including their original group/epoch/digest and Raft positions.
Both the fixed 64 collection identities and 64 completions per identity remain
lifetime limits without eviction. New authority must not reset those limits.
Historical receipt verification requires explicit committed move lineage;
never substitute a destination term/index for an old target commit.
Old visibility tokens receive explicit stale/refresh refusal unless verified
lineage is implemented and tested. No claim of transparent token translation.

Retain old immutable assets/live roots while held readers or durable retry
obligations can reach them. Reclamation uses existing pins and reachability;
never age-based deletion of persistent value-log segments.

## Remaining full checkpoint control

`TestDomainMoveCrashAtEveryCutoverBoundaryHasOneOwnerV1` is unfinished.
It must exercise the real public operator API, completed public insert and
receipt history, source fence, destination atomic publication, catalog cutover,
runtime authority adoption and reopen at every before/after boundary.
Require exact graph/live/revision/tombstone/coverage/history equality,
unchanged canonical rows, one owner per epoch, stale-token disposition,
idempotent move retries, conflicting-move refusal, and held-reader retention.
No test-only runtime or marker/reflection test qualifies.

Canonical source movement, concurrent catch-up, deletion workloads beyond
retained tombstone preservation, domain split/rebuild and automatic placement
stay OPEN under #4812. Before full move acceptance, update canonical storage,
recovery, lifecycle, operations and verification contracts, plus generated
command matrices if command IDs or schemas change; measure enabled move cost,
temporary memory/storage and ordinary-path guardrails.
