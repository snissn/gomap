# Raft Placement Catalog

Status: normative for the initial `TreeDB/internal/raftplacement` package.

This document records the first executable slices of #3046. The package defines
a v1 placement catalog and a pure route-decision boundary for collection-level
Raft groups plus document-token batch classification. It also accepts token/ring
placement entries as validated internal catalog data and includes token-ring
helpers that validate virtual partition coverage against known groups. It does
not change native-wire read policies, `raftentry` single-group scope handling,
server routing, submitter APIs, meta-group replication, rebalancing, request
fanout, command splitting, or horizontal-scale claims.

## V1 Catalog Shape

The catalog contains:

- a fail-closed catalog format version and required-feature floor;
- group definitions keyed by `raftcluster.GroupID`;
- group members and optional leader hints keyed by `raftcluster.NodeID`;
- collection placements keyed by `{database, catalog, collection}`;
- one owning `raftcluster.GroupID` for each placed collection.
- optional token/ring placements keyed by `{database, catalog, collection}`;
- a full-cover token partition list for each token/ring placement, where every
  partition names a known `raftcluster.GroupID`.

The initial catalog format is version `1.0`.

The initial required feature is
`treedb.raftplacement.collection_groups` at version `1.0`.

Unknown required features, newer major versions, and newer minor versions MUST
fail closed before route resolution is available.

## Validation Rules

Validation MUST reject:

- an empty group set;
- duplicate group IDs;
- empty or duplicate group members;
- invalid group/member/leader-hint IDs;
- leader hints that are not members of their group;
- collection placements that reference unknown groups;
- invalid `{database, catalog, collection}` identities;
- duplicate collection placements;
- placement modes other than `collection`, `token`, or `ring`;
- collection-mode placements that include token partitions;
- token/ring placements that also set a collection-wide `GroupID`.

The v1 resolver maps each placed `{database, catalog, collection}` identity to
exactly one owning `GroupID`. Resolving an unplaced collection MUST fail closed.
Collection-only resolution MUST also fail closed for token/ring placements;
callers must use an explicit token-aware helper to resolve those placements.

## Route Decision Boundary

`ResolvedCatalogV1.Route` accepts a `RouteRequestV1` with an explicit
collection identity and route shape. It returns a `RouteDecisionV1` that carries
the target group metadata, including group members and the current leader hint,
for future submitter adapters. The decision is only a catalog-derived answer; it
does not submit to Raft, open a network route, choose a live leader, or prove
that native-wire or Mongo request routing is production-ready.

Supported v1 route shapes are:

- `collection`: routes collection-scoped requests only when the placement mode
  is `collection`;
- `token`: routes token/ring placements only when the request supplies an
  explicit uint64 token.

Route decisions MUST fail closed for:

- unplaced collections;
- `collection` route requests against `token` or `ring` placements;
- `token` route requests without an explicit token;
- `token` route requests against collection-mode placements;
- unsupported query, scatter-gather, range, or otherwise multi-group shapes.

Token route decisions include the matched token partition metadata. Collection
route decisions do not infer shard keys or scatter across token partitions.

`ResolvedCatalogV1.ClassifyDocumentTokenBatch` accepts an explicit document-token
batch for token/ring placements and classifies it as:

- `single_token`: one token, equivalent to the exactly-one-ID route boundary;
- `same_partition`: multiple tokens resolved to the same token partition;
- `same_group_multi_partition`: multiple partitions owned by one Raft group;
- `fanout_required`: tokens cross Raft group ownership.

This classifier is pure catalog metadata. It does not split commands, submit to
Raft, open network routes, or perform fanout. For non-fanout classes it returns
the single resolved group metadata; for `fanout_required` it returns the involved
groups without selecting one submit target.

## Document-ID Token Rule

`DocumentIDTokenV1` maps a deterministic document ID byte identity to a uint64
token by hashing:

- the fixed domain string `TreeDB/RaftPlacement/DocumentIDTokenV1\0`;
- the exact document ID bytes.

The hash is SHA-256, and the token is the first eight digest bytes interpreted
as a big-endian uint64. The rule is stable across processes and platforms and
MUST NOT use Go's randomized `maphash` seed. Native-wire document IDs use the
bytes in `SectionDocumentIDs`. Mongo gateway document IDs use the already
encoded TreeDB primary-key bytes, not the raw BSON value display form.

## Cluster Submitter Adapter Preflight

Native-wire and Mongo gateway cluster submitter adapters MAY run an optional
route preflight before handing an accepted R3a command entry to the submitter.
The preflight records request-only metadata: catalog route identity, route
shape, target group ID, group members, leader hint, placement mode, and, for
token/ring decisions, the document token and token partition ID. That metadata
is excluded from deterministic command entry bytes and command digests, but it
is binding at the single-group submit boundary. A `SingleGroupSubmitter` for
`group-a` must reject a request whose route metadata targets `group-b` before
command preflight, commit-source invocation, or local apply.

`nativewire.CatalogRouteResolverV1` is a read-only bootstrap and inspection
adapter from `ResolvedCatalogV1` to `nativewire.ClusterRouteTarget`. Its method
is deliberately named `ResolveCatalogRouteV1`: it does not implement
`ClusterRouteProvider` and therefore cannot be composed with a production
routed submitter. Production adapters use
`nativewire.CatalogMetaClusterRouteProvider`, which resolves the same
collection, token, and token-batch shapes from the locally applied replicated
authority and attaches its exact proof. Non-fanout token-batch targets may
include the single resolved group metadata, but adapters still reject
token/ring multi-ID writes before submit until command split or fanout
execution exists.

Native-wire route preflight uses the default database and catalog plus the
collection name encoded in the deterministic command sections. `create_collection`
uses the collection metadata name; mutation commands use the collection-name
ref. Exactly-one-ID `insert_batch`, `replace_batch`, `delete_batch`, and
`update_bson_set` requests carry a document-token route request. Multi-ID
native-wire batches carry a token-batch route request. Collection-mode placements
may accept that request by returning a collection route decision. Token/ring
placements classify the tokens and then fail closed before
`SubmitCommandEntryV1`: same-partition and same-group batches require an
explicit command split, while cross-group batches require fanout.

Mongo gateway route preflight uses the original Mongo namespace: `$db` plus the
command collection name before the gateway flattens it to TreeDB's internal
`db.collection` collection name. Exactly-one-ID insert, update, and delete
writes carry a document-token route request derived from the prepared encoded
primary key. Multi-document insert/delete batches and multi-update commands
carry token-batch route requests when their document IDs are known. Collection
placements preserve collection-mode routing; token/ring placements reject before
submit with an explicit split-required or fanout-required error.

If no route provider is configured, adapters keep the previous submitter
behavior. If a route provider is configured, the provider MUST fail closed for
unplaced collections, missing token/ring token metadata, and unsupported
token-batch classifications. Collection placements may still accept
token-capable single-ID and multi-ID requests by returning a collection route
decision, preserving collection-mode write behavior. Leader hints remain hints;
they are not live leadership proof, read-index evidence, or a network routing
guarantee. Route group mismatches are local not-owned/not-writable rejections;
this layer does not forward writes, fan out token batches, or choose another
network target.

## Token/Ring Catalog Placements

Token/ring placement entries are accepted as internal catalog data for
validation and early integration only. Each token/ring placement contains token
partitions with:

- a unique token partition ID;
- a known owning `raftcluster.GroupID`;
- inclusive `Start` and `End` uint64 token bounds.

Token/ring catalog validation MUST reject:

- an empty token-partition set;
- invalid or duplicate token partition IDs;
- token partitions that reference unknown groups;
- ranges whose start is greater than their end;
- gaps in token coverage;
- overlapping token ranges;
- placements that do not cover the full uint64 token space exactly once.

Resolved token partition data is defensively copied. Mutating exported resolved
placement slices MUST NOT affect later token-aware resolution.

## Token Ring Simulation

The token-ring planner remains a design and validation aid only. `PlanTokenRing`
builds deterministic virtual token partitions over the full uint64 token space
and assigns those logical partitions to known catalog groups in stable group-ID
order. The planner does not attach the plan to a catalog or route requests. It
is useful for exercising #3046 placement math before the single-group HA and
read/snapshot dependencies are ready for production routing.

`ValidateTokenRingPlan` MUST reject:

- an empty token-partition set;
- invalid or duplicate token partition IDs;
- token partitions that reference unknown groups;
- ranges whose start is greater than their end;
- gaps in token coverage;
- overlapping token ranges;
- plans that do not cover the full uint64 token space exactly once.

`ResolveToken` maps a token to its simulated virtual partition only after the
plan has passed validation. `RouteToken` can wrap catalog-backed token/ring
placements in a route decision when the caller supplies an explicit token, and
`ClassifyDocumentTokenBatch` can classify multiple explicit tokens without
choosing a multi-group submit target. Native-wire and Mongo gateway adapters may
use those decisions as fail-closed request preflight metadata only; they are not
live network routing or leadership proof.

## Replicated catalog/meta authority (M4A)

`raftplacement.CatalogMetaAuthorityV1` is the generic M4A state-machine seam
for the catalog. It replaces the unsafe idea that a process-local/static
`ResolvedCatalogV1` can activate ownership. A deployment must designate one
fixed-peer Raft meta group and open it with
`raftcluster.OpenCatalogMetaRaftProviderV1`. Only that provider can mint the
capabilities accepted by `ApplyCatalogMetaCommittedV1` and
`InstallCatalogMetaSnapshotBytesV1`; constructing an authority does not install
a catalog, and neither a local file nor an adapter exposes an activation API.
Every configured meta voter must declare
`raftcluster.FeatureCatalogMetaAuthority` at the supported floor. Provider open
fails before bootstrap, reopen, or participation when any fixed voter is
missing that capability.

Each generation contains a monotonic `Epoch`, the complete catalog, and a
SHA-256 digest. The digest input is the canonical JSON object
`{"format":...,"epoch":...,"catalog":...}` in that field order; the record's
`digest` field is excluded rather than serialized as an empty string.
Canonicalization sorts features, groups, group members, collection placements,
and token partitions; therefore equivalent input has one byte representation
and digest. The command envelope carries `ExpectedEpoch`: exact committed bytes
are idempotent, stale writers, skipped epochs, and different bytes for a
committed epoch fail closed.
Because M4A does not include a migration or rebalance workflow, generation
changes also preserve the topology of every existing catalog entry. An
existing group cannot be removed or change members, and an existing placement
cannot be removed or change mode, collection owner, route key, or token
partitions. Such a committed command returns
`ErrCatalogMetaTopologyChange` before replacing the resolved state; forward
snapshot installation enforces the same rule. Compatible feature/version
metadata, leader hints, and additive groups or placements remain valid. A
future topology-changing generation must first define an explicit workflow
that transfers data, apply progress, and idempotency state before route
publication.
The bounded v1 payload limits command and snapshot bytes, nesting, JSON
objects/arrays, numeric tokens, strings, groups, members, features, placements,
and per-placement plus aggregate token partitions. Command, record, and
snapshot decoders stream-preflight those counts before `encoding/json` may
allocate catalog slices. They also reject duplicate JSON keys, duplicate
catalog identities, truncation, integer overflow, unknown fields/versions, and
non-canonical bytes before publication.

Routed submit/read/lifecycle callers must present `CatalogProofV1{Epoch,
Digest}`. `CatalogMetaAuthorityV1.Route` rejects an unavailable authority,
missing proof, stale/future epoch, or digest mismatch before resolving a group.
`CurrentCatalogProof` returns the exact locally applied proof under the same
read lock as status/route access. It does not contact the meta group, so a
steady-state request adds no meta-group round trip. Loss of the meta leader
blocks new catalog commands; it does not invalidate an already-applied local
generation. Before the first committed generation, or when the application
cannot supply its locally applied proof, route admission remains unavailable.

`nativewire.CatalogMetaClusterRouteProvider` is the corresponding preflight
adapter for collection, single-token, and token-batch shapes. It refuses stale
or missing proofs before request success. On the owner side,
`raftcluster.NewCatalogMetaGroupRoutedSubmitter` is the production constructor:
it re-resolves the request-only route fields against the same applied
generation and requires exact equality for collection identity, route shape,
group, members, leader hint, placement mode, route key, token/partition, epoch,
and digest before group lookup or mutation. It is the only exported routed
dispatcher constructor. There is no zero-validator constructor or production
static-catalog `ClusterRouteProvider`; test-only permissive adapters are
confined to `_test.go` files.

`ExportCatalogMetaSnapshotV1` serializes one canonical record plus its applied
index and exact last committed command. Restore validates the complete
record/digest/command identity before one atomic publication, rejects rollback
and same-epoch conflicts, and preserves exact-command retry identity. The Raft
FSM bounds and installs that byte payload for snapshot/reopen/rejoin.
`CatalogMetaRaftProviderV1.ExportCatalogMetaBackupV1` forces or reuses a
retained HashiCorp snapshot and packages its version, term, index, bounded
payload, and checksum. `RestoreCatalogMetaBackupV1` validates the complete
archive and payload without mutation, requires the current meta leader and a
fresh local authority, waits for the fixed-voter configuration to apply, and
then invokes HashiCorp Raft `Restore`. HashiCorp propagates that snapshot to
followers and commits a no-op before returning. Restore is a disaster-recovery
operation for a fresh cluster only: a live generation rejects an old archive
instead of rolling back. The integration contract verifies leader/follower
behavior, corrupt archive refusal, all three real authorities, exact retry,
feature and route identity, snapshot reopen, failover, and old-leader rejoin.

The conservative failure matrix is:

- a follower rejects catalog mutation with `ErrNotLeader`;
- a node without a known meta leader/quorum rejects mutation with
  `ErrAdmissionUnavailable`;
- cancellation before enqueue is ordinary cancellation, while cancellation
  after Raft enqueue is `ErrCommitAmbiguous`;
- replay of the exact command is idempotent, while stale, skipped, conflicting,
  partial, or mixed generations fail closed;
- owner, member, mode, route-key, partition, removal, or other existing-entry
  topology changes fail closed until an explicit migration workflow exists;
- failover can commit the next generation, and rejoin/reopen restores or catches
  up monotonically;
- unsupported fixed voters fail provider open before local apply or routing;
- missing, stale, future, digest-mismatched, or route-mismatched request
  metadata is rejected before the data-group owner is selected.

The complete test and capacity matrix and reproducible benchmark results are in
[catalog-meta M4A closeout evidence](catalog-meta-m4a-closeout-3970.md).
This issue deliberately does not add live membership changes, rebalance or
migration, a second production Raft topology, or the vector-specific lifecycle
state machine.

### Immutable ACTIVE lifecycle continuity during replica replacement (#4811)

The catalog authority permits an ordinary replacement BEGIN for a non-source,
non-owner group while
an immutable generation is ACTIVE only when no collection or index mutation is
pending and every live lifecycle record is immutable ACTIVE. BUILDING and other
live states refuse. Completion changes the catalog epoch and digest, rebinds
all lifecycle identities (including ABSENT records), and recalculates each
ACTIVE ready-set digest. Source, asset, and READY receipts remain unchanged.
Snapshot restore accepts that epoch transition only with the exact completed
replacement roster and deterministic lifecycle rebind; ordinary catalog
publication remains blocked by live lifecycle state. Activating the lifecycle
feature over older completed replacement evidence is also refused. The fixed-peer
runtime keeps source, router, mutable, and catalog-voter replacement closed.

An authenticated configured immutable ANN owner outside the catalog voting set
may prepare a Nodes-only target with an optional canonical BEGIN
`owner_preparation` identity. It binds the complete configured lifecycle identity,
immutable manifest/placement digests, and current catalog epoch/digest.
Authoritative `RequiredGroups` establishes owner membership even without token
partition metadata. Unmarked owner BEGIN refuses.
A lifecycle-feature-enabled incoming snapshot with pending replacements requires
the complete BEGIN invariant: every live lifecycle record is immutable ACTIVE,
no mutation fence or collection barrier is pending, and canonical source/token
ownership remains compatible with the replaced group. Missing evidence refuses
even if both marker and lifecycle records were erased; no pending replacement
leaves ordinary empty cold imports unchanged. The marker independently caps
committed phases at `add-intent`, including exact retries, cold snapshot import,
forward restore, and direct phase requests; it grants no READY or serving authority.

Before nonvoter enrollment, the receiver verifies the installed native snapshot
and durable command boundary, then checks the actual current FSM database for
the exact definition, source, generation, scoped owner identity, and hosted asset
bytes under the storage barrier. Each preparation inspection requires fresh
authenticated catalog ACTIVE authority. Current semantic leader-tail proof occurs
after enrollment while the target remains a nonvoter; it does not establish
current leader-tail readiness before enrollment. Missing assets or unavailable
fresh authority refuse preparation. The old replica remains a voter. No serving
topology is attached or warmed, no public listener is advertised, and promotion,
removal, and completion remain independently unavailable. A separate authenticated
catalog-member `replacement-owner-warm` control can retain private owner-local
generation/domain searchers on this already prepared nonvoter. A domain searcher
opens through its first physical pack ID and retains all colocated sections;
physical pack count does not equal searcher cache cardinality. Legacy per-pack
graphs retain their existing per-pack opens. Its authenticated
exact target may inspect the existing leader-tail read only for the currently
authorized immutable marked BEGIN at add-intent; ordinary/unmarked and other-node
tail callers remain catalog-member-only. Each explicit warm
requires the exact BEGIN/seed at add-intent, old-voter retention and target
nonvoter membership, the current semantic leader tail, fresh immutable ACTIVE
authority, and the exact current FSM database. Cold initialization captures the
current collection/DB under the storage barrier; it cannot use the startup DB
that native installation replaced. Cache reuse rechecks operation/ACTIVE/DB
authority and opens no topology or endpoint. Consuming completed warm work
rechecks current semantic tail/hosted assets; a prior successful worker is not a
grant for a later poll. Runtime shutdown cancels and waits for the actual worker
before closing its retained source. A failed asset or DB check retires
the source after its worker returns; restart remains cold. Ordinary observations
do not warm it. The source may reuse/create the schema source manager, but no
ANN serving runtime, READY receipt, or promotion authority is created.
This pending operation
retains the existing lifecycle/mutation freeze; completion and owner cutover are
still later work. Static fixed-peer configuration is unchanged.

The explicit `replacement-owner-endpoint` control reuses that retained source
and the installed current FSM DB to open a credentialed private shard listener.
An immutable Nodes-only standby may have a preauthorized shard address, but it
does not enter ordinary owner routing or membership. Listener construction,
authenticated connections, probes and searches require the exact marked BEGIN
at add-intent, fresh ACTIVE, current semantic tail and old-voter/target-nonvoter
roster. Ordinary strong searches retain the local leader ReadIndex contract and
return typed NOT_LEADER with no hits on this nonvoter. The listener grants no
READY, public route, promotion or completion. Restart remains cold until explicit
preparation; refusal closes the request, and a refused preparation retry retires
the listener before its shared cache. Shutdown drains shard requests before
closing the cache and FSM DB.

An explicit catalog-coordinator qualification commit can record one historical
receipt for that exact marked operation while it remains at add-intent. The
authenticated control request accepts BEGIN and a bounded private ANN request,
not receipt bytes. The trusted producer performs the existing private ANN
qualification, then rechecks exact BEGIN/seed, ACTIVE/current roster and the
leader's verified target semantic tail before the dedicated catalog command.
The receipt stores bounded query/result/ReadySet digests, the original proof
issuer/read index, independent target Raft application index and semantic tail;
it stores no query vectors or neighbors. Query identity omits request/cancellation
IDs and transport deadline and canonicalizes the accepted empty live-domain slice.
Exact logical-query retry returns the same owned
historical bytes without another ANN execution or catalog entry. A receipt-only
catalog producer gate serializes concurrent commands under existing bounded control
admission and the earlier caller/query deadline; queued identical retries re-read
owned authority before ANN. This is a per-coordinator serialization ceiling, not
distributed exactly-once execution: catalog leadership changes may duplicate ANN
across coordinators. Leadership-loss or ambiguous submission errors remain typed
failures; an exact caller retry with a live context discovers the current leader
and returns any matching committed historical receipt. Conflict reconciliation
succeeds only after a fresh authoritative read by a still-authoritative producer;
it never returns unfenced local state or replaces the owned proof indexes.
The immutable result digest excludes operational counters, memory and timing. It is not a
current readiness observation. A conflicting query or changed receipt refuses
atomically. Generic advance cannot install receipts, including through raw FSM
apply, and external catalog-publish rejects the dedicated command.
Snapshot admission reserves an extra entry for the first receipt and known
restore rejects receipt erasure or alteration. Cold restore validates bounded
canonical identity/phase/ACTIVE bindings; a privileged raw in-process command or
trusted backup cannot itself prove ANN execution. The receipt never lifts the
permanent add-intent cap, warms restart, grants READY/voting/routes, or permits
promotion/removal. Fresh preparation and actual private qualification remain
necessary after restart. Enabled receipt creation adds private qualification,
post-search authority/tail reads and one catalog commit. Caller encoding is
covered by existing request admission before JSON allocation; producer result
ownership and JSON/hash scratch retain an actual fanout/top-k byte reservation
until submission finishes. These fail-fast reservations overlap the ordinary
qualification transport lease and may refuse requests above local capacity.
Ordinary healthy search/status paths add no work.

An explicit private ANN qualification request carries canonical marked BEGIN
bytes to that endpoint. It obtains the unchanged authenticated original group
leader's quorum ReadIndex proof, then separately waits for the replacement FSM
and Raft application through that index. The leader proof retains its issuer;
the response separately names the nonvoter serving node and uses a distinct
private frame/proof kind that ordinary M5 dispatch/coordinators refuse. The
shared bounded shard search body reuses the retained hosted domain searchers.
Private qualification requires immutable requests with basic statistics. The
private service and authenticated TCP receiver enforce this shape independently
of the client; the receiver counts the actual BEGIN bytes against the caller
request limit before private dispatch or response reservation. Each
qualification reserves its outbound request/response budget before discovering
the authenticated catalog leader and reading the exact marked BEGIN under a
fresh catalog fence. Its issuer must belong to that operation's committed
current roster, including earlier replacement members; removed startup peers
are refused. This adds catalog discovery and one fenced catalog read per
qualification, without a cached membership grant. Canonical BEGIN frame bytes
count toward the caller's request byte limit. The
client validates the requested partition set, HNSW routes, result ordering,
finite unique neighbors, proof/counter accounting and request byte/work limits.
Exact operation, ACTIVE, semantic tail/assets and current DB checks run before
and after ANN; late refusal clears the entire response. Warm cache cannot replace
leader quorum. Pending preparation continues to refuse legal ACTIVE invalidation
producers. This private evidence grants no READY, public route, vote, promotion,
removal or completion, and restart remains cold.

## Vector partition placement (M1)

`VectorPartitionPlacementRecordV1` validates a complete generation-bound
logical partition mapping against catalog-known groups. It binds collection,
index name, SHA-256 index-definition digest, source generation and partition
generation. It is deliberately not a `token`/`ring` placement and does not
route, activate, or fan out a request.

The v1 catalog validates token/ring placement shape and can return explicit
route decisions over that validated catalog. It makes no server-routing,
meta-group replication, rebalance execution, or horizontal-scale claim. Future
production token/ring work needs shard-key rules, query routing contracts,
unique-index semantics, rebalance execution, native-wire and Mongo gateway
integration, and benchmark evidence before those fields can be used to route
live requests.

## Remote consumers without catalog voting

The [fixed-peer TCP runtime](fixed-peer-tcp-runtime-v1.md) exposes bounded,
request-scoped catalog route and complete-metadata validation to configured
nodes outside the catalog voting set. The actual catalog leader obtains a fresh
linearizable applied-index fence and checks its applied authority before each
decision. Leader discovery is observational only. A consumer holds no installed
catalog state, lease, watch cursor, snapshot or apply capability. Failure to
reach fresh authority refuses the operation; previously observed epoch/digest
metadata cannot authorize a new write without a new fence.

Configured immutable ANN owners can also consume fresh catalog lifecycle
checks through this authenticated request-scoped path while remaining outside
`Catalog.Peers`. They host their data group without local catalog stores,
voting, or installed authority. The source holder and router remain catalog
voters. See the [fixed-peer immutable owner contract](vector-partition-raft-v1.md#fixed-peer-immutable-multi-owner-serving-bounded-profile).

After native snapshot recovery replaces an ordinary immutable owner's FSM DB,
only an explicit authenticated lifecycle Warm may retire its old serving topology
and capture the installed collection and DB together. Each installed source,
shard service, status/readiness observation and final response keeps that exact
DB witness; another Warm cannot validate an old handle or in-flight response.
Cold observations and ordinary search do not rebind. Router recovery, mutable
serving, BUILD/Stage and source attestation retain their startup-manager rules.
The captured collection is not a DB lease: source access still uses the existing
storage barrier and fresh current-DB checks. Construction is cancelable; Close
prevents late installation and drains listeners before sources outside the
initialization, storage and FSM locks. This recovery does not grant replacement
READY, voting or public routing authority.

Publication, epoch/digest validation and exact route comparison remain existing
catalog-authority operations. Consumers cannot publish or serve authoritative
catalog reads locally. Fixed membership validation and unsupported topology
change refusals are unchanged. General route and metadata RPCs assume the
existing trusted private network; configuration digests are not cryptographic
authentication. Immutable owner lifecycle consumption requires configured peer
authentication.
