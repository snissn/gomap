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
The preflight is adapter metadata only. It records the catalog route identity,
route shape, target group ID, group members, leader hint, placement mode, and,
for token/ring decisions, the document token and token partition ID in request
metadata that is excluded from deterministic command entry bytes and command
digests.

`nativewire.CatalogClusterRouteProvider` is the reusable adapter from
`ResolvedCatalogV1` to `nativewire.ClusterRouteTarget`. It converts collection
route decisions to collection targets, exactly-one-token token/ring decisions to
token targets with partition metadata, and token-batch decisions to
`token_batch` targets that carry the catalog classification
(`same_partition`, `same_group_multi_partition`, or `fanout_required`).
Non-fanout token-batch targets may include the single resolved group metadata,
but adapters still reject token/ring multi-ID writes before submit until command
split or fanout execution exists.

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
guarantee.

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

## Deferred Scope

The v1 catalog validates token/ring placement shape and can return explicit
route decisions over that validated catalog. It makes no server-routing,
meta-group replication, rebalance execution, or horizontal-scale claim. Future
production token/ring work needs shard-key rules, query routing contracts,
unique-index semantics, rebalance execution, native-wire and Mongo gateway
integration, and benchmark evidence before those fields can be used to route
live requests.
