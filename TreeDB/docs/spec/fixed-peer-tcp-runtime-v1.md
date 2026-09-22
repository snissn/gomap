# Fixed-peer TCP runtime V1

`nativewire.OpenFixedPeerTCPRuntimeV1` assembles the existing HashiCorp data and
catalog providers, durable Bolt log/stable stores, file snapshot stores, TreeDB
command WAL/FSM apply stores, and catalog-validated `GroupRoutedSubmitter`.
Only catalog members join the catalog group; other processes consume fresh
catalog decisions remotely. Each process opens only its configured data groups.
Remote submission is a bounded private HTTP/TCP forwarding adapter, not
another consensus implementation or a public native-wire mutation protocol.

## Configuration and lifecycle

Build the small node command with
`GOWORK=off go build ./cmd/treedb-fixed-peer`, then run
`./treedb-fixed-peer -config /absolute/path/node.json` once per member.
SIGINT/SIGTERM closes HTTP admission, Raft providers/transports, FSMs, and DBs.
The TCP stream layer owns accepted and dialed sockets. Close interrupts their
idle reads and cancels pending dials before releasing connection pools; it does
not depend on another node shutting down to release local Raft handlers.
The initial stdout JSON identifies the binary and normalized configuration; it is **not readiness**.
Use `-mode ready` for fresh quorum/apply evidence, `-mode status` for observational
Raft state, and `-mode diagnostics` for process/disk/network counters. `-mode inspect`
validates and hashes configuration without opening credentials, stores, listeners
or cloud resources. `-expected-binary-sha256` refuses a different executable
before any network/store activity.

The JSON is `nativewire.FixedPeerTCPConfigV1` (strict fields, maximum 1 MiB):

| Field | Contract |
| --- | --- |
| `Credentials` | Node-local absolute `TrustRootsFile`, `CertificateFile`, `PrivateKeyFile` PEM paths. The production command requires them; legacy plaintext is an explicit `-trusted-network-test` fixture only. |
| `ResourceLimits` | Local shared connection/request/byte/proposal/snapshot and per-scope capacities. Omitted fields select bounded defaults; impossible group reservations refuse before stores open. |
| `ClusterID` | Optional stable bootstrap identity. Empty preserves the legacy configuration digest and manifest encoding; a nonempty value is part of both. It does not authorize topology edits. |
| `NodeID` | Unique local identity, present in `Nodes`; catalog membership is optional. |
| `Nodes` | Shared list of `{ID, Address}` private RPC endpoints. |
| `Catalog`, `Groups` | Shared `{ID, Peers, BootstrapNode, Features}` records; each peer has `{ID, Address, Capabilities}`. Peers are voters within their own group and must be present in `Nodes`. Catalog peers may be a strict subset of `Nodes`. |
| `ListenAddress` | Exactly this node's advertised RPC IP:port. |
| `RaftListen` | Group-ID to IP:port map, exactly the locally hosted groups and their advertised peer addresses. |
| `DataRoot`, `RaftRoot` | Distinct, absolute, non-overlapping persistent directories for this node. Never share roots between processes. |
| `RequestTimeout` | JSON integer nanoseconds, 1 ms through 1 minute. Applies to RPC/transport/apply. |
| `RaftTimeout` | JSON integer nanoseconds, 50 ms through 30 seconds; heartbeat, election, and leader lease. |

Use canonical numeric IP addresses and nonzero ports; wildcard advertised
addresses, duplicate endpoints/identities/groups, missing bootstrap voters,
unsupported floors, and inconsistent local listeners fail closed. There are at
most 1,024 inventory nodes, 128 declared data groups, 32 locally hosted data
groups plus an optional catalog group, and 32 voters per group. These are
admission limits, not measured deployment capacities. A three-voter catalog
does not grow when data nodes are added. Identity strings are bounded to 128
bytes of valid UTF-8 without path separators, control characters, or surrounding whitespace.
Configuration containers and a conservative 1 MiB escaped-JSON byte budget are
checked before copying caller input or creating stores/listeners. The normalized
shared encoding is checked again. Numeric advertised addresses are parsed without
DNS resolution. The catalog group and all its voters require
`treedb.raftcluster.catalog_meta_authority` V1 alongside
`treedb.raftcluster.single_group_provider` V1. Data groups may use the default
single-group floor. Vector lifecycle requirements are refused in this slice.

Exactly one designated voter bootstraps each group, and only when its persistent
Raft stores have no existing state. Every node must receive identical
`ClusterID/Nodes/Catalog/Groups/timeouts`; their normalized SHA-256 digest is carried on
requests, replies, and status. Roots/listeners/node identity are local and do not
enter that shared digest. The complete local normalized configuration is synced
to `RaftRoot/fixed-peer-v1.json`, then its directory is synced before any provider
store opens; restart requires an exact match. A changed or
corrupt manifest refuses startup. Do not edit manifests or reuse roots to change
membership. No replacement/reconfiguration or format migration is provided.

Manifest contents are file-synced on every platform. The parent directory is
also synced on Unix. Windows follows the existing Raft-store convention: the
file flush covers creation metadata, but directory handles cannot be synced.
This is not a Windows directory-rename/removal durability or snapshot-install
qualification; the snapshot-restore conformance test requires those capabilities.

Data lives in `DataRoot/<group-id>`. Existing provider layout under
`RaftRoot/nodes/<node-id>/groups/<group-id>` holds consensus log/stable state,
snapshots, and durable apply progress/results. Preserve both roots together.
The value log remains persistent storage, separate from the command WAL.

## Route, submit, and failure boundaries

Use `NewFixedPeerTCPClientV1` with the same configuration, then `Status`,
`PublishCatalog`, `Route`, and `Submit`; close the client when done. Publish an
existing encoded `CatalogMetaCommandV1` to the observed meta leader. Publications
must name only configured data groups with exactly their fixed members.

Routing obtains a fresh quorum-backed catalog read from the meta leader. A
catalog voter requires its local authority to cover that applied epoch/digest.
A consumer sends a request-scoped `catalog-route` or `catalog-validate` operation
to the observed catalog leader. That endpoint obtains its own linearizable
applied-index fence before resolving the route or validating complete metadata.
Consumers never create, install, cache, or restore catalog authority. Leader
discovery status is only a hint; it cannot replace the authoritative fence.
Restart or reconnection reacquires authority on the next operation. Ingress and owner re-resolve the
actual deterministic command's collection and validate the exact route metadata.
The ingress dispatches through the existing routed submitter to one TCP owner
leader; the owner's separate local-only registry cannot forward again. Success
requires existing production-consensus and local recoverable apply evidence.

The initial supported mutation slice is existing collection placement. Existing
token/ring multi-ID, query/index, and vector lifecycle admission boundaries are
not widened; no cross-group fanout or partial success is added. A failed proof,
unavailable catalog/data quorum, stale leader hint, or unsupported route refuses
the operation. There is no automatic mutation retry or HTTP redirect following.
Exact retries retain the existing durable idempotency result. A fresh write is
not an idempotency replay and still requires quorum.

Pre-canceled and definite connection-refused requests are not commit-ambiguous.
After a mutation is written, lost/malformed responses or canceled waits are
commit-ambiguous: callers must not infer non-commit. Provider cancellation after
enqueue also preserves `ErrCommitAmbiguous`; a committed command can apply after
the caller stops waiting. No-quorum refusal may occur before enqueue or become
ambiguous afterward, depending on the observed leader lease.

HTTP requests/replies are capped at 8 MiB (including JSON/base64). Each server
admits up to 32 ingress handlers, 32 owner forwards, and 32 non-recursive
status/catalog reads independently. Each client has separate ordinary and read
pools, each capped at 8 connections per endpoint, 32 admitted calls globally,
and 32 idle connections globally (at most four idle per endpoint). Admission
has no waiter queue and rejects before encoding/sending. Endpoint lookup uses
a startup-built node index; no all-inventory scan occurs on each call. This keeps admitted callers
from exhausting the capacity needed by their nested RPCs. Headers, body
reads, writes, idle connections, and requests have deadlines. Invalid/trailing
frames and destination/config mismatches fail closed. Oversized/lost replies
after a mutation remain ambiguous. These are small control-plane/conformance
bounds, not tuned throughput targets. Authoritative catalog handlers use a local
Raft fence directly and never issue a nested catalog HTTP request while holding
a read slot. Inventory growth alone opens no peer connections or remote stores.

Authenticated configuration uses TLS 1.3 with mutual certificate verification on
control, Raft, snapshots, native-wire and shard TCP. A certificate carries exactly
one `spiffe://treedb/cluster/<escaped-cluster>/node/<escaped-node>` URI plus the
normal SAN and client/server EKUs. Trust, validity, exact cluster/node, destination
identity and group membership are checked; configuration digests remain mismatch
detection, not credentials. Advertised endpoints must be canonical private or
loopback IPs. No plaintext fallback, TLS session resumption or mutation retry is
introduced. Certificate-chain expiry bounds existing connection deadlines.

Use the runtime's `PeerTransportV1()` for every local native/shard listener and
coordinator/client (`NewFixedPeerTCPClientWithTransportV1` for borrowed control
clients). A second independently created handle is a second budget: do not create
one per listener. Borrowed client close closes its HTTP pools, not the node handle.
Credential paths and limits stay local; authenticated mode and cluster identity
are bound into the shared digest. Replace credential contents at the same paths
and restart one compatible voter at a time; there is no live credential watcher.
Never edit a persistent manifest to downgrade to plaintext.

The shared default budget is 512 connections, 512 admitted requests, 256 MiB
inflight bytes, 64 proposals and four snapshots, with per-scope defaults of 64
connections, 64 requests, 64 MiB, at most eight proposals, and one snapshot.
Reservations preserve control/read/diagnostic progress and separate hosted groups;
all configured reservations must fit the declared node capacity. Raise the
explicit capacity when the declared inventory requires it. Admission rejects
without queues. Native frames default to 1 MiB; shard requests to 64 KiB, 32
partitions and top-k 256. These bounds do not enable any otherwise unsupported
vector operation. The standalone shard dispatcher additionally caps 64 aggregate
active/idle/pending sockets and 256 requests, with 32 requests per group and one
reserved request per declared group. Only idle sockets are evicted.

Raft MessagePack lengths, nesting and containers are checked before upstream
allocation. Bounded connection buffers and request copies are charged before
consumption. Secure append batches currently carry at most one log entry;
HashiCorp pipelining remains enabled with budget leases held through real futures.
This is a throughput qualification constraint, not a claim that bulk catch-up is
fast. Incoming/outgoing snapshot admission and a per-group snapshot slot last
through actual installation/response, so stalled transfers cannot occupy all
snapshot capacity. Byte accounting bounds admitted transport/proposal/snapshot
buffers; it is not a hard bound on kernel socket buffers, caller-owned values or
the whole storage-engine heap. P2/storage working-set qualification remains separate.

Secure persistent roots contain matching `fixed-peer-storage-v1.json` records
before stores open. Loss of one paired root, nonempty roots without identity,
mismatched pairs or overlapping resolved paths refuse startup. Reopened nodes
never bootstrap a new Raft group. Losing **both** roots is indistinguishable from
fresh empty storage without deployment inventory; restore/replacement must follow
the external volume identity and backup runbook. A partial first initialization
also fails closed. Consumers need only their persistent Raft/config root.

`BeginDrainV1` refuses new public mutations/native/shard work while preserving
consensus and observational control operations; `Close` drains HTTP and interrupts
all owned streams before provider shutdown. Readiness obtains a fresh catalog
fence and a leader ReadIndex proof for each locally hosted data group, and checks
local durable/consensus applied progress. It is never a reusable read capability
or proof that an ANN generation is ready. A group with no durable applied command
remains unready. One unavailable hosted group makes the node unready.

See the [EC2 operations guide](../operations/fixed-peer-ec2.md) for deployment,
paired backup/restore, rolling changes and actual network/AZ evidence boundaries.

`Status` reports configured members/features/endpoints, leader/term, Raft commit,
Raft applied, durable FSM applied, catalog epoch/digest, shared config digest, and
`new`/`reopened` origin. `CatalogRole` is `voter` or `consumer`; consumers report
zero local catalog state/Raft progress and only their locally hosted data groups.
Storage-free consumers create their persisted configuration under `RaftRoot`,
but no data root or catalog Raft stores. Status is observational, not a read-safety or readiness proof.
Admission still requires fresh consensus/catalog checks; `reopened` does not mean
the member has caught up. Consumers compare applied indices and catalog identity
with the returned commit/catalog evidence.

## Replay local conformance

The test configuration is generated in `fixedPeerTestConfigsV1` in
`TreeDB/nativewire/fixed_peer_tcp_runtime_v1_test.go`: three distinct child OS
processes, three catalog voters, one ingress-only group-a voter, and two remote
group-b voters. **The two-voter group-b has no one-member failure tolerance**;
group-a is also not redundant. This topology proves remote ownership, not HA.
Ports are reserved uniquely during configuration, roots/logs are test-owned
temporary directories, and children/listeners are shut down and checked.

```sh
GOWORK=off go test ./TreeDB/nativewire -run '^TestFixedPeerTCPRuntimeRemoteOwnerWriteCommitsAppliesAndReplicatesV1$' -count=3 -v
GOWORK=off go test ./TreeDB/internal/raftcluster ./TreeDB/internal/raftplacement ./TreeDB/nativewire ./TreeDB/mongo_gateway -run 'Test.*(FixedPeer|TCP|CatalogMeta|GroupRouted|RemoteOwner|Recovery).*' -count=1
GOWORK=off go test ./TreeDB/internal/raftcluster ./TreeDB/internal/raftplacement ./TreeDB/nativewire ./TreeDB/mongo_gateway ./cmd/treedb-fixed-peer -count=1
GOWORK=off go test -race ./TreeDB/internal/raftcluster ./TreeDB/nativewire ./cmd/treedb-fixed-peer -run 'Test.*(FixedPeer|TCP|CatalogMeta|GroupRouted|RemoteOwner).*' -count=1
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkFixedPeerTCPRemoteOwnerCreateV1$' -benchtime=10x -count=3 -benchmem
```

The first test publishes through the actual meta leader, submits through the
non-owner, checks both remote commit/apply progress and offline visible data,
checks zero ingress mutation, exact retry, restart/recovery, stale/unsupported
refusal, and fresh-write missing-quorum refusal. Separate deterministic tests
prove post-send wire cancellation and actual TCP provider post-enqueue
cancellation with apply completion. No production fault hook is installed.

The benchmark excludes process startup and initial route lookup, includes entry
encoding plus ingress/owner revalidation, forwarding, quorum commit and apply,
and creates a fresh collection/idempotency key per operation. `B/op` and
`allocs/op` cover only the parent/client process, not node allocations. Child
CPU/resource usage and persistent logical bytes are logged after stop. The
benchmark counts outer client JSON request-body bytes only, not HTTP headers,
responses, inter-node forwarding, or Raft replication traffic. The integration
test logs recovery time. These are local conformance measurements,
not multi-host or horizontal-scale evidence. No comparable in-process durable
remote-owner create benchmark exists; other command benchmarks are not a
substitute for an apples-to-apples regression control.

## Sparse catalog qualification and allocation audit

Run the sparse conformance and resource harness with:

```sh
GOWORK=off go test ./TreeDB/nativewire -run 'TestSparseCatalog' -count=3 -v
GOWORK=off go test -race ./TreeDB/nativewire -run 'TestSparseCatalog' -count=1 -v
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkSparseCatalogRemoteOwnerCreateV1$' -benchtime=25x -count=3 -benchmem -v
GOWORK=off go test ./TreeDB/nativewire -run '^$' -bench '^BenchmarkSparseCatalogConfigV1$' -benchtime=100x -count=3 -benchmem
```

The scoped qualification workflow checks out the exact PR base and head, copies
only the identical test-only benchmark harness into the base, and alternates
three base/candidate runs on one Linux runner/toolchain. `AllVoters` uses four
processes/four catalog voters and unchanged durable remote-owner create semantics
on both versions. `Sparse` uses the same four processes/data groups with three
catalog voters and a nonvoting ingress. `Inventory40` retains those four active
processes and declares 36 additional dormant endpoints. Its result measures
inventory overhead, not 40-instance throughput. Neither four catalog voters nor
the existing two-voter data group is a recommended HA topology.

`route_ns/op` includes route resolution and its quorum fence before the timed
write section. Standard `ns/op`, `B/op` and `allocs/op` measure the parent/client
durable-write section. Child `nodes_B/op`/`nodes_allocs/op` include the sum of each
node's Go allocation deltas, background Raft activity and bounded test metrics
reporting between samples. Each child's before/after heap, Go-reserved memory,
GC count/pause, goroutines, descriptors, threads, Linux RSS and Linux peak RSS
are logged. `nodes_RSS_bytes` sums retained samples; `sum_node_peak_RSS_bytes`
is the sum of individual process high-water marks, an upper bound on simultaneous
aggregate peak, not a simultaneous-peak sample. Child CPU is logged at shutdown.
The metrics collector is test-only stdin/stdout instrumentation and does not
add a production diagnostics or mutation endpoint.

| Frequency / lifetime | Allocation and resource contract |
| --- | --- |
| Configuration, retained for client/runtime lifetime | Preflight bounds lengths and worst-case escaped bytes before cloning. Clone node/group/peer/feature containers once; remove the former full JSON encode/decode clone. Preserve caller isolation. The shared digest encoding remains one bounded startup allocation. |
| Runtime open, until close | Build one endpoint map and one entry per declared data group; open providers, transports, stores and apply state only for locally hosted groups. A consumer allocates no catalog authority. |
| Per RPC, until response completion | Use indexed destination lookup, independent fixed admission channels and reusable HTTP pools. Request JSON, response bytes and existing decode/route metadata remain bounded transient allocations; no inventory-sized per-call status slice is built. |
| Per supported mutation | Retain existing deterministic entry decode, complete route comparison, owner forwarding and durable Raft/FSM/idempotency path. Remote consumer validation pays an explicit catalog round trip instead of maintaining replicated local state. |
| Maintenance / reconnect | No watch stream, lease cache, consumer catalog snapshot, all-inventory polling or preconnected peer mesh. HTTP idle connections expire and are globally bounded. Raft sockets have one connection-lifetime tracking entry and shutdown closes it. Restart reads the exact local manifest and subsequent operations acquire fresh authority. |

Configuration benchmarks vary nodes (4/40/1,024) and declared groups (2/32/128),
report encoded bytes and constructor allocations, and do not open runtime stores.
Measurements are local conformance evidence; multi-host/network/AZ-byte evidence
and representative horizontal scaling remain in #4250, with fault evidence in
#3983. No EC2 or ANN quality/capacity claim follows from these cases.

The initial matched results and per-node samples are retained in
[P1 sparse catalog evidence](../evidence/sparse-catalog-4807/README.md).

## Authenticated transport and operations qualification

The `peer-security-qualification.yml` workflow tests the exact PR head, including
unknown/expired/wrong-cluster peers, plaintext native/shard denial, certificate
rotation/reopen/downgrade refusal, pre-decode exhaustion, global idle-socket bounds,
hot-group progress, a stalled real snapshot, durable replicated writes and
pipeline shutdown under the race detector, quorum readiness, drain, and missing
persistent roots. The existing retained M8 topology test remains a guardrail.

`PeerNodeResourceStatsV1.Current/Peak/Rejected` indices are connections, requests,
inflight bytes, proposals and snapshots. `NetworkStatsV1` is a diagnostics-only
snapshot of a bounded startup IP inventory plus one unknown bucket. It records
raw TCP stream bytes, including TLS handshakes, and asserts no authenticated
identity from the IP address. `DiagnosticsV1` reuses the existing process-runtime
counter schema, reports Linux process CPU, current/high-water RSS, descriptors,
filesystem free/total capacity without corpus directory walks, and host interface
counters. Missing/unsupported counters are listed in `Unavailable`. Compare
leader/commit/Raft-applied/durable-applied observations to compute distribution and
lag; admission Current/Peak/Rejected expose queued-work avoidance and overload.

Public-path plaintext/TLS measurements use the same existing durable remote-owner
create operation, four real processes and three repeats for both sparse and
40-entry inventories. They include parent allocations, aggregate child allocations
and per-node RSS/FDs/goroutines. The dormant inventory is not a 40-machine run.
See [P7 evidence](../evidence/peer-security-4813/README.md); no EC2, ANN-quality or
horizontal-speedup acceptance follows from these local measurements.
