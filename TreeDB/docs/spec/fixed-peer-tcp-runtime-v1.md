# Fixed-peer TCP runtime V1

`nativewire.OpenFixedPeerTCPRuntimeV1` assembles the existing HashiCorp data and
catalog providers, durable Bolt log/stable stores, file snapshot stores, TreeDB
command WAL/FSM apply stores, and catalog-validated `GroupRoutedSubmitter`.
Each process joins the same catalog group and opens only its configured data
groups. Remote submission is a bounded private HTTP/TCP forwarding adapter, not
another consensus implementation or a public native-wire mutation protocol.

## Configuration and lifecycle

Build the small node command with
`GOWORK=off go build ./cmd/treedb-fixed-peer`, then run
`./treedb-fixed-peer -config /absolute/path/node.json` once per member.
SIGINT/SIGTERM closes HTTP admission, Raft providers/transports, FSMs, and DBs.
The initial stdout JSON is local status, **not readiness**.

The JSON is `nativewire.FixedPeerTCPConfigV1` (strict fields, maximum 1 MiB):

| Field | Contract |
| --- | --- |
| `NodeID` | Unique local identity, present in `Nodes` and catalog membership. |
| `Nodes` | Shared list of `{ID, Address}` private RPC endpoints. |
| `Catalog`, `Groups` | Shared `{ID, Peers, BootstrapNode, Features}` records; each peer has `{ID, Address, Capabilities}`. All are voters. Catalog peers must exactly cover `Nodes`. |
| `ListenAddress` | Exactly this node's advertised RPC IP:port. |
| `RaftListen` | Group-ID to IP:port map, exactly the locally hosted groups and their advertised peer addresses. |
| `DataRoot`, `RaftRoot` | Distinct, absolute, non-overlapping persistent directories for this node. Never share roots between processes. |
| `RequestTimeout` | JSON integer nanoseconds, 1 ms through 1 minute. Applies to RPC/transport/apply. |
| `RaftTimeout` | JSON integer nanoseconds, 50 ms through 30 seconds; heartbeat, election, and leader lease. |

Use canonical numeric IP addresses and nonzero ports; wildcard advertised
addresses, duplicate endpoints/identities/groups, missing bootstrap voters,
unsupported floors, and inconsistent local listeners fail closed. There are at
most 32 nodes and 32 data groups. The catalog group and all its voters require
`treedb.raftcluster.catalog_meta_authority` V1 alongside
`treedb.raftcluster.single_group_provider` V1. Data groups may use the default
single-group floor. Vector lifecycle requirements are refused in this slice.

Exactly one designated voter bootstraps each group, and only when its persistent
Raft stores have no existing state. Every node must receive identical
`Nodes/Catalog/Groups/timeouts`; their normalized SHA-256 digest is carried on
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

Routing obtains a fresh quorum-backed catalog read from the meta leader and
requires the local authority to cover that exact applied epoch/digest. It does
not serialize or trust a process-local lease. Ingress and owner re-resolve the
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
pools, each capped at 8 connections per endpoint. This keeps admitted callers
from exhausting the capacity needed by their nested RPCs. Headers, body
reads, writes, idle connections, and requests have deadlines. Invalid/trailing
frames and destination/config mismatches fail closed. Oversized/lost replies
after a mutation remain ambiguous. These are small control-plane/conformance
bounds, not tuned throughput targets.

Both the private HTTP protocol and HashiCorp TCP transport require a **trusted,
isolated private network**. Configuration digests detect mismatches; they are not
credentials or cryptographic peer authentication. Do not expose these listeners
to untrusted clients or the public internet. TLS/authentication, live membership,
rebalance, fault qualification, and multi-host/cloud deployment are not provided.

`Status` reports configured members/features/endpoints, leader/term, Raft commit,
Raft applied, durable FSM applied, catalog epoch/digest, shared config digest, and
`new`/`reopened` origin. It is observational, not a read-safety or readiness proof.
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
