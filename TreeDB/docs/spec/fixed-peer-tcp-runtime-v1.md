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
Startup binds the local control and all hosted Raft endpoints, plus applicable
vector public/shard endpoints, before starting providers that can dial peers.
The runtime retains each raw socket until its transport or serving backend
consumes it; a dormant shard reservation promptly closes incoming connections
without authenticating or serving requests. Takeover interrupts that temporary
Accept loop, clears its deadline, and returns the same socket after the existing
lifecycle checks succeed. No reservation goroutine remains on the serving path.
Bind errors still refuse startup. Failure and shutdown close both consumed and unused sockets. Nodes-only
immutable standbys retain their existing absence of a public vector endpoint.
This pool is bounded by local configured roles, independent of remote inventory;
it adds startup bookkeeping and no lookup to ordinary RPC/query/mutation paths.
The conservative configured-role ceiling is 1 control, at most 33 hosted Raft,
1 active public vector and at most 32 hosted shard sockets (67 total). Relative
to deferred cold shard binding, bootstrap retains at most one earlier socket
and one temporary refusal goroutine per dormant hosted shard role (at most 32);
there is no additional descriptor duplication in the refusal wrapper.
Backend retirement can release and later rebind its endpoint under the existing
lifecycle checks. Restart rebinds the configured endpoints before starting Raft.

Test fixtures retain OS-selected reservations through this same private opener.
Subprocess fixtures transfer the actual TCP sockets using inherited descriptors
on Unix, keeping the parent reservation until the child owns the socket. On
Windows, the eventual runtime child binds each role socket before reporting its
selected address to the subprocess fixture builder. It waits with those same
sockets until the existing start helper supplies the finalized configuration;
serving begins in the original bootstrap order. Restart binds the stored exact
addresses after the intentional shutdown. Private staged children are bounded
and killed and reaped if configuration construction fails. Ordinary linker
checks and hosted Windows runtime tests cover this supported path. The Windows
reply reader shares delete access so it can read a published reply while the
rename handle is still open; publication remains a closed temporary file followed
by rename. Config-only
probes and intentionally absent standby public roles release their reservations explicitly. Future replacement
role reservations transfer to the target runtime and are consumed only by its
existing transport/endpoint creation; they do not grant membership or readiness.
There is no public listener-injection API, port scheduler, mutation retry, or
collision suppression. A dormant refusal loop recovers only from errors classified
by `net.Error.Temporary`, with 5ms doubling backoff capped at 1s and reset on
success. Takeover and Close interrupt that backoff; closed/permanent errors stop
the loop. This recovery does not retry binds or mutations. The historical
interference owner remains unidentified;
controlled tests prove competing binds on every platform and outbound source-port
reuse causing a listener collision on Linux. Darwin permits the latter bind, so
that Linux mechanism is not a portable collision assertion.
The initial stdout JSON identifies the binary and normalized configuration; it is **not readiness**.
Use `-mode ready` for fresh quorum/apply evidence, `-mode status` for observational
Raft state, and `-mode diagnostics` for process/disk/network counters. `-mode inspect`
validates and hashes configuration without opening credentials, stores, listeners
or cloud resources. `-expected-binary-sha256` refuses a different executable
before any network/store activity.

The JSON is `nativewire.FixedPeerTCPConfigV1` (strict fields, maximum 8 MiB of config input, independent of the native frame and inspector-output limits):

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
Configuration containers have a conservative 32 MiB preclone inventory budget,
checked before copying caller input or creating stores/listeners. The normalized
shared JSON encoding has a separate 8 MiB limit. These are admission bounds, not
a whole-process memory limit. Numeric advertised addresses are parsed without
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
Credentialed immutable shard listeners may use canonical private or loopback
addresses only after the fixed-peer topology binds that shared authenticated
transport. The public vector listener remains loopback; credentialed legacy
mutable shard listeners remain loopback. Shard TLS authenticates the inventory
node before serving; the endpoint and request proof separately bind the group.
There is no plaintext fallback.
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

`BeginDrainV1` refuses fresh public/native ingress and capability-free local
outbound work. Descendants of an admitted request retain a private, process-local,
owner-bound capability only until that originating request ends. HTTP forwarding
uses this same lifetime rule. Authenticated inbound shard RPCs remain available
as internal dependency traffic during drain, including newly arriving RPCs: the
receiver cannot distinguish continuations without new wire authority. No such
wire capability is introduced. TLS identity, group authorization, read proofs,
framing and resource bounds remain enforced. Consensus and observational control
operations remain available. `Close` waits for admitted request lifetimes for at
most `RequestTimeout`, then atomically freezes admission, cancels remaining work
and interrupts owned streams before provider shutdown. After draining requests,
it retires both outbound HTTP idle pools before shutting down the inbound HTTP
server; unused authenticated self-dials must not keep that server alive. Idle-pool
retirement leaves in-use connections untouched and does not extend the shutdown
deadline or suppress shutdown errors. Forced transport close
cancels immediately without waiting for handlers. Readiness obtains a fresh catalog
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


## First-boot vector initialization intent

An authenticated fixed-peer configuration may supply `VectorInitialization`
instead of `Vector`. This is an immutable initialization identity, retained by
JSON configuration, the normalized shared digest, and the existing paired
persistent-root markers. It must be present before the first root is opened;
adding, removing, or changing it on existing roots is refused. Omitted intent
preserves the existing non-vector configuration identity and behavior.

The bounded v1 layout admits exactly three (RF3) or four (RF4) nodes in one
data group, with every node also a catalog voter and hosting that data group.
The catalog and data voter rosters must match the complete immutable node
inventory; no other replication factor is admitted. RF4 needs a quorum of
three. A two-host 2+2 placement loses quorum after either host is lost and
does not itself establish corpus or serving capacity; dataset preparation has
its separately enforced16,384-row/32-MiB input envelope.
Two-group/six-node
initialization is refused before persistent roots open because prepare and
serving activation do not support it yet; it remains follow-up work under #4250.
Ordinary replica replacement is refused for initialization runtimes, including
removal, completion, and reconciliation, so the retained roster stays immutable. No
spare or dormant destination is admitted. The intent binds `SourceGroupID`,
`Collection`, `IndexDefinition`, `CatalogEpoch` (1), nonzero `Generation`,
`MaxSourceRows` (1..16384), and complete `PublicAddresses`/`ShardAddresses` maps.
`Collection.Database` and `Collection.Catalog` must both be `default`, the
scope supported by the existing routed consensus create/ingest commands. Other
scopes are refused during config validation before persistent roots are opened.
Public vector addresses remain loopback-only; shard addresses require canonical
private or loopback IP endpoints under the authenticated transport boundary.
Both must be distinct from all configured endpoints. During initialization,
the local public address and locally hosted shard addresses are bound and
reserved; occupied addresses prevent startup. These listeners accept and close
connections without serving traffic until preparation takes them over. A
two-host placement does not guarantee quorum after either physical host is lost.

The index uses the existing collection definition normalizer and is limited to
cosine float32 `column_graph`, dimensions up to 4096, M up to 64, and construction
and search effort up to 4096. Representation, quantized variants, and an already
assigned schema generation are outside this checkpoint. The catalog lifecycle
feature floor is reserved in the catalog config and every catalog voter's
capabilities from first boot. The canonical initial catalog contains all fixed
data groups and exactly the intended collection's placement on `SourceGroupID`.
Its feature inventory uses the placement `collection_groups` floor plus the
vector lifecycle requirement. Raft runtime/catalog-authority features remain
in the separate fixed catalog Raft configuration, not the placement catalog.
Operators publish this catalog through the existing authenticated catalog API,
then create the collection with the intended index and ingest source documents
through the ordinary authenticated routed consensus command API. Initialization
never creates a collection implicitly or invents Raft apply/WAL progress.

Status and readiness report `VectorPhase: "initializing"`. The control and real
Raft runtimes can be live and data group fences can succeed, but aggregate
`Ready` remains false. Every vector search, routed insert, and lifecycle action
remains unavailable. Generic source ingest remains available; `MaxSourceRows`
is a future preparation input bound, **not** a generic insert quota or a promise
of long-lived vector-write throughput. No per-request source scan is added.

This prerequisite provides no prepare command, manifest acceptance, vector
listener, or transition to serving. The next checkpoint must validate real
committed source/schema/index evidence, prepare and durably accept assets, then
bind serving to this retained intent without editing root identity or weakening
FSM/WAL coverage, root authentication, or snapshot catch-up requirements.


Dataset initialization remains on this authenticated fixed-peer runtime, with one
mutable owner. Optional `-dataset` initialize/qualify modes use real routed chunk
commits and durable prepare/restart proofs; they never import benchmark runtime
or configured applied-index evidence. See
[dataset admission](fixed-cluster-vector-prepare-v1.md#dataset-preparation-admission-4956).


## Mutable fixed-peer owner admission (#4958)

The ordinary single-owner strict-search path retains its RWMutex reader lease
through coordinator/shard live pins and response/current-DB fences. An ordinary
insert retains exclusive admission through owner proof, actual Raft submission,
committed/applied checks and live-document proof. These boundaries are unchanged.

After the first failed reader TryRLock, reader intent is registered once and
retired on acquisition or cancellation. A later ordinary writer waits for
registered intents to retire before queuing its existing exclusive Lock. A
writer that already passed the zero-intent check may precede a reader registered
concurrently; an already queued writer keeps RWMutex writer preference, so new
readers cannot renew its gap indefinitely. Intent ends on read admission rather
than response completion; the retained reader lease then excludes publication.
Contended waits use a deadline-aware ticker and no background lock waiter.
Uncontended admission creates no ticker. Waiting-reader cancellation retires
its intent without waiting for a writer, and a writer waiting for intents can
cancel promptly. Once a writer is queued in RWMutex, cancellation is checked
when its already-retained reader pins retire and acquisition completes; no
cancellable-RWMutex guarantee is added. No orphan goroutine is introduced.

These local admission rules do not establish a latency bound, runtime resource
qualification or sustained fairness under every load. #4958 owns the retained
representative-corpus probe failure and fresh >64 ordinary-write reconciliation;
#4250 owns sustained QPS/p99/recall/resources. Increasing explicit driver budgets
alone cannot make the failed unpaced trial a pass.

### Explicit bounded audit attachment

`treedb-fixed-peer -mode diagnostics -colocated-audit-plan plan.json` loads and
validates a version1 <=524288-byte declared 6..63-outcome plan before client
networking. The six-outcome default is preserved; every declared original must
be present and the actual retained count must match exactly.
It reuses the existing authenticated diagnostics operation and all-voter live
readiness/proof paths. No serving flag, shutdown hook, endpoint or offline opener
is added. The optional receipt proves actual current-FSM applied/root/WAL state,
all planned original witnesses (6..63)/chain and final known-ID canonical source/absence
plus live membership. The prepared-owner wrapper may flush pending work; pending
state or any changed physical/root/applied/summary binding refuses observation.
One fresh routed leader/quorum proof at the highest validated commit covers the
strictly ascending prefix of original tokens bound to the same complete scope;
only that proof is forwarded. Each original witness is still reconstructed and
checked against the current local FSM. Witness, source and live membership
observations remain local to each voter. Current-FSM DB identity checks bracket
prepared collection admission; the callback never takes the FSM lock, because follower apply holds that lock
before taking the same admission. Physical root/WAL and summary checks remain
inside admission, followed by current-DB, applied, ACTIVE and catalog rechecks.
Concurrent apply must complete and make a changed-state audit refuse its receipt.
Keep every voter live until all audit receipts are independently verified, then
retain separate clean-stop evidence. Ordinary diagnostics omit the attachment.

The optional `Population` expectation extends this same operation with a complete
**current canonical source-vector** observation. It does not enumerate or prove
equality of the live ANN graph, and does not prove full reconstructed documents.
A plan may attach it to the existing complete declared-outcome/final-known-ID plan (6..63 originals).
An initial population-only plan instead has empty `Writes` and `Final`, zero
`HighestNewCommitIndex`, and positive `RequiredAppliedIndex`. A partial ledger
cannot select this mode. For example:

```json
{"Version":1,"RunID":"initial-population","HighestNewCommitIndex":0,"RequiredAppliedIndex":92,"Writes":[],"Final":[],"Population":{"Rows":10005,"Dimensions":128,"SHA256":"<64 lowercase hex characters>","Limits":{"MaxRows":16384,"MaxIDBytes":1024,"MaxSourceRecordBytes":1048576,"MaxTotalBytes":536870912,"MaxInspected":1048576}}}
```

`Rows` and `SHA256` come from the independently retained full canonical oracle,
including every live anchor/insert, not the prepared graph's original `SourceRows`.
The receipt adds observed rows, dimensions, fixed encoding `id-le32-fp32-le-v1`,
SHA256 and accounting. Each row contributes little-endian uint32 ID length, raw
ID bytes, then each unnormalized vector's little-endian `Float32bits`, in strictly
increasing raw-byte ID order. Signed zero is preserved. Missing, extra, invalid,
nonfinite, wrong-dimensional or changed vectors refuse the entire attachment.
There is no bulk vector response, collection-sized vector array or sort.

The admitted scan resolves the captured catalog definition once and iterates
the current primary root and overlays, charging superseded entries and tombstones.
Supported source representations are inline retained JSON and JSON noncolumn payloads
with a declared nonnullable, fixed-D, raw uncompressed FP32 typed column. The latter
uses the current row/scoring locator (including metadata-only preserved vectors),
current manifest references, existing prepared projection and checksum-verified
assets; unrelated document columns and pointer-backed primary payloads are not
materialized. Current pointer entries supply ID/existence and the fixed 16-byte
descriptor only. Retained JSON pointers refuse before value-log decoding because
grouped frames and omitted length hints cannot supply the declared pre-decode
bound. Empty supported collections require no source manifest or asset. Other representations
refuse observation. The existing native runtime owns ACTIVE/generation, authenticated
quorum, current-FSM DB identity and applied/root/WAL/summary fences before and after
the source scan. The collections helper alone supplies no consensus or live-graph
authority. Any drift, cancellation, iterator or close error returns no receipt.

All limits are positive and mandatory: `MaxRows` <=65536, `MaxIDBytes` <=65536,
`MaxSourceRecordBytes` <=1048576, `MaxTotalBytes` <=536870912 and `MaxInspected`
<=1048576; dimensions are 1..4096. `MaxSourceRecordBytes` bounds each materialized
source entry: inline primary bytes or a 16-byte pointer descriptor, plus fixed-D
vector bytes for stripped column payloads. The receipt's `SourceRecordBytes`
reports that sum, **not retained payload, value-log frame or full document size**.
No pointer length hint is treated as a payload bound; no primary pointer is
read or prefetched. Source entry/hash bytes are charged before vector decoding.
`HashedBytes` reports encoded oracle input; `AssetBytes` reports loaded typed
images (including reloads); `TotalBytes` additionally charges bounded metadata
and locator payloads. `Inspected` charges physical primary merge work, locator
probes, both manifest preparation passes, and scalar rows of each loaded part.
The scan reserves work before decoder calls; it retains one decoded generation
and one vector scratch. Before preparation, legacy manifests are limited to
4096 records/8 MiB, typed images and declared decoded section bytes to 64 MiB,
and each part to 65536 rows. These are refusal ceilings, not capacity, latency
or whole-process peak-memory claims.

This scan may hold shared mutation admission during an untimed diagnostic.
Run it after draining the measured writer while all owned voters remain live.
Each successful receipt is a local current-FSM observation at its actual applied
position, at or above the requested floor; four receipts do not invent a cluster
snapshot. Retain the separate stop/catchup/resource evidence. The operational
qualification owner remains #4250.
