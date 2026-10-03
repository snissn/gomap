# Bounded two-host fixed cluster

This provisional #4250 operator path admits one colocated RF3 or RF4 data group with an identical catalog voter roster. RF4 runs two SERVER daemons on each of 192.168.0.111 and 192.168.0.185, plus a separate driver container on185. The existing RF3 path remains two daemons on111 and one on185. Clients, drivers and standby processes do not count as servers. WAL, immutable initialization and prepare predecessors, final-base integration, review, CI and retained runtime validation remain gates. This source is not merged operational evidence.

The fixture creates three known 2D cosine vectors and one fresh write. Preparation admits at most16,384 source rows and32MiB of actual FP32+ID input; this helper defaults to3 seeds and optionally consumes a validated frozen dataset. It makes no100K capacity, throughput, elastic membership, split owner, replacement, delete or arbitrary crash-repair claim. RF4 needs three voters for quorum, so either host loss in the 2+2 layout loses quorum. In RF3, loss of111 loses quorum; loss of185 retains the mathematical majority but loses this driver. Neither layout establishes host-failure or AZ resilience. The earlier64 ordinary-write population was a qualification limit; RF4 alone qualifies no sustained writes or capacity.

## Prepare existing inputs

Build the actual cmd/treedb-fixed-peer binary from reviewed source with the repository Go toolchain. Put it in an immutable Docker image already present on both hosts. Record the image digest and executable SHA-256. The harness does not build, pull images, install tools, issue certificates or change host networking. This bounded harness requires the verified mikers SSH account on both hosts and reserves stores beneath its existing /home/mikers directory; another --ssh-user is rejected. Establish SSH access, Docker, free storage, port availability and resource allocation first. Every daemon and the driver has a2 CPU /2 GiB container limit with matching memory-swap limit. RF4 therefore permits4 CPU /4 GiB of daemon limits on each host, plus2 CPU /2 GiB for the separate driver on185. Reserve host/Docker headroom and record actual memory, exit and readiness receipts; these limits are not measured resident usage.

Supply exactly three or four JSON files using the [production configuration contract](../spec/fixed-peer-tcp-runtime-v1.md). Use identical nonempty ClusterID, Nodes, Catalog, Groups, timeouts and VectorInitialization. Every configured server votes in both the catalog and the single data group. Catalog features and every catalog voter's capabilities include the existing catalog-authority and vector-partition-lifecycle floors. Designate one real voter as BootstrapNode per group, retaining that identity on restart. Each node has two distinct RaftListen entries. Do not reduce feature floors or synthesize another wire protocol.

An RF4 example port inventory is (omit node-d and its complete roster/address entries for RF3):

| Node / host | Control | Catalog Raft | Data Raft | Public loopback | Private shard |
|---|---|---|---|---|---|
| node-a /111 | 192.168.0.111:17101 | 192.168.0.111:17201 | 192.168.0.111:17301 | 127.0.0.1:17401 | 192.168.0.111:17501 |
| node-b /111 | 192.168.0.111:17102 | 192.168.0.111:17202 | 192.168.0.111:17302 | 127.0.0.1:17402 | 192.168.0.111:17502 |
| node-c /185 | 192.168.0.185:17103 | 192.168.0.185:17203 | 192.168.0.185:17303 | 127.0.0.1:17403 | 192.168.0.185:17503 |
| node-d /185 | 192.168.0.185:17104 | 192.168.0.185:17204 | 192.168.0.185:17304 | 127.0.0.1:17404 | 192.168.0.185:17504 |

Reserve these ports. ListenAddress, advertised control addresses and local RaftListen must agree with actual host placement. Containers use Linux host networking. Public addresses remain loopback-only. Use canonical private shard endpoints reachable from both hosts under the existing authenticated transport; loopback shard addresses are accepted only for a local-host fixture and do not prove cross-host routing. The existing authenticated control path forwards public operations to the actual owner leader. The driver uses node-c's config and public address, permitting a leader on111 without exposing public ingress remotely.

Set VectorInitialization source to the single group, Collection to default/default/docs, CatalogEpoch1, Generation1, MaxSourceRows3..16384, and complete public/shard maps. Use this production index definition; omitted encoding gets the validated FP32 default:

```json
{"name":"embedding_graph","field":"embedding","metric":"cosine","dimensions":2,"m":2,"ef_construction":8,"ef_search":8,"strategy":"column_graph"}
```

Do not set Vector or supply physical manifest/progress. Quantized or schema-generation variants are unsupported. Credentials in the config point to actual regular PEM files accessible on the machine running the harness. Each leaf needs client/server EKUs, endpoint IP SANs and exact spiffe://treedb/cluster/<cluster-id>/node/<node-id> identity. Use a cluster trust bundle and mode0600 leaf private keys. Follow [existing credential requirements](../operations/fixed-peer-ec2.md#credentials-and-network). Never distribute the CA private key. Copies of leaf private keys remain in protected owned remote directories; exclude those directories from published evidence. Restrict control/Raft ports to the two hosts and authorized operators.

## Plan and execute

Create a local manifest; substitute real digests and paths:

```json
{"image":"registry.example/treedb@sha256:REPLACE_64_LOWERCASE_HEX","binary":"/usr/local/bin/treedb-fixed-peer","binary_sha256":"REPLACE_64_LOWERCASE_HEX","nodes":[{"host":"192.168.0.111","config":"/absolute/config/node-a.json"},{"host":"192.168.0.111","config":"/absolute/config/node-b.json"},{"host":"192.168.0.185","config":"/absolute/config/node-c.json"},{"host":"192.168.0.185","config":"/absolute/config/node-d.json"}]}
```

The manifest accepts only the exact RF3 2+1 or RF4 2+2 server placement. Every configured server must appear exactly once, in both the catalog and single data group. Other roster sizes and placements are refused.

The existing image string selects the same immutable reference on both hosts. Docker image stores can assign different immutable image IDs to the same loaded image. In that case, replace only `image` with an exact mapping for both hosts:

```json
{"image":{"192.168.0.111":"sha256:REPLACE_HOST111_64_LOWERCASE_HEX","192.168.0.185":"sha256:REPLACE_HOST185_64_LOWERCASE_HEX"}}
```

The helper uses each host's reference for inspection, daemons and the driver, and records it in the node plan. Missing or extra hosts, mutable tags and invalid identities are rejected. The shared `binary_sha256` remains mandatory, and every node must still pass the same executable and normalized shared-config checks. Images must already be installed; `--pull=never` is unchanged.

```sh
python3 scripts/treedb_fixed_cluster_2host.py --manifest /absolute/manifest.json --run-id trial01
python3 scripts/treedb_fixed_cluster_2host.py --manifest /absolute/manifest.json --run-id trial01 --execute --output /absolute/new-receipts
```

The default plan makes no network calls and creates no stores or containers. Execution reserves exclusively new per-node directories under /home/mikers/gomap-4250-twohost-trial01 and unique container names. It changes only local container credential/root paths. Actual binary/config inspection must succeed with identical normalized shared digests before any daemon starts. Existing stores and containers are never overwritten or removed. Every Docker run uses --pull=never, so a missing locally installed immutable image fails without pulling. The image entrypoint is explicitly replaced with the pinned binary.

Initialize publishes the exact epoch1 catalog via authenticated Raft, creates physical column metadata with production encoders, and seeds seed-x, seed-minus-x and seed-minus-y via routed consensus commands. Existing public Prepare verifies real all-voter source/completion agreement. Initialize accepts success only when the returned durable completion records exactly3 prepared source rows. A different count refuses the fixture after real preparation has committed; the actual completion remains in the failure receipt. On failure, the command receipt retains the actual Stage and any returned Catalog, Create and Seed results. The existing Prepare API may return a zero completion after submit or polling failure; the report does not reconstruct durable partial prepare progress. Investigate the actual durable prefix and original identity before deciding recovery, without automatic mutation retry.

The harness gracefully stops all configured owned daemons, requires exit0, and starts the same containers/stores. Before any public dial or write, Qualify reads existing authenticated preparation status from every source voter, requires the same request ID, exactly3 prepared source rows, identical command and logical asset-set digest, and retains the observed completion in its receipt. It does not call Prepare or submit preparation mutations. Missing observations may converge within the deadline; completed disagreement refuses immediately. Qualify then performs strict generation-snapshot search against the known exact cosine top-one oracle, inserts trial01-fresh-y through actual public ingress, and submits an exact retry. Both responses remain separate genuine receipts. It waits for every voter's actual FSM to apply the retry prefix, checks fresh strict-search visibility and obtains active readiness from every voter. Read-only asynchronous recovery observations have a finite deadline. Mutations are never automatically retried after errors or ambiguity.

The harness retains each serve-returned full Docker container ID for stop, restart, logs and final inspection. PASS requires the final object to match that original ID, the run ownership label and Running=true. Each SSH/SCP/Docker command retains arguments, exit status, stdout/stderr and timestamps. PASS is written only after the whole workflow succeeds. The plan and PASS scope report planned_source_rows=3 and planned_total_documents=4 as intended fixture workload counts. The qualification receipt records the observed frozen preparation count in Prepare.Command.SourceRowCount. These counts do not prove current live cardinality or exclusion of later foreign writes. Exactly3 prepared rows also do not independently prove unchanged membership or contents of all seeded documents. It is scoped functional evidence, not performance or host-failure qualification. Retry carries new consensus progress and stable live identity; CommitIndex need not equal the original. This public fixture does not inspect canonical roots or graph structure; those at-most-once assertions remain in the predecessor's actual Raft tests.

Failures stop execution without cleanup or relabeling. Existing owned stores and Docker logs remain available. Never rerun initialize against partial state or blindly retry an ambiguous mutation. Investigate the actual durable prefix and original identity. Success leaves the three or four owned SERVER daemons running. An RF3 receipt remains evidence only for its original three-server/config identity; RF4 requires fresh roots and a new four-server runtime receipt with the actual executable, images and config pinned. Cleanup is a separate operator action: verify the exact run label on each named container, stop/remove only those containers and remove only the exact run directory. Never prune foreign containers or persistent value-log segments by age.

Standalone driver modes use authenticated credentials and reject the plaintext fixture switch:

```sh
treedb-fixed-peer -config /config.json -mode initialize -request-id trial01 -operation-timeout 120s -expected-binary-sha256 SHA256
# After actual graceful close/reopen of every configured daemon:
treedb-fixed-peer -config /config.json -mode qualify -request-id trial01 -operation-timeout 120s -expected-binary-sha256 SHA256
```

Initialize requires fresh empty catalog and collection state. Qualification assumes the known fixture and no intervening mutations. Runtime, normal/race tests and final-base evidence are still required.


## Dataset checkpoint (#4956)

The existing initializer/qualifier optionally consumes a frozen dataset; empty
`-dataset` preserves the three-row fixture. The shared preparation envelope is
16,384 rows / 32 MiB actual FP32+IDs / 1,024-byte individual IDs, enforced before
owned source materialization. The initial packet counts 10,000 unchanged128D
rows plus three separated oracle anchors. See
[the preparation contract](../spec/fixed-cluster-vector-prepare-v1.md#dataset-preparation-admission-4956)
for exact input eligibility, stream/chunk identities, UNKNOWN/fail-stop behavior,
restart proofs and allocation evidence gates. The optional65-new-ID public
probe reconciles historical64 ordinary-write qualification claims; it changes
no split identity capacity and establishes no sustained throughput or broad
mutation/recall guarantee. #4250/#4810 remain open.


For the dataset packet, export the existing representative corpus with exactly
10,000 unchanged128D rows. Set all four immutable initialization definitions to
128 dimensions and agreed bounded M/construction/search effort, with
MaxSourceRows>=10003 (three counted anchors). Run the same plan/execute command
with `--dataset /absolute/frozen-export`; the plan retains manifest/vector hashes,
count, actual input bytes and plane eligibility. The driver bundle snapshots
only `manifest.json` and `documents.f32`, verifies their hashes before SSH, and
mounts them read-only. The initializer independently freezes and validates them
before mutation. Do not change or substitute a corpus after a refusal. Keep
this functional packet separate from sustained #4250 collection.
