# Fixed-peer EC2 operations

This is the authenticated fixed-peer **substrate**, not completion of the
multi-owner ANN product or its EC2 performance/fault campaign. #4250 owns final
assembly after P1/P3/P4/P5/P7 and the representative ANN gates; #3983 owns fault
evidence. Do not relabel a loopback test, a dormant inventory, or a successful
CloudFormation launch as a large-data benchmark.

## Build, inspect, start and observe

Use the existing production command, configs and process/artifact conventions;
no alternate cluster manager is required. Build from the exact clean admitted
commit with the repository's Go toolchain:

```sh
GOWORK=off go build -trimpath -buildvcs=true -o /absolute/artifacts/treedb-fixed-peer ./cmd/treedb-fixed-peer
/absolute/artifacts/treedb-fixed-peer -mode version
/absolute/artifacts/treedb-fixed-peer -mode inspect -config /absolute/config/node.json
/absolute/artifacts/treedb-fixed-peer -config /absolute/config/node.json -expected-binary-sha256 SHA256 -diagnostics-interval 10s
/absolute/artifacts/treedb-fixed-peer -mode ready -config /absolute/config/node.json
/absolute/artifacts/treedb-fixed-peer -mode diagnostics -config /absolute/config/node.json
```

Substitute actual paths/digests. `inspect` validates without opening stores or
credentials. Startup identifies executable SHA-256, Go version, GOOS/GOARCH,
VCS revision/dirty status and normalized shared/local config digests. A
credential-free command refuses unless explicitly marked `-trusted-network-test`;
that switch is for local conformance fixtures. Use fresh `ready` results for
traffic admission. A running process, open port, known leader, historical success
or ANN capability alone is insufficient. Empty data groups stay unready until
initialized by a real durable command through their authoritative path.

SIGTERM/SIGINT begins drain, refuses new public work, drains bounded HTTP work,
then closes sockets/providers/stores. Preserve commit ambiguity for mutations
already sent. Never retry an ambiguous mutation as a fresh operation; use its
existing exact durable idempotency identity.

## Credentials and network

Issue one node certificate from the cluster PKI with both client/server EKUs,
its private endpoint IP SANs, and exactly one URI:
`spiffe://treedb/cluster/<escaped-cluster-id>/node/<escaped-node-id>`.
Use the same nonempty `ClusterID` in every configuration. Copy only the public CA
trust bundle to nodes; never distribute the CA private key. Deliver each leaf
private key through the existing restricted instance role/secret mechanism to a
regular absolute file with mode 0600, owned by the service account. Keep secrets
out of user data, images, plans, logs and benchmark artifacts. PEM limits are
128 KiB trust, 64 KiB chain (at most eight certificates), and 16 KiB private key.

Allow only declared TCP peer ports between this deployment's instance security
groups or exact private host /32 addresses. No public IP or inbound internet
access is needed. Operations use the existing SSM/private management channel;
SSM, object artifacts, PKI and telemetry need explicitly allowed VPC endpoints
and least-privilege roles/endpoint policies. The plan adapter checks peer ingress,
not your entire VPC/IAM/egress policy; review those concrete existing resources.
TLS identity/group/proof checks remain necessary inside the VPC.

Rotate trust with an overlap window: distribute old+new CA trust, restart and
verify one voter at a time, issue the new leaf, then remove old trust only after
all peers have moved. Keep credential filenames fixed; contents may change.
Existing connection deadlines cannot outlive verified certificate expiry.
Credential rotation requires restart; there is no certificate watcher.

## Inventory and plan/apply boundary

`scripts/treedb_peer_ec2.py` uses Python 3 and AWS CLI v2. `plan` is offline and
inspects the **actual production binary and node config files**. It does not call
AWS. Input is the schema in
[`fixed-peer-ec2-inventory.example.json`](../examples/fixed-peer-ec2-inventory.example.json).
Replace every placeholder with the specific deployment before planning. It
contains account/region/run ID, exact binary/source/config identities, nodes,
private addresses, AZ names and stable AZ IDs, image owner/ID, instance types,
CPU/memory limits, storage/temp capacity and roots, peer ports, voters, roles,
existing all-traffic flow log, expiry and recently checked all-in prices.
`config_sha256` and `shared_config_sha256` are the normalized hashes returned by
`-mode inspect`, not a hash of arbitrary whitespace in the JSON file.

Every group has an odd 3..31 distinct-instance voter set; losing any one AZ must
leave a majority. CPU/memory declarations must cover process/admission limits;
DB and Raft capacity each reserve at least twice the declared live bytes, and
temporary capacity covers the declared peak. These are admission assumptions,
not proof of sufficient build/snapshot/dataset working sets. Measure those in the
owning campaign and raise the declarations before launch if necessary.

Each bounded packet owns 1..64 instances and must fit the CloudFormation inline
51,200-byte template limit. Groups in that packet have complete voter inventory;
this adapter does not invent cross-packet membership or elastic reconfiguration.
Larger final deployment assembly remains #4250 and the runtime's separate
1,024-node inventory ceiling is not evidence that any topology has been measured.

```sh
python3 scripts/treedb_peer_ec2.py plan --input inventory.json --binary /absolute/artifacts/treedb-fixed-peer --out plan.json
# Review the exact plan, hashes, account, region, expiry, prices and retained volumes.
python3 scripts/treedb_peer_ec2.py stage --input plan.json --out staged.json
# Read staged change-set status/actions with AWS CLI; wait for CREATE_COMPLETE.
python3 scripts/treedb_peer_ec2.py apply --input plan.json --receipt staged.json --out applied.json
python3 scripts/treedb_peer_ec2.py inventory --input plan.json --receipt staged.json --out inventory-observed.json
```

`stage` verifies caller account, live subnet/AZ, instance CPU/memory, image owner
and architecture, private peer ingress and successful all-traffic flow-log
configuration. It creates only a CREATE change set. `apply` requires that exact
available change set, stack ARN, execution role, ownership tags and unchanged
template, then executes it once. No update/replace-existing-stack mode exists.
Use a unique run ID and preserve the receipt. A failed/ambiguous API call is not
proof that nothing happened; inspect the deterministic `treedb-<run_id>` stack
and `plan-<first-32-digest-characters>` change set, verify ownership, and recover
their exact IDs before retry/cleanup. Never substitute an arbitrary stack name.

An existing CloudFormation execution role creates/deletes only these tagged
EC2/EBS/Scheduler resources, passes only the chosen instance and expiry roles,
and accesses only the declared images, network and artifacts. The separate
Scheduler role trusts `scheduler.amazonaws.com` and may call
`cloudformation:DeleteStack` only for the exact deployment stack name/ARN scope.
The stack retains its execution role so scheduled deletion can release compute.
Configure a dead-letter/alert/management watch for failed deletion as part of
account operations; do not give the node role permission to edit its expiry.

The template creates the expiry schedule before either compute or persistent
volumes. Its one-time UTC target deletes the exact containing stack; the explicit
lease is at most 24 hours. Retries are bounded to one hour. Scheduler/API delays
and failures mean expiry is **not a hard billing cap**. The plan rejects declared
estimates above `max_cost_usd`, including network and retained-storage reserves;
prices are operator-verified attestations (at most seven days old), not invented
quotes. Maintain independent account budget/expiry alerts and stop promptly on
unexpected usage. Retained EBS continues billing until explicitly reclaimed.

## Image/bootstrap and storage boundary

Use a pinned tested Linux image matching the admitted binary architecture.
Node user data must be the reviewed existing bootstrap procedure: obtain the
exact binary/config artifacts, verify hashes, mount the **specific CloudFormation
Data/Raft volume IDs**, deliver credentials to local protected paths, set
GOMAXPROCS/GOMEMLIMIT/TMPDIR, then run the production command. The adapter never
formats an existing volume or embeds secrets. EC2 NVMe device names can differ
from requested `/dev/sdf` and `/dev/sdg`; resolve the actual EBS volume IDs before
mounting. Wait for attachments. Refuse missing, wrong, nonempty-unexpected or
instance-store devices for either persistent root. Record mount/volume IDs and
free space in the runtime inventory before starting.

The template owns distinct encrypted gp3 Data and Raft volumes and retains both
on cleanup/replacement. Persist the DB, value log, command WAL, roots, resource
manifests, Raft stable/log state and apply/snapshot state. Instance store and
TMPDIR are disposable build/spill only; losing them must not delete either
persistent root. The runtime's matching root markers detect loss of one root
and reject symlink overlap, missing identities and changed local configuration.
They are not an AWS mount attestation. Loss of both roots needs the saved external
volume inventory to distinguish replacement from fresh bootstrap.

A partial first initialization fails closed. Do not delete/edit markers to make
a retained node look new. Investigate its saved volume/config identities; discard
only proven never-admitted empty initialization or restore a complete pair.
Fixed membership/configuration does not authorize adopting an arbitrary new IP,
node ID or replacement voter. Online replacement belongs to P5/P6.

## Backup, recovery and compatible rolling changes

Raft replication is not backup. For the supported operator backup boundary:

1. Stop new ingress, begin drain and resolve outstanding exact operation IDs.
2. Stop the affected service cleanly. Preserve quorum in other failure domains;
   if a coherent cluster-wide restore point is needed, stop/quiesce the whole
   workload and record every group's catalog/commit/durable-applied position.
3. Snapshot/copy **both complete persistent roots from the stopped node**, their
   pair marker, immutable configuration and executable/source identities. Include
   all persistent value-log/resource files. Hash the backup manifest and test a
   restore in an isolated environment. Do not copy live directories piecemeal or
   treat a Raft log alone as the database backup.
4. Restore the complete pair to the same admitted identity/configuration and
   volume/mount locations; preserve credential paths while restoring keys through
   PKI. Reopen with the compatible verified binary and reacquire fresh quorum
   readiness. Never start the restored identity while its old process is alive.

A compatible rolling binary update keeps wire/features/storage/config identity
compatible: drain one voter, stop it, replace only the verified binary, restart,
wait for fresh local catch-up/readiness, then proceed. Do not remove enough voters
to lose any group's majority. There is no automatic schema migration or general
on-disk downgrade: unsupported required features and mismatched manifests must
refuse. Rehearse every format-affecting update with its recovery gates; never
force it by editing persistent manifests. Credential-free/old binaries cannot
open the authenticated local manifest without mismatch.

## Counters, network and evidence

`diagnostics` reports the existing process-runtime schema with availability
markers: Linux process CPU, current/peak RSS, allocations, GC, descriptors,
goroutines, filesystem available/total space and host NIC byte counters. It does
not recursively walk a large dataset every interval. Status supplies leader,
term, commit, Raft applied and durable applied indices; derive group lag and
leader distribution from those observations. Current/peak/rejected admission
counters distinguish active requests/proposals/snapshots and overload; there is
no unbounded waiter queue. Run-queue delay/timeslice fields remain unavailable
because summing only live threads loses exited-thread history.

Transport counters include all raw TLS stream reads/writes and handshakes across
shared node protocols, using a fixed startup IP map plus one unknown bucket.
They exclude TCP/IP headers and retransmissions and are not total host traffic
or AWS billing bytes. IP attribution is physical, not certificate identity.
Host NIC counters include other processes and cannot by themselves identify AZs.

Use VPC flow logs for all accepted/rejected traffic with custom fields including
`bytes az-id flow-direction interface-id srcaddr dstaddr pkt-srcaddr pkt-dstaddr
log-status`; `stage` verifies this format and successful delivery. Export a
specific complete observation interval, keep the original log format/order and
input files, then stream the records through:

```sh
python3 scripts/treedb_peer_flow_bytes.py --plan plan.json --input flow-records.txt --fields '${bytes} ${az-id} ${flow-direction} ${interface-id} ${srcaddr} ${dstaddr} ${pkt-srcaddr} ${pkt-dstaddr} ${log-status}' --out flow-bytes.json
```

Use the actual exported format, including any additional fields, with no header.
The report separates ingress, egress, same-AZ/cross-AZ egress, unknown destinations,
records outside inventory and incomplete/SKIPDATA records. It never marks capture
complete automatically. Check delivery interval/ENI coverage and runtime/host
counter deltas; report gaps, collector traffic and service/unknown destinations.
Do not add both endpoints' ingress+egress to claim unique transferred bytes, and
do not call these observations a billing invoice. Preserve hashes and per-node
metrics/profiles with the existing source/config/dataset/truth/campaign artifacts.

## Scoped cleanup and failure recovery

```sh
python3 scripts/treedb_peer_ec2.py inventory --input plan.json --receipt staged.json --out before-cleanup.json
python3 scripts/treedb_peer_ec2.py cleanup --input plan.json --receipt staged.json --out cleanup.json
# Poll the exact stack ARN to DELETE_COMPLETE; inspect failures and retained volumes.
```

Cleanup verifies account, region, exact stack ARN, execution role, ownership tags
and unchanged template. It works after partial/failed provisioning; repeats while
deleting/deleted are harmless. CloudFormation owns the partial resource graph;
there is no broad tag search followed by account-wide termination. Cleanup does
not override Retain or delete unrelated resources. After the stack's history has
expired, a missing-ARN error remains a refusal, not proof of completed cleanup.
Keep the final inventory/deletion evidence. Reclaim retained volumes/backups only
through a separately reviewed exact-ID action after confirming backup retention
and no live dependency; this adapter intentionally does not erase user data.

No live AWS acceptance is claimed by the local fake-provider tests. Replay them
with `python3 -m unittest discover -s scripts -p 'treedb_peer_*test.py' -v`, then
validate the concrete plan in the authorized account before declaring deployment
acceptance. Real ANN, throughput/latency, recovery and one-AZ-failure acceptance
remain the owning graph gates.

Primary AWS contracts: [EC2 instance resource](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ec2-instance.html),
[change sets](https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_CreateChangeSet.html),
[retention](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-attribute-deletionpolicy.html),
[Scheduler](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-scheduler-schedule.html),
[universal targets](https://docs.aws.amazon.com/scheduler/latest/UserGuide/managing-targets-universal.html),
and [flow records](https://docs.aws.amazon.com/vpc/latest/userguide/flow-log-records.html).
