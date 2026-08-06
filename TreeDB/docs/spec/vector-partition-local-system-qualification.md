# Vector-partition local-system qualification

Status: pre-alpha M0 contract plus a bounded M2 TreeDB topology-tax baseline
at head `95c60cbef0b0cb824a74a29e9304784e76745d9d`. The full five-row frozen
plan remains `planned_no_measurement`; the M2 evidence and scoped claims are
retained under
`docs/evidence/vector-partition-local-system-qualification-4019/m2-95c60cbe`.

## Authoritative inputs

The contract consumes the accepted #4022/#4027 structured qualification at
head `eed54bc0b9ec3b705e9170be26ab069bdc9b9771` and the public
`vectorpartition.OperationsV1` production boundary from #4018. The accepted
corpora are the deterministic 100k and 250k embedding-mixture fixtures, each
with 1,000 held-out queries, 128 dimensions, cosine distance, and top-k 10.
Their exact checksums and truth identities are frozen in
`artifacts/vector-partition-local-system-qualification-4019-plan.json`.

This does not add a 1M or high-entropy claim.

## Required rows

| Row | Headline client boundary | Topology |
| --- | --- | --- |
| TreeDB single daemon | bounded TCP client to `OperationsV1.Search` | one process owns four groups |
| TreeDB native multi-daemon | bounded TCP client to `OperationsV1.Search` | four processes, ports, roots, identities, one group each |
| TreeDB container multi-daemon | bounded TCP client over an isolated bridge | four pinned containers and volumes, one group each |
| Milvus Standalone | PyMilvus to official Standalone gRPC | pinned Milvus, etcd, and MinIO containers |
| PostgreSQL + pgvector | Psycopg over the PostgreSQL wire protocol | one pinned PostgreSQL+pgvector container |

All rows run serially on the same host. In-process TreeDB calls are not a
headline comparator row. Same-host TCP and bridge results are not multi-host
evidence; #3983 owns that qualification.

## TreeDB production topology adapter

`treedb_vector_partition_bench system-node` consumes one bounded JSON config
and exposes only `vectorpartition.OperationsV1.Search` over the framed TCP
client boundary. The config must name `production_public_v1`; `m8_loopback`
is rejected. `system-check-topology` validates the complete config set before
launch: one owner per group, exactly one public listener, a shared endpoint and
applied-index identity, and distinct node IDs, ports, state roots, ready files,
and persistent database roots. A native or container set has four configs and
one local group per config; the single-daemon set has one config with all four
groups. Each multi-daemon config supplies the same positive
`group_applied_indexes` map, and its local daemon proves that its production
Raft applied index has reached the declared floor before readiness. Each ready
artifact retains `node_config_sha256`; the checked topology evidence carries
the same digest beside that node. Before loading the fixture or running a cell,
`system-bench` probes every checked shard listener and requires its live group
and config identity to match that node, binding every serving endpoint to the
exact checked database/state roots rather than checking only the public node.

Launch each native node with its own `TMPDIR` equal to its configured state
directory. Retain the topology check and each exclusive ready JSON before
running the client matrix:

```sh
treedb_vector_partition_bench system-check-topology \
  -configs node-a.json,node-b.json,node-c.json,node-d.json \
  -out /absolute/evidence/topology.json

TMPDIR=/absolute/state-a treedb_vector_partition_bench system-node \
  -config /absolute/state-a/node-a.json

treedb_vector_partition_bench system-bench \
  -endpoint 127.0.0.1:19000 -topology /absolute/evidence/topology.json \
  -dataset /absolute/fixture \
  -truth-cache /absolute/truth-cache -truth-cache-sha256 <sha256> \
  -probes 1,2,4,8,16 -concurrency 1,8,32,64 -top-k 10 \
  -ef-search 128 -warmup 1000 -out /absolute/evidence/search.json
```

The container row uses the same binary and config boundary. Build the binary
statically from the exact clean source head, attest its SHA256/build metadata,
then build a no-base-image container with the committed Dockerfile:

```sh
install -d <build-context>
CGO_ENABLED=0 GOWORK=off go build -buildvcs=true \
  -o <build-context>/treedb_vector_partition_bench \
  ./cmd/treedb_vector_partition_bench
docker build --network=none \
  -f benchmarks/vector_db_compare/treedb-system.Dockerfile \
  -t treedb-vector-system:<head-sha> <build-context>
```

Create one fresh internal bridge and four fresh persistent roots. Each
container receives one read-only fixture mount, one distinct read-only retained
M3 database mount, and one distinct writable state mount. Both the host source
and in-container destination are unique per node (for example `/database-a`
and `/state-a` through `/database-d` and `/state-d`), and each node sets
`TMPDIR` to its unique configured state destination. This lets the retained
topology check bind the same distinct paths that each container opens. Give each
container three CPUs and 6 GiB with swap disabled so the set totals the frozen
12-CPU/24-GiB envelope. Only the coordinator container publishes the public
port. Record the image ID/digest, bridge identity, mounts, cgroup limits, ready
JSON, commands, and total four-container resources; stop/remove all four
containers and remove the fresh bridge after retained evidence is closed. The
scratch image contains no shell or sidecar and therefore cannot conceal an
alternate in-container search path.

## Fairness and evidence

- Use the exact frozen corpus, query, metric, top-k, and oracle identities.
- Give every complete row the frozen 12-CPU, 24-GiB, zero-swap envelope and
  account for every server and sidecar process.
- Time load, index build, checkpoint/flush, reopen/reconnect, readiness,
  warmup, search, and cleanup separately.
- Start every repetition from fresh persistent state, durably flush before
  reopen/reconnect, keep the OS cache untouched, and warm each cell immediately
  before timing it.
- Sweep the engine-native budgets in the plan. TreeDB retains the accepted
  p1/p2/p4/p8/p16, EF128, c256 ladder and selects p2; Milvus and pgvector use
  the first pinned HNSW budget whose three-run median reaches recall@10 0.90.
  TreeDB's selected p2 budget must meet the same 0.90 floor.
- Measure concurrency 1, 8, 32, and the declared 64 saturation row. The three
  repetitions use forward, reverse, and one-position rotated budget order.
- Preserve unsupported, failed, incomplete, and host-noise-tainted rows. They
  do not qualify and are not averaged into a claim.
- Bind the exact source head, clean binary or image identity, topology,
  commands, environment, host, artifact paths, resources, and TreeDB public
  path counters in the machine-readable result.
- Retain the generation identity and coordinator stage timings for every TreeDB
  cell. Shard timings are summed work and can exceed client wall time when
  groups execute concurrently; client elapsed and raw request durations remain
  the topology-tax wall-clock evidence.

Milvus uses the official v2.6.20 Standalone compose deployment and PyMilvus
2.6.16. PostgreSQL 16.14 and pgvector 0.8.6 use the repository's existing
full-vector HNSW adapter. External relative ranking is observational; it is not
a TreeDB release threshold.

Validate the frozen plan or a produced result with only the Python standard
library:

```sh
python3 benchmarks/vector_db_compare/system_qualification.py \
  --plan TreeDB/docs/spec/artifacts/vector-partition-local-system-qualification-4019-plan.json

python3 benchmarks/vector_db_compare/system_qualification.py \
  --plan TreeDB/docs/spec/artifacts/vector-partition-local-system-qualification-4019-plan.json \
  --result /path/to/result.json --require-complete
```

The validator fails closed on missing identities, boundary drift, incomplete
budget/concurrency/repetition matrices, changed measurement order, malformed
metrics, absent matched-recall buckets, and non-valid required rows.
