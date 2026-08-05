# Vector-partition local-system qualification

Status: pre-alpha M0 contract for issue #4019. No measurements are claimed by
this document or its frozen plan.

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
- Measure concurrency 1, 8, 32, and the declared 64 saturation row. The three
  repetitions use forward, reverse, and one-position rotated budget order.
- Preserve unsupported, failed, incomplete, and host-noise-tainted rows. They
  do not qualify and are not averaged into a claim.
- Bind the exact source head, clean binary or image identity, topology,
  commands, environment, host, artifact paths, resources, and TreeDB public
  path counters in the machine-readable result.

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
