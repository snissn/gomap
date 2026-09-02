# #4019 local-system qualification

Status: complete and validator-qualified at source head
`ab674362838365e68297a49816872012d273039f`.

This is the compact, hash-bound result from five local-system rows, two
accepted corpora, and three serialized repetitions. The committed directory is
about 12.1 MB; the roughly 5 GB working bundle remains outside Git because it
contains rebuildable databases, container volumes, profiles, and service state.

## Matched-recall medians

The selected budgets are TreeDB p2, Milvus EF64/EF128, and pgvector
EF64/EF128 for the 100k/250k corpora respectively. Every selected row exceeds
recall@10 0.90. QPS and p95 are medians over three repetitions.

| Row | Corpus | Recall | c1 QPS / p95 ms | c8 QPS / p95 ms | c32 QPS / p95 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| TreeDB single daemon | 100k | .9810 | 42.62 / 32.44 | 99.08 / 123.27 | 154.44 / 265.28 |
| TreeDB native four daemon | 100k | .9810 | 42.03 / 30.18 | 72.02 / 145.60 | 100.30 / 479.37 |
| TreeDB container four daemon | 100k | .9810 | 41.93 / 32.02 | 65.55 / 216.12 | 142.72 / 318.88 |
| Milvus Standalone | 100k | .9180 | 440.64 / 2.92 | 1804.74 / 6.64 | 1827.74 / 30.10 |
| PostgreSQL + pgvector | 100k | .9285 | 987.82 / 1.36 | 4741.45 / 2.66 | 4034.15 / 14.89 |
| TreeDB single daemon | 250k | .9247 | 43.37 / 30.76 | 100.97 / 110.65 | 143.87 / 370.20 |
| TreeDB native four daemon | 250k | .9247 | 22.16 / 71.02 | 39.08 / 281.09 | 60.51 / 694.11 |
| TreeDB container four daemon | 250k | .9247 | 40.06 / 39.95 | 75.48 / 168.45 | 126.40 / 432.70 |
| Milvus Standalone | 250k | .9328 | 404.41 / 2.97 | 1708.19 / 6.79 | 1324.61 / 43.59 |
| PostgreSQL + pgvector | 250k | .9269 | 294.19 / 4.85 | 1559.22 / 7.41 | 1595.27 / 39.92 |

TreeDB returned identical selected-budget recall across its three topologies,
with zero query errors, timeouts, retries, redirects, or swap. The native and
container rows show a material topology tax, strongest for 250k and concurrent
queries. Retained stage timings attribute it mainly to generation-open and RPC
time rather than local HNSW search or payload bytes. This is a guardrail and
diagnosis, not a parity claim.

Each container daemon was constrained to a distinct three-CPU set and 6 GiB of
memory with swap disabled; the retained runner evidence binds those allocations.

External ranking is observational: all rows ran serially on one shared host,
and the TreeDB multi-daemon rows are same-host TCP or container-bridge tests,
not multi-host evidence. Issue #3983 owns multi-host qualification.

## Contents and validation

- `result.json`: complete machine-readable result accepted by the frozen
  validator.
- `matched-recall.json`: compact selected-budget medians and spread.
- `raw-evidence.json`: paths, sizes, and SHA256 values for the external working
  evidence.
- `commands.json`, `environment.json`, `sysstat-load.json`: execution and host
  context.
- `runners/`: exact retained campaign controllers.
- `SHA256SUMS`: hashes for the committed machine-readable inputs and runners.

Validate with only the Python standard library:

```sh
python3 benchmarks/vector_db_compare/system_qualification.py \
  --plan TreeDB/docs/spec/artifacts/vector-partition-local-system-qualification-4019-plan.json \
  --result TreeDB/docs/evidence/vector-partition-local-system-qualification-4019/m3-ab674362/result.json \
  --require-complete
```

Several failed harness attempts were preserved outside Git: an obsolete
pgvector recall guard, Python bytecode contaminating the clean-source check, an
extension-ordering bug, unavailable in-container disk tooling, and initial
pgvector rows without network accounting. None is included in `result.json`.
