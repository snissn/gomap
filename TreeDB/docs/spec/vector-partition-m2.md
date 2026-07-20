# Vector partitioning M2: offline graph and balanced artifact

M2 supplies an offline, clean-room builder at
`TreeDB/internal/vectorpartition` and a command-facing shim at
`TreeDB/vectorpartition`. It is not a TreeDB server feature, Raft manifest,
router, overlap implementation, or runtime FFI.

The reference builder accepts finite, equal-dimension cosine vectors with
unique stable IDs. It orders IDs canonically, repeatedly samples pivots from a
seeded permutation, permits the two nearest pivots at depth zero, uses one
nearest pivot at deeper levels, chunks degenerate duplicate buckets, computes
exact neighbors in bounded leaves, and unions them into a bounded directed
graph. `symmetric=false` is the sole supported policy in v1; it is recorded in
the artifact rather than pretending bounded reverse insertion is symmetric.

The deterministic `treedb_reference_greedy_v1` backend is CI-safe and has no
third-party backend provenance: its license is the repository license. It
assigns every ordinal exactly once while enforcing
`ceil((1 + imbalance) * vectors / partitions)`. `ValidateArtifact` independently
checks canonical IDs, graph ordinals/self/duplicates/degree, assignment range,
coverage, cap, and recomputed metrics. `DecodeArtifact` additionally rejects
oversized, unknown-field, trailing, and non-canonical JSON; a backend cannot
publish a forged metric report.

An optional external adapter (`RunExternalJSON`) is offline only. It receives
an explicit command, private input/output paths, context cancellation, and an
output-byte cap. It removes the entire private temporary directory on command
failure, cancellation, timeout, malformed output, or success. No external
partitioner is currently selected or required.

## Reproducible M0 fixture invocation

```sh
OUT=$(mktemp -d /tmp/treedb_vector_partition_m2_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_partition_bench \
  -dataset testdata/vector_partition_10k -stage partition \
  -partitions 16 -imbalance 0.05 -seed 1 -format json -out "$OUT"
```

This writes `vector_partition_artifact_v1.json` (canonical immutable artifact)
and `vector_partition_build_v1.json` (measured build wall time plus digest).
The latter is a local offline builder report, not production routing or a
universal speedup claim. The command's existing `-stage simulation` remains
the M0 sequential simulator and continues to use its frozen M0 vocabulary.

Before a production backend can be chosen, pin its version, source and license,
input/output command, resource limits, and independent validator evidence.

## Exporter-corpus builder and reproducibility evidence

`cmd/treedb_vector_partition_build` consumes the repository-owned
`treedb_vector_dataset_export` manifest plus checksummed `documents.f32` and
`queries.f32` source files. It rejects malformed manifests, altered checksums,
wrong float dimensions, corpus byte overruns, and unsupported identity
contracts before corpus allocation. The builder emits an immutable artifact
and a JSON report containing wall/CPU time, peak RSS, temp/final bytes,
bytes/vector, balance, graph degree/cut/hash-cut, sampled graph-neighbor
recall, and fixed-probe partition-oracle and hash baseline recall@10.

The following local, deterministic exporter corpus runs were captured on this
host. They are offline quality/resource evidence, not a server speed claim:

| corpus and builder configuration | wall | peak RSS | artifact bytes/vector | graph recall sample | partition recall@10 / hash | cap/balance |
|---|---:|---:|---:|---:|---:|---:|
| 10k x 64, r4/d16/leaf128, 16 parts, 2 probes | 1.560s | 36.2 MB | 95.59 | 0.900 | 0.594 / 0.163 | 1.0512 |
| 100k x 16, r2/d8/leaf64, 16 parts, 4 probes | 3.023s | 112.1 MB | 64.62 | 0.775 | 0.475 / 0.250 | 1.0048 |
| 1M x 16, r2/d8/leaf64, 16 parts, 4 probes | 34.266s | 1.443 GB | 72.47 | 0.794 | 0.300 / 0.100 | 1.000112 |

The 1M artifact was
`/tmp/treedb_m2_build1m_7jEG/vector_partition_9274f9f3d7b900dd.json`
(SHA-256 `9274f9f3d7b900dd931881c2ce544b68ac6ddac198008ab307c6b57469f62ec1`);
the matching report sits beside it. Its deterministic source corpus is
`/tmp/treedb_m2_export1m_rHdo`, generated with:

```sh
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET" -docs 1000000 -queries 1 -dims 16 -truth-queries 0
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET" -out "$OUT" -partitions 16 -probes 4 -seed 1 \
  -repetitions 2 -degree 8 -max-leaf-bucket 64
```

At the same fixed four-probe candidate budget, the declared 1M corpus clears
the M2 quality gate (`0.300 > 0.100` recall@10). The lower-budget 100k
two-probe/r1/d4 attempt failed (`0.113 <= 0.150`) and was not substituted for
the reported four-probe gate.
