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
| 10k x 64, r4/d16/leaf128, 16 parts, 2 probes | 1.597s CPU 1.901s | 34.6 MB | 95.59 | 0.809 (64 samples) | 0.863 / 0.413 | 1.0512 |
| 100k x 16, r2/d8/leaf64, 16 parts, 4 probes | 2.888s CPU 5.016s | 108.4 MB | 64.62 | 0.759 (64 samples) | 1.000 / 0.588 | 1.0048 |
| 1M x 16, r2/d8/leaf64, 16 parts, 4 probes | 34.927s CPU 58.008s | 1.330 GB | 72.47 | 0.781 (64 samples) | 1.000 / 0.700 | 1.000112 |

The 1M artifact was
`/tmp/treedb_m2_final1m_HPRB/vector_partition_9274f9f3d7b900dd.json`
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

The reported recall is a true M2 partition oracle: global exact top-10 first
selects the partitions containing the most truth neighbors (stable partition
ID ties), then the same selected-partition exact scan runs for graph and
stable-hash assignments. It deliberately contains no centroid/router loss.
At the same fixed four-probe candidate budget, the declared 1M corpus clears
the M2 quality gate (`1.000 > 0.700` recall@10). The lower-budget 100k
two-probe/r1/d4 attempt failed (`0.113 <= 0.150`) and was not substituted for
the reported four-probe gate.

The 1M exporter shape intentionally has **one deterministic query** and no
exporter truth stream (the builder computes its own exact query truth); its
quality row is therefore scoped to that declared one-query fixed-budget corpus,
not a population-level claim. The 10k and 100k rows use 16 and 8 deterministic
query vectors respectively. `FinalBytes` is artifact plus compact provenance
report bytes; reports no longer embed or print a duplicate artifact payload.
The final 1M report records graph build 30.488s, backend partition 0.310s,
validation 2.336s, temporary disk 0 bytes, artifact 72,473,746 bytes, report
2,415 bytes, and final output 72,476,161 bytes.
