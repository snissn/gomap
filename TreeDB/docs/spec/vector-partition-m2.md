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

The usable optional external adapter (`RunExternalJSONForSource`) is offline
only; the unbound `RunExternalJSON` deliberately errors. It receives
an explicit command, private input/output paths, context cancellation, a
fully bound expected source snapshot, and an output-byte cap. It removes the
entire private temporary directory on command failure, cancellation, timeout,
malformed output, or success. No external partitioner is currently selected or
required.

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
wrong float dimensions, non-finite or non-unit-normalized float32 rows, corpus
byte overruns, and unsupported identity contracts before corpus allocation.
It loads and validates all declared query rows but caps quality evaluation to
the first 128 deterministic queries. Graph construction enforces global scalar
distance work (including dimensions), bounded partition work, and chunks non-progressing skew buckets
deterministically; leaf top-k selection is bounded by degree rather than leaf
size. The builder emits an immutable artifact and a JSON report containing
load, build, quality, and total-command wall time, build and total-command CPU time, peak RSS, temp/final bytes,
bytes/vector, balance, graph degree/cut/hash-cut, sampled graph-neighbor
recall, and fixed-probe partition-oracle and hash baseline recall@10.

The following local, deterministic exporter corpus runs were captured on this
host. They are offline quality/resource evidence, not a server speed claim:

| corpus and builder configuration | wall | peak RSS | artifact bytes/vector | graph recall sample | partition recall@10 / hash | cap/balance |
|---|---:|---:|---:|---:|---:|---:|
| 10k x 64, r4/d16/leaf128, 16 parts, 2 probes | build 1.466s / total 1.609s, build/total CPU 1.476/1.624s | 34.1 MB | 95.59 | 0.809 (64 samples) | 0.863 / 0.413 | 1.0512 |
| 100k x 16, r2/d8/leaf64, 16 parts, 4 probes | build 2.316s / total 2.840s, build/total CPU 2.374/2.909s | 108.1 MB | 64.62 | 0.759 (64 samples) | 1.000 / 0.588 | 1.00608 |
| 1M x 16, r1/d4/leaf32, 16 parts, 4 probes | build 11.234s / total 14.631s, build/total CPU 11.697/15.313s | 1.192 GB | 44.67 | 0.394 (64 samples) | 1.000 / 0.700 | 1.000000 |

The 1M artifact was
`/tmp/treedb_m2_final1m5_WF6G/vector_partition_f76fba39db8a51fb.json`
(SHA-256 `f76fba39db8a51fb11b669962b92a97d64956f75a76137cea0e80cbe42d42413`);
the matching report sits beside it. Its deterministic source corpus is
`/tmp/treedb_m2_export1m_rHdo`, generated with:

```sh
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET" -docs 1000000 -queries 1 -dims 16 -truth-queries 0
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET" -out "$OUT" -partitions 16 -probes 4 -seed 1 \
  -repetitions 1 -degree 4 -max-leaf-bucket 32
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
The final 1M report records load 0.302s, graph build 9.148s, backend
partition 0.221s, validation 0.804s, quality 1.052s, temporary disk 0 bytes,
artifact 44,672,391 bytes, report 2,544 bytes, and final output 44,674,935
bytes.

Peak RSS is `/proc/self/status` `VmHWM` for the builder process. It includes
the loaded corpus, graph, assignment, quality-oracle work, Go runtime and GC;
it does not represent a TreeDB server resident-memory claim.
