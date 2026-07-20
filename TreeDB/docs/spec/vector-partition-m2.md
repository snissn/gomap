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

This writes digest/provenance-named canonical artifact and compact build-report
JSON files (not the false fixed filenames `vector_partition_artifact_v1.json`
or `vector_partition_build_v1.json`).
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
| 10k x 64, r4/d16/leaf128, 16 parts, 2 probes | build 4.271s / total 4.485s, build/total CPU 4.104/4.296s | 34.2 MB | 95.59 | 0.809 (64 samples) | 0.863 / 0.413 | 1.0512 |
| 100k x 16, r2/d8/leaf64, 16 parts, 4 probes | build 4.517s / total 5.065s, build/total CPU 4.569/5.133s | 105.1 MB | 64.62 | 0.759 (64 samples) | 1.000 / 0.588 | 1.00608 |
| 1M x 16, r1/d4/leaf32, 16 parts, 4 probes | build 21.766s / total 25.325s, build/total CPU 22.228/25.862s | 1.123 GB | 44.67 | 0.394 (64 samples) | 1.000 / 0.700 | 1.000000 |

The exact handoff artifacts (artifact then compact report) were:

| corpus | artifact path and SHA-256 | report path |
|---|---|---|
| 10k | `/tmp/treedb_m2_final10k4_q8zh/vector_partition_c270226154a4e741.json` `c270226154a4e7410bb4140d63abfe36bebc0d0de9cdbf7c894298225d76c935` | `/tmp/treedb_m2_final10k4_q8zh/vector_partition_report_c270226154a4e741.json` |
| 100k | `/tmp/treedb_m2_final100k4_FPsO/vector_partition_627778e026d17732.json` `627778e026d177320e4aace2f32883aa1656c1d36f9f5005d1d9e41f55fdc3ae` | `/tmp/treedb_m2_final100k4_FPsO/vector_partition_report_627778e026d17732.json` |
| 1M | `/tmp/treedb_m2_final1m6_IqbC/vector_partition_f76fba39db8a51fb.json` `f76fba39db8a51fb11b669962b92a97d64956f75a76137cea0e80cbe42d42413` | `/tmp/treedb_m2_final1m6_IqbC/vector_partition_report_f76fba39db8a51fb.json` |

These paths are host-local handoff locations, while the manifest/source digest,
exact commands, and compact report fields make the run reproducible without
committing a 45 MB generated artifact.
The durable reviewer-accessible digest ledger is
`TreeDB/docs/spec/artifacts/vector-partition-m2-evidence-v1.json`.

The 1M artifact was
`/tmp/treedb_m2_final1m6_IqbC/vector_partition_f76fba39db8a51fb.json`
(SHA-256 `f76fba39db8a51fb11b669962b92a97d64956f75a76137cea0e80cbe42d42413`);
the matching report sits beside it. Its deterministic source corpus is
`/tmp/treedb_m2_export1m_rHdo`, generated with:

```sh
DATASET=$(mktemp -d /tmp/treedb_m2_dataset1m_XXXXXX)
OUT=$(mktemp -d /tmp/treedb_m2_out1m_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET" -docs 1000000 -queries 1 -dims 16 -truth-queries 0
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET" -out "$OUT" -partitions 16 -probes 4 -seed 1 \
  -repetitions 1 -degree 4 -max-leaf-bucket 32
```

The other exact runs were:

```sh
DATASET_10K=$(mktemp -d /tmp/treedb_m2_dataset10k_XXXXXX)
OUT_10K=$(mktemp -d /tmp/treedb_m2_out10k_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET_10K" -docs 10000 -queries 16 -dims 64 -truth-queries 0
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET_10K" -out "$OUT_10K" -partitions 16 -probes 2 -seed 1 \
  -repetitions 4 -degree 16 -max-leaf-bucket 128
DATASET_100K=$(mktemp -d /tmp/treedb_m2_dataset100k_XXXXXX)
OUT_100K=$(mktemp -d /tmp/treedb_m2_out100k_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET_100K" -docs 100000 -queries 8 -dims 16 -truth-queries 0
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET_100K" -out "$OUT_100K" -partitions 16 -probes 4 -seed 1 \
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
The final 1M report records load 0.338s, graph build 19.185s, backend
partition 0.271s, validation 0.812s, quality 1.113s, temporary disk 0 bytes,
artifact 44,672,391 bytes, report 2,545 bytes, and final output 44,674,936
bytes.

Peak RSS is `/proc/self/status` `VmHWM` for the builder process. It includes
the loaded corpus, graph, assignment, quality-oracle work, Go runtime and GC;
it does not represent a TreeDB server resident-memory claim.
