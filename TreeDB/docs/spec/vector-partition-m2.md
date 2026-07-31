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
graph. After the leaf sketch, exact duplicate bit-pattern classes receive
deterministic zero-distance links between adjacent canonical ordinals. Required
links are preserved while farther sketch candidates are pruned to the
configured degree; degree one uses a directed ordinal cycle. These links are
corpus-only and keep identical vectors connected even when the class is larger
than the degree or bounded pivot leaves separate its members. A
SHA-256 fingerprint narrows candidate classes, exact float-bit comparison
verifies membership, and collision variants fail closed at a fixed bound.
`symmetric=false` is the sole supported policy in v1; it is recorded in the
artifact rather than pretending bounded reverse insertion is symmetric.

The deterministic `treedb_reference_greedy_v1` backend is CI-safe and has no
third-party backend provenance: its license is the repository license. It
assigns every ordinal exactly once while enforcing
`ceil((1 + imbalance) * vectors / partitions)`. `ValidateArtifact` independently
checks canonical IDs, graph ordinals/self/duplicates/degree, assignment range,
coverage, cap, and recomputed metrics. `DecodeArtifact` additionally rejects
oversized, unknown-field, trailing, and non-canonical JSON; a backend cannot
publish a forged metric report. Every graph row is an array: zero-degree rows
are encoded as `[]`, never `null`; strict decoding rejects `null` rather than
normalizing it. This structural rule keeps canonical request binding and
artifact digests stable for isolated vectors.

The usable optional external adapter (`RunExternalJSONForRequestWithLimits`) is
offline only; source-only and unbound entrypoints deliberately error because
they cannot bind a response to the requested graph. It receives
an explicit command, private input/output paths, context cancellation, a
fully validated requested artifact (source, IDs, config, and graph), and
independent request/output-byte caps. The default request cap is derived from bounded M2 graph ordinals and
worst-case JSON escaping for IDs and source/backend identity fields; callers
may set a smaller request cap without inflating the
output cap. The input bytes must be the requested artifact's exact canonical
JSON, so the graph being executed and the graph to which the response is bound
are the same object. It removes the
entire private temporary directory on command failure, cancellation, timeout,
malformed output, or success. On Unix it also kills the dedicated process group
after `Wait`, including when the root exits normally while a same-group child
holds an inherited pipe. A child that deliberately calls `setsid` escapes that
OS process-group boundary; the portable Go adapter cannot claim containment of
such hostile descendants. The selected KaHIP backend below is offline only;
no online external partitioner is selected or required.

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
For diagnosis only, `-partition-truth-oracle` may be added to `-stage
partition`; it reports exact truth-neighbor primary-home coverage, pair
co-location, unique vector bit patterns, and zero-distance truth share. The
flag does not affect partition construction and is rejected for other stages.
Its query count, generated matrix bytes, exact corpus-by-query work, top-k, and
fixture checksum are validated before the diagnostic data is allocated or
used.

The selected offline comparison backend is `kahip==3.25` (MIT Python module,
wheel SHA-256 `e6ea76524e9fc01b27e6f5c5f00b7eec71c94cbd1e84678ce2a14d64dfc9eda4`),
ECO mode, single OpenMP worker, seed from the validated request, epsilon .05,
and an unweighted symmetrized copy of the directed graph. It is invoked only
through an explicit `-partition-kahip-script` path (normally
`scripts/treedb_kahip_partition.py`) and the bound external JSON seam; it is
never an online dependency. The adapter rejects any distribution version other than
3.25 or RECORD SHA-256
`7ff011253147286fcebc9185573662bf31dbcfbab1944f9b4940032f49ea5217`, and requests above the V1 1M-by-degree-16 (16M directed edge)
envelope, above `16384` partitions, or with more partitions than vectors.
`-partition-kahip-python` is a trusted local offline execution substrate. V1
attests the pinned adapter bytes and installed KaHIP RECORD payloads, not the
Python interpreter, OS, shared-library loader, or container; results from an
untrusted runtime are outside this evidence contract.
On the retained 100k embedding-mixture graph
`5a095727ed0f82815643daddb47bd11a08c9630ede6f9b1d7e7ec427dc8e9937`,
the reference greedy result was p4 `.8303`, cut `1122051`, load `6521/6563`;
the KaHIP artifact
`022359b1aedfa738cde7f2e82e01263c855eb72075b1f2a927d3a5753d6fde9c`
was p1/p2/p4/p8/p16 `1.0`, cut `0`, load `6283/6563`. This is a bounded
offline placement result, not a claim about graph quality or online serving.

## Exporter-corpus builder and reproducibility evidence

`cmd/treedb_vector_partition_build` consumes the repository-owned
`treedb_vector_dataset_export` manifest plus checksummed `documents.f32` and
`queries.f32` source files. It rejects malformed manifests, altered checksums,
wrong float dimensions, non-finite or non-unit-normalized float32 rows, corpus
byte overruns, and unsupported identity contracts before corpus allocation.
It streams and validates all declared query rows (including checksum, exact
size, and trailing-byte checks) but retains only the first 128 deterministic
queries for quality evaluation; query-memory use therefore does not scale with
the declared query-row count. Graph construction enforces global scalar
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
| 10k x 64, r4/d16/leaf128, 16 parts, 2 probes | build 3.400s / total 3.524s, build/total CPU 3.414/3.543s | 34.5 MB | 95.59 | 0.809 (64 samples) | 0.863 / 0.413 | 1.0512 |
| 100k x 16, r2/d8/leaf64, 16 parts, 4 probes | build 4.184s / total 4.693s, build/total CPU 4.239/4.762s | 102.9 MB | 64.62 | 0.759 (64 samples) | 1.000 / 0.588 | 1.00608 |
| 1M x 16, r1/d4/leaf32, 16 parts, 4 probes | build 20.667s / total 23.977s, build/total CPU 21.141/24.577s | 1.245 GB | 44.67 | 0.394 (64 samples) | 1.000 / 0.700 | 1.000000 |

The exact handoff artifacts (artifact then compact report) were:

| corpus | artifact path and SHA-256 | report path |
|---|---|---|
| 10k | `/tmp/treedb_m2_out10k_final6_EEB82V/vector_partition_c270226154a4e741.json` `c270226154a4e7410bb4140d63abfe36bebc0d0de9cdbf7c894298225d76c935` | `/tmp/treedb_m2_out10k_final6_EEB82V/vector_partition_report_c270226154a4e741.json` |
| 100k | `/tmp/treedb_m2_out100k_final6_JOpR05/vector_partition_627778e026d17732.json` `627778e026d177320e4aace2f32883aa1656c1d36f9f5005d1d9e41f55fdc3ae` | `/tmp/treedb_m2_out100k_final6_JOpR05/vector_partition_report_627778e026d17732.json` |
| 1M | `/tmp/treedb_m2_out1m_final6_mZHdfj/vector_partition_f76fba39db8a51fb.json` `f76fba39db8a51fb11b669962b92a97d64956f75a76137cea0e80cbe42d42413` | `/tmp/treedb_m2_out1m_final6_mZHdfj/vector_partition_report_f76fba39db8a51fb.json` |

These paths are host-local handoff locations, while the canonical manifest/source digest,
exact commands, and compact report fields make the run reproducible without
committing a 45 MB generated artifact.
The durable reviewer-accessible digest ledger is
`TreeDB/docs/spec/artifacts/vector-partition-m2-evidence-v1.json`.
Its `canonical_manifest_digest` is the SHA-256 of `json.Marshal` on the
decoded typed exporter manifest (including its canonically ordered file map),
which is the logical manifest binding used in `source_id`; it is explicitly
not the raw `manifest.json` file-byte hash. The manifest itself lists the raw
SHA-256 and byte count for every exported corpus file.
All three runs used binaries built from implementation commit
`58db38e173093126e5b1ef3c92d79e160d63b0cf`, after merging
`origin/main` `3e225cb99`. The following evidence commit changes only
documentation/evidence. This separates
the exact algorithm binding from the necessarily later commit that records its
measurements.

The 1M artifact was
`/tmp/treedb_m2_out1m_final5_pG2Xts/vector_partition_f76fba39db8a51fb.json`
(SHA-256 `f76fba39db8a51fb11b669962b92a97d64956f75a76137cea0e80cbe42d42413`);
the matching report sits beside it. Its deterministic source corpus is
`/tmp/treedb_m2_dataset1m_final5_nC3l7B`, generated with:

```sh
DATASET=$(mktemp -d /tmp/treedb_m2_dataset1m_XXXXXX)
OUT=$(mktemp -d /tmp/treedb_m2_out1m_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET" -docs 1000000 -queries 1 -dims 16 -top-k 10 -truth-queries 0 -json=false
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET" -out "$OUT" -partitions 16 -probes 4 -seed 1 \
  -repetitions 1 -pivots 8 -degree 4 -max-leaf-bucket 32 -imbalance 0.05
```

The other exact runs were:

```sh
DATASET_10K=$(mktemp -d /tmp/treedb_m2_dataset10k_XXXXXX)
OUT_10K=$(mktemp -d /tmp/treedb_m2_out10k_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET_10K" -docs 10000 -queries 16 -dims 64 -top-k 10 -truth-queries 0 -json=false
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET_10K" -out "$OUT_10K" -partitions 16 -probes 2 -seed 1 \
  -repetitions 4 -pivots 8 -degree 16 -max-leaf-bucket 128 -imbalance 0.05
DATASET_100K=$(mktemp -d /tmp/treedb_m2_dataset100k_XXXXXX)
OUT_100K=$(mktemp -d /tmp/treedb_m2_out100k_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_dataset_export \
  -out "$DATASET_100K" -docs 100000 -queries 8 -dims 16 -top-k 10 -truth-queries 0 -json=false
GOWORK=off go run ./cmd/treedb_vector_partition_build \
  -dataset "$DATASET_100K" -out "$OUT_100K" -partitions 16 -probes 4 -seed 1 \
  -repetitions 2 -pivots 8 -degree 8 -max-leaf-bucket 64 -imbalance 0.05
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
The final 1M report records load 0.293s, graph build 18.665s, backend
partition 0.099s, validation 0.812s, quality 1.012s, temporary disk 0 bytes,
artifact 44,672,391 bytes, report 2,638 bytes, and final output 44,675,029
bytes.

CPU and RSS values are emitted only when their corresponding `*_available`
flag is true. CPU is collected through Unix `getrusage`; peak RSS is Linux
`/proc/self/status` `VmHWM`. On unsupported platforms the value is omitted and
the availability flag is false rather than treating zero as a measurement.
Peak RSS is `/proc/self/status` `VmHWM` for the builder process. It includes
the loaded corpus, graph, assignment, quality-oracle work, Go runtime and GC;
it does not represent a TreeDB server resident-memory claim.
