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
