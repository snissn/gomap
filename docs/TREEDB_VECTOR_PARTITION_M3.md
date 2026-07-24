# TreeDB vector partitioning M3

M3 derives optional, bounded ANN memberships from the immutable M2 disjoint
artifact. A membership contains a stable vector/document ID and local search
assets only; it never carries a canonical document or changes token/Raft
ownership.

`vectorpartition.BuildOverlap` first validates the M2 artifact. In each
bulk-synchronous round it proposes the non-home partition with the highest
neighbor affinity, ranks proposals by affinity, stable ID, and partition ID,
and accepts only proposals that remain within both the global
`floor(ratio*source_count)` budget and the original per-partition hard cap.
It records unspent budget rather than overflowing a cap. Ratio zero produces
only the original home memberships.

`collections.VectorPartitionLocalSearcherV1` is an immutable,
generation/checksum-bound no-document searcher. It validates all IDs, FP32
vectors, membership kinds, and optional HNSW adjacency before open; malformed,
stale/retired, or pinned-for-removal assets fail closed. Exact scores are
computed only from the validated local FP32 vectors. Search responses are
stable IDs plus scores, never documents.

The M3 benchmark matrix must include overlap `0,0.20`. The current offline
fixture harness accepts the partition stage; reported overlap quality/cost is
evidence, not an enablement default. Routing, RPC, Raft placement, and document
fetch remain later milestones.
