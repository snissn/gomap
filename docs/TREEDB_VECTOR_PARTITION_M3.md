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

`collections.MaterializeVectorPartitionLocalSearchAssetsV1` appends immutable
packs through the existing column-asset manager. Its returned descriptors are
installed in M1's manifest, which binds each partition ID to exact asset ref,
length, CRC, and SHA-256; M1 reachability/reclaim therefore retains the pack
until the generation is deleted and reclaimed. `OpenVectorPartitionLocalSearcherForGenerationV1`
rechecks that binding, decodes the bounded pack, and holds M1's generation
reader pin until `Close`. Missing, corrupt, stale-generation, or malformed
assets fail closed. Exact scores are computed only from validated local FP32
vectors; responses are stable IDs plus scores, never documents.

The M3 benchmark matrix must include overlap `0,0.20`. The current offline
fixture harness accepts the partition stage; reported overlap quality/cost is
evidence, not an enablement default. Routing, RPC, Raft placement, and document
fetch remain later milestones.
