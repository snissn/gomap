# #4142 locality-matrix contract

This is the small, fail-closed M0 gate before expensive matrix execution. It
reuses the #4027 qualification campaign descriptor instead of inventing a
second benchmark format. `preflight_matrix.py` binds an explicit clean measured
source checkout and binary; the evidence-PR checkout is intentionally not the
measured source. It accepts the #4140 measured source `21a57f93...` and rejects
another source revision. Historical recall or overlap rows are not promoted to
this issue.

The reducer schema reserves the required 16/32/40 × overlap × probe × EF ×
layout rows and binds source, executable, dataset, truth, graph, membership,
router, and query-union SHA-256 identities. It rejects nonterminal, mixed,
duplicate, reordered, incomplete, or filler-containing rows. All generated
rows, DBs, profiles, and logs belong under `/mnt/fast4tb/gomap-4142-locality-matrix-evidence`.

Smoke preflight:

```sh
python3 TreeDB/docs/evidence/vector-partition-locality-matrix-4142/tools/preflight_matrix.py \
  --source-evidence TreeDB/docs/evidence/vector-partition-qualification-4027/eed54bc0 \
  --source-checkout /mnt/fast4tb/gomap-4135-combined-measure-runtime/e2e/source \
  --binary /mnt/fast4tb/gomap-4135-combined-measure-runtime/e2e/bin/treedb_vector_partition_bench \
  --out /mnt/fast4tb/gomap-4142-locality-matrix-evidence/preflight.json
```

The expected disposition is `ready` when the #4140 source and binary are
supplied. The retained #4027 descriptor is source-older and is used only as a
frozen input anchor; it is never passed off as fresh matrix evidence. No target
byte/row, probe, overlap, or page-objective contract is selected by this
scaffold.

## M0 page-objective checkpoint

The v4 offline trace/simulation is a held-out graph+vector page selection
only, not a recall, throughput, or production-layout result. It uses the
frozen 250k graph artifact and frozen calibration/holdout manifests; snapshots
bind each pack document ID to the artifact ordinal and the simulator fails
closed unless identity remapping reproduces every captured holdout page count.

| held-out objective | median pages | p95 pages |
| --- | ---: | ---: |
| identity current / BFS | 1892 | 2347 |
| source | 2496 | 3057 |
| edge window | 1843 | 2323 |
| Gorder-like | 1791 | 2224 |
| co-visitation | 1583 | 2057 |
| hybrid | 1604 | 2066 |

`co-visitation` is the selected M0 page objective: -16.33% held-out median
pages versus identity/BFS. The raw calibration capture SHA-256 is
`84ca7f00e6e658ff951b841d8f367cab4d1809d106516900e4193e1a0c85ec5f`,
holdout is `2d07e255c7c4eecd04596215c523940fe77daec66ed76603ed65f595b3632dd1`,
and the reduced simulation is
`a170741fa6a2dfa6685b5a3a96b751e3ba20ea4f3743baafa259069ff78f5c1d`.
All raw artifacts remain under the task-specific `/mnt/fast4tb` root.
