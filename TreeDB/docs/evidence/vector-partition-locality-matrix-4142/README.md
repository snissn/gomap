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
