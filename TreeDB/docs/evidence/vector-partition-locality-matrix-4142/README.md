# #4142 locality-matrix contract

This is the small, fail-closed M0 gate before expensive matrix execution. It
reuses the #4027 qualification campaign descriptor instead of inventing a
second benchmark format. `preflight_matrix.py` refuses to run a matrix if the
current source head differs from the frozen campaign and its selected 250k
descriptor. That mismatch requires a fresh exact-source asset build; historical
recall or overlap rows are not promoted to this issue.

The reducer schema reserves the required 16/32/40 × overlap × probe × EF ×
layout rows and binds source, executable, dataset, truth, graph, membership,
router, and query-union SHA-256 identities. It rejects nonterminal, mixed,
duplicate, reordered, incomplete, or filler-containing rows. All generated
rows, DBs, profiles, and logs belong under `/mnt/fast4tb/gomap-4142-locality-matrix-evidence`.

Smoke preflight:

```sh
python3 TreeDB/docs/evidence/vector-partition-locality-matrix-4142/tools/preflight_matrix.py \
  --source-evidence TreeDB/docs/evidence/vector-partition-qualification-4027/eed54bc0 \
  --out /mnt/fast4tb/gomap-4142-locality-matrix-evidence/preflight.json
```

The expected current disposition at `e70ce2e` is `blocked_source_identity`:
the retained #4027 qualification assets were built at `eed54bc0`, so they are
not valid inputs for a current-head selection matrix. No target byte/row,
probe, overlap, or page-objective contract is selected by this scaffold.
