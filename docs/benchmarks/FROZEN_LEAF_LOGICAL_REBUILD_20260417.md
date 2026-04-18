# Frozen Leaf Logical Rebuild Harness

This note measures a frozen, leaf-only logical rebuild against the same
Celestia-shaped post-dwell source that the earlier transcode harness used.

## Source

- Source DB: `/home/mikers/.celestia-app-mainnet-treedb-20260417111043/data/application.db`
- Source copy strategy: hardlinked clone on the same filesystem

## Commands

Build branch binary:

```bash
GOWORK=off go build -o /tmp/gomap_leaf_logical_rebuild_bin/treemap ./TreeDB/cmd/treemap
```

Leaf-only logical rebuild on a frozen clone:

```bash
/tmp/gomap_leaf_logical_rebuild_bin/treemap \
  leafgen-logical-rebuild-bench /tmp/leaf_logical_rebuild_hardlink/application.db \
  -rw -json -leaf-floor-bytes 2139653385
```

Full offline rewrite floor on a separate frozen clone:

```bash
/tmp/gomap_leaf_logical_rebuild_bin/treemap \
  vlog-rewrite /tmp/leaf_logical_rewrite_floor/application.db -rw
```

## Results

Source before logical rebuild:

- `application.db`: `2,576,582,655`
- `index.db`: `61,341,696`
- `leaf_vlog`: `2,345,168,025`
- `value_vlog`: `169,695,667`

Frozen logical rebuild:

- elapsed: `44.48s`
- `application.db`: `2,332,080,023`
- `index.db`: `39,321,600`
- `leaf_vlog`: `2,122,685,489`
- `value_vlog`: `169,695,667`
- `CommitSeq`: `3118 -> 3119`
- `LeafFiles`: `71 -> 16`
- `RecordsCopied`: `978,406`
- `LeafDictID`: `15208934947383264178`
- `LeafDictUseRawPages`: `false`

Exact full offline rewrite floor on the same source:

- `application.db`: `2,190,316,098`
- `index.db`: `39,321,600`
- `leaf_vlog`: `2,139,653,385`
- `value_vlog`: `10,948,411`
- `vlog-rewrite`: `segments_before=83 segments_after=17 bytes_before=2490028712 bytes_after=2125746336 records=1000483`

## Gap Closure

Leaf-only gap against the exact full-rewrite floor:

- leaf gap before: `2,345,168,025 - 2,139,653,385 = 205,514,640`
- leaf gap after: `2,122,685,489 - 2,139,653,385 = -16,967,896`
- leaf gap closed: `222,482,536`

Interpretation:

- The logical rebuild closes the leaf-side gap completely on this frozen source.
- It lands about `17.0 MB` smaller than the matching full offline rewrite
  `leaf_vlog`.
- The remaining overall `application.db` gap is outside the leaf subsystem:
  primarily `value_vlog` and a small `dictdb` delta.

## Comparison To Page-Copy Transcode

Earlier frozen transcode best result on the same source left:

- `leaf_vlog`: `2,225,095,409`
- remaining leaf gap: `85,641,440`

Logical rebuild improves from that to:

- `leaf_vlog`: `2,122,685,489`
- remaining leaf gap: `-16,967,896`

So the logical rebuild recovers about `102.4 MB` beyond the best page-copy
transcode path, which matches the earlier hypothesis that the remaining gap was
mostly page-layout / repacking, not dict selection.
