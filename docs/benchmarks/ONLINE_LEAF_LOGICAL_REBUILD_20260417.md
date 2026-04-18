# Online Leaf Logical Rebuild

This note measures the manual online incremental logical rebuild operator added
on `codex/online-leaf-logical-rebuild`.

## Goal

Take the frozen logical rebuild proof from `#986` and turn it into a bounded
online-style operator that:

- rebuilds one sealed outer-leaf candidate at a time
- publishes through the vacuum-style index cutover path
- makes durable incremental progress
- stops instead of publishing a regression

## Command

Binary:

```bash
GOWORK=off go build -o /tmp/treemap-online-leaf ./TreeDB/cmd/treemap
```

Campaign on a frozen copy of a recent `main wal_on_fast` post-dwell DB:

```bash
/tmp/treemap-online-leaf leafgen-logical-rebuild-run \
  /tmp/online_leaf_rebuild_watermark_gate_1776482663 \
  -rw \
  -json \
  -max-published-commit-seq 3118
```

After each successful run:

```bash
/tmp/treemap-online-leaf leafgen-gc /tmp/online_leaf_rebuild_watermark_gate_1776482663 -rw -json
```

The `3118` watermark pins candidate selection to the original sealed generation
population from the source DB, so the campaign does not immediately start
rewriting its own outputs.

## Source

Frozen source copy from:

```text
/home/mikers/.celestia-app-mainnet-treedb-20260417111043/data/application.db
```

Measured working copy:

```text
/tmp/online_leaf_rebuild_watermark_gate_1776482663
```

Offline rewrite leaf floor for the same source, from `#986`:

- `leaf_vlog = 2,139,653,385`

## Result

Before campaign:

- `application.db = 2,576,631,807`
- `leaf_vlog = 2,345,184,409`
- `index.db = 61,341,696`
- `value_vlog = 169,699,763`

After 3 successful runs plus GC after each run:

- `application.db = 2,557,070,258`
- `leaf_vlog = 2,332,962,892`
- `index.db = 54,001,664`
- `value_vlog = 169,699,763`

Net improvement:

- `application.db`: `-19,561,549`
- `leaf_vlog`: `-12,221,517`
- `index.db`: `-7,340,032`
- `value_vlog`: unchanged

Leaf gap against the matching offline rewrite floor:

- before: `205,531,024`
- after: `193,309,507`
- closed: `12,221,517` (`5.95%`)

## Successful Runs

Run 1:

- generation `149`
- raw file `2139095189`
- source leaf pages `73,675`
- source bytes `131,137,625`
- replacement leaf pages `67,559`

Run 2:

- generation `151`
- raw file `2139095191`
- source leaf pages `59,395`
- source bytes `59,827,719`
- replacement leaf pages `46,881`

Run 3:

- generation `147`
- raw file `2139095187`
- source leaf pages `48,221`
- source bytes `76,769,028`
- replacement leaf pages `45,270`

Run 4:

- stopped with `leaf logical rebuild: no eligible sealed single-file candidate`
- this is the intended gated behavior for v1: once the remaining original
  candidates fail the size-improvement check, the operator stops instead of
  publishing a larger replacement

## Interpretation

This branch proves three things:

1. The online operator can make real durable incremental progress on the real
   Celestia-shaped sealed population.
2. The vacuum-coupled publish path works for this leaf-only logical rebuild.
3. A hard size gate is required. Without it, the campaign eventually starts
   publishing larger rebuilt files.

This is not yet a high-closure maintenance system. It is a safe bounded online
primitive with real positive progress and a clear stop condition.

The next likely improvement is candidate quality, not more cutover work:

- broader candidate search within the same watermark
- better predicted-bytes-saved admission before rebuild
- possibly candidate grouping beyond a single sealed file where the live refs
  are too fragmented for a single-file rebuild to recover much density
