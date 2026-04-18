# Online Leaf Logical Rebuild

This note measures the manual online incremental logical rebuild operator added
on `codex/online-leaf-logical-rebuild`, including the planner iterations after
the initial clustered-file prototype.

## Goal

Take the frozen logical rebuild proof from `#986` and turn it into a bounded
online-style operator that:

- rebuilds one bounded outer-leaf candidate at a time
- publishes through the vacuum-style index cutover path
- makes durable incremental progress
- stops instead of publishing a regression or spending unbounded time searching
  low-yield candidates

## Command

Binary:

```bash
GOWORK=off go build -o /tmp/treemap-online-leaf ./TreeDB/cmd/treemap
```

Bounded campaign on a frozen copy of a recent `main wal_on_fast` post-dwell DB:

```bash
for run in 1 2 3 4 5 6 7 8 9 10 11 12; do
  /tmp/treemap-online-leaf leafgen-logical-rebuild-run \
    /tmp/online_leaf_rebuild_ranges_1776489806 \
    -rw \
    -json \
    -max-published-commit-seq 3118 \
    -cluster-files-max 8 \
    -candidate-try-max 4 >> /tmp/online_leaf_rebuild_ranges_1776489806/campaign.jsonl \
  && /tmp/treemap-online-leaf leafgen-gc /tmp/online_leaf_rebuild_ranges_1776489806 -rw -json \
    >> /tmp/online_leaf_rebuild_ranges_1776489806/campaign.jsonl \
  || break
done
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
/tmp/online_leaf_rebuild_ranges_1776489806
```

Offline rewrite leaf floor for the same source, from `#986`:

- `leaf_vlog = 2,139,653,385`

## Result

Before campaign:

- `application.db = 2,576,631,807`
- `leaf_vlog = 2,345,184,409`
- `index.db = 61,341,696`
- `value_vlog = 169,695,667`

After 12 successful runs plus GC:

- `application.db = 2,521,680,293`
- `leaf_vlog = 2,298,855,009`
- `index.db = 52,690,944`
- `value_vlog = 169,695,667`

Net improvement:

- `application.db`: `-54,951,514`
- `leaf_vlog`: `-46,329,400`
- `index.db`: `-8,650,752`
- `value_vlog`: unchanged

Leaf gap against the matching offline rewrite floor:

- before: `205,531,024`
- after: `159,201,624`
- closed: `46,329,400` (`22.54%`)

Compared to the earlier safe-but-weak online planner result on the same source:

- old planner closed `7,513,124` of leaf gap (`3.66%`)
- current planner closes `46,329,400` of leaf gap (`22.54%`)
- improvement over the old planner: `+38,816,276` leaf bytes

## Campaign Shape

The campaign stayed incremental all the way through 12 bounded runs:

- runs `1-5` found smaller single-, two-, and three-generation candidates
- runs `6-12` continued making progress with wider three- and four-generation
  candidates
- every successful run was followed by `leafgen-gc`, and the post-GC DB stayed
  smaller after each publish

Largest later successful candidates:

- run `10`
  - generations: `185, 19, 30, 31`
  - source files: `4`
  - source bytes: `106,454,649`
  - replacement pages: `43,604`
  - elapsed: `35.27s`
- run `11`
  - generations: `33, 34, 35, 110`
  - source files: `4`
  - source bytes: `108,591,276`
  - replacement pages: `45,270`
  - elapsed: `35.98s`
- run `12`
  - generations: `28, 29, 49`
  - source files: `3`
  - source bytes: `100,666,724`
  - replacement pages: `34,383`
  - elapsed: `34.50s`

## Planner Iterations

The initial online planner had two structural problems:

1. it collapsed each source file to one `firstIndex..lastIndex` span, which
   forced rebuilds across bridge pages between disjoint live runs
2. it had no cheap pre-rebuild estimate, so larger search budgets could spend a
   lot of time doing full logical rebuilds only to reject them at publish time

The current manual operator now does four things differently:

1. candidate groups are still ranked narrow-first
2. within a width tier, ranking prefers higher retireable-bytes per rebuilt page
3. each sealed file contributes the union of its live contiguous ranges rather
   than one giant bridged span
4. a pilot rebuild estimate samples bounded ranges before the full rebuild and
   skips candidates that are unlikely to clear the total-byte savings gate

## Interpretation

This branch now proves five things:

1. The online operator can make real durable incremental progress on the real
   Celestia-shaped sealed population.
2. The vacuum-coupled publish path works for this leaf-only logical rebuild.
3. The pre-rebuild planner matters more than the cutover path now. The jump
   from `3.66%` to `22.54%` leaf-gap closure came from better candidate
   modeling, not from changing the publish mechanism.
4. A hard total-byte gate is still required. The operator remains safe because
   it only publishes candidates that leave the DB smaller after GC.
5. The online path is still materially below the frozen logical rebuild ceiling.
   It now closes a meaningful fraction of the leaf gap, but not enough to claim
   parity with offline rewrite.

This is now a real online primitive, not just a one-shot proof. But it is still
not the final maintenance design.

The next likely improvement is not more cutover work. It is one of:

- better grouping of multi-generation candidates before pilot rebuild
- stronger stop conditions based on marginal savings per run
- eventually, scheduler integration after the manual operator is judged good
  enough to automate
