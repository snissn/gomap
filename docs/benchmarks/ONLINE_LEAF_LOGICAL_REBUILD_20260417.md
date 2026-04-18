# Online Leaf Logical Rebuild

This note measures the manual online incremental logical rebuild operator added
on `codex/online-leaf-logical-rebuild`, including the later planner/admission
iterations after the initial clustered-file prototype.

## Goal

Take the frozen logical rebuild proof from `#986` and turn it into a bounded
online-style operator that:

- rebuilds one bounded contiguous outer-leaf window at a time
- publishes through the vacuum-style index cutover path
- makes durable incremental progress
- stops instead of publishing a regression or spending unbounded time searching
  wide low-yield clusters

## Command

Binary:

```bash
GOWORK=off go build -o /tmp/treemap-online-leaf ./TreeDB/cmd/treemap
```

Conservative campaign on a frozen copy of a recent `main wal_on_fast`
post-dwell DB:

```bash
/tmp/treemap-online-leaf leafgen-logical-rebuild-run \
  /tmp/online_leaf_rebuild_ranked_try1_1776487209 \
  -rw \
  -json \
  -max-published-commit-seq 3118 \
  -cluster-files-max 8 \
  -candidate-try-max 1
```

After each successful run:

```bash
/tmp/treemap-online-leaf leafgen-gc /tmp/online_leaf_rebuild_ranked_try1_1776487209 -rw -json
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
/tmp/online_leaf_rebuild_ranked_try1_1776487209
```

Offline rewrite leaf floor for the same source, from `#986`:

- `leaf_vlog = 2,139,653,385`

## Result

Before campaign:

- `application.db = 2,576,631,807`
- `leaf_vlog = 2,345,184,409`
- `index.db = 61,341,696`
- `value_vlog = 169,699,763`

After 1 successful run plus GC:

- `application.db = 2,562,828,875`
- `leaf_vlog = 2,337,671,285`
- `index.db = 55,050,240`
- `value_vlog = 169,699,763`

Net improvement:

- `application.db`: `-13,802,932`
- `leaf_vlog`: `-7,513,124`
- `index.db`: `-6,291,456`
- `value_vlog`: unchanged

Leaf gap against the matching offline rewrite floor:

- before: `205,531,024`
- after: `198,017,900`
- closed: `7,513,124` (`3.66%`)

## Successful Run

Run 1:

- generation `52`
- raw file `2139095092`
- source leaf pages `8,308`
- source bytes `33,555,865`
- replacement leaf pages `8,458`
- elapsed: `4.17s`

Run 2:

- stopped with `leaf logical rebuild: no eligible sealed incremental candidate`
- this is with `candidate-try-max=1`, so the operator refused to spend more
  rebuild work on lower-ranked same-width candidates once the top-ranked
  width tiers had no publishable winner

## Planner Iterations

The earlier clustered prototype was not a good admission policy:

- scattered 4-file clusters produced tens of thousands of disjoint run slices
- later runs could regress `leaf_vlog` after GC even when the local `.log` gate
  passed
- larger `candidate-try-max` values reintroduced long rebuild-and-reject search

The current manual operator now does three things differently:

1. candidate windows are ranked narrow-first
2. within a width tier, ranking prefers higher bytes-per-page density
3. publish gating uses total leaf footprint, including `.lenidx`, not only the
   raw `.log` byte size

## Interpretation

This branch now proves four things:

1. The online operator can make real durable incremental progress on the real
   Celestia-shaped sealed population.
2. The vacuum-coupled publish path works for this leaf-only logical rebuild.
3. A hard total-byte gate is required. The earlier `.log`-only gate was too
   weak and allowed flat or regressive clustered publishes.
4. Candidate search cost is now the dominant issue. `candidate-try-max=1`
   gives a fast safe positive step; larger search budgets start paying a lot of
   rebuild work just to discover that most remaining candidates are not worth
   publishing.

This is still not a high-closure maintenance system. It is a safer manual
online primitive with one clear positive incremental step and a tighter
admission policy.

The next likely improvement is a better pre-rebuild planner:

- expose candidate windows without rebuilding them
- estimate total bytes saved before invoking `bulk.BuildWithOptions(...)`
- only spend rebuild work on candidates with a strong expected total-byte win
