# TreeDB Checkpoint Wait Classification M1 Report

Issue: [#2946](https://github.com/snissn/gomap/issues/2946)
Parent tracker: [#2943](https://github.com/snissn/gomap/issues/2943)
Root parents: [#2916](https://github.com/snissn/gomap/issues/2916) / [#2899](https://github.com/snissn/gomap/issues/2899)

## Verdict

Classification: **productive checkpoint debt drain**.

The immediate c4/c8/c16 checkpoint rows all show `active_background_flush_wait`
draining the full pre-checkpoint frontier visible at request time. Total wall for
`random_write + checkpoint` is not worse than `random_write + settle + checkpoint`
for the same c4/c8/c16 shape. There is no close/checkpoint span-native fallback
and no post-frontier queue debt in these checkpoint snapshots.

Action: **do not open a checkpoint handoff/ownership implementation issue from
this milestone**. The wait is real wall time, but the counters classify it as
background workers already draining checkpoint-relevant debt, not idle
coordination loss. Keep #2916/#2899 open for the broader parent gates; this
report does not claim parent closure.

## Scope And Harness Notes

- Production checkpoint ownership/coordinator behavior was not changed.
- This PR adds benchmark-only/reporting support to preserve checkpoint-local
  TreeDB stats under `benchprof_results.json` and to optionally wait before a
  selected checkpoint row. That was needed because end-of-run stats can be
  overwritten by later no-op checkpoints.
- The `settled` rows used `-checkpoint-settle-before-tests=random_read`, which
  waits for TreeDB queue/backlog and active in-flight counters to become idle
  before the `random_read` checkpoint. On this host, c8/c16 still sometimes had
  a new active worker by the time checkpoint requested `flushMu`; those rows are
  therefore treated as partially settled harness rows, not proof that active wait
  can be eliminated. They still support the classification because the active
  wait drains all visible frontier debt and total wall is not worse.
- A redundant later run root, `/tmp/gomap_2946_checkpoint_wait_20260623_124218`,
  was stopped and is intentionally ignored. The complete evidence root is the
  one listed below.

## Evidence Identity

- Evidence root: `/tmp/gomap_2946_checkpoint_wait_20260623_122739`
- Evidence head SHA: `a7f1f9cb11cd939bec6a28d6c2caa6d76e9acd65`
- Evidence base SHA: `4613e89013a7708413185dedbf0bbb89025577a1` (`origin/main`, includes
  #2945/#2949 and #2950)
- Branch was later rebased onto `8d430e1b4e6e837f10c09db7bbb2b6a341768e64`
  (PR #2962 / #2947) without rerunning the full matrix, per coordinator
  direction.
- Host: `mikers-B560-DS3H-AC-Y1`
- CPU: Intel i5-11400F, 6 cores / 12 threads (`nproc=12`)
- OS: Linux `6.8.0-124-generic` x86_64
- Host notes/artifacts: `manifest.txt`, `lscpu.txt`, `uname.txt` in the evidence
  root.

All substantial benchmark commands were run under:

```sh
flock /tmp/gomap_diag_bench.lock -c '<benchmark command>'
```

Command template:

```sh
./bin/unified-bench \
  -dbs treedb \
  -test sequential_write,batch_random,random_write,random_read \
  -keys 10000000 -valsize 128 -batchsize 8000 \
  -checkpoint-between-tests \
  -treedb-flush-admission-policy=explicit \
  -treedb-flush-apply-span-native \
  -treedb-flush-backlog-coalescing \
  -treedb-flush-apply-min-entries=1 \
  -treedb-flush-apply-min-spans=1 \
  -treedb-flush-apply-min-bytes=1 \
  -treedb-flush-apply-concurrency=<4|8|16> \
  -profile-dir <row-dir> \
  -path-label m8-m14-10mm-gate \
  -read-require-hit \
  -progress=false
```

Settled rows additionally used:

```sh
-checkpoint-settle-before-tests=random_read -checkpoint-settle-timeout=10m
```

Each row directory contains `command.txt`, `run.log`, `benchprof_results.json`,
`benchprof_results.md`, `insights.{md,json,html}`, CPU/alloc profiles,
checkpoint CPU profiles, block/mutex profiles, and `trace.out`.

## Total-Wall Rows

`write wall` is computed as `10,000,000 / random_write_ops_per_sec`. `checkpoint
wall` is the checkpoint before `random_read`, i.e. the checkpoint after the
`random_write` phase.

| c | row | random_write ops/s | write wall s | settle wall s | checkpoint wall s | total wall s | artifact |
|---:|---|---:|---:|---:|---:|---:|---|
| c4 | immediate | 274,088 | 36.48 | 0.00 | 9.21 | 45.69 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/immediate_c4` |
| c4 | settled | 250,018 | 40.00 | 1.91 | 6.37 | 48.28 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/settled_c4` |
| c8 | immediate | 335,408 | 29.81 | 0.00 | 10.16 | 39.97 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/immediate_c8` |
| c8 | settled | 348,151 | 28.72 | 3.95 | 6.91 | 39.59 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/settled_c8` |
| c16 | immediate | 355,888 | 28.10 | 0.00 | 10.68 | 38.78 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/immediate_c16` |
| c16 | settled | 314,057 | 31.84 | 0.005 | 7.17 | 39.02 | `/tmp/gomap_2946_checkpoint_wait_20260623_122739/settled_c16` |

Immediate-vs-settled total comparison:

| c | immediate total s | settled total s | immediate - settled s | interpretation |
|---:|---:|---:|---:|---|
| c4 | 45.69 | 48.28 | -2.58 | immediate is faster/equal |
| c8 | 39.97 | 39.59 | +0.39 | similar within run noise |
| c16 | 38.78 | 39.02 | -0.23 | immediate is faster/equal |

## Checkpoint Productivity Counters

The key signal is whether active wait drains checkpoint-relevant frontier debt.
All immediate rows drained the full frontier captured at request. c8/c16 have no
checkpoint-owned drain because the frontier was already gone by the time
checkpoint owned the drain path.

| c | row | debt at request units / MiB | active workers / frontier lanes at request | drained during active wait units / MiB / lanes | wait drain MiB/s | checkpoint-owned drain units / MiB | checkpoint-owned MiB/s | productive wait ratio |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| c4 | immediate | 32 / 128.00 | 1 / 1 | 32 / 128.00 / 1 | 34.81 | 16 / 16.91 | 3.10 | 11.24 |
| c4 | settled | 0 / 0.00 | 0 / 0 | 0 / 0.00 / 0 | 0.00 | 16 / 16.97 | 2.69 | 0.00 |
| c8 | immediate | 16 / 16.96 | 1 / 1 | 16 / 16.96 / 1 | 3.52 | 0 / 0.00 | 0.00 | 0.00 |
| c8 | settled | 16 / 16.95 | 1 / 1 | 16 / 16.95 / 1 | 3.96 | 0 / 0.00 | 0.00 | 0.00 |
| c16 | immediate | 16 / 16.96 | 1 / 1 | 16 / 16.96 / 1 | 5.60 | 0 / 0.00 | 0.00 | 0.00 |
| c16 | settled | 16 / 16.95 | 1 / 1 | 16 / 16.95 / 1 | 2.78 | 0 / 0.00 | 0.00 | 0.00 |

Notes:

- `productive_wait_ratio=0` in c8/c16 is a denominator artifact: the checkpoint
  owned no frontier bytes after active wait, so no owned-drain rate exists for a
  ratio comparison.
- c4 is the clean side-by-side comparison: immediate active wait drains 128 MiB
  at 34.81 MiB/s while the later checkpoint-owned drain rate is 3.10 MiB/s.
  That is productive, not underproductive.

## Checkpoint Stage Counters

`active-bg wait` is row-local (`*_last`). The public stats currently expose
`barrier_wait`/`flushmu_wait` totals and max, not a row-local last value; for
rows with active background wait, the row-local active wait is the relevant
flushMu/barrier wait component.

| c | row | active-bg wait s | barrier/flushMu note | flush_all s | backend_boundary s | reducer_publish s | leaf_value_log_sync s | WAL rotate s | WAL cleanup s |
|---:|---|---:|---|---:|---:|---:|---:|---:|---:|
| c4 | immediate | 3.68 | active wait last | 5.46 | 0.00 | 0.31 | 0.006 | 0.065 | 0.000002 |
| c4 | settled | 0.00 | no active wait last; only max counter is run-scoped | 6.32 | 3.74 | 0.33 | 0.005 | 0.046 | 0.000002 |
| c8 | immediate | 4.83 | active wait last | 6.13 | 0.99 | 0.33 | 0.006 | 0.087 | 0.000003 |
| c8 | settled | 4.28 | active wait last | 6.80 | 0.36 | 0.33 | 0.006 | 0.091 | 0.000002 |
| c16 | immediate | 3.03 | active wait last | 6.74 | 0.41 | 0.33 | 0.006 | 0.090 | 0.000002 |
| c16 | settled | 6.09 | active wait last | 7.07 | 1.22 | 0.34 | 0.006 | 0.091 | 0.000002 |

## Fallback And Post-Frontier Counters

| c | row | close/checkpoint fallback ops / spans | any fallback ops | checkpoint-owned workers / lanes | background drain units / MiB | wait post units / MiB | frontier post units / MiB |
|---:|---|---:|---:|---:|---:|---:|---:|
| c4 | immediate | 0 / 0 | 0 | 1 / 1 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |
| c4 | settled | 0 / 0 | 0 | 1 / 1 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |
| c8 | immediate | 0 / 0 | 0 | 0 / 0 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |
| c8 | settled | 0 / 0 | 0 | 0 / 0 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |
| c16 | immediate | 0 / 0 | 0 | 0 / 0 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |
| c16 | settled | 0 / 0 | 0 | 0 / 0 | 0 / 0.00 | 0 / 0.00 | 0 / 0.00 |

All span-native fallback reason `ops_total` counters were zero in these
checkpoint snapshots, including `close_or_checkpoint`.

## Classification Against #2946 Rules

| Rule | Evidence | Result |
|---|---|---|
| Productive debt drain | Active wait drains all request frontier in immediate c4/c8/c16. Total wall immediate is equal/faster than settled for c4/c16 and within 0.39s for c8. | **Pass** |
| Underproductive wait | Would require wait to drain much slower or with fewer lanes/workers than checkpoint-owned drain. c4 shows wait rate above owned drain; c8/c16 leave no owned drain to compare. | Not supported |
| Wasteful wait | Would require large active wait with little checkpoint-relevant drain. All immediate waits drain the full visible frontier. | Not supported |
| Harness/noise | c8/c16 settled rows are only partially settled, but the immediate rows and total-wall comparison are consistent enough for this classification. | Caveat, not blocker |

## Follow-Up Guidance

- Do not pursue a checkpoint ownership/coordinator PR just to move
  `active_background_flush_wait` into `flush_all`; #2946 evidence says that would
  mostly relabel productive debt drain.
- If a future decision needs a strictly quiescent settled row, make the harness
  require a stable idle window for queue/backlog/active in-flight counters before
  the settled checkpoint. That is not needed to classify #2946.
- Continue #2943 through apply/hardware-ceiling and branch-selection gates. Do
  not close #2916/#2899 based on this report alone.
