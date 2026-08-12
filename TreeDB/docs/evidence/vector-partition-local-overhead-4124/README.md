# Vector partition local-overhead reprofile (#4124)

Status: **timing gate failed** at exact clean source head
`6a2a34a8abdba5dd157856dccd4f580872327598`.

This is the minimum decisive integrated replay after #4121, #4122, and #4123
removed the measured membership-scan, row-sized search-scratch, and M5
JSON/double-MAC costs. It reuses the existing 48-cell #4108 final
qualification contract; it does not regenerate the partition graph or the
external five-system matrix.

All 48 cells completed 1,000/1,000 queries with zero query errors, timeouts,
retries, or redirects. The M18/eFC256 candidate preserves the selected quality:

- 250k recall: p2 `.9580`, p16 `.9598`;
- 100k recall: p2 `.9910`, p16 `.9930`;
- exact p2 routing recall: `.9981` at 250k and `.9980` at 100k;
- unchanged graph, router, query union, truth, p2/p16 probes, request EF128,
  candidate budget 256, top-k 10, and four-group topology.

The fail-closed reducer exited 1 with
`local HNSW final qualification timing gate failed`, so it correctly did not
publish a qualifying top-level report.

## What the optimization stack achieved

Every one of the 16 corpus/variant/probe/concurrency medians improved in
absolute QPS and p95 versus the pre-optimization #4108 run on the same host and
frozen graph identities.

| Corpus | Variant | Cell | QPS before -> after | p95 before -> after |
|---|---|---|---:|---:|
| 250k | M16/eFC128 | p2/c1 | 867.6 -> 1,521.5 (`1.754x`) | 1.321 -> 0.771 ms (`0.584x`) |
| 250k | M18/eFC256 | p2/c1 | 844.1 -> 1,405.9 (`1.666x`) | 1.336 -> 0.855 ms (`0.640x`) |
| 250k | M16/eFC128 | p2/c32 | 4,192.6 -> 7,088.2 (`1.691x`) | 11.561 -> 6.687 ms (`0.578x`) |
| 250k | M18/eFC256 | p2/c32 | 3,995.5 -> 6,418.3 (`1.606x`) | 12.264 -> 7.536 ms (`0.614x`) |
| 250k | M16/eFC128 | p16/c1 | 427.7 -> 650.5 (`1.521x`) | 2.852 -> 1.928 ms (`0.676x`) |
| 250k | M18/eFC256 | p16/c1 | 412.9 -> 594.9 (`1.441x`) | 2.896 -> 2.048 ms (`0.707x`) |
| 250k | M16/eFC128 | p16/c32 | 1,092.9 -> 1,531.7 (`1.402x`) | 40.596 -> 27.499 ms (`0.677x`) |
| 250k | M18/eFC256 | p16/c32 | 1,049.5 -> 1,460.0 (`1.391x`) | 40.332 -> 28.299 ms (`0.702x`) |
| 100k | M16/eFC128 | p2/c1 | 1,084.2 -> 1,654.8 (`1.526x`) | 1.127 -> 0.764 ms (`0.677x`) |
| 100k | M18/eFC256 | p2/c1 | 1,102.1 -> 1,638.2 (`1.486x`) | 1.101 -> 0.774 ms (`0.703x`) |
| 100k | M16/eFC128 | p2/c32 | 5,029.7 -> 7,372.2 (`1.466x`) | 9.300 -> 6.193 ms (`0.666x`) |
| 100k | M18/eFC256 | p2/c32 | 5,085.9 -> 6,501.9 (`1.278x`) | 9.287 -> 7.321 ms (`0.788x`) |
| 100k | M16/eFC128 | p16/c1 | 448.4 -> 557.3 (`1.243x`) | 2.551 -> 2.062 ms (`0.808x`) |
| 100k | M18/eFC256 | p16/c1 | 407.1 -> 532.7 (`1.309x`) | 2.781 -> 2.184 ms (`0.785x`) |
| 100k | M16/eFC128 | p16/c32 | 998.7 -> 1,206.6 (`1.208x`) | 42.334 -> 33.950 ms (`0.802x`) |
| 100k | M18/eFC256 | p16/c32 | 780.4 -> 1,144.9 (`1.467x`) | 58.245 -> 35.541 ms (`0.610x`) |

Most importantly, the original 100k p16/c32 blocker is repaired: candidate
QPS changes from `.7814x` to `.9489x` of baseline, while candidate p95 changes
from `1.3758x` to `1.0469x`. Both frozen limits now pass.

## Remaining failed gates

The final candidate still must retain at least `.90x` baseline QPS and at most
`1.10x` baseline p95 in every matched cell.

| Corpus | Cell | candidate / baseline QPS | candidate / baseline p95 | Result |
|---|---|---:|---:|---|
| 250k | p2/c1 | `.9241x` | `1.1096x` | fail p95 |
| 250k | p2/c32 | `.9055x` | `1.1270x` | fail p95 |
| 100k | p2/c32 | `.8819x` | `1.1821x` | fail QPS and p95 |

The three 100k p2/c32 repetitions all show lower candidate QPS and higher
candidate p95 than their order-balanced M16 controls, so this is not a single
outlier. The candidate performs the same requests and RPCs but, as expected
from its denser local graph, traverses more local work. At 100k p2 it visits
`3,962,475` candidates and `9,406,282` edges versus baseline `3,660,617` and
`8,376,700` (about `+8.2%` and `+12.3%`). At 250k p2 the corresponding deltas
are about `+10.5%` candidates and `+11.8%` edges.

Merged CPU profiles no longer show the full membership scan, and the retained
search profiles confirm the hot owner is the local HNSW/dot-product traversal,
not JSON framing or row-sized scratch construction. The denser candidate's
extra traversal is therefore the single measured blocker; it is not described
as residual network topology tax.

## Disposition and evidence

The conditional broader strict/fast/pinned x single/native/container matrix
was not run. The decisive product replay itself failed, so a larger unchanged
matrix cannot qualify this candidate and would only spend more evidence time.

- [`summary.json`](summary.json) records all 16 medians, repetition values,
  frozen gates, provenance, and a SHA-256 digest over all 432 raw artifacts.
- Raw evidence remains at
  `/mnt/fast4tb/gomap-4124-runtime/final-6a2a34a8/final/qualification-48cell-merged-stack-attempt2`
  (about 220 MB, including 48 reports, 48 transcripts, and 336 profiles).
- Runtime: 2:19:42, peak RSS 1,963,868 KiB, no swaps.
- The pre-optimization comparison remains at
  `/mnt/fast4tb/gomap-4108-runtime/qualified-261d8e21a/vcs-bound-rebuild-11f721/final/qualification-48cell-vcs-bound`.

The local-overhead implementations are retained because every absolute cell
improved materially and semantics remained exact. The M18/eFC256 candidate is
not qualified; #4104/#4108 remain open behind the one measured local-search
performance blocker, tracked by #4131.
