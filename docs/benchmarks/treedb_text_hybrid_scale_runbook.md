# TreeDB text-v2/hybrid scale harness runbook (#2731)

This runbook is the #2731 capture contract for 1M-10M TreeDB text-v2 and
text+vector hybrid scale validation. The harness is intentionally B-tree-native:
it uses collection text-v2 indexes, optional column-graph vectors, scalar indexes,
normal TreeDB checkpoint/reopen, text-index rewrite, and ordinary storage cleanup.
It does not introduce an external IR sidecar, standalone text assets, or a
separate text-block GC path.

The default script run is a bounded smoke. Full 10M runs can take multiple hours
and tens of GB; **do not start a 10M run without explicit coordinator approval**.

## Stable commands

### Bounded local smoke

Use this before a PR or after touching the harness:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_smoke_$(date +%Y%m%d_%H%M%S) \
SMOKE_ROWS=128 SMOKE_QUERIES=3 SMOKE_BATCH_SIZE=64 \
DIMS=4 M=4 EF_CONSTRUCTION=32 EF_SEARCH=32 READERS=2 \
scripts/bench_text_hybrid_scale.sh
```

The smoke exercises load, text-only retrieval, hybrid retrieval, reopen,
concurrent search/write sanity, maintenance rewrite postconditions, and text
backfill with tiny deterministic fixtures.

### 1M scale matrix

The selected 1M command is opt-in via `RUN_1M=true`:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_1m_$(date +%Y%m%d_%H%M%S) \
RUN_1M=true RUN_SMOKE=false \
ONE_M_ROWS=1000000 ONE_M_QUERIES=25 ONE_M_BATCH_SIZE=16384 \
ONE_M_BACKFILL_ROWS=100000 \
ONE_M_MAINTENANCE_UPDATES=10000 ONE_M_MAINTENANCE_DELETES=5000 \
ONE_M_CANDIDATE_LIMIT=65536 \
DIMS=16 M=8 EF_CONSTRUCTION=128 EF_SEARCH=128 \
TOP_K=10 READERS=4 \
scripts/bench_text_hybrid_scale.sh
```

Direct command equivalent:

```sh
OUT=/tmp/gomap_text_hybrid_scale_1m_direct_$(date +%Y%m%d_%H%M%S)
GOWORK=off go run ./cmd/treedb_text_hybrid_scale \
  -out-dir "$OUT" \
  -rows 1000000 -batch-size 16384 -dims 16 -m 8 \
  -ef-construction 128 -ef-search 128 \
  -top-k 10 -candidate-limit 65536 -queries 25 -readers 4 \
  -backfill-rows 100000 \
  -maintenance-updates 10000 -maintenance-deletes 5000 \
  -keep-db=false -base-ref origin/main \
  -base-sha "$(git merge-base HEAD origin/main)"
```

### Retrieval-only qualification

Use the Go command directly when load and query evidence is required without
running concurrent, maintenance, or backfill probes:

```sh
OUT=/tmp/gomap_text_hybrid_retrieval_1m_$(date +%Y%m%d_%H%M%S)
GOWORK=off go run ./cmd/treedb_text_hybrid_scale \
  -out-dir "$OUT" \
  -rows 1000000 -batch-size 16384 -dims 16 -m 8 \
  -ef-construction 128 -ef-search 128 \
  -top-k 10 -candidate-limit 65536 -queries 25 -readers 4 \
  -phases retrieval -keep-db=false -base-ref origin/main \
  -base-sha "$(git merge-base HEAD origin/main)"
```

`-phases retrieval` selects load, queries, and reopen; the default `-phases all`
retains the full historical campaign. The wrapper equivalent is
`PHASES=retrieval`. For the selected 1M wrapper row, set
`RETRIEVAL_REPETITIONS=N` to run `N` fresh processes in separate output
directories; values greater than one are rejected unless `PHASES=retrieval`.

The command atomically rewrites its JSON and Markdown reports after each
completed phase and guardrail failure. `selected_phases`, `completed_phases`,
and `complete` distinguish surviving partial evidence from a completed run. A
failed guardrail is always incomplete evidence: strict mode returns the
guardrail error after persisting phase data, while
`-allow-guardrail-failures` may continue eligible later diagnostic phases
without completing the failed phase or report.

### Selected 10M matrix (approval gated)

The script writes `$RUN_DIR/10m_selected_matrix_commands.md` on every run. A
10M selected row is gated twice: `RUN_10M=true` asks for the row, and
`APPROVE_10M=true` confirms explicit coordinator approval.

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_10m_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_10M=true APPROVE_10M=true \
TEN_M_ROWS=10000000 TEN_M_QUERIES=10 TEN_M_BATCH_SIZE=32768 \
TEN_M_BACKFILL_ROWS=1000000 \
TEN_M_MAINTENANCE_UPDATES=10000 TEN_M_MAINTENANCE_DELETES=5000 \
TEN_M_CANDIDATE_LIMIT=655360 \
DIMS=16 M=8 EF_CONSTRUCTION=128 EF_SEARCH=128 \
TOP_K=10 READERS=4 \
scripts/bench_text_hybrid_scale.sh
```

Do not run the command above unless the coordinator has explicitly approved the
runtime and storage cost for the current machine. The selected 10M command keeps
strict guardrails: if current text/hybrid bounded candidate generation fails
closed on common-term rows, treat that as scale evidence and a follow-up sizing
input, not as a passing latency row.

### Optional allocation benchmark rows

The scale command records wall-clock latency and counters. For one selected
hybrid row allocation profile, start the process with `GODEBUG=memprofilerate=1`,
select exactly one hybrid row with `-query-rows`, and pass `-alloc-profile`.
For example, this complete small capture selects the real
`hybrid_text_scalar_no_docs` row:

```sh
OUT=/tmp/gomap_text_hybrid_alloc_$(date +%Y%m%d_%H%M%S)
GODEBUG=memprofilerate=1 GOWORK=off go run ./cmd/treedb_text_hybrid_scale \
  -out-dir "$OUT" -rows 96 -batch-size 48 -dims 4 -m 4 \
  -ef-construction 32 -ef-search 32 -top-k 5 -candidate-limit 16 -queries 3 \
  -include-vector=false -phases=retrieval -run-reopen=false \
  -query-rows=hybrid_text_scalar_no_docs -alloc-profile "$OUT/allocs.pprof"
go tool pprof -base "$OUT/allocs.pprof.base" -ignore='runtime/pprof' "$OUT/allocs.pprof"
```

It writes a cumulative baseline immediately before the timed samples and a
cumulative post-query profile afterward. Interpret the allocation delta with
the `-base` command above. Subtraction includes allocations from child
goroutines, including concurrent vector-candidate workers; the `runtime/pprof`
ignore removes profile-serialization work performed after the baseline.
Fixture construction and warm-up precede both captures. The baseline and final
profile are an inseparable evidence pair; old single-file allocation profiles
must be regenerated. Use companion Go benchmarks when making allocation
claims:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_bench_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_GO_BENCH=true GO_BENCH_ROWS=1000000 \
GO_BENCHTIME=1x GO_COUNT=1 \
scripts/bench_text_hybrid_scale.sh
```

## Runtime, storage, and cleanup gates

Recommended planning gates before 1M/10M runs:

- verify the target filesystem has enough free space with `df -h "$RUN_DIR"`;
- keep `KEEP_DB=false` unless you need post-run forensics;
- use a dedicated `/tmp/gomap_text_hybrid_scale_*` output directory per run;
- avoid running several scale jobs on the same host at once;
- treat 1M as a potentially long local job and 10M as a multi-hour/tens-of-GB job;
- stop and preserve the artifact directory if guardrails fail unexpectedly.

The 1M selected command uses `ONE_M_CANDIDATE_LIMIT=65536` as the fixed
requested budget so the hybrid+scalar row can cover the synthetic 6.25% tenant
allow-set while preserving score-only, zero-document candidate generation.
Current reports also include adaptive budget counters (`text_budget=effective/requested`,
`vector_budget=effective/requested`, `budget_policy`, `budget_stop`, and
`budget_fallback`) so top-k RRF rows can show exact effective-budget reductions
without changing the requested-budget guardrail. Smaller candidate budgets are
still useful as diagnostic fail-closed probes, but do not cite them as passing
1M hybrid rows if guardrails fail.

By default, the primary, backfill, and maintenance DB directories are removed at
successful process exit. Set `KEEP_DB=true` only when you need to inspect storage
contents after the run; then remove the artifact directory manually, for example:

```sh
rm -rf /tmp/gomap_text_hybrid_scale_1m_YYYYmmdd_HHMMSS
```

## Artifacts and schema

`cmd/treedb_text_hybrid_scale` writes schema
`treedb_text_hybrid_scale/v2`. Version 2 adds selected/completed phase state,
atomic partial-report evidence, invocation/VCS provenance, and raw per-query
latency samples with row-boundary/query-shape provenance so each retrieval claim
can be inspected independently rather than inferred only from percentiles.

Primary artifacts:

- `$RUN_DIR/context.txt` — branch, commit, base SHA, Go version, host context,
  and disk context captured by the wrapper script.
- `$RUN_DIR/scale_*/command.txt` — exact command for each scale row.
- `$RUN_DIR/scale_*/run.log` — stdout/stderr for the row.
- `$RUN_DIR/scale_*/scale_report.json` — machine-readable report.
- `$RUN_DIR/scale_*/scale_report.md` — human-readable report.
- `$RUN_DIR/10m_selected_matrix_commands.md` — reproducible gated 10M commands.
- `$RUN_DIR/go_bench_*/*.txt` — optional `go test -benchmem` output.
- `context.command` — labeled `process_argv`; under `go run` its first element
  is Go's temporary executable, so use the wrapper's `command.txt` as the
  reproducible caller command.

The JSON/Markdown report includes:

- load/build timings, rows/s, text storage, vector rebuild status, checkpoint
  time, storage bytes, bytes/doc, and text-v2 lane bytes/doc (docid, docmap,
  postings, norms, positions, term stats, status/format);
- text-only common, rare, multi-term AND, and multi-term OR score-only query rows;
- hybrid text-only, text+scalar, and optional text+vector(+scalar) query rows;
- raw per-query latency samples plus p50/p95/p99/mean latency and derived
  ops/sec for each retrieval row;
- optional CPU (`-cpu-profile`) or paired baseline/post-query allocation
  (`-alloc-profile`) capture for one selected hybrid row via `-query-rows`;
  allocation capture requires process startup with `GODEBUG=memprofilerate=1`
  and is interpreted by subtracting the generated `.base` profile with
  `go tool pprof -base`;
- reopen close/open/probe timings;
- concurrent reader/write sanity timing and guardrail state;
- maintenance update/delete/rewrite/checkpoint timings and stale-posting purge
  postconditions;
- text backfill timing/storage when enabled;
- ranked bottleneck rows for follow-up sizing.

## Guardrails and interpretation

A passing candidate-generation row must keep these counters at zero:

- `docs_fetched` / `DocumentsFetched`;
- `full_doc_fallbacks` / `FullDocumentScanFallbacks`;
- `fail_closed` / `FailClosed`;
- `text_state_lookups`;
- `text_match_details`.

Guardrail failures mean the row is not valid zero-document candidate-generation
evidence. `-allow-guardrail-failures` is only for diagnosing broken runs; do not
cite such a report as passing scale evidence.

The corpus is deterministic synthetic customer-support text plus synthetic dense
vectors. It is useful for scale, storage, maintenance, and regression triage, but
it is not relevance-quality evidence and is not an industry-parity claim by
itself. Benchmark/optimization claims still need baseline-vs-candidate evidence,
exact commands, commit/SHA, host context, measured boundary, `ns/op`/ops/sec and
`B/op`/allocs/op where applicable, plus the domain counters above.
