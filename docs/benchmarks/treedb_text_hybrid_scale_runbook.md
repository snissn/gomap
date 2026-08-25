# TreeDB text-v2/hybrid scale harness runbook (#2731)

This harness qualifies TreeDB text-v2 and text+vector hybrid retrieval using
collection indexes, normal checkpoint/reopen, and ordinary storage cleanup. It
uses no external IR sidecar or standalone text assets.

The default script run is a bounded smoke. Full 10M runs can take multiple hours
and tens of GB; **do not start a 10M run without explicit coordinator approval**.

## Phase selection

`cmd/treedb_text_hybrid_scale` writes schema
`treedb_text_hybrid_scale/v2` and accepts:

- `-phases=all` (default): load, queries, reopen, concurrent, maintenance, and
  backfill.
- `-phases=retrieval`: the bounded #4327 qualification: load, retrieval matrix,
  and reopen.

Selectors are comma-separated tokens, so every token is validated: an unknown
or empty token fails the command. `all,retrieval` resolves to the full ordered
set. Disabled probes are removed from the selected set; for example,
`-phases=retrieval -run-reopen=false` selects only load and queries.

Use the bounded retrieval command for repeatable retrieval qualification:

```sh
RUN_DIR=/tmp/gomap_text_hybrid_retrieval_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_1M=true PHASES=retrieval RETRIEVAL_REPETITIONS=2 \
ONE_M_ROWS=1000000 ONE_M_QUERIES=25 ONE_M_BATCH_SIZE=16384 \
ONE_M_CANDIDATE_LIMIT=65536 \
DIMS=16 M=8 EF_CONSTRUCTION=128 EF_SEARCH=128 TOP_K=10 READERS=4 \
scripts/bench_text_hybrid_scale.sh
```

`RETRIEVAL_REPETITIONS>1` is accepted only with exactly
`PHASES=retrieval`; the wrapper rejects a repeated full campaign.

## 10M plan

The wrapper writes `$RUN_DIR/10m_selected_matrix_commands.md` on every run.
It propagates `PHASES` both to the generated wrapper command and its direct
command equivalent. A 10M run is gated twice: `RUN_10M=true` requests it and
`APPROVE_10M=true` confirms coordinator approval.

```sh
RUN_DIR=/tmp/gomap_text_hybrid_scale_10m_$(date +%Y%m%d_%H%M%S) \
RUN_SMOKE=false RUN_10M=true APPROVE_10M=true PHASES=retrieval \
TEN_M_ROWS=10000000 TEN_M_QUERIES=10 TEN_M_BATCH_SIZE=32768 \
TEN_M_CANDIDATE_LIMIT=655360 \
DIMS=16 M=8 EF_CONSTRUCTION=128 EF_SEARCH=128 TOP_K=10 READERS=4 \
scripts/bench_text_hybrid_scale.sh
```

## V2 reports and failure handling

The command updates `scale_report.json` and `scale_report.md` atomically after
each completed phase. Both artifacts carry the ordered `selected_phases`,
`completed_phases`, and `complete` state. Markdown labels a non-final artifact
as **INCOMPLETE (partial/resumable evidence; not a completed qualification)**.
A partial report is evidence of completed work only; it is not a passing
qualification.

Each retrieval row records its query shape, measured boundary, raw per-query
latency samples (`raw_latency_ns`), summaries, result count, and guardrail
counters. This keeps raw observations attached to the exact retrieval boundary
rather than requiring interpretation from percentiles alone.

Report provenance includes repository root, branch, commit, requested base and
base SHA, command line, VCS cleanliness and status captured together, Go and
host context, binary state, deterministic corpus identity, cache state,
durability statement, and noise policy.

Primary wrapper artifacts are:

- `$RUN_DIR/context.txt` — wrapper host and repository context.
- `$RUN_DIR/scale_*/command.txt` and `run.log` — exact invocation and output.
- `$RUN_DIR/scale_*/scale_report.json` and `scale_report.md` — atomic phase
  reports.
- `$RUN_DIR/10m_selected_matrix_commands.md` — approval-gated 10M commands.

## Guardrails

A passing candidate-generation row keeps these counters at zero:

- `docs_fetched` / `DocumentsFetched`;
- `full_doc_fallbacks` / `FullDocumentScanFallbacks`;
- `fail_closed` / `FailClosed`;
- `text_state_lookups`;
- `text_match_details`.

Guardrail failures invalidate the row as zero-document candidate-generation
evidence. `-allow-guardrail-failures` is only for diagnosis; do not cite such a
report as passing evidence. The deterministic synthetic corpus is useful for
scale and regression triage, not relevance-quality or industry-parity claims.
