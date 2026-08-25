# TreeDB text-v2/hybrid scale report

- schema: `treedb_text_hybrid_scale/v2`
- generated: `2026-08-25T03:22:13Z`
- branch/commit: `codex/4327-highdf-requalification` / `d83b70cfc16d85fbe1bba60c37bd010dacc46c7f`
- base: `origin/main` / `6b07740e25bf663b2df3594ed74532601c23ac96`
- rows: `1000000`, dims: `16`, batch: `16384`, queries/row: `25`
- phases: selected `load,queries,reopen`; completed `load,queries,reopen`; status **COMPLETE**
- db dir: `/Users/michaelseiler/orca/workspaces/gomap/4327-highdf-requalification/artifacts/4327/profile_1m/hybrid_text_vector_no_docs/primary_db` (kept=false)

## Load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 71.800 | 13927.6 | 1524362158 | 1524.4 | 109.5 | 594042904 |

Load breakdown: generation `1.125s`, insert `21.535s`, flush `0.000s`, vector rebuild `48.536s`, checkpoint `0.141s`.

### Text-v2 lane bytes/doc

| snapshot | docid | docmap | postings | norms | positions | terms | status/format |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 21.0 | 19.1 | 62.2 | 7.1 | 0.0 | 0.0 | 0.0 |
| `after_reopen` | 21.0 | 19.1 | 62.2 | 7.1 | 0.0 | 0.0 | 0.0 |

## Retrieval latency

| row | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | guardrail | key counters |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `hybrid_text_vector_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 714.970ms | 756.113ms | 791.601ms | 720.563ms | 1.4 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=65536/65536, text_candidates=65536, vector_candidates=65536, scalar_prefilter=0, fused=131072, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |

## Reopen

Close `0.076s`, open `1.074s`, open collection `0.000s`, probe `0.596s`, total `1.746s`.

## Ranked bottlenecks / follow-ups

| rank | row | metric | value | follow-up |
| ---: | --- | --- | ---: | --- |
| 1 | `fixture_load` | `total_seconds` | 71.800 s | Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs. |
| 2 | `vector_rebuild` | `seconds` | 48.536 s | If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence. |
| 3 | `hybrid_text_vector_no_docs` | `p95_ns` | 756112875.000 ns | Profile this retrieval row first if it is on the target production query mix. |

## Guardrails

- `hybrid_text_vector_no_docs`: PASS

## Caveats

- Synthetic corpus uses deterministic customer-support text, scalar tenants, and small dense vectors; do not use as relevance-quality evidence.
- Retrieval rows time warm in-process queries after fixture load, before close/reopen; B/op and allocs/op should be captured with the companion Go benchmark commands when making allocation claims.
