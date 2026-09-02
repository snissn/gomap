# TreeDB text-v2/hybrid scale report

- schema: `treedb_text_hybrid_scale/v2`
- generated: `2026-08-25T12:57:02Z`
- branch/commit: `` / `c11afbf0486a51083d0ec2c0efb46e597e61330b`
- base: `origin/main` / `4a5448e22`
- rows: `1000000`, dims: `16`, batch: `16384`, queries/row: `25`
- phases: selected `load,queries,reopen`; completed `load,queries,reopen`; status **COMPLETE**
- db dir: `/tmp/gomap-4327-accepted-scalar/primary_db` (kept=false)

## Load/storage

| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| load | 387.817 | 2578.5 | 1523313584 | 1523.3 | 109.5 | 594042904 |

Load breakdown: generation `22.607s`, insert `253.132s`, flush `0.000s`, vector rebuild `101.676s`, checkpoint `0.132s`.

### Text-v2 lane bytes/doc

| snapshot | docid | docmap | postings | norms | positions | terms | status/format |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `after_load` | 21.0 | 19.1 | 62.2 | 7.1 | 0.0 | 0.0 | 0.0 |
| `after_reopen` | 21.0 | 19.1 | 62.2 | 7.1 | 0.0 | 0.0 | 0.0 |

## Retrieval latency

| row | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | guardrail | key counters |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `hybrid_text_scalar_no_docs` | hybrid | warm no-document hybrid candidate generation/fusion | 1.294s | 1.308s | 1.312s | 1.287s | 0.8 | 10 | PASS | docs_fetched=0, fail_closed=0, text_budget=65536/65536, vector_budget=0/0, text_candidates=62500, vector_candidates=0, scalar_prefilter=62500, fused=62500, budget_policy=fixed, budget_stop=exact_bound_insufficient, budget_fallback=exact_bound_insufficient |

## Reopen

Close `0.113s`, open `1.178s`, open collection `0.001s`, probe `10.641s`, total `11.934s`.

## Ranked bottlenecks / follow-ups

| rank | row | metric | value | follow-up |
| ---: | --- | --- | ---: | --- |
| 1 | `fixture_load` | `total_seconds` | 387.817 s | Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs. |
| 2 | `vector_rebuild` | `seconds` | 101.676 s | If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence. |
| 3 | `hybrid_text_scalar_no_docs` | `p95_ns` | 1307936167.000 ns | Profile this retrieval row first if it is on the target production query mix. |

## Guardrails

- `hybrid_text_scalar_no_docs`: PASS

## Caveats

- Synthetic corpus uses deterministic customer-support text, scalar tenants, and small dense vectors; do not use as relevance-quality evidence.
- Retrieval rows time warm in-process queries after fixture load, before close/reopen; B/op and allocs/op should be captured with the companion Go benchmark commands when making allocation claims.
