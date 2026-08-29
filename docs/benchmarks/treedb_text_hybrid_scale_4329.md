# TreeDB 10M text-v2/hybrid scale decision (#4329)

## Decision

The exact 10M campaign is **blocked**. The mixed text/vector load completed, but the frozen retrieval matrix failed closed at `hybrid_text_scalar_rare_no_docs` because the accepted fixed 655,360 candidate bound was insufficient at 10M. The artifact is incomplete and must not be cited as passing 10M evidence. Issue #4329 remains open and continues to block the final #4326 comparator.

Retained raw evidence: [`artifacts/4329-text-hybrid-10m-failed-v1`](../../artifacts/4329-text-hybrid-10m-failed-v1/README.md).

## Candidate and command

- commit: `c1c644ead34839570bcdcdac6ee4747cbbbaca6e`
- tree: `fb8f32dac0889e22fee0bb9070a9f9c56d8939d1`
- TreeDB subtree: `848d455b64cdd13ba7d76e66a2934f32734eff40`
- harness subtree: `15a76c34bc05ca39d55f9bb003faa6054974bb6b`
- binary SHA-256: `b02847b9459f9d0cfbe26c2f7f34bc631f2944d291d6f27fd38ef7f20d5ea23a`
- frozen config SHA-256: `80341e126af74686137d8bc6b6ea2ff3ea1163803791cf7b1aa4872dcc88ab61`
- host: Apple M3, Darwin arm64, Go 1.26.0

```sh
RUN_DIR=/Users/michaelseiler/orca/workspaces/gomap/4329-final-10m-v1 \
RUN_SMOKE=false RUN_1M=false RUN_10M=true APPROVE_10M=true \
PHASES=all KEEP_DB=false \
bash scripts/bench_text_hybrid_scale.sh
```

The run used 10,000,000 rows, batch size 32,768, three retained query repetitions, top-k 10, candidate limit 655,360, four readers, 16-dimensional vectors, 10,000 maintenance updates, and 5,000 deletes. Provenance was linked into the binary and attested immediately after build.

## Completed load evidence

| metric | 1M preflight | exact 10M | interpretation |
| --- | ---: | ---: | --- |
| total load | 84.508s | 8,566.689s | 101.4x elapsed for 10x rows |
| load throughput | 11,833.3 rows/s | 1,167.3 rows/s | 10.1x lower normalized throughput |
| insert | 35.624s | 4,418.519s | 124.0x elapsed for 10x rows |
| vector rebuild | 46.825s | 3,981.051s | 85.0x elapsed for 10x rows |
| checkpoint | 0.114s | 2.516s | completed |
| physical storage | 1,884,023,751 B | 18,867,259,265 B | approximately linear |
| physical bytes/doc | 1,884.0 | 1,886.7 | stable |
| WAL-excluded storage | 1,509,688,485 B | 15,113,949,570 B | approximately linear |
| text encoded bytes/doc | 396.1 | 404.1 | 2.0% increase |

The exact process ran for 9,435.07 seconds before exiting. `/usr/bin/time -l` recorded maximum resident set size 9,445,326,848 bytes and peak memory footprint 21,136,331,536 bytes. Load throughput and vector rebuild scaling are material unexplained regressions against the 1M preflight; they remain scale-owner inputs even after the candidate-bound failure is resolved.

## Retrieval evidence before failure

| row | p95 | result | key path evidence |
| --- | ---: | --- | --- |
| common score-only | 930.170ms | PASS | 32 postings scored; 156,249 blocks skipped; zero document fetch/fallback/fail-closed |
| rare score-only | 284.707ms | PASS | 9,766 postings scored; zero document fetch/fallback/fail-closed |
| exact AND | 903.511ms | PASS | 64 postings; 312,498 blocks skipped; zero document fetch/fallback/fail-closed |
| OR/WAND | 762.626ms | PASS | 64 postings; 312,498 blocks skipped; zero document fetch/fallback/fail-closed |
| phrase | 323.179s | PASS | 10,000,000 postings scanned; 5,000,000 candidates scored |
| common top-k fetch | 784.199ms | PASS | exactly 10 documents fetched; no fallback/fail-closed |
| hybrid text-only | 686.938ms | PASS | adaptive text budget 10/655,360; zero documents fetched |
| hybrid text+scalar rare | n/a | **FAIL CLOSED** | scalar prefilter 625,000; fixed text budget 655,360 exhausted; `exact_bound_insufficient` |

The failure occurred during warm-up, before timed samples. TreeDB returned an explicit index-unavailable error and incremented `fail_closed`; the harness did not convert the failure into a latency result.

## Completeness and cleanup

Only `load` completed. Queries, reopen, concurrent serving/writes, maintenance, backfill, text-only ingestion, and source/chunk ingestion are incomplete. No retained-artifact seal exists.

The candidate report was written before the failure-cleanup persistence fix and has a blank cleanup row. The process defer removed the primary fixture; a post-run coordinator observation confirms all five fixture paths absent. That observation is retained separately and is not represented as validator-qualified candidate output. PR #4435 fixes future failed campaigns to persist status, failure records, and cleanup before exit.

## Required next owner

A follow-up must resolve or deliberately revise the 10M scalar-hybrid exact-bound contract, then rerun the full frozen matrix. Any budget change is a new prospectively frozen campaign, not a reinterpretation of this failed execution. The owner must also disposition the 10.1x normalized load-throughput regression, 85x vector-rebuild elapsed scaling, and 323-second phrase p95 before #4329 can close.
