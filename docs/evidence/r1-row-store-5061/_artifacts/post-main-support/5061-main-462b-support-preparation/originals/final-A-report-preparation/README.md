# Private A report preparation

This helper reads an original `gomap-r1-row-v1` packet and emits descriptive six-engine tables. It does not validate retained acceptance, compare sources, certify equivalence, or derive speedups. Root must independently qualify the final source/build/run packet before using its output in E.

Run after the actual final capture, supplying its original packet path and two **new** output paths:

```sh
python3 /tmp/gomap-r1-execution-20261005/final-A-report-preparation/report-a.py \
  /ACTUAL_FINAL_CAPTURE/packet.json \
  --json-out /NEW_OUTPUT/final-A-descriptive.json \
  --markdown-out /NEW_OUTPUT/final-A-descriptive.md
```

Alternatively omit output paths and use `--format json` or `--format markdown` for stdout. Output parent directories must exist; files are created exclusively and existing paths are refused. The input packet is never written.

Each engine has its original observed repetition IDs, unsupported/rejected cells and capabilities. Phase groups retain per-repetition raw measurements and counters. Read-state transition, view setup, first fetch, load, reads, mutations and checkpoint are separate sections. Grouping separates different call/row denominators and skip reasons; an incomplete group is marked in JSON and its actual count remains visible in Markdown. No missing repetition or skipped timing is imputed.

Metrics are independent medians across repetitions, with minimum/maximum/spread in JSON. Markdown includes median ns/call with min/max, calls/s, rows/s, Go B/call and objects/call, median per-repetition p50/p95/p99, and calls/s spread. Rows/s is first derived within each repetition as `calls/s * actual_rows / actual_calls`; zero-row maintenance/setup phases have no rows/s. Percentiles are **not pooled**. Go allocation figures exclude SQLite C allocations, and heap-after is a harness-inclusive runtime snapshot. Timing spread above 15% stays descriptively inconclusive.

Storage tables report median/range of persistent, WAL and transient bytes at each original declared boundary. All original per-repetition stats strings/components and complete raw cells remain in JSON. The helper does not infer an additive component decomposition from diagnostic stats.

The supplied smoke outputs are the unchanged **0216 baseline**, not a final candidate. All 92 enabled groups match the original comparator's median/spread arithmetic; four skip groups remain visible. Original baseline noise leaves 69 enabled groups inconclusive. `checks.json` records the actual Python argv/exit codes plus 1,214 checks of arithmetic, raw-byte preservation, denomination separation, nonfinite refusal and exclusive output paths. Synthetic format-only inputs are explicitly nonmeasurement fixtures. No Go build/test, capture, acceptance, CI query, publication or repository write occurred.

Original comparison helper and future signed-off equivalence adapter remain separate and unchanged. Private report output is not a public artifact URL.
