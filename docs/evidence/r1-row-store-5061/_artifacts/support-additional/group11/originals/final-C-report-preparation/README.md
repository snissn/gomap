This private formatter renders the supplied `gomap-r1-mutation-sweep-summary-v1`
JSON from the existing reviewed summarizer. It performs presentation-shape checks,
not packet/source/receipt validation or acceptance. It has no Go, host, GitHub,
benchmark, validator or source-capture side effects; output is Markdown on stdout.

The upstream summarizer is
`../mutation-receipt-preparation/summarize.py`, reviewed SHA256
`cdede02aa3be5a0769649c539d497963f7f1db0214b2089b469fac5f423d5749`.
After the coordinator independently validates/freezes the actual completed original
packet and receipts, derive its summary with that unchanged summarizer. Render
that supplied summary with explicit links to the raw summary and original packet:

```sh
python3 format.py /path/to/summary.json \
  --summary-link './summary.json' --packet-link './packet.json' > report.md
```

The formatter requires all 16 width/request/index/change cells, recorded original
repetitions, every displayed field and all 13 ACK/Flush counters. It does not
recompute medians or overwrite a packet/summary. Source and packet hashes are
reported as supplied; they are not authenticated by formatting. The rendered
summary byte hash binds the exact supplied formatter input. The coordinator
remains responsible for matching that summary to the reviewed summarizer's output
and the separately accepted original packet and receipts.

ACK and Flush retain separate timings, spreads, noise labels and counter tables.
Counter work uses the supplied medians of **aggregate per-repetition deltas**,
without normalization; calls/rows/bytes/ns remain aggregate units. No timer sums or
per-call attribution is invented. Requests/s is the supplied median of per-run
throughput, distinct from the reciprocal of median ns/request. Percentiles are
medians of per-repetition percentiles. Observed zero counter deltas remain zero;
missing counters refuse. Go B/request and objects/request include process
background/observer work. No peak/RSS/owned-memory claim follows.

Scope is generic public UpdateBatch costs only, at the supplied finite serial
fixture. No native partial setter, metadata-reference-only cost, larger population,
concurrency, optimization ratio, or physical-growth acceptance is inferred.
`within_15pct` is a descriptive noise label, never an acceptance decision.

`diagnostic-format-smoke-summary.json` and `diagnostic-format-smoke.md` are derived
only from the existing 8848 rehearsal packet (32 rows, 2 requests, 1 repetition).
They check formatting and remain historical diagnostics, not current measurements
or retained evidence. Original packet bytes remain untouched. Actual current
80-cell results and acceptance are pending the coordinator's reviewed/landed
source freeze, independent receipts and completed retained capture.

Numeric display rounds noninteger values to three decimal places; integer values
retain exact digits. Full precision remains in the linked raw summary/packet.
