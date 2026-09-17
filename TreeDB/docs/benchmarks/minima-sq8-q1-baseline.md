# Minima SQ8 Q1 diagnostic baseline

Status: retained diagnostic evidence for #4723. This is not a qualification or
promotion result and is not a command to rerun a 500K matrix during Q1.

The run used 500,000 Cohere rows at 768 dimensions, 200 frozen queries, top-10,
E=R=64, `GOMAXPROCS=6`, Go 1.26.0, and an AMD Ryzen 7 9700X. It was bound to
candidate commit `f9a6b0bbfaa8f6202936f1ece876f880849ed749` and a fresh public
create/upsert/build/ensure database. Collection results projected ID and score;
service/native results projected ID, content, metadata, and score;
`return_embedding` was false.

## Useful observations

| Seam | Median mean | Median p50 / p95 | Throughput | Interpretation |
| --- | ---: | ---: | ---: | --- |
| packed rerank, same shortlist | 7.14 us | 7.11 / 7.94 us | 140,095 QPS | preserves the intended fast rerank shape |
| legacy stable rerank, same shortlist | 51.93 us | - | 19,258 QPS | explicitly not the successor path |
| SQ8 candidate generation | 80.06 us | - | 12,490 QPS | candidate guardrail passed |
| collection exact | 304.79 us | 305.30 / 367.34 us | 3,281 QPS | retained exact guardrail |
| collection SQ8 | 232.27 us | 229.12 / 260.22 us | 4,305 QPS | 31.2% higher throughput than exact |

The run observed FP32/SQ8 recall@10 of approximately 0.9245/0.9290 against both
the normalized-dot and original-cosine truth sets. These values justify Q2's
production implementation seam; they do not qualify that implementation before
it exists.

The analysis marked the legacy baseline structurally valid and passed the
candidate, packed-receipt, quality, stable-path-rejection, and legacy-native-
shape-rejection checks. It also marked `promotion_eligible=false`: legacy exact
stability failed, particularly at the Python seam. That failure is retained as
a limitation, not hidden by warming or a rerun.

## Artifact identity

The local retained packet used for this summary was:

- `q1-analysis.json` SHA-256
  `bde1a19d5939144ddde15c612b091d1664777646e9cfbe67e107b5047ed02541`;
- `q1-legacy-baseline.json` SHA-256
  `46988aeb880017bc83aa3137d1cb96de2740758ae7015ff659a9d6ce5755e5b3`;
- dataset manifest SHA-256
  `9144cbf1a20f7e8dad47eafb66173c738444017656ba1e0fdebab5cf3a0ac2f3`.

The packet remains machine-local diagnostic evidence. The hashes make this note
auditable without checking multi-megabyte timing/proof artifacts into Git.

## Reusable repository seams

Q2 should develop against the existing bounded seams rather than restore the
discarded universal Q1 runner:

```sh
go test ./TreeDB/collections -run 'TestCosineNormalizedF32V1ReferenceContract|TestTypedGraphScalarU8PreparedCandidates' -count=1
go test ./TreeDB/collections -run '^$' -bench 'BenchmarkColumnHNSWPreparedScalarU8RerankScratchReuse4227|BenchmarkTypedGraphScalarU8PreparedCandidatesFilteredBudget4684' -benchtime=100x -count=1
go test ./TreeDB/collections -run '^$' -bench BenchmarkTypedGraphQuantizedRerankReadView/smoke-8d -benchtime=20x -count=1
```

The first two benchmark names isolate candidate and prepared-rerank work. The
public read-view benchmark checks the selected route and its scalar-u8/rerank
counters. Q2 may extend these fixtures only where the new representation needs
direct coverage. Q4 owns the sole fresh integrated 500K qualification after the
Q2 engine and Q3 native/Python surfaces have landed.
