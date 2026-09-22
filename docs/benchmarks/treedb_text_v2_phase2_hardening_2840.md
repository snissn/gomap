# TreeDB text-v2 phase-2 hardening matrix (#2840)

This note records the hardening coverage added around the phase-2 text-v2
surface for parent #2833. The focus is interaction coverage across analyzer
options, scalar-aware pruning, query explain, phrase/proximity, and block-max
serving.

## Added coverage

`TestTextV2Phase2HardeningRandomizedAnalyzerScalarExplain2840` builds randomized
small corpora with persisted stopword analyzer options and validates:

- AND block-max explain parity against the exhaustive scorer;
- OR/WAND scalar allow-set explain parity against the exhaustive filtered scorer;
- phrase/proximity with stopwords plus scalar allow-set parity;
- no document fetches, text-state lookups, or fail-closed fallback on the native
  paths under test;
- explain payloads are present for all covered shapes.

This complements the existing phase-2 matrix:

- position value v2 corruption/legacy-read tests (#2835);
- scalar-aware WAND/filter pruning contract tests (#2836);
- query explain shape/counter/fail-closed tests (#2838);
- analyzer stopword/phrase/fail-closed tests (#2839).

## Local validation

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestTextV2Phase2HardeningRandomizedAnalyzerScalarExplain2840'

GOWORK=off go test ./TreeDB/collections ./TreeDB/docs
```

## Remaining risk / optional stress mode

The checked-in test is intentionally bounded for normal CI. For a longer local
soak, increase the seed range and corpus size in the test or run the broader
package/race matrix:

```sh
GOWORK=off go test ./TreeDB/collections -run 'TestTextV2' -count=10
GOWORK=off go test -race ./TreeDB/collections -run 'TestTextV2(QueryExplain|Phase2Hardening|BlockMax|Phrase)'
```

Crash/replay hardening remains covered by existing reopen/legacy-read tests and
should be extended if future phase-2 work adds new durable metadata.
