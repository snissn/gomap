# TreeDB retained RAG application baseline runbook (#4289)

This runbook reproduces the repaired M1 application baseline. The historical
C1 code is retained only as an unfiltered hashing regression cell; it is not a
product or ingestion claim. The authoritative artifact schema is
`treedb_rag_application_baseline/v2`.

Product base: `99929cdeb2ae2ec1e411236c853eb36942075d72` (accepted #4293 and
#4294). Harness revision used by the committed baseline:
`e0eb90f15a4a3de2cafed54509235dbeea96bd83`.

## What the repaired harness proves

- Recall claims fail unless the returned ranking depth is at least K. The
  retained quality rows report only @5 and @10.
- One declared dimension is checked against corpus, query, index, and vector
  widths for each embedding cell.
- Embedding timing stops before independently declared judgments are loaded.
- Pre-generated child-row setup is never labeled source ingestion. The
  ingestion rows time `Collection.IngestSources`, vector publication, and
  checkpoint end to end on a fresh DB.
- Judgments are committed separately for unfiltered, tenant, tenant+workspace,
  and tenant+workspace+range contexts. Validation rejects a child or parent
  outside the declared context.
- Final QPS/p99 evidence requires at least 1,000 timed queries and three
  repetitions. The committed rows use 1,008 samples and forward/reverse/forward
  query order.
- Artifact guards reject cross-tenant/workspace results, unbounded document
  fetch, full-document-scan fallback, and parent-cap violations. Comparison
  identities bind work, projection, and quality digests.

## Application fixture

`treedb-rag-application/v1` contains 19 initial sources and 57 chunks. One
lifecycle-only source is deterministically deleted, leaving 18 sources and 54
chunks. Every source has three 128-rune chunks, a valid #4293 parent ID,
`tenant_id`, `workspace_id`, source URI/type, ACL tags, and update year. There
are two tenants and two workspaces. Billing, outage, and access judgments are
human-declared categorical labels rather than outputs of either scoring
function. Each query has duplicate-heavy relevant chunks from the same parent.

The lifecycle performs unchanged re-ingest, an updated source replacement,
source+children deletion, checkpoint, close, cold reopen, and byte-identical
child snapshot validation. Parent metadata remains in the source-ingestion
collection. Because #4290 has not propagated it to children, all tenant rows
fail closed as typed capability evidence.

Query rows use the exact stored child bytes produced by `IngestSources`.
After the cold-reopen check those bytes are projected, without re-chunking or
re-embedding, into the document-service column-graph collection used by both
direct and HTTP cells. This insert-only query projection is setup, excluded
from query timing, and is not reported as source ingestion.

## Embedding cells

### Hashing regression

The built-in deterministic hashing provider remains the hermetic regression
cell at 64 dimensions. Its digest binds the provider name, dimensions, and
fixture digest.

### Independent semantic cell

- model: `sentence-transformers/all-MiniLM-L6-v2`
- revision: `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`
- license: Apache-2.0
- dimensions: 384
- preprocessing: `SentenceTransformer.encode(normalize_embeddings=True)` with
  the pinned model tokenizer and `max_seq_length=256`
- corpus license: MIT (the repository-owned fixture)
- input manifest: `cmd/treedb_rag_benchmark/testdata/semantic_inputs.json`
- vectors: `cmd/treedb_rag_benchmark/testdata/semantic_vectors.json`
- canonical vector digest:
  `aff8b31fad35f45c862c943b19717ddf9979b09726b2ac9352e159a4815663a4`

Regeneration has no Go/runtime dependency:

```sh
python3 TreeDB/cmd/treedb_rag_benchmark/testdata/generate_semantic_vectors.py \
  --inputs TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_inputs.json \
  --output TreeDB/cmd/treedb_rag_benchmark/testdata/semantic_vectors.json
```

The committed generation used sentence-transformers 5.4.1, transformers 5.8.0,
and torch 2.11.0. `TestApplicationFixtureAndSemanticVectorsAreStable` verifies
model/revision/license/dimensions, fixture coverage, and the canonical digest.
No model or tokenizer is loaded by the Go benchmark.

## Row matrix and unsupported capabilities

The artifact retains the Cartesian matrix for text/vector/hybrid,
score-only/fetch-topk, four filter contexts, collapse disabled/enabled cap 2,
direct/HTTP, hashing/semantic, and c1/c4. Supported baseline cells are the 36
unfiltered, collapse-disabled direct rows plus HTTP fetch rows. Every supported
cell uses the declared column-graph ANN product route. Offline exhaustive
cosine controls over the exact hash-bound vectors are reported separately and
never counted as product QPS or fallback work.

The remaining 348 rows have zero results and exact
`*main.capabilityError` evidence:

- #4290: `source_metadata_not_propagated`
- #4292: `multi_field_filter_unavailable`
- #4291: `parent_collapse_unavailable`
- service contract: `http_score_only_route_unavailable`
- #4284 fault boundary: `storage_boundary_fault_injection_unavailable`

No unsupported row is silently skipped, partially ranked, or substituted with
an exact fallback.

## Exact commands

```sh
export PATH="$HOME/.gvm/gos/go1.26.0/bin:$PATH"
export GOROOT="$HOME/.gvm/gos/go1.26.0"
export CGO_ENABLED=1
export GOCACHE="$HOME/.cache/gomap-go126"
export GOWORK=off

go build -trimpath -o /tmp/treedb_rag_benchmark_e0eb90f15 \
  ./TreeDB/cmd/treedb_rag_benchmark

/tmp/treedb_rag_benchmark_e0eb90f15 \
  -out-dir TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23 \
  -dir /tmp/gomap-4289-rag-baseline-db-e0eb90f15-go126 \
  -product-base-sha 99929cdeb2ae2ec1e411236c853eb36942075d72 \
  -harness-revision e0eb90f15a4a3de2cafed54509235dbeea96bd83 \
  -host-note "Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1"
```

A bounded diagnostic uses `-smoke`. Its authority is
`DIAGNOSTIC_NOT_FINAL_EVIDENCE`; it cannot emit a final p99/QPS claim.

## Frozen M1 numbers and #4284 gate

Host: Apple M3, Darwin arm64, 8 logical CPUs, Go 1.26.0, CGO enabled.

Actual `IngestSources` fresh-DB source docs/s:
`149.13, 243.89, 308.75, 318.69, 343.83` in execution order. Median/p95 are
`308.75 / 338.80` docs/s. Median/p95 allocation is
`2,162,690 / 2,571,587` B/source. Every repetition reopened with identical
parent/child state.

The historical 37.59 docs/s / 132 GiB-per-operation regime did not reproduce.
Before any #4284 candidate is measured, the frozen attainable objective is:

- source docs/s >= `355.06` (15% over the M1 median);
- B/source <= `1,946,421` (10% below the M1 median);
- all structural, durability, and matched-quality gates remain mandatory.

Noise policy: fresh DB per ingestion repetition; median is the decision
statistic and p95 is disclosed; query rows use three counterbalanced
repetitions; an unexplained >10% QPS or p99 regression blocks an unaffected
matched-quality row. Base/final work, projection, quality, fixture, config, and
vector digests must match exactly.

Representative hashing c1 rows (QPS / p99 ms): text score
`24,133 / 0.111`, vector score `68,114 / 0.035`, hybrid score
`16,276 / 0.215`, and HTTP hybrid fetch `1,315 / 2.245`. Representative semantic
c1 rows: text score `23,233 / 0.121`, vector score `57,409 / 0.031`, hybrid score
`14,126 / 0.195`, and HTTP hybrid fetch `361 / 5.237`.

## Durable artifacts

`TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23/` contains:

- `treedb_rag_application_baseline.json`: raw rows, 1,008 latency samples per
  supported cell, ingestion repetitions, counters, failures, lifecycle, and
  frozen gates;
- `treedb_rag_application_baseline.md`: the human summary;
- `treedb_rag_application_manifest.json`: SHA-256 and byte length for both
  artifacts plus product/harness/binary/fixture/config/vector bindings.

The retained explicit DB root is
`/tmp/gomap-4289-rag-baseline-db-e0eb90f15-go126`. All DB handles, document
services, and HTTP servers are closed after collection. The DB root is retained
only for local forensic inspection; the committed raw artifacts are the durable
repository evidence.

## Focused validation

```sh
go test ./TreeDB/cmd/treedb_rag_benchmark -count=1
go vet ./TreeDB/cmd/treedb_rag_benchmark
go test ./TreeDB/collections \
  -run 'TestIngestSources|TestChunkChildren|TestReopen' -count=1
go test ./TreeDB/documentservice \
  -run 'Test.*RAG|TestHTTP.*Search|Test.*Parity' -count=1
python3 -m py_compile \
  TreeDB/cmd/treedb_rag_benchmark/testdata/generate_semantic_vectors.py
```

These gates cover metric hand calculations, dimension/timing/filter corruption,
fixture/vector stability, insufficient-sample rejection, counter corruption,
source lifecycle/reopen, direct/service smoke, and artifact hashing.
