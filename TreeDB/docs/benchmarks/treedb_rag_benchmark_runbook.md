# TreeDB retained RAG application baseline runbook (#4289)

## Minima native-path development (#4614)

Minima is a separate workload from the retained application baseline below.
Its execution contract is
[minima-native-execution.md](../spec/minima-native-execution.md). The Go command
owns manifests and evidence validation; the Python runner executes real client
requests against the document service. M0 adds bounded diagnostics, not mutable
`column_graph` support or a new performance result.

Use a clean, committed **standalone clone** and a writable `/mnt/fast4tb` mount.
The Go 1.26.0 toolchain on the development runner omits VCS stamping in linked
worktrees; the binary provenance check rejects those builds. Do not inject a
revision string or disable validation to work around it.

```sh
mountpoint -q /mnt/fast4tb && test -w /mnt/fast4tb
MINIMA_RUN=$(mktemp -d /mnt/fast4tb/gomap-minima-bounded-XXXXXX)
mkdir -p "$MINIMA_RUN/tmp"
TMPDIR="$MINIMA_RUN/tmp" GOWORK=off MODE=bounded-50k \
  RUN_DIR="$MINIMA_RUN" MINIMA_WALL_SECONDS=600 \
  scripts/bench_minima_qualification.sh
```

Linux GNU `timeout` bounds the entire command, including builds, to 600 seconds
plus a 10-second forced-stop grace period. Individual service requests and
startup are bounded at 120 seconds. A timeout/failure is incomplete evidence,
not permission to reuse a previous output. Use a fresh run directory every time.
`MODE=bounded-250k` selects 250,000 total rows; `MINIMA_WALL_SECONDS` sets an
explicit run budget. These totals span all scenarios. They preserve the 4,097
cutoff but not the full fixture's <1% sparse case. Both emit a diagnostic schema
that cannot pass full qualification. `MODE=representative` retains the frozen
full workload and its existing validation.

The M0 runner still executes `native_runtime`. An explicit
`TREEDB_STRATEGY=column_graph` request fails as unavailable until M4 connects the
native lifecycle; null native counters do not mean zero fallback work. Keep
the same client/transport, projections, durability and fixture for timing
comparisons. For diagnostic overhead characterization, repeat three matched
fresh-DB runs with diagnostics disabled/enabled in counterbalanced order; set
`TREEDB_DIAGNOSTICS_DIR` beneath the run directory to enable existing diagnostics.
Do not use this small lane as a full-scale speedup claim.

`resource_measurement.peak_rss_bytes` is the maximum measured Linux service
process-lifetime `VmHWM` through the captured segment endpoints. Per-process
identity and source are retained. It is not the sum of peaks, whole-host memory,
or a phase-specific peak. The historical `rss_bytes` field retains its old
endpoint-growth meaning. Missing allocation, live-heap or client-memory evidence
remains unavailable; use focused Go benchmarks/profiles for allocation budgets
before optimizing each affected production feature.

### M0 baseline limitation

The first public bounded-50k run at `8eb50f829` (unchanged TreeDB production
code from `c2781c147`) completed load in 10.627 seconds, then failed: the
`broad_10pct` filter had 1,000 valid IDs but `complete_finite_ann` returned none
(2,064 visited/scored, zero admitted). This is failed recall evidence, not a
completed latency result. The old runtime exact cap is 512, and its complete
finite branch lacks the eligible-region seeding used by its broad-filter branch.
Keep this fixture/oracle as a native-path regression; do not retune it to hide
the failure. The already-defined bounded-250k shape is the separate M0
characterization candidate. Neither shape certifies the full sparse case.

Focused harness validation:

```sh
GOWORK=off go test ./TreeDB/cmd/treedb_rag_benchmark \
  -run '^TestMinima' -count=1 -timeout=120s
python3 -m unittest discover -s benchmarks/vector_db_compare -p 'test_minima*runner.py'
bash scripts/bench_minima_qualification_test.sh
make docs-check
```

## Retained application baseline (not Minima)

This runbook reproduces the repaired M1 application baseline. The historical
C1 code is retained only as an unfiltered hashing regression cell; it is not a
product or ingestion claim. The authoritative artifact schema is
`treedb_rag_application_baseline/v3`.

Product base: `99929cdeb2ae2ec1e411236c853eb36942075d72` (accepted #4293 and
#4294). Harness revision used by the committed baseline:
`43e9568e0059806b9a7f735a5e383800880d1865`.

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
- Final evidence reads `debug.ReadBuildInfo` and rejects an absent, dirty, or
  mismatched `vcs.revision`; the CLI harness SHA is an assertion, not an
  override. `config_sha256` covers only TopK/candidate/index parameters,
  warmup/repetition/sample counts, and ingestion repetitions, so paths, command,
  revisions, and host provenance cannot split matched workload identities.
- Warmup query ordinal rotates through the complete committed query set,
  independently of timed samples.
- Direct score-only quality attribution comes from separate untimed compact
  queries with identical work, route, and filter. Timed score-only rows retain
  zero document fetches and never publish projection-stripped attribution as
  zero.

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
child snapshot validation. Before and after reopen it also executes the
fixture-known `guidance` text query, exact native vector-index queries for every
live child vector, and `updated_year` scalar-index lookups. Both sides must
equal the exact live fixture set; stale, missing, corrupt, or false parity fails
the M1 run and artifact writer. Parent metadata remains in the source-ingestion
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

SOURCE_ROOT="$PWD"
rm -rf /tmp/gomap-rag-evidence-43e9568e0
git clone --no-checkout "$SOURCE_ROOT" /tmp/gomap-rag-evidence-43e9568e0
git -C /tmp/gomap-rag-evidence-43e9568e0 \
  checkout 43e9568e0059806b9a7f735a5e383800880d1865
cd /tmp/gomap-rag-evidence-43e9568e0

go build -buildvcs=true -trimpath \
  -o /tmp/treedb_rag_benchmark_43e9568e0 \
  ./TreeDB/cmd/treedb_rag_benchmark
go version -m /tmp/treedb_rag_benchmark_43e9568e0

/tmp/treedb_rag_benchmark_43e9568e0 \
  -out-dir "$SOURCE_ROOT/TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23" \
  -dir /tmp/gomap-4289-rag-baseline-db-43e9568e0-go126 \
  -product-base-sha 99929cdeb2ae2ec1e411236c853eb36942075d72 \
  -harness-revision 43e9568e0059806b9a7f735a5e383800880d1865 \
  -host-note "Apple M3 arm64, macOS 26.2, 8 logical CPUs, quiet local host, Go 1.26.0, CGO_ENABLED=1"
```

A bounded diagnostic uses `-smoke`. Its authority is
`DIAGNOSTIC_NOT_FINAL_EVIDENCE`; it cannot emit a final p99/QPS claim.

## Frozen M1 numbers and #4284 gate

Host: Apple M3, Darwin arm64, 8 logical CPUs, Go 1.26.0, CGO enabled.

Actual final repaired fresh-DB end-to-end source docs/s:
`160.18, 232.21, 283.00, 291.75, 297.29` in execution order. Median/p95 are
`283.00 / 296.18` docs/s. Median/p95 allocation is
`2,163,595 / 2,577,008` B/source. Every repetition reopened with identical
parent/child and queried text/vector/scalar index state. The retained artifact
binds harness `43e9568e0059806b9a7f735a5e383800880d1865`.
The commit is retained on remote branch `evidence/4289-retained-harness`, so a
normal clone can resolve and inspect the exact checkout.

The historical 37.59 docs/s / 132 GiB-per-operation regime did not reproduce.
The final repaired M1 artifact replaced the earlier evidence-integrity sample
before #4284 candidate construction. Its prospective frozen #4284/#4288
objective is:

- source docs/s >= `325.45` (15% over the retained M1 median);
- B/source <= `1,947,235` (10% below the retained M1 median);
- all structural, durability, and matched-quality gates remain mandatory.

Noise policy: fresh DB per ingestion repetition; median is the decision
statistic and p95 is disclosed; query rows use three counterbalanced
repetitions; an unexplained >10% QPS or p99 regression blocks an unaffected
matched-quality row. Base/final work, projection, quality, fixture, config, and
vector digests must match exactly.

Representative hashing c1 rows (QPS / p99 ms): text score
`24,967.58 / 0.0631`, vector score `69,989.97 / 0.0269`, hybrid score
`18,279.29 / 0.0734`, and HTTP hybrid fetch `1,634.47 / 1.0138`.
Representative semantic c1 rows: text score `24,734.90 / 0.0683`, vector score
`60,905.41 / 0.0199`, hybrid score `17,451.46 / 0.1527`, and HTTP hybrid fetch
`409.96 / 4.5564`.

## Durable artifacts

`TreeDB/docs/benchmarks/treedb_rag_application_baseline_2026-08-23/` contains:

- `treedb_rag_application_baseline.json`: raw rows, 1,008 latency samples per
  supported cell, ingestion repetitions, counters, failures, lifecycle, and
  frozen gates;
- `treedb_rag_application_baseline.md`: the human summary;
- `treedb_rag_application_manifest.json`: SHA-256 and byte length for both
  artifacts plus product/harness/binary/fixture/config/vector bindings.

The retained explicit DB root is
`/tmp/gomap-4289-rag-baseline-db-43e9568e0-go126`. All DB handles, document
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
