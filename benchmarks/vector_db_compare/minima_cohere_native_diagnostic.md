# Real 768D native Minima-style diagnostic

This is a separate, nonqualifying workload, not a replacement for the frozen 8D
Minima/M5 manifests. It opens only the already-used Cohere diagnostic export;
no protected final holdout, download, zero-padding or dimensional projection.
The harness must be reviewed and landed before collecting the 500K run.

The public service uses typed Collections `column_graph` with
`command_wal_durable`: native upsert/search/GetMany; explicit HTTP schema,
build/fold/ensure, metadata-filter deletion and full-state cursor verification.
Search timing includes Python request preparation, native transport, public
collection admission/filter/search, result materialization and client decode.
It is not a bare HNSW microbenchmark. The protocol's producer work record is
retained separately after each search timer.

The declared 500K workload uses 200 existing diagnostic queries, an independent
float64 cosine top-10 oracle, EF128/256/512/1024/2048 diagnostic curves, the
EF32/64/128/256/512/1024/2048 RSS control grid, and eligible populations
4096/4097/5000/50000/500000. Scalar labels are synthetic, dispersed unique ranks
with range predicates, **not** a categorical tenant posting-list benchmark.
First-predicate timings are predicate-cache-cold, not OS-cache-cold. Each curve
follows that predicate's first query. Reopen curves use EF512.
Diagnostic mode requires exactly 500,000 exported rows and 200 queries: an oracle
from a larger corpus uses different rank membership and cannot be prefix-sliced.

Four readers execute 64 EF512 queries each at 4097 eligible rows while one writer performs eight 256-row
same-ID replacements. This is fixed-live-count replacement throughput, not
concurrent ingestion. Writer-active flags are approximate; event intervals
allow exact observed temporal-overlap analysis. Payload preparation is outside
write-call timers; native packing and transport are inside. Lifecycle probes
update 256 rows, delete their file through a public scalar filter, check missing
GetMany values, reinsert, fold, close/reopen/ensure, then verify every ID, payload
and normalized full vector through bounded 256-row public HTTP pages. Full-state verification
is a separate verification-only phase, not query or point-fetch throughput.

Reuse `/mnt/fast4tb/gomap-768-lanes-20260911/scale-data`; raw vectors are 1.536 GB.
No full-corpus Python float lists or DB copies are created. Estimated incremental
disk 5–8 GiB, hard output cap 11 GiB with 10 GiB free reserve and a 24 GiB combined
runner plus service RSS guard. Estimated runtime 15–30 minutes, hard 45-minute guard, 600-second
per-operation deadline. Budget failure preserves partial events and stops the
owned service; it does not retry or weaken caps. Whole-run guards are approximate
one-second samples, not hard filesystem quotas. A dedicated runner is unavailable:
collection uses a serialized quiet window on the shared workstation.

Freeze with a clean reviewed harness checkout and unmodified product binary.
Both the native and Qdrant producers independently reject tracked or untracked
changes beneath `benchmarks/vector_db_compare` and `clients/python/treedb_client`
at freeze and run time; this attestation does not trust an imported runner or
client module to certify its own bytes:

Review the serving configuration against the exact combined candidate after
any serving-contract change. It must use the complete
`ColumnGraphServingOptions` schema, including positive `Owners.Physical`
`segments`, `descriptors`, `mapped_bytes`, `fallback_bytes`, and
`inventory_bytes` limits. Freeze and run both reject incomplete, extra, or
non-integer fields before starting a service.

```sh
python benchmarks/vector_db_compare/minima_cohere_native_diagnostic.py \
  --freeze /mnt/fast4tb/TASK/plan.json --run-dir /mnt/fast4tb/TASK/run \
  --dataset /mnt/fast4tb/gomap-768-lanes-20260911/scale-data \
  --service-bin /mnt/fast4tb/TASK/treedb-document-service \
  --product-commit FULL_PRODUCT_COMMIT --serving /mnt/fast4tb/TASK/serving.json \
  --ef-construction 32
```

Repeat those same arguments/environment using `--run PLAN` instead of
`--freeze PLAN`, adding `--expected-plan-sha256 HASH`. The plan binds exact
product/harness commits and trees, service binary, dataset/truth/serving bytes,
CPU affinity, GOMAXPROCS and Python/NumPy versions. The run directory must not
already exist. Set `GOWORK=off GOMAXPROCS=6 PYTHONDONTWRITEBYTECODE=1`, an explicit
fastmount `TMPDIR`, and client/harness `PYTHONPATH`; use `taskset -c 0-5` on this
workstation. Set `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1` to keep independent
oracle checks single-threaded. Construction EF defaults to and is frozen at 32
for this candidate workload. Existing benchmark Python with NumPy is sufficient.

Use `--rows 512` for the bounded
public lifecycle smoke. It uses the real prefix and independently computes a
four-query oracle with eligible counts 64/65/128/256/512 and overlap population 65;
it never supplies retained 500K performance evidence. Its tiny population cycles
the same two replacement batches: the first two writes publish and the remaining
six, plus the following explicit update, are no-ops. Successful no-ops do not
advance the expected manifest generation or coverage LSN. The 500K run instead
has eight distinct changed overlap batches and a changed explicit update.

## Normalized-v4 campaign profile

`--campaign-profile normalized_v4` selects the production query shape introduced
by the normalized-v4 path. It fixes E=R=64, stores one canonical normalized FP32
typed-column authority, derives the optional scalar-u8 plane from it, uses native
command v4 with `return_embedding=false`, and retains original-cosine truth only
as a secondary report. The default `legacy_v2_v3` profile above is unchanged.

The normalized profile accepts a real-prefix 5,000-row rehearsal and the final
500,000-row campaign. It records the production filter policy: populations at
or below 4096 are always typed-exact, while 4097 through 5000 may use the bounded
packed typed-exact fallback while their cached filter navigation is built and
then use typed HNSW. Larger filtered populations and nonempty unfiltered
populations use typed HNSW. The 5K run exercises this cold handoff and runs the
real production matrix and same-shortlist engine diagnostic; it is a wiring
check, not qualifying evidence. The Q3 100K integration gate remains
`minima_cohere_v4_production_gate.py`. Only Q4 runs the fresh 500K exact and SQ8
arms.

Both freeze and run commands must add the reviewed Go helper and exact Go tool:

```text
--campaign-profile normalized_v4 --go-helper /abs/treedb_v4_production_gate
--go /abs/go --rows 5000
```

The SQ8 arm also adds
`--query-mode quantized_rerank --quantized-index-name minima_sq8`; the exact arm
uses `--query-mode exact` and no quantized index. The producer emits canonical
and original-cosine truth, full lifecycle events, five resource inventories,
and, for SQ8, the production matrix plus actual-campaign engine diagnostic.
Resource inventory explicitly distinguishes the topology-only HNSW pack from
the single normalized FP32 vector asset and its derived SQ8 assets.

The producer and analyzer share the normalized-dot oracle. Admission math follows
Go's left-to-right float64 squared-norm accumulation, reciprocal multiplication,
and float32 rounding; shared Go/Python bit-pattern fixtures cover extreme finite
inputs. Top-k resolves cutoff ties by document ID and clamps scores to `[-1, 1]`.
The analyzer recomputes truth from the frozen source files; this is not a second
independently implemented Python oracle. Explicit embedding output is checked
against the canonical float32 values without renormalizing the returned vector.

The engine diagnostic emits generation from its captured Go query owner and
checks that the serving inventory names that same owner. Python retains the
original Go output bytes and rejects a missing or different generation. Packet
file-size/SHA256 validation protects the whole artifact; no nested shortlist
JSON hash or wrapper-side identity injection is used.

## Scalar-u8 rerank opt-in

Add `--query-mode quantized_rerank --quantized-index-name minima_sq8` to both
freeze and run commands to select the separate nonqualifying SQ8 diagnostic.
The Cohere runner deliberately has no fixed rerank-width flag: every coordinate
in its ordered EF grid derives requested `R=E`, and the plan, quality curves and
native-v3 producer proof all record both values. The immutable SQ8 plan also
binds graph `M=16`, which is sent explicitly and verified from the live index.
The exact default still creates
no quantized index and sends the existing native-v2 request.

For the 500K diagnostic, query sets 0..99 and 100..199 are both observed and both
must reach mean recall@10 >= 0.90 at a coordinate. The all-row coordinate is not
selected again: the full run requires `--all-rows-sq8-rss-artifact` and
`--expected-all-rows-sq8-rss-artifact-sha256`, completely validates that prior
SQ8 RSS packet against the frozen dataset/runtime/provenance, and binds its
selected `(E,R=E)` into the new plan. On a distinct fresh service lifetime it
emits `coordinate_lock_consumed` bound to the first owned service lifetime,
executes exactly queries 0..199 once at that
coordinate with phase `locked_all_rows_revalidation`, and stops without trying
another EF if either fixed set misses the target. Each filtered population above
4096 still selects independently; failed lower coordinates remain in the
artifact. Filtered populations at or below 4096 are typed-exact/empty correctness
cases; a nonempty unfiltered population remains the quantized-rerank route. After
selection, fixed curves, overlap and reopen measurements reuse the relevant
population's frozen coordinate; the lifecycle empty probe reuses the all-rows
coordinate. There is no same-coordinate graph retry, all-row `quality_selection`,
or post-selection retuning. The consumer revalidates the complete prior RSS
artifact against the full plan—not merely its hash and selected coordinate—and
requires the later paired exact/SQ8 calls to retain the locked revalidation's
same graph owner.

Both initial-ready FP32 RSS producers retain a compact, ordered per-query ID
ledger in addition to their aggregate quality curves. The Q5 consumer rebuilds
TreeDB `ef_search` and Qdrant `hnsw_ef` recall/NDCG from those IDs and frozen
truth, rejects missing/repeated/reordered coordinate-query calls, and requires
the post-boundary Qdrant exact reference IDs to equal canonical truth in order.
The SQ8 RSS arm keeps its richer native-v3 result/score/proof ledger.

For every overlap/lifecycle request, the validator derives live base rows `B`,
shadowed original rows `S`, and live suffix rows `D` from the authored whole-batch
state. Exact suffix work must equal `D`. A filtered request may use either the
request-local current-postings plan over `B` or the cached immutable-base plan
over `B+S`; candidate widths and observed shadow work must match that same plan.
Unfiltered search has only the immutable-base alternative. This distinction also
preserves the producer's suffix-only exact route when a current filtered plan has
no base rows, even if its live suffix is larger than the normal exact threshold.
The FP32 overlap arm also requires every returned document projection to match
one whole authored batch state whose mutation window intersects that request;
it does not validate old/new fields independently.

The full SQ8 diagnostic also emits one
`treedb_cohere_768_sq8_paired_query/v1` event after quality selection and before
any mutation. It warms one batch, then runs five measured repetitions of queries
0..19 (all four queries in the 512-row smoke) at the frozen all-rows `(E,R=E)`
coordinate. Each repetition runs one complete native-v2 FP32/exact batch and
one complete native-v3 scalar-u8 rerank batch over the identical frozen query
list; which arm batch runs first alternates by repetition. Both use the same public
`query_by_embedding` method, collection, immutable code-declared graph, query,
filter, TopK and full-document output. Every call must carry the same producer
snapshot; its raw public-call timers, result projections and dense/score-plane
work are retained. These calls reuse already-observed queries and cannot select
or retune a coordinate. They are not added to the strict RSS selection ledger;
`--rss-only` still stops at its original quality-gated population boundary.

Each arm of every measured repetition is separately bracketed by a drained diagnostics snapshot and
the owned Linux process CPU clock. The packet retains raw Go `TotalAlloc` and
`Mallocs` endpoints and normalized deltas, current heap endpoints, server CPU,
client harness CPU, total DB bytes with WAL included, and the aggregate
installed/base/owner typed-graph asset gauges. These Go totals are process-wide
phase deltas and include diagnostics endpoint handling; they are not claimed as
an allocation profile for an individual query. Scalar-u8 logical code size is
reported as 768 bytes/vector and its row-count product. The public score-plane
reports logical bytes read and the diagnostics endpoint reports only aggregate
graph assets, so the physical selected SQ8 TVIS length is explicitly
`producer_unavailable` (`actual_quantized_tvis_bytes: null`), never inferred by
subtraction or reported as zero. Internal `VectorIndexSearchStats` owns
mapped/heap-copy quantized asset counters, but the current public
dense-work/score-plane transport does not carry them. Obtaining that split would require a separate
reviewed product-observability contract.

With `--rss-only`, the output uses
`treedb_cohere_768_sq8_rss_boundary/v1`. It preserves the v3 workload and process
RSS boundary while adding a distinct representation declaration and every
successful call's dense-work/score-plane proof. Source documents, exhaustive
truth and returned scores remain FP32; scalar-u8 is only the server-side derived
candidate score plane.

`events.jsonl` preserves successes and failures; `terminal.lifecycle_complete`
is true only after full verification and clean owned-process shutdown. A consumer
treats smoke and full logs as complete execution ledgers rather than coverage
supersets: one service/schema setup, every ordered 256-row initial ingest, one
initial graph build, quality selection, paired batches, fixed-coordinate curves,
overlap, typed-empty lifecycle, reopen curves, and final verification must each
have the producer-defined multiplicity. Every deterministic search is adjacent
to its public-call receipt except for interleaved resource samples. Only overlap
event order is flexible; its query multiset and matching public-call receipts
remain exact, and any unknown or additional call invalidates the run. A completed
SQ8 producer reports `producer_gates_passed`. A frozen quality miss reports
`valid_unqualified`, stops before timing/mutation, and remains valid scientific
evidence only when its error names the independently recomputed failed cohort keys
in producer order, rather than authorizing another coordinate or graph attempt.
Other failures remain invalid. Final-state assurance is deliberately limited to
the reviewed producer's exhaustive semantic comparison of every ordered ID,
payload and normalized 768D vector after reopen; it is `producer_attested`, not
an independently reconstructed cryptographic state commitment. Preserve all
failed runs and producer identities; no silent reruns or reuse of a mutated DB.
The service log and wait4 process-lifetime peaks are retained alongside it.

Tiny source checks (no service or collection):

```sh
PYTHONPATH=benchmarks/vector_db_compare:clients/python/treedb_client/src \
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
  python -m unittest test_minima_cohere_native_diagnostic
```
