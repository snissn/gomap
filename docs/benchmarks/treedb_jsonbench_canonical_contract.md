# TreeDB JSONBench canonical evidence contract

This runbook defines when a TreeDB-versus-ClickHouse JSONBench result may be
called canonical. The comparison harness remains in the JSONBench repository;
gomap owns the fail-closed sidecar schema and validator described here.

The contract is evidence infrastructure. Validation runs after collection and
outside all measured intervals. It does not change TreeDB storage or query
semantics, and the 1M matrix is not a numeric CI test.

## Known July 10 mismatch

The July 10 strict result at
`/mnt/fast4tb/gomap_jsonbench_1m_20260710_current_main` is useful historical
evidence but is not a canonical durable baseline. Its prose summary says
`durable`, while every row in `artifacts/strict_canonical_1m/treedb/report.json`
records `profile: fast`. The JSONBench runner defaults `PROFILE` to `fast`.

The validator treats the machine-readable TreeDB result as authoritative. A
sidecar that requests `durable` and points at that report fails with:

```text
requested profile "durable" does not match recorded profile "fast"
```

Never repair this mismatch by editing the result or prose after a run. Recapture
with `PROFILE=durable` set explicitly.

## Canonical collection shape

Pin the gomap and JSONBench worktrees before collection. Store heavyweight
outputs under `/mnt/fast4tb`, not `/tmp`. The canonical timing lane uses five
attempts and records the median. Validation is a separate control from the same
heads and dataset so reconstruction or hashing cannot warm the timing lane.

The current JSONBench script accepts `PROFILE` through its environment even
though the preferred wrapper does not list it in its help. Set it on the same
command invocation:

```sh
OUT=/mnt/fast4tb/gomap_jsonbench_1m_$(date -u +%Y%m%d_%H%M%S)

cd /path/to/JSONBench/treedb
PROFILE=durable \
  DATA_DIR=/home/mikers/data/bluesky \
  ROWS=1000000 \
  TRIES=5 \
  GOMAP_REPLACE=/path/to/pinned/gomap \
  OUT_DIR="$OUT/timing" \
  TREEDB_QUERY_MODE=one_shot_end_to_end \
  TREEDB_METADATA_MODE=no_aggregate_metadata \
  TREEDB_VALIDATE_RECONSTRUCTION=0 \
  ./run_preferred_columnstore_clickhouse_compare.sh
```

Run the reconstruction/hash control separately. It may perform validation work,
but none of its timings may replace the timing lane:

```sh
cd /path/to/JSONBench/treedb
PROFILE=durable \
  DATA_DIR=/home/mikers/data/bluesky \
  ROWS=1000000 \
  TRIES=1 \
  GOMAP_REPLACE=/path/to/pinned/gomap \
  OUT_DIR="$OUT/validation" \
  RUN_CLICKHOUSE=0 \
  TREEDB_QUERY_MODE=one_shot_end_to_end \
  TREEDB_METADATA_MODE=no_aggregate_metadata \
  TREEDB_VALIDATE_RECONSTRUCTION=1 \
  ./run_preferred_columnstore_clickhouse_compare.sh
```

Record q2, q3, q5, and load-preparation CPU profiles from the timing heads. Do
not reuse an older bottleneck label without current profile support.

## Sidecar manifest

Copy
[`internal/jsonbenchcontract/testdata/canonical_manifest.json`](../../internal/jsonbenchcontract/testdata/canonical_manifest.json)
beside the collected artifacts and replace every smoke value. The validator
uses strict JSON decoding; unknown fields are rejected instead of ignored.

Required comparison identity:

- full 40-character gomap and JSONBench commits;
- ClickHouse version;
- dataset path/identity, exact row count, and SHA-256;
- stable host identity;
- artifact root and TreeDB result path;
- ClickHouse result path;
- requested TreeDB profile;
- query, aggregate-metadata, fallback, cache, and warmth policies;
- attempt count, statistic, targets, q4 regression guardrail, and target
  revision policy.

For the current v1 contract, the frozen targets are median TreeDB/ClickHouse
ratios no greater than `1.5` for each q1-q5/qexpr lane and load. q4 additionally
has a `1.05` same-host regression ratio guardrail. A target may change only with
linked same-host evidence and an explicit tracker decision; a PR cannot revise
it silently.

The TreeDB result is a report containing `rows[]`. Select the canonical headline
lane explicitly with `treedb.row_selector`; the current lane is
`storage_layout=column-store-full-prepared` and `projection=full`. This avoids
confusing the full retained-data headline with query-shaped attribution rows in
the same report. Selected query IDs must be unique and must cover q1, q2, q3,
q4, q5, and qexpr in `comparison.query_order`. Additional uniquely named q4a
and q4b rows are allowed. Every selected row must consistently record:

- the requested TreeDB profile;
- the pinned dataset row count;
- query and metadata modes matching the sidecar;
- an explicit document-scan fallback flag;
- an explicit reconstruction status;
- at least five positive `attempts_seconds` values.

One mismatching or incomplete row rejects the entire report. A recorded
validation failure always rejects the report. A timing row may say
`not_validated` only when the sidecar links a separate passing validation
artifact with `timing_boundary: outside_measured_intervals`.

The ClickHouse result is also mandatory and must live under the artifact root.
The validator cross-checks its system, pinned version, requested/dataset/loaded
row counts, six ordered q1-q5/qexpr result arrays, at least five attempts per
query, and positive timings. A TreeDB-only sidecar cannot present itself as a
canonical comparison.

## Query-ready counters

All canonical evidence must record the following non-negative counters and a
source artifact/key for each value:

- `visible_base_generations`;
- `visible_delta_generations`;
- `tombstones_applied`;
- `parts_decoded`;
- `query_time_dictionaries_built`;
- `query_time_ranks_built`;
- `query_time_offsets_built`;
- `document_fallbacks`;
- `row_fallbacks`;
- `result_hash_validated` (exactly `1`).

Zero is evidence, not absence. Each zero still needs a source. Until the
base-plus-delta milestones expose direct counters, a collector may derive a
value from an existing result/manifest field, but its `source` must name that
field or artifact. Do not enter unexplained zeros.

## Resource evidence

Resource rows use one of three source kinds:

| `source_kind` | Canonical use | Required fields |
| --- | --- | --- |
| `go_benchmem` | Per-operation query/open/build allocation evidence | at least five samples plus `ns_per_op`, `bytes_per_op`, and `allocs_per_op` |
| `process_peak` | Whole-process load/open memory evidence | `peak_rss_bytes` or `live_heap_bytes` |
| `cumulative_alloc_profile` | Supporting attribution only | `contextual_only: true`; it must not report `bytes_per_op` or `allocs_per_op` |

Every `query/<name>` scope mentioned by a resource row requires a direct
`go_benchmem` row. A cumulative allocation profile can accompany that row but
cannot substitute for it. The canonical v1 manifest specifically requires
direct `go_benchmem` rows for q2, q3, and q5, plus `process_peak` rows for load
or open.

Collect focused Go evidence with the setup outside the benchmark timer and with
the same fixture/mode before and after a performance PR:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '<focused-query-or-open-benchmark>' \
  -benchmem -count=5 \
  > "$OUT/resources/query.bench.txt"
```

On Linux, capture whole-process peak RSS separately from the timed query
attempts. Preserve the complete tool output:

```sh
/usr/bin/time -v -o "$OUT/resources/load.time-v.txt" \
  <fresh-load-or-open-command>
```

CPU and cumulative allocation profiles remain useful for attribution. Label
them contextual and do not translate their cumulative byte totals into `B/op`.

## Validate

Run the deterministic smoke contract in CI or locally:

```sh
GOWORK=off go test ./internal/jsonbenchcontract ./cmd/jsonbench_contract
GOWORK=off go run ./cmd/jsonbench_contract \
  -manifest internal/jsonbenchcontract/testdata/canonical_manifest.json
```

Validate a collected run after its sidecar is complete:

```sh
GOWORK=off go run ./cmd/jsonbench_contract \
  -manifest "$OUT/canonical_manifest.json"
```

Success emits a small machine-readable JSON record. Any missing pin, mode,
counter, validation field, resource dimension, TreeDB query row, ClickHouse
half, artifact file, or requested versus recorded mismatch returns nonzero and
lists all deterministic validation errors. Result, validation, and resource
paths must resolve to regular files beneath `artifact_root`.

## Interpretation and closeout

- Keep the accepted TreeDB/ClickHouse storage ratio as a reported guardrail,
  not an optimization target.
- Do not put the 1M numeric threshold in CI.
- Do not mix timing rows from the reconstruction control into the timing
  headline.
- Do not compare runs from different hosts, commits, datasets, modes, cache
  policies, warmth policies, or sample rules as if they were a paired result.
- Keep instrumentation and validation outside measured intervals.
- Link heavy artifacts from the tracker; do not commit them to gomap.
