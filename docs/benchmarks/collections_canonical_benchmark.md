# TreeDB Collections Canonical Benchmark

This workflow is the canonical way to compare TreeDB collection storage against
SQLite. It exists to prevent the earlier reporting mistake where TreeDB's
partial online maintenance size was presented as if it were a fully compacted
state.

The canonical insert/read rows are single benchmark-driver rows. Use
`scripts/bench_collections_concurrency.sh` and the concurrency report when making
claims about concurrent collection-layer reads or mixed read/write workloads.

## Run

The canonical end-to-end run target is:

```bash
make bench-collections-canonical
```

Equivalent direct command:

```bash
./scripts/bench_collections_canonical.sh
```

`make collection-canonical-bench-bin` builds only the runner binary. The older
`make collection-canonical-bench` target is kept as a build alias for
compatibility; it is not the canonical benchmark run target.

The runner creates a timestamped directory under the system temporary directory
returned by `os.TempDir()` by default (often `/tmp` on Linux, but it may be
another location such as `$TMPDIR` on macOS) and writes:

- `benchmark_results.json`: canonical machine-readable results and guardrails.
- `benchmark_summary.md`: human-readable report.
- `benchmark_matrix.csv`: flat table for spreadsheet/diff tooling.
- `timed_matrix/`: existing timed TreeDB/SQLite benchmark matrix artifacts.
- `offline_compact/`: production high-level TreeDB compaction artifacts.
- `exhaustive_compact/`: byte-minimized high-level TreeDB compaction artifacts.
- `full_leafgen_pack_gc/`: full leaf-generation pack/GC fixture artifacts.

Use `-out-dir <dir>` to keep a specific run directory:

```bash
./scripts/bench_collections_canonical.sh -out-dir /tmp/collections_canonical_run
```

For cleanup safety, a user-supplied `-out-dir` must either not exist, be empty,
or contain the harness sentinel file `.collection_canonical_bench_run` from a
previous canonical run. The runner refuses filesystem roots and non-empty
directories without that sentinel.

## Canonical Phases

`post_insert`

Size after insert and the flush/checkpoint needed for correctness. This is not a
compacted state. It can be compared with other post-insert rows.

`online_one_pass_maintenance`

The current benchmark harness's online maintenance path. It is useful for
tracking what a bounded online maintenance pass accomplishes, but it is partial
maintenance and must not be described as full compaction.

`offline_compact`

The production high-level TreeDB compaction path:

```bash
treemap compact <dir> -rw -mode full
```

This is a policy/full compaction comparison point. Compare it with SQLite after
`VACUUM`, not SQLite post-insert, but do not use it as the byte-minimized public
storage-floor headline.

`exhaustive_compact`

The benchmark/VACUUM-equivalent TreeDB compaction path:

```bash
treemap compact <dir> -rw -mode exhaustive
```

This is the preferred TreeDB byte-minimized compacted-size headline. Compare it
with SQLite after `VACUUM`.

`full_leafgen_pack_gc`

The explicit full leaf-generation pack/GC path. The canonical runner records
the exact knobs:

- `leaf-segment-target-bytes`
- `leafgen-pack-gc`
- `leafgen-pack-force`
- `leafgen-pack-max-generations`
- `leafgen-pack-frame-k`
- `index-vacuum`

This is a diagnostic compacted TreeDB comparison point. Compare it with SQLite
after `VACUUM` if reported, but prefer `exhaustive_compact` for public
byte-minimized storage-floor claims.

`sqlite_vacuum`

SQLite compacted baseline after `VACUUM`. This is the required SQLite baseline
when comparing against TreeDB `exhaustive_compact`, `offline_compact`, or
`full_leafgen_pack_gc`.

## Fair Comparisons

Fair post-insert comparison:

- TreeDB `post_insert`
- SQLite `post_insert`

Fair compacted-state comparison:

- TreeDB `exhaustive_compact` for byte-minimized/VACUUM-equivalent public storage-floor claims
- TreeDB `offline_compact` for production policy compaction comparisons
- TreeDB `full_leafgen_pack_gc` as a diagnostic maintenance primitive
- SQLite `sqlite_vacuum`

Do not compare TreeDB compacted rows only against SQLite post-insert rows. The
generated report includes guardrail checks for missing SQLite VACUUM rows and
labels `online_one_pass_maintenance` as partial maintenance.

## Default Shape

The default run uses:

- 100,000 documents
- batch size 16,000
- two secondary indexes for the primary TreeDB/SQLite comparison
- TreeDB collection benchmark engine `command_wal_relaxed`
- raw TreeDB compaction/profile fixtures remain on profile `command_wal_relaxed`
- TreeDB document formats `template-v1`, `bson`, and `json`
- SQLite JSON and SQLite native-column baselines
- full leafgen pack/GC with:
  - `leaf-segment-target-bytes=1048576`
  - `leafgen-pack-force=true`
  - `leafgen-pack-max-generations=0`
  - `leafgen-pack-frame-k=16`
  - `index-vacuum=offline`

## Adding Configurations

For TreeDB document formats, pass `-formats`. The default exercises
`template-v1`, `bson`, and `json` for timed inserts and full leafgen/GC
compacted-size fixtures:

```bash
./scripts/bench_collections_canonical.sh -formats template-v1,bson,json
```

For a different primary index-count shape:

```bash
./scripts/bench_collections_canonical.sh -indexes 3
```

For compaction sweeps, vary the leafgen flags:

```bash
./scripts/bench_collections_canonical.sh \
  -leaf-segment-target-bytes 1048576 \
  -leafgen-pack-frame-k 16 \
  -leafgen-pack-max-generations 0 \
  -index-vacuum offline
```

If adding a new SQLite or TreeDB shape to the underlying benchmark matrix, keep
the canonical names stable and explicit:

- `treedb_template_v1_collection_2_indexes`
- `treedb_template_v1_raw`
- `sqlite_json_2_indexes`
- `sqlite_native_columns_2_indexes`

## Comparing Runs

Prefer comparing `benchmark_results.json` or `benchmark_matrix.csv` rather than
scraping Markdown. The stable fields are:

- `config_name`
- `phase`
- `format`
- `shape`
- `index_count`
- `document_count`
- `total_bytes`
- `bytes_per_doc`
- `docs_per_sec`
- `ns_per_doc`
- `measurement_kind`
- `compaction_flags`

`benchmark_results.json` also contains a `comparisons` array with derived
compacted-state ratios. Those ratios are computed from the run's TreeDB
compacted rows and SQLite `sqlite_vacuum` rows; they are not hardcoded.

`bytes_per_doc` is always `total_bytes / document_count`. If document count is
missing, the guardrail checks report an error because B/doc would be ambiguous.

## Known Example Shape

The canonical report can represent the PR 1096-style finding without hardcoding
it as production output:

- production offline compact:
  - raw TreeDB template-v1: about 25.7 B/doc
  - collection, 0 indexes: about 22.1 B/doc
  - collection, 1 index: about 34.4 B/doc
  - collection, 2 indexes: about 46.7 B/doc
- exhaustive compact:
  - raw TreeDB template-v1: about 15.8 B/doc
  - collection, 0 indexes: about 15.5 B/doc
  - collection, 1 index: about 20.2 B/doc
  - collection, 2 indexes: about 22.8 B/doc
- full leafgen pack/GC:
  - collection template-v1, 2 indexes: about 27.8 B/doc
  - collection JSON, 2 indexes: about 33.7 B/doc
- SQLite after `VACUUM`:
  - JSON: about 231.7 B/doc
  - native columns: about 156.7 B/doc

The exact numbers in a report should always come from that run's generated JSON,
not from this document.
