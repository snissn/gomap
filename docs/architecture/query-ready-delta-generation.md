# Query-ready delta generations and bounded multipart consolidation

TreeDB's query-ready delta generation (`QRDG` v1) is a rebuildable,
non-authoritative envelope around M1's validated `QRBG` part container plus a
deterministic tombstone table. It supplies a physical delta representation and
a base-plus-delta preparation contract without becoming a WAL record, root,
recovery selector, primary document store, snapshot registry, or GC/unlink
owner.

## Visibility and dictionary semantics

`NewQueryReadyBaseDeltaReader` prepares one latest-visible view by decoding the
selected immutable parts and delegating inserts, updates, deletes,
same-generation tie-breaking, and tombstone visibility to the existing
`PartSetReader`. This is preparation state reused by warm lookups; it is not
claimed as query-independent open state. Persisting that state is owned by M3.

Low-cardinality domains remain part-local. Preparation decodes each local
dictionary once, and `DictionaryValueAtLatest` resolves a selected code through
the selected row's part. Thus code reuse such as `0=user` in a base and
`0=moderator` in a delta retains the correct semantic value without a global
dictionary construction or code translation. Counters distinguish local
dictionary decodes from global dictionary constructions and translations.

M3 additionally provides file-backed QRDG and consolidated-base open, including
exact nonzero segment offset/length ranges. These mappings retain the QRDG
envelope and embedded QRBG as encoded direct views; their mapping lifetime is
closed exactly once and is pinned by the collection-scoped M3 reader lease.

## Bounds and consolidation

The default policy permits at most four visible delta generations and eight
accumulated delta-derived parts. The latter is relative to the original base:
an existing base with any valid part count remains usable at `N=0`. A
consolidated envelope persists the original-base part count and accumulated
delta-derived part count; repeated consolidation may only increase the latter.
Only a later true physical base rewrite may reset that lineage.

Rows and encoded bytes can be bounded with explicit overrides. Decisions and
the public `QueryReadyDeltaBoundError` report original base parts,
delta-derived parts already inside a consolidated base, newly visible delta
parts, total parts, rows, bytes, and the specific generation/part/row/byte
trigger. Reader preparation and consolidation gate before whole-part decoding
or output copying, so callers can schedule physical rewrite from structured
evidence without parsing error text.

M2 consolidation is deliberately a **bounded multipart replacement**, not a
physical row compaction. It deterministically embeds the old base parts and a
selected delta prefix in one standalone consolidated QRDG, carries forward the
latest tombstone per primary ID, and records lineage. Reopen without the
consumed deltas preserves visibility, including deletes, dictionary domains,
nullable state, and same-generation part-ID tie-breaking. Old base/delta
objects remain independently usable by old snapshots. The output generation
must equal the highest consumed prefix generation (or the unchanged base
generation when no delta is consumed), preventing an envelope from claiming a
gap it did not consume.

The multipart bound is fail-closed. When accumulated delta-derived work reaches
the configured ceiling, M2 exposes a **physical-rewrite-required** condition;
another multipart consolidation cannot relieve the bound. It does not fall
back to document scans, silently reset lineage, or emit an immediately unusable
replacement. M4 may replace this multipart execution with a true encoded
physical rewrite.

## Measurement contract

Focused benchmarks separate preparation from warm reused lookup:

- `BenchmarkQueryReadyBaseDeltaPrepareCurve` covers `N=0,1,2,4,8,9`, low- and
  high-cardinality domains, tombstone-heavy deltas, and nullable mixed
  update/delete/reinsert generations;
- `BenchmarkQueryReadyBaseDeltaWarmLookup` measures lookup after preparation;
- `BenchmarkQueryReadyBaseDeltaConsolidation` reports write amplification and
  the peak encoded output buffer bytes;
- `BenchmarkQueryReadyDeltaGenerationBuild` compares validated typed-part
  container production with QRDG envelope production.

Run them on one host with `-benchmem -count=5`. The checked-in default of eight
accumulated delta-derived parts is the last allowed point on the required
`N=0..8` curve; `N=9` is the explicit beyond-bound point. The generation limit
of four triggers earlier for the ordinary default query path. Build/consolidate
copy counters include typed-column images copied directly into the final QRDG
buffer and tombstone-table bytes. QRDG construction uses one final encoded
buffer rather than simultaneously retaining inner and outer generation images.
The encoded-buffer counter intentionally excludes caller-owned inputs and Go
metadata; bounded pipeline admission reports a conservative in-flight
reservation, while process peak RSS remains a separate same-host measurement.

The benchmark fixture defaults remain CI-sized. Same-host production-shape
captures may set `QUERY_READY_BENCH_BASE_ROWS` and
`QUERY_READY_BENCH_DELTA_ROWS`; both must be positive integers. For example,
the M2 evidence used 1,000,000 base rows and 10,000 rows per delta while
selecting the low-cardinality `N=0`, `N=4`, and `N=8` sub-benchmarks.
