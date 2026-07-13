# Query-ready typed-column base generation v1

TreeDB's query-ready base generation (`QRBG` v1) is an immutable,
query-independent container for a snapshot-visible set of typed-column part
images. It is a rebuildable derived asset: it is not a primary document store,
durability root, WAL record, recovery selector, or GC owner.

## Identity and compatibility

Every image records an exact base generation and 32-byte collection schema
hash. Open requires both expected values and fails closed on a mismatch. The
format is pre-alpha; incompatible versions are rejected and should be rebuilt
rather than migrated.

The part table is sorted by source generation and part ID. Each dependency
records the source generation, part ID, row count, byte range, manifest length,
and SHA-256 image checksum. Header and table CRC-32C checksums protect the
container metadata. Embedded typed-column images retain their self-describing
descriptor, null/default bitmaps, encoded values, dictionary/code domain,
offsets, sort marks, layout contract, statistics, and pruning metadata.

The builder validates each complete typed-column image before embedding it.
For identical identity and logical input parts, the output bytes are
deterministic regardless of caller iteration order.

## Open contract

`OpenQueryReadyBaseGenerationFile` uses a read-only file mapping on supported
platforms. Open validates container bounds and checksums, parses each embedded
part manifest, and validates bounded structural metadata before exposing a
view. Encoded column payloads and dictionaries remain direct slices of the
mapping; open does not decode the complete payload, construct a query-shaped
dictionary, or copy part bytes.

Open stats make the contract observable: mapped/read/validated/decoded/copied
bytes, part and row counts, whole-part decodes, dictionary constructions, and
validation/open duration. Build stats report input/output/copied bytes, rows,
parts, and build/validation duration.

`OpenQueryReadyBaseGenerationFileRange` applies the same validation to an exact
column-asset segment offset/length. It retains the page-aligned mmap owner while
exposing only the logical QRBG range, so close always unmaps the correct owner.

Callers must close file-backed views before replacing or deleting their files.
Lifecycle registration, authoritative publication/recovery, and reclamation
remain owned by the existing asset-root and GC work. A caller may place QRBG
files in the column asset manager's prepared namespace, but QRBG v1 does not
publish or unlink them itself.

## Performance guardrails

The performance objective is lower reopen preparation cost without hidden
whole-part decode or payload copying. Focused evidence uses:

- `BenchmarkQueryReadyBaseGenerationBuild`
- `BenchmarkQueryReadyBaseGenerationOpen`
- `BenchmarkQueryReadyBaseGenerationDirectViewAccess`

Run with `-benchmem -count=5` on one host. Report build/open throughput,
`B/op`, `allocs/op`, peak RSS for a production-shaped reopen, and output asset
bytes. Payload duplication, unbounded RSS or allocations, and work deferred
silently into the first query are regressions. The canonical 1M cold/reopen
profile remains a graph-level validation owned by the later integration and
closeout milestones once production queries consume this asset.
