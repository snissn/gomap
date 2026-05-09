# Malformed File, Path Safety, Security, and Abuse-Resistance Review

## Role / persona

You are a security and robustness reviewer. Your job is to harden the collection
WAL against corrupt files, malicious local directories, path traversal, symlink
surprises, decompression bombs, integer overflow, excessive allocation, and
tenant/name confusion.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/db/permissions.go`
- `TreeDB/dir_layout.go`
- `TreeDB/db/layout.go`
- `TreeDB/internal/commitlog/reader.go`
- `TreeDB/internal/commitlog/writer.go`
- `TreeDB/internal/valuelog/reader.go`
- `TreeDB/internal/valuelog/block_codec.go`
- `TreeDB/internal/limits/record.go`
- `TreeDB/internal/crc/crc.go`
- `TreeDB/internal/lockfile`
- `TreeDB/internal/nativewire/*`
- `TreeDB/collections/api.go`
- fuzz and corruption tests under `TreeDB/internal/commitlog`,
  `TreeDB/internal/valuelog`, `TreeDB/internal/nativewire`, `TreeDB/page`, and
  `TreeDB/internal/memtable`

## Task

Review whether the collection WAL spec and likely implementation are robust
against malformed on-disk data and hostile or confused local environments. This
prompt is distinct from crash red-team review: focus on invalid inputs, bounds,
isolation, and fail-closed behavior.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Transaction length limits, field length limits, root count limits, delta count
  limits, side-ref count limits, collection name limits, path length limits, and
  allocation caps.
- Integer overflow in offset, size, checksum range, length prefix, varint,
  count, and memory preallocation calculations.
- Decompression or decoding bombs if WAL or side payloads are compressed.
- Path traversal or absolute-path leakage through `RelativePath` side refs.
- Symlink, hardlink, world-writable directory, and cross-device rename risks for
  WAL and side files.
- Multi-tenant or multi-collection isolation: one collection's WAL transaction
  must not be able to publish roots or side refs for another collection by name
  collision.
- Malicious or corrupt unknown side-ref classes.
- Error behavior for corrupt middle record versus corrupt tail.
- Native-wire command fields that eventually feed collection names, index names,
  schema names, or side-effect paths.
- Redaction of sensitive document contents in errors, logs, metrics, and
  forensic tools.
- Fuzz coverage for collection WAL decode, side-ref decode, root-delta decode,
  and recovery ordering.

## Focus questions

1. What is the maximum legal encoded transaction size?
2. Can a corrupt WAL record force unbounded allocation before checksum
   verification?
3. Can a side ref escape the DB root through `../`, symlink, or absolute path?
4. Are collection names used as paths or only as logical identifiers?
5. Can an unknown future `RefClass` be ignored safely?
6. Does recovery fail closed when corruption appears before the tail?
7. Are all checks performed before publishing roots?
8. Can logs disclose raw keys, documents, or tenant names?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Security or fail-open corruption risk
- Finding:
- Evidence:
- Exploit/corruption scenario:
- Required remediation:

## P1 - Required hardening before WAL decode/recovery

## P2 - Required fuzz/bounds coverage

## P3 - Defense-in-depth

# Solution phase

## Exact spec edits
Include:
- Bounds table
- Path rules
- Unknown-field and unknown-ref behavior
- Redaction rules
- Corruption handling rules

## Implementation constraints
- Decoder order of checks:
- Allocation caps:
- Path canonicalization:
- Directory permission checks:
- Error classification:

## Tests
- Fuzz targets:
- Corrupt fixture tests:
- Path traversal tests:
- Symlink/permission tests:
- Allocation cap tests:
- Redaction tests:

## Benchmarks
- Bounds-check overhead:
- Fuzz corpus minimization if relevant:

## Sequencing
- Hardening required before any reader lands:
- Hardening required before operator tooling:
- Hardening required before native-wire integration:

## Open questions
```

## Required solution phase

Every P0/P1 remediation must include a concrete bound, fail-closed rule, path
rule, or decoder-order constraint. Include exact tests for malformed files that
must not allocate excessive memory, publish roots, or delete files.

