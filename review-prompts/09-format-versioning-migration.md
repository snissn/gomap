# On-Disk Format, Versioning, Migration, and Compatibility Review

## Role / persona

You are a storage-format maintainer responsible for preventing accidental format
forks, unrecoverable upgrades, ambiguous WAL decoding, and future migration dead
ends.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/verification.md`
- `TreeDB/db/layout.go`
- `TreeDB/dir_layout.go`
- `TreeDB/db/format_config.go`
- `TreeDB/db/wal_recovery.go`
- `TreeDB/internal/commitlog/commitlog.go`
- `TreeDB/internal/commitlog/writer.go`
- `TreeDB/internal/commitlog/reader.go`
- `TreeDB/internal/atomicfile/atomicfile.go`
- `TreeDB/page/meta.go`
- existing format and recovery tests under `TreeDB/db`, `TreeDB/page`, and
  `TreeDB/internal/commitlog`

## Task

Review whether the proposed collection WAL can be safely encoded, decoded,
versioned, upgraded, skipped, quarantined, or migrated over time. Do not
re-litigate whether root-delta WAL is the right architecture except where the
chosen architecture creates concrete format risks.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- WAL transaction envelope format: magic bytes, version, flags, length,
  checksum coverage, endian choice, compression marker, record type, and
  segment-level versus record-level CRC.
- Whether `Version uint8` in `CollectionWALTransaction` is sufficient for
  future optional fields, unknown field preservation, old reader rejection, and
  forward/backward compatibility.
- Segment naming and discovery: interaction between `commit-l*.log`,
  `collection-l*.log`, value-log files, leaf-log files, flat layout, root
  layout, `maindb`, `dictdb`, and `templatedb`.
- How format changes are gated by `storage-format.md`, `format_config.go`,
  metadata roots, feature flags, or explicit pre-alpha "rebuild required"
  behavior.
- How recovery distinguishes incomplete tail, old unsupported segment, unknown
  future segment, hard corruption, malicious length, and mixed-version WAL.
- Whether collection identity uses stable names, IDs, schema epochs, or
  descriptors in a way that survives rename, delete/recreate, restore, and
  future migrations.
- Whether on-disk paths and side references can be canonicalized without locking
  in unsafe path semantics.
- Whether there is a clear upgrade story from "no collection WAL exists" to
  "collection WAL required under WAL-on profiles."
- Whether downgrade behavior is explicitly unsupported, detected, or safe-failed.
- Whether golden fixtures are required for WAL transactions, watermarks, side
  refs, root deltas, and malformed cases.

## Focus questions

1. What exact bytes are written before the first root delta?
2. Can a future implementation add a new side-ref class without older binaries
   silently deleting or ignoring it?
3. Does recovery fail closed when the format is unknown but might contain
   durable acknowledged writes?
4. Is "pre-alpha compatibility not guaranteed" precise enough for this feature,
   or does the spec need explicit migration states?
5. Are mixed directories possible after crash during an upgrade?
6. Can collection WAL cleanup accidentally remove files that a newer format
   would still need?
7. Are version and feature gates stored in the system root, meta page, WAL
   header, or all three?
8. What test fixture would prove that a transaction encoded today remains
   decodable after future refactors?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Must fix before implementation
For each finding:
- Finding:
- Evidence from files:
- Failure mode:
- Why existing prompts may miss it:
- Required remediation:

## P1 - Must fix before enabling WAL-on collection durability

## P2 - Should fix before column-store gate

## P3 - Nice-to-have / documentation polish

# Solution phase

## Exact spec edits
For each edit:
- File:
- Section:
- Replace/add text:
- Rationale:

## Implementation constraints
- Required encoder/decoder constraints:
- Required migration/upgrade constraints:
- Required cleanup constraints:

## Tests and fixtures
- Golden fixtures:
- Corruption fixtures:
- Upgrade/downgrade fixtures:
- Recovery fixtures:

## Benchmarks
- Encoding/decoding overhead:
- Recovery scan overhead:
- Large-transaction overhead:

## Sequencing
- Before PR1:
- Before WAL-on default:
- Before column-store writes:

## Open questions
```

## Required solution phase

For every P0/P1 finding, propose concrete remediations including exact spec
edits, implementation constraints, tests, benchmarks where relevant, sequencing,
and open questions. Be explicit about what must be added to `storage-format.md`
versus `collection-wal-durability-plan.md`.

