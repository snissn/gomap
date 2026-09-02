# TreeDB Value-Log Autotune (Implementation Runbook)

Scope: modifying TreeDB’s value-log compression autotuner and related write/read
paths.

This is an implementation runbook (agent-oriented). Operator-facing semantics
live in:
- `docs/TREEDB_VALUELOG_AUTOTUNE.md`
- `docs/contracts/DURABILITY.md`
- `docs/TREEDB_STORAGE_FORMAT.md`

## Key invariants (must hold)

- **Attempted vs kept**
  - *attempted* means zstd encoding was executed (CPU spent)
  - *kept* means the compressed bytes were actually stored
  - it is expected that `attempted != kept` on incompressible data
- **Fail closed**
  - missing/invalid dictionary bytes must degrade to safe behavior (store raw / keep last known-good dict)
  - decoding must bounds-check before allocation; corruption must not panic
- **Persistent pointers**
  - value-log segments are persistent storage; never treat them as an ephemeral WAL
  - GC/rewrite are the space-reclamation mechanisms

## Code map

- Core format + reader/writer:
  - `TreeDB/internal/valuelog/valuelog.go`
  - `TreeDB/internal/valuelog/writer.go`
  - `TreeDB/internal/valuelog/reader.go`
  - `TreeDB/internal/valuelog/reader_mmap.go`
- Autotune policy:
  - `TreeDB/internal/valuelog/autotune_options.go`
  - `TreeDB/internal/valuelog/autotune_keep.go`
  - `TreeDB/internal/valuelog/encode_cost_model.go`
- Cached-mode integration:
  - `TreeDB/caching/vlog_dict.go` (dict policy + dictdb integration)
  - `TreeDB/caching/db.go` (append boundaries, flush/checkpoint interactions)

## Tests and benches

Fast correctness loop:

```bash
go test ./TreeDB/internal/valuelog -count=1
go test ./TreeDB/caching -count=1
```

Fuzz / corruption hardening (keep deterministic and bounded):

```bash
go test ./TreeDB/internal/valuelog -run '^$' -fuzz FuzzDecodeFrame -fuzztime 10s -count=1
```

Microbenching:

```bash
go test ./TreeDB/internal/valuelog -run '^$' -bench . -benchmem -count=3
```

CI-grade benchmark methodology:
- `docs/benchmarks/VLOG_AUTOTUNE.md`

## Common failure modes

- Changing record/frame encoding without updating `page.ValuePtr` flag usage (`ValuePtrMarkGrouped`, record length checks).
- Keeping compression on incompressible workloads (high attempted fraction, low kept fraction, throughput regression).
- Dict lifecycle bugs (dictdb current pointer not refreshed; missing dict bytes on restart).
- Incorrect durability attribution when WAL is disabled (WAL/journal off does not imply value-log is ephemeral).
