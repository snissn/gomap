# pebblecompat (TreeDB-backed)

`pebblecompat` exposes a Pebble-shaped API over TreeDB with focus on:

- deterministic batch `Repr` / `ApplyBatchRepr`,
- ingest APIs (`Ingest`, `IngestWithStats`, `IngestExternalFiles`, `IngestAndExcise`),
- range-key + `ScanInternal` callback surface.

## On-disk metadata

The wrapper stores compatibility metadata in reserved keys under an internal
prefix (default `\x00pebblecompat\x00`):

- `seq` key: monotonic sequence counter.
- `pt/` keys: latest point-key sequence + kind metadata.
- `rg/` keys: append-only range operation log.

User keys with the reserved prefix are rejected.

## Status

Pre-alpha compatibility layer intended for experimentation and conformance work.
Shared-object ingest (`[]pebble.SharedSSTMeta`) is not implemented yet.

## Shared Objects (`.pcobj`)

This package now includes a TreeDB-native shared object format for span export/ingest:

- `ExportSharedObject(path, span)`
- `IngestSharedObject(path, opts)`

Format goals for this sprint:

- immutable file artifact suitable for transfer,
- sequential buffered IO,
- chunked TreeDB batch apply (`~8k ops` / `~4 MiB`) during ingest.

`IngestWithStats`, `IngestExternalFiles`, and `IngestAndExcise` automatically
fast-path `.pcobj` files.

Excise behavior now preserves non-overlapping fragments of existing range
records (split-left/split-right) when an excise span intersects them.
