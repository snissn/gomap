# TreeDB Cached vs Backend (Deprecated)

Backend-only mode has been removed. TreeDB now exposes a single engine with
WAL on/off semantics (see `docs/TREEDB_WRITE_PATHS.md`).

If you were previously relying on backend-only mode, migrate to `treedb.Open`
and choose WAL on/off via `Options.Durability`.
