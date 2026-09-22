# TreeDB Cached vs Backend (Deprecated)

Backend-only mode has been removed. TreeDB now exposes a single engine; current
public write paths should use the command-WAL profiles described in
`docs/TREEDB_WRITE_PATHS.md`.

If you were previously relying on backend-only mode, migrate to `treedb.Open`
and choose the appropriate command-WAL profile. Legacy WAL on/off durability
names remain compatibility-only terminology for old low-level cached redo-journal
configuration and older benchmarks.
