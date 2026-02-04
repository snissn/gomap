# TODO / Roadmap (High Priority)

Legacy note: TreeDB now exposes a single entrypoint (`treedb.Open`) with WAL
on/off semantics. Backend-only mode and legacy `Mode*` switches have been
removed. Historical roadmap items that depended on those modes are deprecated.

For current plans and optimization work, see:
- `TREEDB_OPTIMIZATION_CHECKLIST.md`
- `docs/DEV_NOTES.md`
- `docs/TREEDB_VALUELOG_AUTOTUNE.md`
- `docs/agents/README.md`
