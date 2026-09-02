# Collection WAL Review Prompts

These prompts complement the first eight collection WAL review prompts. They
focus on the remaining maintainability gates: format evolution, diagnostics,
resource bounds, formal invariants, public API semantics, malformed input
hardening, maintenance lifecycle, MVP scope control, documentation consistency,
and catalog identity evolution.

Recommended run order:

1. `09-format-versioning-migration.md`
2. `12-formal-invariants-state-machine.md`
3. `18-schema-catalog-identity-evolution.md`
4. `16-minimal-correctness-preserving-slice.md`
5. `14-malformed-input-security-hardening.md`
6. `15-maintenance-backup-restore-lifecycle.md`
7. `11-resource-cost-capacity-model.md`
8. `10-observability-forensics-operator-debugging.md`
9. `17-documentation-source-of-truth-consistency.md`

The prompts assume the current canonical spec is:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`

