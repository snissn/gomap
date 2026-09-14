# Collection WAL review playbooks

These existing prompts provide deeper review questions, not a mandatory review
pipeline. Load only the prompt whose risk matches the change. The integration
owner remains responsible for the outcome and evidence; do not launch one
reviewer per file or repeat reviews without a new diff, failure or other evidence.
Routine documentation or workflow changes do not require a storage-design audit.

| When the change affects... | Read |
| --- | --- |
| Format/version compatibility | [09 - format evolution](09-format-versioning-migration.md) |
| Diagnostics or operator troubleshooting | [10 - observability](10-observability-forensics-operator-debugging.md) |
| Memory, disk or other resource bounds | [11 - resource cost](11-resource-cost-capacity-model.md) |
| State transitions or invariants | [12 - state machine](12-formal-invariants-state-machine.md) |
| Public success/error/acknowledgement semantics | [13 - user contract](13-public-api-user-contract.md) |
| Untrusted or malformed input | [14 - input hardening](14-malformed-input-security-hardening.md) |
| GC, maintenance, backup or restore | [15 - lifecycle](15-maintenance-backup-restore-lifecycle.md) |
| An oversized implementation needs a bounded slice | [16 - scope control](16-minimal-correctness-preserving-slice.md) |
| Conflicting or duplicated documentation | [17 - source-of-truth consistency](17-documentation-source-of-truth-consistency.md) |
| Schema/catalog identity changes | [18 - catalog evolution](18-schema-catalog-identity-evolution.md) |

The prompts were written for the
[collection WAL plan](../TreeDB/docs/spec/collection-wal-durability-plan.md).
Check applicable current [TreeDB specs](../TreeDB/docs/spec/README.md) and code
before applying historical assumptions. Use
[CONTRIBUTING](../CONTRIBUTING.md#completion-and-review) for the common completion
and verification workflow; do not duplicate that policy in individual prompts.
