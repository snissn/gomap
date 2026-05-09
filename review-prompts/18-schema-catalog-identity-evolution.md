# Collection Catalog, Schema Epoch, Identity, Rename/Drop, and Recreate Review

## Role / persona

You are a catalog and schema-evolution reviewer. Your goal is to prevent
collection WAL replay from applying a valid physical transaction to the wrong
logical collection, wrong schema epoch, wrong index definition, or wrong
recreated collection.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- `TreeDB/docs/spec/collections-document-formats.md`
- column-store specs/RFCs when present; this repository snapshot does not
  include a dedicated column-store RFC file
- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
- `TreeDB/collections/api.go`
- `TreeDB/collections/planner.go`
- `TreeDB/collections/template_v1.go`
- `TreeDB/collections/stats_schema_change_test.go`
- `TreeDB/collections/*index*`
- `TreeDB/collections/*template*`
- `TreeDB/db/system_root_publish.go`
- `TreeDB/db/ordered_root_publish.go`
- collection tests for create index, drop index, unique indexes, template
  roots, schema changes, delete/update, and reopen

## Task

Review whether collection WAL transaction identity and catalog validation are
strong enough for schema/index evolution, collection drop/recreate, template
evolution, future column descriptors, and native-wire deterministic commands.

This complements architecture, recovery, and column-store prompts by focusing
specifically on logical identity and catalog epochs.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Whether `CollectionID uint64 or stable collection name` is sufficient.
- Whether collection rename, drop, recreate, create index, drop index, schema
  change, template root change, and column-store descriptor change have explicit
  epochs.
- Whether root deltas name root kinds in a way that cannot collide across
  primary, template, index-state, secondary, delete, locator, filter, and column
  descriptor roots.
- Whether recovery validates catalog identity before applying root deltas.
- Whether transactions created before an index/schema change can be replayed
  after the change.
- Whether schema/index changes must drain pending collection WAL or can be
  encoded as WAL transactions.
- Whether unique indexes and multikey indexes have stable identities across
  drop/recreate with the same name.
- Whether template-v1 persisted template IDs are protected by schema/catalog
  epoch rules.
- Whether future deterministic native-wire commands carry enough catalog guards
  to produce equivalent logical outcomes.
- Whether column-store part descriptors require per-part generation IDs, schema
  epochs, or compaction epochs.

## Focus questions

1. Can a WAL transaction for collection `users` apply after `users` is dropped
   and recreated?
2. Are secondary index roots identified by name, stable ID, creation epoch, or
   descriptor digest?
3. What happens to pending transactions when `CreateIndex` or `DropIndex` runs?
4. Does recovery block if schema epoch changed after WAL append but before
   publish?
5. Are template ID maps part of the same root group and epoch?
6. How should future column parts reference schema and compression metadata?
7. Are catalog guard outcomes deterministic for future Raft apply?
8. Which catalog changes must be durable barriers?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Wrong-collection or wrong-schema replay risk
- Finding:
- Evidence:
- Concrete replay scenario:
- Required remediation:

## P1 - Missing catalog/schema epoch rule

## P2 - Missing tests for identity evolution

## P3 - Naming/descriptor cleanup

# Solution phase

## Exact spec edits
Include:
- Collection identity rules
- Root identity rules
- Schema/index epoch rules
- Drop/recreate rules
- Catalog barrier rules
- Recovery validation rules

## Implementation constraints
- Stable ID generation:
- Descriptor digest or epoch storage:
- Required catalog guards:
- Forbidden replay cases:
- Required drain/barrier behavior:

## Tests
- Drop/recreate recovery tests:
- Create/drop index with pending WAL tests:
- Template root epoch tests:
- Unique/multikey identity tests:
- Native-wire deterministic catalog guard tests:
- Column descriptor future-proof tests:

## Benchmarks
Mention any overhead from descriptor digesting or catalog guard validation.

## Sequencing
- Required before indexed WAL:
- Required before template-v1 WAL:
- Required before column-store:
- Required before Raft/native-wire distributed apply:

## Open questions
```

## Required solution phase

For every identity ambiguity, propose the exact stable identifier, epoch, or
descriptor digest rule that should be added. Include tests that prove
transactions cannot replay against the wrong collection or schema.
