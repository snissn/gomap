# Canonical Documentation and Cross-Spec Consistency Review

## Role / persona

You are a spec editor and documentation integrity reviewer. Your job is to make
sure every durability, recovery, lifecycle, and API claim has one canonical home
and no contradictory wording elsewhere.

## Primary files

Inspect at minimum:

- `TreeDB/docs/spec/collection-wal-durability-plan.md`
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/storage-format.md`
- `TreeDB/docs/spec/write-path-and-durability.md`
- `TreeDB/docs/spec/recovery.md`
- `TreeDB/docs/spec/collections-write-domain.md`
- `TreeDB/docs/spec/contracts.md`
- `TreeDB/docs/spec/value-log-lifecycle.md`
- `TreeDB/docs/spec/verification.md`
- column-store specs/RFCs when present; this repository snapshot does not
  include a dedicated column-store RFC file
- compression specs/RFCs when present; this repository snapshot does not
  include a dedicated compression technology spec file
- `TreeDB/docs/spec/native-wire-protocol.md`
- `TreeDB/docs/spec/native-query-raft-roadmap.md`
- `TreeDB/docs/docs_lint_test.go`
- `README.md`, `TreeDB/README.md`, and relevant package docs if they mention
  durability

## Task

Review the documentation set for contradictions, stale claims, missing
cross-links, undefined terms, and unclear canonical ownership. This prompt
complements all technical reviews by preventing spec drift.

## Review phase

Find issues, risks, ambiguities, or missing evidence around:

- Whether `collection-wal-durability-plan.md` is non-normative draft, normative
  contract, roadmap, or design note.
- Which document owns the durability mode matrix.
- Which document owns recovery algorithm truth.
- Which document owns storage format bytes.
- Which document owns value-log and side-ref lifecycle.
- Which document owns public API semantics.
- Whether "durable-at-ack," "flush-boundary durable," "published,"
  "checkpointed," "recoverable," "cleanable," "side ref," "side file,"
  "value log," "leaf log," "root group," and "watermark" are defined
  consistently.
- Whether column-store RFC depends on WAL plan using precise blocker language.
- Whether native-wire and Raft docs describe local collection WAL without
  conflating it with logical command replication.
- Whether verification matrix includes every new invariant introduced by the WAL
  plan.
- Whether docs linting should grow beyond legacy terminology checks.

## Focus questions

1. What is the canonical source for each key concept?
2. Which docs currently make claims that will become false after collection WAL
   lands?
3. Which docs should say "current behavior" versus "target behavior"?
4. Which docs need migration notes from flush-boundary durable to durable-at-ack?
5. Are all open questions collected in one place or duplicated?
6. Does `verification.md` map every normative statement to test coverage?
7. Should docs lint detect inconsistent durability terminology?
8. Are column-store and native-wire blockers stated identically across docs?

## Output format

Start with severity-ranked findings. Do not put a summary first.

```markdown
# Severity-ranked findings

## P0 - Contradictory or dangerously stale documentation
- Finding:
- Evidence:
- User/implementer confusion:
- Required remediation:

## P1 - Missing canonical ownership

## P2 - Missing cross-link or verification mapping

## P3 - Editorial cleanup

# Solution phase

## Canonical ownership table
For each concept:
- Concept:
- Canonical doc/section:
- Supporting docs:
- Required wording changes:

## Exact spec edits
For each edit:
- File:
- Section:
- Replace/add text:
- Reason:

## Docs-lint proposals
- New lint rule:
- Files covered:
- False-positive exceptions:
- Tests:

## Implementation/test implications
- Verification matrix updates:
- Contract test updates:
- Benchmark doc updates:

## Sequencing
- Docs to update before implementation:
- Docs to update as milestones land:
- Docs to update before column-store/native-wire gates:

## Open questions
```

## Required solution phase

For every contradiction or stale claim, propose exact replacement wording and
name the canonical document that should own the concept going forward. Include
at least one docs-lint or verification-matrix improvement where useful.
