# R3a apply closeout 3043

This note records the local R3a apply closeout boundary for #3043. It is
local-only harness evidence for future Raft work; it does not expose networked
Raft, leader routing, snapshots, read-index, or production
`ack_policy=raft_committed`.

## Local metadata boundary

The R3a harness decodes committed deterministic `CommandEntryV1` bytes, lowers
accepted commands to local command-WAL `CommandEnvelope` frames, applies them
through the normal collection/catalog executor, and records result/idempotency
and apply-progress metadata only after local `AppliedCommandLSN` coverage is
present. Result and progress records carry the covered `AppliedCommandLSN`; a
stored result whose coverage outruns the reopened DB state is recovery-required
and cannot advance progress.

Result records are keyed by `ApplyEntryID` and by deterministic idempotency key.
The same idempotency key with the same command digest replays the recorded
result without a second command-WAL append. The same idempotency key with a
different digest fails before mutation.

## Fault seams

The named local apply fault seams are:

- `before-local-wal-append-v1`
- `after-local-wal-append-before-visible-v1`
- `after-visible-before-result-record-v1`
- `after-result-record-before-progress-v1`
- `after-progress-record-v1`

These seams are deterministic test hooks. They are not timing races.

## Accepted command list

The #3043 closeout applies only to the R3a v1 accepted command set already
landed by #3041 and #3042:

- `create_collection`
- `insert_batch`
- `replace_batch`
- `delete_batch`

## Rejection list

All other native-wire commands remain rejected by the R3a v1 allowlist until a
future issue defines deterministic lowering, local command-WAL recovery,
result/idempotency, and logical-digest coverage for that command. In particular,
index DDL, query-wide mutation, callback/resolver mutation, and
`update_bson_set` are not accepted by this closeout.

## Benchmarks and evidence hooks

Representative local evidence command:

```sh
GOWORK=off go test ./TreeDB/internal/raftapply \
  -run 'Test(Fault|StoredResult|Idempotency|CreateCollectionApplyCreatesCatalog|Progress)' \
  -bench 'BenchmarkApplyCommittedEntryCloseout3043' \
  -benchmem -count=1
```

The benchmark labels are:

- `supported_create_collection`
- `rejected_unsupported_before_append`
- `duplicate_result_replay`
- `close_reopen_replay_boundary`

The benchmark intentionally reports local harness latency and allocations only.
It does not measure networked Raft consensus, leader routing, snapshots,
read-index, or production `raft_committed` acknowledgement.
