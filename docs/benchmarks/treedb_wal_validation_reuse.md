# V2 WAL validation reuse: post-pipeline candidate

Parent: #3067. Follow-up: #4819/#4820. Canonical comparison owner: #3900.
Baseline: `6012614bf19db94b444ed75b6863ff235b515668`.

## Mechanism and unchanged contracts

`Writer.AppendCommandV2` previously called the fully validating size preflight
and then the fully validating encoder. It now performs full preparation once,
checks all encoded lengths and the writer's maximum frame size, then encodes
that same normalized envelope and canonical preconditions. Reuse is confined
to one synchronous call: no retained validation cache, trusted caller flag,
changed ownership, or journal-admission bypass is introduced.

The existing journal size preflight remains fully validating. Authoritative
LSN assignment, error precedence, checks before destination growth, raw V2
framing, CRC, file and directory synchronization, acknowledgment, poison,
recovery, and the prepared-insert memory ledger are not changed. The encoder
body is shared rather than copied into a specialized production fast path.
The removed work includes one redundant payload-validation pass and, for
RawKV RID frames, one redundant canonical fence scan/sort/hash.

This is a CPU/allocation candidate, not a new sync policy. Linux fdatasync and
close/sync attribution already landed in #3151; that lane stays independent.

## Correctness and mechanism commands

Run with the repository-required Go toolchain (1.26+), from the root:

```sh
GOWORK=off go test ./TreeDB/internal/commitlog -count=1
GOWORK=off go test -race ./TreeDB/internal/commitlog -run 'TestWriterV2ValidationReuse|TestCommandWAL.*V2|TestCommandJournalV2' -count=3
GOWORK=off go test ./TreeDB/db ./TreeDB/collections -run 'CommandWAL|PreparedInsert' -count=1 -timeout=30m
GOWORK=off go test ./TreeDB/internal/commitlog -run '^$' -bench '^BenchmarkWriterV2ValidationReuse$' -benchtime=5x -benchmem -count=5
```

The test-only reference writer retains the pre-change two-validations sequence.
Byte equality and strict reopen tests cover collection and RawKV payloads,
implicit/explicit RID fences, nil payload normalization, barriers, durability
classes, direct/buffered sizes, compression options, and scratch reuse.
Rejection tests cover identity/semantic error precedence, exact size limits,
unchanged scratch/I/O on failure, and revalidation after an input changes.

The benchmark uses synthetic 16-row/16,000-row collection batches. Append and
Sync are both timed; setup is excluded. Keep every sample from both arms.
It diagnoses this WAL seam only. It is not the real Bluesky load benchmark.

## Integrated evidence required before a load-performance claim

Pin engine and JSONBench commits, binary/fixture hashes, toolchain, idle host,
CPU budget, storage placement, 16K batches, durable full-retained/no-aggregate
settings, and both pipeline depths. Compare fresh-DB baseline/candidate at 1M
and 10M with at least five interleaved pairs and a noise rule declared before
collection. Keep preparation/drain inside the existing load boundary and
report query-ready preparation separately. Retain all failed/noisy packets.

Report total load and rows/s, current exclusive WAL/publication CPU/wait
attribution, allocations, request reservation peak, whole-process RSS, durable
bytes, row/query hashes, and separate reopen reconstruction. Do not add nested
counters or subtract historical WAL/sync timings to forecast a load saving.
Do not weaken the existing 4.5 GiB request-owned budget or durability contract.

No measured integrated gain or ClickHouse parity is claimed by this draft.
#3900 still owns fresh canonical comparison and full 10M reconstruction. Keep
this candidate draft until current-head tests and internal review are recorded;
real-workload performance acceptance is a separate outstanding gate.
