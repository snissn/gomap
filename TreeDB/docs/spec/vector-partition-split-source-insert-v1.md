# Split canonical source insert: bounded first checkpoint

This candidate for #4810 supports one JSON column-store canonical source Raft
group and one distinct mutable ANN group under an unchanged V1 generation and
collection placement. Multi-owner ANN, immutable generations, M7/source-format
2, token/ring placement, update/delete/replace and cross-group batches remain
unsupported and must refuse before canonical mutation. Existing colocated and
legacy local paths retain their current behavior.

The dedicated authenticated runtime producer validates current fixed groups,
source routing, current FSM DB identity, fresh ACTIVE/readiness and exact router
selection before and after consensus. Certificate-bound cluster/node identity
must belong to the declared caller group; projection additionally requires its
current source leader. Configuration digests are not authentication. Public
commands and generic peer submit/forward cannot send the deterministic split
command; the producer uses the existing Entry control envelope.

Source phase atomically publishes the canonical row and one owned durable intent
with its actual source FSM EntryID in the existing native ordered-root/WAL
boundary. Projection phase carries only ID, vector, original source position and
semantic/document digests; it has no canonical document bytes. It proves a fresh
source quorum/applied read of that exact intent, then atomically publishes native
ANN graph deltas and the durable target receipt. It never inserts a canonical
row on the ANN target. Clear phase retains the exact receipt on the source and
retires the pending slot in the same native publication boundary. Lost delivery
or acknowledgement preserves the pending slot; retries keep the original source
EntryID and target receipt rather than interpreting a newer ReadIndex as that
semantic operation.

The one retry goroutine is owned by the source runtime, begins only after
construction, has one 500 ms ticker and one bounded request per tick, and must be
canceled and joined before collection/DB shutdown without holding its lifetime
mutex. Constructor/Close hook composition is required before runtime validation;
the independently owned initializer repair controls those hook sites. It reads
only one fixed identity slot, uses existing request/proposal/transport admission,
and never hides admission failure by dropping the durable intent.

Capacity is deliberate: one pending operation and at most 64 completed outcomes
for this collection/index/generation/source/target/catalog identity, at most 64
KiB canonical document, 1,024 dimensions and 1,024-byte identities. The 65th new
operation refuses before source append/row mutation; old completed duplicates
still return the exact durable outcome after reopen. Completed outcomes are not
evicted. This checkpoint therefore has a lifetime write ceiling, not a claim of
indefinite writable operation or general retention/GC. The existing generic FSM
idempotency record is reused for command deduplication but does not carry the
original source position, target durable receipt and graph revision required
for retirement and session visibility; those bounded outcomes remain in native
system metadata. Large records use persistent value-log pointers within the same
atomic publication. Replicated reopen requires the exact prepared manifest and
load-only durable carrier coverage of the current document state; it must never
publish a missing binding or rebuild the graph. Ordinary/M7 prepared validation
continues to require the original immutable source to remain current.

Insert returns an owned bounded opaque visibility token binding the durable
receipt. Strict search with that token repeats current scope/ACTIVE/current-DB
checks and a real target quorum/applied receipt fence before search and before
response. Generic, fast and pinned backends refuse tokens they cannot enforce.
A receipt proves graph membership/revision independently of ANN top-k results;
it does not promise that approximate top-k contains the inserted ID. Ordinary
nil-token wire encodings are unchanged. Mutation counters count submissions in
the invocation (three on completion, one on a completed duplicate), not latency
or bytes attributable to a phase.

Actual FSM GroupID and EntryID are authoritative during Raft apply. Validated
local WAL replay is a different trust boundary: it replays locally admitted
bounded canonical envelopes and original commit positions without inventing
fresh remote authority. Raw privileged in-process deterministic apply and trusted
backup/WAL bytes remain operator trust inputs; this is not a cryptographic proof
that arbitrary local bytes originated in consensus.

Focused controls cover authenticated canonical/graph separation, exact duplicate
and conflict, fixed-generation capacity and old duplicate after reopen, real
receipt/token visibility and malformed-token refusal. The deterministic recovery
control intercepts the real authenticated HTTP transport only after source row
plus intent publication, loses the source leader, and reopens the native DB
before automatic replay/clear without another client mutation. Graceful Close
and reopen are not SIGKILL or power-loss certification; command-WAL truncation
and power-loss evidence and broader #4810 throughput/backlog/capacity acceptance
remain separate required work. No runtime acceptance is claimed by this source
checkpoint.

Replicated mutable source/router startup also requires the exact durable live binding already to exist. Startup restores and validates it without publishing missing binding coverage outside Raft; a ready TVIS with no durable binding is refused. Ordinary prepared-generation opens remain strict.
