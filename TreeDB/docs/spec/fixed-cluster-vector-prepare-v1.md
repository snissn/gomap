# Bounded real-Raft vector initialization prepare

This provisional checkpoint supports one RF3 or RF4 data group and a catalog
with the exact same three or four voters, one generation and one physical partition, with a nonempty source of at most
16,384 documents within the actual32-MiB FP32+ID input envelope. The collection uses the production column_graph cosine float32
definition and a physical typed float32 vector column. Dimensions are capped
at 4096, configured M at 64, and configured construction/search ef at 4096.
Quantized definitions, schema-generation/representation variants, split owners,
movement, subsequent generations and automatic activation without restart are
outside this checkpoint. RF4 is bounded operational conformance, not a larger
capacity or fault-tolerance claim: its quorum is three and a 2+2 two-host
placement cannot survive either host loss. Preparation admission is separate from live overlay limits. The optional ordinary
insert probe plans65 distinct IDs. Fresh trial10 completed its bounded
198-operation corpus probe; this is ordinary growth qualification, not sustained
capacity. Split mutation ledgers retain their own64 bounds.

Create and InsertBatch use routed native Raft commands. Physical creation uses
additive collection-metadata version 6 carrying the production column schema;
a missing asset manager is filled by the existing collection defaults. Version 5
remains byte-identical for nil column configuration. This checkpoint bounds the
wire schema to 16 KiB and 32 columns, including normalized defaults. Metadata
responses preserve durable physical state; create requests cannot supply it.

Prepare rejects a nonphysical collection, mismatched typed raw float32 field,
missing active/recovery manifest or unauthenticated manifest root before Raft
Append. It reads the current persisted catalog without constructing a graph.
The RF3 and RF4 real-Raft witnesses create and ingest through the wire, verify the physical
schema reached every voter, then rebuild and prepare. A separate refusal
witness checks real leader log/applied positions and catalog remain unchanged,
and the refused persisted cluster can reopen.
`FixedPeerTCPClientV1.PrepareVectorInitializationV1` first commits a collection
owned source rebuild. Every voter must report the same authenticated source.
It then commits PartitionPrepare with that frozen source/group/generation.
The same request ID can resume after source commitment or partial preparation.
The authenticated owner restores the original expected catalog version only
from that key's locally covered durable apply result, then requires the entire
canonical command digest to match. Source and prepare use distinct keys and
retain their original frozen payloads. A stale Prepare guard may reach shared
submit preflight, but commit still requires a known exact idempotency replay.
Unknown keys and changed source or payload receive no stale-guard grant.
All-voter source and completion agreement and real committed apply remain
required; a successful all-completed retry returns the durable completion.

The FSM assigns its actual term/index and deterministic command digest;
clients cannot supply those positions. The collection retains its actual
schema, native admission, coverage persistence, mutation and partition storage
leases through the local WAL append, execution, and finalize/abort.

The owner acquires its actual stable index-generation pin before storage,
schema, native admission, mutation or raw staging. A rebuild may take a
current ordinary snapshot after flush while retaining that generation pin;
it does not reacquire maintenance with an assigned raw guard.

Append acquires teardown once and returns a typed DB staging guard bound to
the exact assigned intent. Pack materialization, router output and final READY
authentication borrow capture ownership from that live guard. Borrowers cannot
release the parent's teardown reader and may complete capture after Close has
queued its writer. Releasing the borrower, Finalize or Abort invalidates its
use. Copies of the opaque staging handle share one DB-minted ownership state,
so copying cannot extend lifetime or release teardown twice. Captures must
finish before Finalize/Abort; this callback contract does not admit asynchronous
producers. Existing closing publication refusal is
preserved, so finishing capture does not authorize new root publication after
Close starts. No independent capture lease is admitted before preflight or
Append that could cause another teardown read acquisition.

Startup kind107 replay instead uses the DB-minted replay intent, verified
against its exact DB and actual active LSN/token on every owner validation.
It has no inherited raw guard: flush and root publication take their ordinary
unheld-raw startup paths. Its ordinary asset capture leases end before final
native root publication. Callback exit expires the replay intent, including
when a later callback has the same LSN. This adds no reconstructed progress,
root rebinding or automatic real-runtime result recovery.

Prepare allocates fresh O_EXCL pack outputs before BUILD. Failure before the
durable BUILD leaves those files unreachable; local WAL replay allocates fresh
outputs rather than guessing the old physical refs. BUILD and READY are
durable staged lifecycle states, with no active pointer. After BUILD, replay
requires the exact command origin, source tuple, layout, generation and
canonical pack IDs, then authenticates all retained asset bytes. A fresh router
output promotes the same staged manifest to READY. One native root publication
records the live binding and prepare completion at the real prepare WAL LSN.
Its logical asset-set digest excludes local physical refs and local READY
digests. Physical READY/integrity digests remain local and fully authenticated.

A clean reopen derives an in-memory serving configuration solely from the
immutable initialization intent, current FSM DB, covered durable prepare
result, exact staged READY assets and durable native live binding. Persisted
intent, shared configuration digest and root identity markers stay unchanged.
Before existing catalog BeginBuild/GroupReady/Prepare/Activate can authorize
serving, every source voter must validate the same command/source/logical
asset-set closure. Prepare itself leaves ready=false; restart is required.
The serving path supports existing public strict search and fresh colocated
Raft insert with exact retry and visibility proof. A colocated exact retry is
another genuine Raft commit: its response carries that attempt's actual term,
index and applied position, rather than promising the original commit position.
Deterministic apply retains the original covered result and returns the prior
logical outcome without repeating the canonical mutation, command-WAL append
or live graph update. The retry still proves the requested generation and document against the actual
current live revision. With no intervening mutation, the real witness checks
unchanged canonical/root/LSN/live state on every voter and retention of the
original covered result before snapshot-plus-tail recovery. This does not change the
separate split-source visibility-token receipt contract. Existing fast/pinned
search limitations remain.

Clean reopen and real provider snapshot followed by a genuine insert tail use
the existing paired-root/FSM recovery paths. Raft startup restores the snapshot
before asynchronous committed-tail apply. The witness waits for every actual
voter FSM to reach the retry prefix before exact local document and original
covered-result checks; a known leader or routed search alone does not prove
that each follower has reached that prefix. A retained serving handle refuses
after a later native snapshot replaces its DB; a new clean process open must
authenticate the current FSM DB. No applied position, coverage token, asset
ref or root binding is manufactured.

Recovery at the root-visible/result-missing cut is deliberately fail closed.
Local WAL replay can finish a partially staged prepare, but a real runtime
cannot invent the corresponding durable FSM result or progress. The focused
tests exercise an executor fault after visible root before result and real
peer reopen after loss of genuine result/progress tail bytes. This is refusal
evidence, not arbitrary peer crash recovery or authenticated reconstruction of
missing progress. Broader #4250/#4811 recovery remains open.

Focused validation proposed for the coordinator:

- Schema6 regression plus schema8 origin roundtrip, corruption, wrong-command,
  wrong-source/group/generation and zero-position refusal.
- Local WAL replay at fresh-output/before-BUILD, durable BUILD, and READY cuts:
  exact assigned coverage, origin/retained pack reuse, and validation-only live
  binding reopen. Orphans remain unreachable and are never overwritten.
- Three and four actual peer create/ingest, separately committed source rebuild,
  all-voter prepare, exact prepare retry/conflict, restart/ACTIVE, public strict
  search, fresh insert/exact retry, real snapshot plus insert tail and reopen.
- Covered FSM result lookup is read only, and missing result stays absent after
  a visible-root executor fault; real runtime refuses missing metadata tail.
- Deterministic actual-Append rebuild versus CompactStorage maintenance,
  actual queued Close versus capture borrowing, wrong DB/intent, borrower
  Release, Finalize/Abort expiry, and actual callback token expiry/remint.
- Local DB-open replay of a pending source rebuild as well as prepare cuts.
- Existing prepared-owner, WAL coverage, column_graph rebuild, native live
  binding, lifecycle, and fixed-peer initialization tests.
- Bounded test elapsed time, maximum RSS and retained fixture bytes, plus
  focused normal/race runs. No 100k/capacity or deployment qualification.

All source is provisional until the stable initialization and WAL/CI
predecessors merge and the candidate is resynchronized. Source/review reasoning
may be reused; local execution, race/resource measurements, final-base CI,
mergeability and review acceptance must be refreshed on that exact final base.

## Prepare FSM resource order

Prepare preflight and committed apply first mint a genuine stable snapshot of
the current FSM DB under a short FSM read lock, then release that read lock.
They wait for the root storage barrier before acquiring the execution FSM
mutex. Under that mutex they recheck the exact DB pointer, canonical root and
open state. Snapshot replacement retires the stale capture and retries before
local WAL Append; it never retries or releases an assigned raw staging guard.
A private callback-scoped storage owner lends the same stable snapshot to both
collection preflight and owner execution without nested barrier acquisition.
It remains live through Finalize or Abort, expires when the outer callback
returns, and closes the snapshot before returning to the caller. No API accepts
an arbitrary snapshot or a boolean claim of storage ownership.

Only prepare uses this outer resource path. Ordinary commands inspect a bounded
magic/version/command-ID prefix and keep their existing FSM path; this selector
confers no validation authority. The existing full deterministic decoder,
bytes/digest/target/catalog checks and committed coverage checks remain intact.
Capture and storage-owner allocations are cold prepare-only costs, independent
of the source row count. Stable capture admission may wait on maintenance, so it
must precede the storage barrier as well as schema/native/raw acquisition.


## Dataset preparation admission (#4956)

The fixed-peer first-generation, single-data-group preparation input admits at
most 16,384 authoritative rows, 32 MiB of actual FP32 vector bytes plus actual
document-ID bytes, and 1,024 bytes per document ID. These limits apply to
preparation, not ordinary insert admission or serving capacity. Primary rows
and actual IDs are scanned without owned source-row allocation before rebuild;
the physical source reader independently checks the same envelope before owned
vector/ID backing buffers. Central prepared-manifest origin validation uses the
same 16,384-row bound during encode, BUILD/READY publication, replay and reopen;
its term/index/digest, single-partition, inline-V1 constraints remain intact.
The manifest carries no actual FP32/ID input bytes, so those admission checks
remain at the authoritative primary scan and physical source reader.
Dimensions remain within the existing 4,096 bound.
WAL and persistent value-log durability/reachability rules are unchanged.

`treedb-fixed-peer -mode initialize|qualify -dataset DIR` consumes the existing
`system-export-dataset` manifest and `documents.f32` identities. The initial
packet targets 10,000 unchanged 128D corpus rows plus three counted oracle
anchors (10,003 prepared rows). Each input row must be finite, nonzero,
normalized within squared-norm tolerance 0.001, and have first-two-coordinate
norm fraction at most 0.9. This eligibility keeps deterministic plane anchors
and fresh self-query probes separated from the corpus. An unsuitable corpus
is refused before mutation; it is never projected, padded, or substituted.

The loader bounds declared dimensions/count/file sizes and actual FP32+ID bytes,
freezes one private admitted raw FP32 snapshot, then validates its full SHA256
and every row before catalog publication. External file changes cannot alter
that accepted snapshot. Only one bounded JSON chunk is decoded/encoded at a
time: at most 128 rows and 1 MiB of document/ID bytes, with a conservative
per-row serialization bound reducing the row count for large dimensions.
All seeds and corpus chunks use existing authenticated routed Raft commands,
unique stable chunk identities, real committed/applied receipts, and all-voter
applied-prefix waits. After each preceding prefix wait, the client observes the
current authenticated group leader/status before freezing the next unissued
chunk's expected catalog version; a failed observation stops without a mutation
retry. There is no automatic mutation retry. Reports retain
planned/unissued chunks and each actual result; a failed attempted chunk remains
UNKNOWN and stops later chunks and Prepare. An observed committed/apply receipt
followed by a failed all-voter wait remains distinct from proving all voters.
Successful Prepare still requires clean restart and matching durable completion
on every voter before native HNSW qualification. Dataset-only internal request
prefixes are `USER/dataset-MANIFEST_SHA256`, derived after the unchanged public
64-byte ASCII user-ID rule. The frozen manifest includes the verified vector
hash. Initialization and qualification derive the identical prefix, and every
voter's durable Prepare request identity must match `PREFIX/prepare` before
public search or fresh insertion. Same-count eligible corpus replacement, or
any changed manifest bytes, therefore refuses instead of assigning replacement
provenance to old preparation. The expanded prefix and suffixed keys fit the
existing 512-byte Prepare request and 1,024-byte idempotency limits. Empty
`-dataset` preserves the
original three-row/2D fixture and sequential qualification behavior.

The query-under-write driver optionally accepts `-fresh-inserts 65`, using the
configured dimensions and search effort, independently connected readers and
writers, the planned identical retry, raw outcomes, and native expected-ID
probes. The default remains 132 operations / 32 new IDs. The optional packet has
198 operations: 132 searches and 66 mutation attempts / 65 new IDs. These probe
an ordinary public insert population beyond 64; they do not alter mutation
identity capacity or establish unlimited growth. The rejecting 64-identity
ledger belongs to split inserts; catalog completion retention evicts old entries.
Existing guide statements about 64 ordinary inserts describe older qualification
limits and are not evidence of an ordinary-path hard refusal. Live overlay
128-Ki-ID / 256-MiB limits remain separate.

Allocation evidence must include loader freeze and JSON chunk B/op/allocs/op,
actual setup/preparation phase totals per corpus row, server/client peaks,
durations, and native/exact path counters, comparing unchanged fixture behavior
on matched base/head. The loader retains one raw-vector representation and only
one transient JSON chunk; preparation owns one contiguous vector buffer and one
ID buffer plus row/slice headers. Native pack, router, authoritative verification,
HNSW adjacency and persistent metadata can retain additional representations;
32 MiB is an input limit, not a heap or RSS bound. Measured costs remain a gate. The identical old-API benchmark file
`vector_partition_source_reader_admission_bench_test.go` runs
`BenchmarkVectorPartitionSourceReaderAdmission512x128V1` on matched baseline and
candidate to measure the shared reader's two ID passes and contiguous buffers.
Candidate-only `BenchmarkFixedPeerVectorDatasetFreezeV1` (512/10,000 rows) and
`BenchmarkFixedPeerVectorDatasetChunkV1` measure the new loader/chunk seam;
setup is excluded and allocations are reported. These microbenchmarks do not
replace actual four-voter preparation and container peak measurements.

This checkpoint neither closes #4250/#4805 nor qualifies sustained throughput,
tail latency, broad ANN recall, multiowner mutable serving, movement, replace,
delete, batch serving or generic writes. Those retain #4250/#4809/#4810/#4812
ownership. RF4 quorum three across a 2+2 host layout cannot tolerate either
whole-host loss. Source/harness review and landing precede sustained collection.


## Ordinary search/write admission followup (#4958)

Loading/prepare/reopen/qualification of the representative 10,000-row/128D
source plus three anchors is a corpus checkpoint. It does not establish the
optional 198-operation query-under-write probe. Retained trial08 stopped after
10 attempts: eight successes, one 10s strict-search deadline failure, one UNKNOWN
mutation canceled by that failure, and 188 unissued operations. Four ordinary
writes acknowledged actual committed/applied indexes89..92 and took3.8..3.91s.
No retry of that ambiguous trial is implicit in this followup.

The driver keeps default120s total/10s RPC budgets, while admitting explicit
1s..600s total and1ms..60s RPC budgets within that total. At the observed write
cost,65 serial writes alone exceed250s. Larger budgets do not establish fairness
or hide the retained unpaced failure. Reader-intent admission protects a waiting
reader's gap before a later ordinary writer queues; authoritative consensus,
current-DB, generation/revision pins and live-document visibility stay intact.
Root normal/race checks passed. Fresh trial10 then completed all 198 planned
operations: 65 distinct ordinary inserts, one identical retry and 132 native
searches. Actual client-call overlap was verified, and all four voters applied
through commit 154. The reviewed sealed archive is
`91333f496872113c8b1942c173a43218810eaf0322921677e6cd53086814aa17`;
all four servers stopped cleanly without OOM and stores remain preserved.
Historical64 ordinary-write qualification is not a hard ordinary ledger cap.
This bounded >64 completion does not establish sustained throughput, recall,
general mutation coverage or host-loss tolerance; #4959/#4250 remain open.
