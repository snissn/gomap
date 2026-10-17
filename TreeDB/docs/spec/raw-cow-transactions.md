# Raw COW transactions

Public `TreeDB.Open` with `MemtableMode="cow_btree"` supports raw transactions
through the existing `NewConditionalTxn` / `InitConditionalTxn` APIs, with or
without the `WithSnapshot` spelling. Every such transaction retains its opening
COW cut. The durable command-WAL, relaxed command-WAL and production NoWAL
profiles retain their respective acknowledgement contracts.

## Ownership and eligibility

The public wrapper privately creates its backend and installs an internal owner
capability before exposing the cache. The backend compatibility transaction
entrypoint rejects under that binding; physical backend publications do not
create a second conflict history. A bare `caching.Open` supplied a caller-owned
backend cannot claim this capability and its COW conditional entrypoints remain
unsupported. A registered higher-level raw publication barrier also rejects this
initial raw-only eligibility, before transaction admission and again at final
append readiness. An empty catalog or a memtable mode name is not an ownership proof.

Ordinary Set/Delete, their sync variants, native revision/view/MVCC-input batch
operations, and transactional commits use the same prepared COW installer.
Direct backend/streaming bypass is unavailable in this mode. Arbitrary external
append callbacks on public NoWAL batches reject before execution; only the
hidden public command-WAL bridge can authorize its append callback. Native
range tombstones, Update callbacks and collection members
remain explicitly unsupported. Transactional DeleteRange uses bounded expansion.

## Reads, writes and conflicts

Get returns owned bytes; GetAppend appends into caller storage. Reads observe the
opening cut plus preceding staged writes, including tombstones and repeated-key
last-write precedence. Caller revisions remain entry metadata and do not define
the engine's logical clock. Missing point reads are dependencies too.

Iterator, ReverseIterator and their Snapshot() wrappers merge the opening cut with the current private
write set. The full requested half-open interval is recorded before results,
including an empty result range, an early close and a later Seek. Overlapping
requested intervals coalesce without narrowing their union. Forward and reverse
traversal use the same opening cut and staged effects; reverse Seek selects the
greatest visible key at or below the target. Nil bounds mean
unbounded; a concrete empty exclusive end is empty. Iterator bytes expire at
Next, Seek or Close. Mutation while any transaction iterator is open returns
`ErrTxnIteratorOpen`. Commit and Close invalidate all children. Handles remain
goroutine-confined. Transaction Snapshot callback iteration and HasPrefixes convenience
wrappers remain explicitly unsupported; prefix predicates use iterator bounds,
including an unbounded end for all-0xff prefixes.

DeleteRange first scans that merged private view, including staged inserts,
then closes its internal iterator before staging tombstones. Expansion admits
its temporary storage and is limited to 4,096 keys and 1 MiB of key bytes.
Oversized expansion publishes nothing; a staging failure that could leave a
partial private expansion terminally closes the transaction.

Writing commit validates the union of all read and write dependencies, then
applies effects to the latest compatible roots. Disjoint writes can both commit.
Overlapping blind writes, insert/delete or value ABA, write skew and intersecting
predicates conflict with `ErrConcurrentModification`. Read-only views serialize
at capture. CommitSync still performs the profile's explicit durability boundary
when the write set is empty.

## Bounded history and publication

One mutation FIFO owns immutable sorted point keys, native ranges and accepted
domain barriers. A fixed open-addressed index derives each key's latest sequence;
it borrows owned FIFO keys and removes an index entry only when its sequence is
the evicted sequence. Point-only validation avoids a FIFO scan when no native
range effects are relevant. Backend and legacy-cache compatibility wrappers use
this same history implementation rather than active-start maps or full-map
minimum pruning.

Every accepted logical producer advances the clock, even with no transactions
open. Equivalent rollover, checkpoint and maintenance handoff preserve the
logical sequence. History eviction advances its floor before removing the first
fragment of a commit. A writing transaction below the floor returns `ErrTxnTooOld`
without a command frame. Old read views do not pin conflict history.

COW history has at most 4,096 effect slots, reduced by powers of two down to eight
for small MaxInFlightBytes budgets. Its record payload allowance is one eighth
of MaxInFlightBytes; the index/control allocations and immutable key copies are
charged to the existing COW budget. Oversized commands retain an accepted barrier
instead of unbounded effects. Preparing a future barrier does not invalidate
transactions until acceptance. Standalone legacy compatibility histories retain
at most 4,096 effects and 4 MiB of record payload; they have no active-start
registry. Capacity limits describe ownership, not RSS or write-latency guarantees.

The existing COW writer lease spans final reserve/evict/validation, canonical
append and complete cut/journal installation. The backend append primitive
borrows the caller's actual staging/teardown guard and neither drains
nor releases it. Owner/barrier readiness preflight occurs before cache admission;
final no-drain guard acquisition occurs inside group append after cache admission
and the COW writer lease. The order is cache admission, writer lease, group gate,
then raw guard. Public index vacuum acquires its raw guard inside the existing
maintenance writer fence. A pre-admission writer holds no group/raw gate needed
by checkpoint. Postassignment errors record recovery-required poison before
that lease ends. Existing request grouping surrounds the entire cached install;
its group wait follows lease release, while solo durable writes preserve their
single-frame sync optimization. No per-item group fsync is added.

The request receipt distinguishes assigned identity/uncertainty, logical install,
WAL completion/stability and sealed logical coverage. Empty NoWAL explicit Sync records a logical coverage
lower bound captured before its real checkpoint, only on success; it creates no
logical publication or LSN. That lower bound is not an exact disk-generation
identity. NoWAL has no command LSN.
The journal and entrypoint reuse existing canonical raw frames and value-log
external-reference custody; replay applies accepted effects without rerunning
client validation. Checkpoint coordinator and exact disk-generation handoff work
remain separate from transaction conflict-history ownership.

COW stats under `treedb.cache.cow.txn` report logical_seq, journal_effects,
journal_bytes, history_floor, index_points, range_effects, too_old_total,
commit_wait_ns, commit_hold_ns and validation_ns. Timings are cumulative actual
lease/validation intervals; they are not independent operation percentiles.


## Allocation and measurement boundaries

The history control, fixed FIFO/index and private effect-staging backing are
admitted once. Serialized preparation reuses that bounded private staging;
accepted effects copy into the FIFO. A by-value capability with a nonreused
preparation nonce prevents stale copies or deferred cancel/install calls from
acting on a later reservation. The nonce grants ownership, not logical order.
Cancellation consumes the reservation under the writer lease before releasing
shared staging; independent cut/resource retirement then runs outside that lease.
Immutable keys use one command-owned backing allocation; the command's final
physical FIFO slot owns its original capacity and admission lease until every
preceding fragment evicts. Reserve-before-validation makes that final slot
unique among retained commands, so it also identifies binary-search chunks.
There is no per-command prepared/effect owner allocation or generic pool.
Each concrete fixed backing, callback environment and dynamic key allocation is
independently conservatively charged; a fixed five-size callback avoids variadic
scratch. Empty barriers require no dynamic key allocation. Copied guard handles
still share one fresh original ownership aggregate; no guard is pooled or reused.
Registration scratch belongs to the admitted public wrapper through ACK.
Retired owner-skip transaction IDs and registration/minimum scans remain absent;
active transaction stats use their existing atomic count.
An immutable public-owned legacy command-WAL cache rejects conditional opens and
maintains its publication counter without an unused cache journal. Public APIs
already reject transactions in that configuration. Bare caching clients retain
their previously supported API and continuous bounded history; this fence never
depends on the number of active transactions.
The existing native batch chunk storage owns transaction read keys and writes;
its lease remains live through acknowledgement and all iterator invalidation.
Child iterators own and release their transient wrapper, staged-copy and original
bound capacity at Close. Coalesced predicate ownership remains transaction-owned;
covered opens allocate no predicate, and replaced bounds/backing drop aliases
before refund. Failed initialization closes a partially admitted batch; temporary
transaction adapters borrow its custody even when wrapper admission fails.
Shared refusal carriers remain immutable during adapter setup and cleanup.
The latest-key index borrows journal-owned immutable strings rather than a second
key copy. Eviction/Close clears aliases before releasing their owners. Journal
cleanup occurs outside the view-install latch under writer exclusion.

`BenchmarkPublicCOWTxn`, captured by `scripts/raw_cow_txn_capture.py`, measures
enabled incremental cost with precomputed dependency keys. Point1/Point10 vary
dependency count; Scan records a full interval and closes after its first result.
History0/History2048 vary retained unrelated effects. Every timed iteration
interleaves one unrelated ordinary write with one writing transaction, so lease
wait/hold per iteration includes both publications. Validation is the transaction
validation interval. Final Close drain is checked and reported separately from
ACK timing. Live/peak values are charged engine bytes, not heap or RSS. These
interleaved diagnostics and deterministic concurrent progress tests do not claim
sustained mixed-load qualification. Existing raw/legacy controls provide the
before/after comparison; an unsupported pre-P1 transaction path is no comparator.


## Preserved maintenance limit

The baseline retained-view fixture (forced pointer value, checkpoint, then public
online vacuum) can refuse with `ErrCOWCapacity` and leave a required basis refresh.
The retained and current logical views and logical sequence remain unchanged.
Subsequent WAL Close can also report that outstanding refresh capacity error.
P1's successful physical-successor gate covers checkpoint and an old transaction's
later commit; its refusal gate does not qualify successful online vacuum.
Reserved maintenance/refresh headroom and completion of that existing failure
belong to the P2 maintenance packet.

Accepted conservative history barriers expose separate effect-capacity, byte-capacity
and logical-replacement totals, current barrier activity, and barrier-caused
validation conflicts. Prepare/cancel does not count a publication; expired history
is reported separately as TooOld. Range validation binary-searches the newer
FIFO suffix and sorted point spans within each retained command, then intersects
its native range effects. The existing final FIFO slot marks spans without another registry or
per-validation materialization.
