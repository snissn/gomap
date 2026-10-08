# Cached native leaf producer handoff (#5098)

The cached append-only native producer can own several physical leaf writers.
Their file IDs encode lane 255, but the workers have distinct current sequences.
Leaf pack previously copied live records without sealing these current writers;
GC correctly protected those files, including their unreachable historical bytes.

Public `LeafGenerationPack`, `LeafGenerationPackFromPlan`,
`LeafGenerationPackRunOnce`, and non-dry-run `LeafGenerationGC` now hand off the
installed cached native producer before invoking the existing planner, pack or
GC. The public cached backend opener installs this provider; no special opener
or rollover threshold is required. Caller-owned logs and the existing COW owner
keep their established handoff policies.

Existing footprint admission runs before handoff, outside its foreground gates,
and captured-input admission still repeats in each ordinary maintenance phase.
Explicit pack generation requests are validated before producer effects and
revalidated after handoff. Automatic/from-plan selection uses the final sealed
frontier; a dirty frontier may become eligible only after this handoff.

## Short publication cut

The transient producer owner takes cached `flushMu`, then cached `writeMu`.
The backend subsequently holds teardown for reading, exclusive command-WAL
admission, raw publication, and finally backend `writeMu`. Backend barriers run
before backend `writeMu`: existing staged collection publishers can finish their
root builds and lane appends, while asynchronously prepared publishers lose the
nonblocking admission claim and relinquish work to the existing drain.

A `PendingDrain` drops raw publication, admission, teardown, AND both cached
locks before running the existing drain. The complete barrier sequence then
restarts. The installed leaf-log version is captured under backend `writeMu` and
revalidated under the final builder gate; replacement during an unlocked drain
cannot rotate or register the old installed owner. No public checkpoint,
`maintenanceMu`, directory scan, live-tree scan, copy, or GC runs under this cut.
Cached Close sets closing and waits for the matching flush/foreground owners;
a cancelled acquisition releases whichever cached lock it acquired.

If every current writer has zero appended bytes, handoff reserves no sequence
and opens no new file. Otherwise the producer reserves an unused sequence above
both the shared allocator and every physical current writer. It advances the
shared allocator BEFORE enumerating writers, then uses existing per-lane
rotation and segment registration to move ALL open writers beyond that floor.
A worker born during or after the cut obtains a fresh sequence beyond the floor.
Identity-pinned stable builder resources and held/root/recoverable readers keep
owning their exact old files; handoff does not release their authority.

Before releasing the cut, the backend consumes the complete created/current
inventory, demotes only old physical current identities, drains pending manifest
registration, and publishes the registered reader set. Empty databases skip
reader refresh. Any partial rotation, registration, or publication error returns
before pack/GC; successfully installed new writers and surviving old writers
retain their existing owners for retry. Handoff grants no deletion authority.

The existing GC remains responsible for reachability, recoverable roots, held
snapshots, exact identity pins and current-file protection. Only when it confirms
that deleted generation files are physically absent does it report their exact
paths/IDs as a transient receipt bound to the captured installed producer. GC
takes a fresh teardown read lease before its apply-phase `writeMu` (the scan
attempt has already released its lease), unlocks `writeMu` before dispatch, and
releases teardown after the receipt callback. A later sidecar or manifest
publication error does not revoke confirmed physical absence. Cached Close
serializes writer teardown with each lane mutex, and backend Close waits for
this lease before completing; the captured producer callback touches only
locked accounting, never its closed reader or writer. The producer uses existing
accounting helpers to forget closed paths and retained-byte entries; callbacks perform no
file deletion, reader refresh or root publication. Pinned zombie files retain
accounting until a later GC observes physical removal.

## Scalar diagnostics and acceptance boundary

`treedb.cache.leaf_log_lanes.lane.NN` identifies a physical worker index, rather
than encoded lane 255. It reports `current_sequence`, `current_file_id`,
`threshold_rotations_total`, and `maintenance_handoffs_total` alongside existing
rotation, idle-rotation and append counters. Creation remains part of the old
rotation total; threshold and maintenance counters distinguish their actual
call sites. Constructors, routing, leaf/hot limits, formats and ACK behavior are
unchanged.

The causal support inquiry showed that reopening without new typed updates
removed six formerly-current physical files. It establishes the writer-frontier
cause, not default rollover, fixed-cap, finite-residency or performance acceptance.
Those claims require the separately frozen actual public R1 fixture: unchanged
default limits, at least two natural threshold rollovers per exercised physical
worker, complete held/current historical posting oracles, equal drained epochs,
reopen, physical censuses and resident/capacity accounting. Maintenance handoffs
must never count as natural threshold rollovers.
