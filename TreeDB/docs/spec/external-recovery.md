# External recovery ownership and retained DATA checkpoints

`OpenExternalRecovery` is an explicit recovery policy for an externally logged
raw DATA materialization. It requires `no_wal_fast`, `cow_ordered`, a writable
owner, a stable owner identity, and supported durable relative namespaces. The
external command/decision log is authoritative. Ordinary `WriteSync`, profile
resolution, and ordinary recovery semantics do not change.

## Ownership and application contract

All mutations MUST execute inside `ExternalRecovery.Apply`; asynchronous work
MUST finish before its callback returns. Checkpoint and Close exclude Apply.
The application MUST hold its semantic apply barrier when sampling progress
metadata and calling Checkpoint. A materialization return is not a local durable
acknowledgement. The external owner MUST durably retain commands and decisions
until a successful matching certificate permits their retirement.

Ordinary public Open, OpenBackend, and cached-backend Open share `INIT.LOCK`
with external initialization. They check `external-recovery.json` before and
again under initialization ownership. An ordinary/external fresh-open race has
at most one successful policy. Failure releases the acquired ownership. Read-only
ordinary opens do not create lock files. External owners retain initialization
ownership for their lifetime, and mark their internal physical namespace too.
Low-level physical backend helpers are engine integration surfaces, not a way
to bypass the public recovery policy.

## V2 certificate and retained root

The owner directory contains a V2 identity marker, `CURRENT`, `checkpoints/`, and
its physical materialization. `checkpoints/<20-digit-generation>/certificate.json`
contains V2 identity, generation, exact persistent StateToken, application
metadata, previous generation/digest, a bounded root-file inventory, and a digest
of the canonical JSON certificate with its Digest field cleared. Metadata is
limited to 1 MiB; encoded certificates to 4 MiB. `db/root.json` binds a V1
RetainedPhysicalRoot operand: existing newest DATA durable meta, index inode and
parent namespace identities, index high-water, exact persistent state, and
conservative resource extents. Root descriptors are limited to 64 KiB on read.
The process-local leaf-generation view version is excluded from persistent
identity. Files describes the descriptor; it is not a copied database image.

Periodic Checkpoint MUST first complete the maintained public checkpoint and
capture PhysicalSnapshotCutV1. That cut retains RecoverableRootSet: its existing
snapshot registry floor prevents DATA/freelist page reuse, its stable-index lease
prevents namespace replacement, and its original stable-resource pins preserve
persistent value/outer-leaf dependencies through rewrite and GC. Only the newest
meta operand is certified. No new allocator, root constructor, or publisher is
introduced. No database bytes are copied or rehashed by certificate publication.
The maintained checkpoint/root-publication work still applies, including any
existing dependency-discovery fallback; descriptor size is not a bound on all
checkpoint CPU or dependency metadata.

The descriptor, certificate, generation rename, and containing namespaces MUST
be stable before CURRENT publication. The prior cut MUST remain retained until
CURRENT is stable. A failed/ambiguous CURRENT publication poisons the owner;
subsequent Apply and Checkpoint fail until Close/reopen. Candidate pins survive
that ambiguity. Successful publication retains current and previous certificate
cuts, then retires older certificate namespaces. A reopened cut can conservatively
retain the previous restored slot union for one further checkpoint interval.
This over-retention is bounded and MUST NOT be treated as immediate reclamation.

## Recovery and restore interruption

The owner MUST validate CURRENT, its certificate and root descriptor, and the
previous certificate before mutating selectable metadata. It MUST reject changed
index inode or parent namespace identity. It then installs current and previous
certified roots as the two ordinary DATA meta slots using the maintained
checksummed durable-meta writer, and syncs the index. Restore does not rewrite
root/dependency pages, copy an image, or rebind live files.

The immutable certificate operands remain stable throughout restore. If power
loss tears either meta image or prevents its sync, the next external Open writes
the same complete images again before opening any backend owner. Thus no partial
restore can serve reads or authorize reuse. Normal backend startup validates and
pins BOTH restored root/dependency closures before external Open captures their
retained union and returns mutation authority. Newer incidental working seals or
clean Close never advance certified application progress. The owner compares the
selected persistent StateToken with CURRENT and fails closed on fallback/mismatch;
it does not silently lower the upstream retention frontier. Without CURRENT,
uncertified genesis is discarded. A corrupt/missing certificate is an error,
not permission to recreate genesis.

## Resources and supported capabilities

MaxCheckpointBytes/Files bound each retained physical closure conservatively,
including index high-water and maintained physical dependency frontiers; defaults
are 8 GiB and 8192 resources. They also bound descriptor output. Two certificate
namespaces are retained. These limits are not a bound on upstream Raft debt,
suffix growth, total RSS, elapsed checkpoint time, or every transient allocation.
The external owner must separately bound materialization/replay debt and choose
checkpoint frequency. The maintained snapshots and existing retained-allocation
owners own closure lifetime; this API adds no per-key allocation domain or pool.
Root JSON and certificate buffers are per-checkpoint immutable outputs; physical
resource descriptor views are temporary per-capture metadata.

Supported roots are raw DATA with inline/persistent values and ordinary
outer-leaf logs/manifests/packs. PRIMARY and independently owned dictionary,
template, typed/native/vector/column assets and side stores are refused. A stable
index lease may delay online index vacuum; rewriting/GC still respects the
retained exact roots. A network transfer needs a separate authenticated export /
install contract with physical identity rebinding. A retained root descriptor is
not a portable backup and MUST NOT be copied into another inode/namespace.

## Verification

`TestExternalRecoveryRetainsRootWithoutCopyingImage` requires bounded descriptor
output, exact forced-pointer recovery through newer seals and GC, and the same
working index inode. `TestExternalRecoveryTwoGenerationClosureAcrossRewriteAndReopen`
checks current and previous closure reacquisition, value rewrite, GC, repeated
seals and direct previous-root selectability for both USER leaf placements.
`TestRetainedPhysicalRootsRestoreTornSlotsAndRetry` models torn meta persistence
and interrupted restore retries; it is a byte-state recovery control, not a
physical power-cut test. Identity replacement and concurrent initialization /
failure-release controls exercise the public ownership boundary. Subprocess
crash cuts remain process-crash controls, and do not certify hardware power loss.
