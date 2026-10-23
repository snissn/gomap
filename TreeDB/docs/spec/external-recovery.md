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
with external initialization. The accepted `<root>` and `<root>/maindb`
spellings share the enclosing root lock, including flat and side-store-disabled
opens. Both marker locations are checked before layout validation and again
under initialization ownership; an external owner cannot nest in an enclosing
ordinary flat database. They check `external-recovery.json` before and
again under initialization ownership. An ordinary/external fresh-open race has
at most one successful policy. Failure releases the acquired ownership. Read-only
ordinary opens do not create lock files. External owners retain initialization
ownership for their lifetime, and mark their internal physical namespace too.
Pure COW/profile validation precedes writable initialization. A fresh ordinary
open persists the lock's parent before building child stores; the corresponding
fresh-layout power-loss witnesses have versioned owned-init-v2 cut identities.
Low-level physical backend helpers are engine integration surfaces, not a way
to bypass the public recovery policy.

External recovery requires exact stable relative namespace primitives (currently
Unix platforms). Unsupported platforms, including Windows, refuse before creating
the owner directory; ordinary TreeDB profiles remain available there.

## V2 certificate and retained root

The owner directory contains a V2 identity marker, `CURRENT`, `checkpoints/`, and
its physical materialization. Each certificate names `data_directory`: initially
`working`, or a private `materialized-*` namespace installed by authorized transfer. `checkpoints/<20-digit-generation>/certificate.json`
contains V2 identity, generation, exact persistent StateToken, application
metadata, physical data namespace, previous generation/digest, a bounded root-file
inventory, and a digest
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
restore can serve reads or authorize reuse. When both certificates name the same physical namespace, normal backend startup
validates and pins BOTH restored root/dependency closures before external Open
captures their retained union and returns mutation authority. When an install
changed namespaces, the previous namespace remains closed and untouched until
its certificate retires; its root operand is restored independently. No current
GC/rewrite owner can mutate that closed namespace. Newer incidental working seals or
clean Close never advance certified application progress. The owner compares the
selected persistent StateToken with CURRENT and fails closed on fallback/mismatch;
it does not silently lower the upstream retention frontier. Without CURRENT,
uncertified genesis is discarded only when no evidence of an earlier published
generation exists: staging directories and a valid lone first generation with
no predecessor may be an interrupted initial publication. Later generations,
unknown checkpoint entries and invalid initial certificates MUST fail before
working/checkpoint/namespace cleanup. A corrupt/missing selected certificate is
an error, not permission to recreate genesis.

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
retained exact roots. A retained root descriptor is
not a portable backup and MUST NOT be copied into another inode/namespace.

## Authorized portable snapshot transfer

`ExportCheckpoint(ctx, writer, generation, digest)` MUST match the currently
certified generation and digest exactly; it never substitutes newer application
progress. It retains the existing physical cut and captures stable child handles
and lengths before copying. Apply may continue while export runs, including
when Checkpoint, Install or Close is queued behind the export. Transfer lifetime
exclusion MUST be acquired before Apply exclusion by those writers, so a queued
transfer waiter cannot stall new Apply calls through RWMutex writer preference.
The maintained PhysicalSnapshotCutV1 writer exports the
captured DATA meta/page image. Persistent value/outer-leaf files are copied from
captured fixed prefixes; unreferenced suffix files may be included. Explicit
transfer is O(image) copying/hashing; periodic certificates remain bounded.

The portable format is a regular-file tar stream: `source.json` (complete source
V2 certificate), `db/index.db`, `db/format.json`, and supported relative
`db/value_vlog/*` / `db/leaf_vlog/*` files. Final `inventory.json` lists prior
entries in order, with sizes and SHA256 hashes. Source/inventory JSON is capped
at 4 MiB, source metadata at 1 MiB, and physical files/bytes by receiver limits.
Traversal, duplicate paths, unsupported namespaces, symlinks, special files,
excess depth/directory counts and missing required files are refused. Discovery
and copying use bounded metadata plus a 64 KiB copy buffer, not an image-sized
heap buffer. The inventory detects corruption; the application MUST authenticate
snapshot transport and validate the source semantic authority.

`InstallCheckpoint(ctx, reader, authorize)` requires an OPEN owner and a mandatory
authorization callback. The caller MUST exclude all application admissions and
drain outstanding DB/MVCC snapshots, iterators, readers, direct handles and queued
writes before calling it. Owner Apply exclusion alone does not drain direct
readers. Success changes `DB()`'s pointer; the caller MUST rebuild MVCC/schema/
cache adapters before admission and health resume. The callback MUST NOT call
back into the owner.

Install extracts a complete private image, verifies every stream hash/path/limit,
and syncs its files and containing namespaces before invoking authorize with the
source certificate. The callback MUST verify exact accepted snapshot group,
index, term and captured membership against source metadata, then explicitly
return receiver progress metadata. It runs BEFORE any CURRENT publication. For
a receiver with a separate durable Raft log, the callback MUST first persist and
sync an accepted-install intent referring to the complete synced archive and
its digest, exact snapshot identity and receiver metadata. The application owns
that cross-store protocol; Gomap does not certify it on the caller's behalf.

After authorization, install gives the image a final private namespace, uses the
maintained durable-root snapshot layout rebind, opens/validates its exact source
StateToken, captures its retained root, and publishes a receiver-owned
certificate through the same CURRENT protocol as Checkpoint. Source owner
identity is never silently adopted. Preparation failures preserve old authority;
ambiguous publication poisons the owner until Close/reopen. Old physical storage
remains available through the previous certificate and is removed only after its
retained cuts and certificate retire. Unpublished install directories are
orphans and can be removed on reopen.

After a crash, the external application MUST resolve an accepted install intent
before normal replay/validation/health: reopen the owner, compare recovered
certificate Root and receiver Metadata with the intent, idempotently finish
install when needed, then complete and sync the accepted Raft snapshot. The
archive/intent must survive until that log step is durable. Log hard state MUST
NOT advance commit past the installed snapshot before the corresponding suffix
entries have been saved. Matching recovered Root plus receiver Metadata permits
skipping a duplicate install generation. This is an application lifecycle
requirement, not automatic multi-store atomicity.

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

`TestExternalRecoveryTransfer*` exercises distinct source/receiver ownership,
exact requested cuts despite persisted suffixes, successor Apply during export,
refusal/traversal/truncation before authorization, pointer replacement and local
checkpoint retirement, poison/reopen after publication errors, and subprocess
crash cuts before and after CURRENT. These are focused API proofs; peer Raft
acceptance, reader drain, durable accepted intent and real server recovery require
consumer-level qualification.

## Component cost diagnostic

`BenchmarkExternalRecoveryCheckpoint` uses the ordinary public no-WAL
`cow_ordered` owner for inline64 and forced-pointer4096 layouts at 1,024,
16,384 and 131,072 records. Run fixed counts, for example:

```sh
go test ./TreeDB -run '^$' -bench '^BenchmarkExternalRecoveryCheckpoint$' -benchmem -benchtime=8x
```

One timed operation updates an existing key and completes its certified
checkpoint, including TreeDB root publication, retained dependency capture,
certificate serialization/sync and generation retirement. Fixture loading,
initial certification, final Close and recovery verification are outside the
timer. Each cell verifies resolved COW/no-WAL routing and exact certificate/value
recovery. `checkpoint_files_B/op` sums files enumerated by the returned
certificate; for retained roots these are descriptor bytes, not all physical
write I/O. Pointer values repeat and use default compression, so cardinality
scaling is not a unique-value-byte scaling claim. The benchmark reports Go
B/op and allocs/op; it does not qualify sustained memory/disk equilibrium,
upstream log debt, crash safety, or Dgraph RPC/transaction tails.
