package db

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

var ErrNativeSemanticUnsupported = errors.New("treedb: command kind has no native semantic preparation")

// NativeSemanticBuildRetryableRefusal proves that PrepareBuild refused before
// mutating its original build ledger, allocating assets, or beginning fallible
// materialization. A capacity error after those effects is terminal instead.
// This classification carries no resource or failure custody of its own.
type NativeSemanticBuildRetryableRefusal struct{ Cause error }

func (e NativeSemanticBuildRetryableRefusal) Error() string { return e.Cause.Error() }
func (e NativeSemanticBuildRetryableRefusal) Unwrap() error { return e.Cause }

// NativeSemanticHandoffRetryableRefusal proves the FIRST PrepareHandoff call
// refused before changing its original handoff ledger or allocating anything.
// A second-call refusal is terminal because the first successor already exists.
// This value classifies the original error; it owns no resource or failure debt.
type NativeSemanticHandoffRetryableRefusal struct{ Cause error }

func (e NativeSemanticHandoffRetryableRefusal) Error() string { return e.Cause.Error() }
func (e NativeSemanticHandoffRetryableRefusal) Unwrap() error { return e.Cause }

// NativeSemanticState is a native immutable semantic/catalog root set paired
// with the SAME raw COW cut and exact backend Snapshot. Implementations belong
// to higher-level packages; db/caching never import their implementation.
// Retain and Release are nonallocating constant work under the existing cut
// latch. Release returns the preborn original record only at the final edge.
type NativeSemanticState interface {
	Retain()
	Release() *rootpublication.SemanticCleanup
	SparseProjections() []rootpublication.ParticipantProjection
	// PrepareRollover prepares an equivalent physical active successor before
	// any raw Freeze or cut installation. Any nonnil result transfers one owned
	// edge, including on error: the caller only releases a failed private result
	// outside publication/writer/cut gates. Its preborn Work already owns every
	// partial allocation. Success must return nonnil (Retain this state if
	// unchanged); nil/error proves preallocation refusal. Frozen capture keeps
	// its distinct original retained edge.
	PrepareRollover() (NativeSemanticState, error)
	// PrepareBuild uses this captured complete logical basis and original
	// teardown lease. It never recaptures current metadata or acquires P.
	// Only NativeSemanticBuildRetryableRefusal permits another build attempt;
	// every other failure preserves the original ledger and poisons continuation.
	PrepareBuild(*RootPublicationBuildGroup, *Snapshot, *StableResourceCaptureLease) error
	// PrepareHandoff prepares a successor against latest logical state using
	// only captured-T touched domains. Distinct suffix-domain headers/sequence
	// survive, and the exact accepted snapshot remains the physical basis.
	// All fallible allocation precedes the logical installation latch. Default
	// failure is terminal and parks the SAME original partial handoff ledger;
	// only a first-call NativeSemanticHandoffRetryableRefusal permits retry.
	// Error returns no state edge; partial resources already belong to Work.
	PrepareHandoff(latest NativeSemanticState, accepted *Snapshot) (NativeSemanticState, error)
}

// NativeSemanticBasisRefresher optionally rebinds an immutable semantic root set
// after logically equivalent backend maintenance. before and after are the exact
// retained old/new physical Snapshots, never newly captured by the participant.
// This is NOT an accepted flush handoff: no logical source, sequence or canonical
// coverage may be consumed. Physical catalog roots may have moved during rewrite.
// Every nonnil result transfers one owned PRIVATE edge, including on error; its
// original Work owns partial construction. The caller adopts it before inspecting
// err and releases failed private edges through its original off-gate cleanup.
// Owners without this capability fail closed when a physical basis refresh is
// requested. The build-only PrepareHandoff contract above remains unchanged.
type NativeSemanticBasisRefresher interface {
	PrepareBasisRefresh(before, after *Snapshot) (NativeSemanticState, error)
}

// NativePreparedPublication is admitted and owned before canonical assignment.
// The native producer holds Basis through the synchronous publication and
// transfers State plus sparse original Custody only on installation. Entries
// use the ordinary canonical raw preparation path; semantic-only changes may
// supply an empty slice. No whole EngineVersion is retained by queue custody.
// NativeSemanticCustody owns only the exact sparse native plans/assets. Last
// release detaches one preborn original cleanup record; physical work and errors
// belong exclusively to that record on the existing cleanup worker.
type NativeSemanticCustody interface {
	ReleaseNativeCustody() *rootpublication.SemanticCleanup
}

type NativePreparedPublication struct {
	// Selected composite raw members require the final inline Set representation.
	RequireInlineCompositeRaw   bool
	Basis                       NativeSemanticState
	State                       NativeSemanticState
	Entries                     []batch.Entry
	Projections                 []rootpublication.ParticipantProjection
	Custody                     NativeSemanticCustody
	AdmittedBytes, CleanupBytes uint64
	Effects                     []txnhistory.Effect
	Dependencies                []txnhistory.Dependency
	StartSequence               uint64
	Finalizer                   NativePublicationFinalizer
	Resources                   *NativePublicationResources
	Reprepare                   NativeLogicalPlan
	// ObserveAssigned is the request-local deterministic apply fault seam. It runs
	// once after successful original append/receipt recording, before visibility.
	// It must not block, reenter the DB, allocate publication work or acquire
	// application locks. It grants no assignment or installation authority.
	// The publisher detaches it before calling, including on assigned failure.
	ObserveAssigned func(uint64) error
}

// NativeLogicalPlan is an original transaction-owned immutable operation plan.
// After validating every original read/write dependency under P, the SAME P1
// installer prepares it against latest compatible roots before journal prepare
// and canonical assignment. This is an internal planner, never an application
// callback: no locks, waits, recapture, or publication. Every nonnil State is ONE
// owned private edge, including on error; lower queues its cleanup off gates.
// Entries, Effects, Dependencies and original sequence remain unchanged.
type NativeLogicalPlan interface {
	PrepareNativePublication(latest NativeSemanticState, exact *Snapshot) (NativeFinalizedPublication, error)
}

// NativeConditionalTxn borrows the opening cut of the existing P1 transaction.
// CommitNative and Close end that original transaction and its opening lifetime.
type NativeConditionalTxn interface {
	Get([]byte) ([]byte, error)
	Set([]byte, []byte) error
	NativeRawEntries() []batch.Entry
	NativeSemanticCapture
	CommitNative(*NativePreparedPublication, *CommandWALIntent, bool) (NativePublicationResult, error)
}

// NativePublicationFinalizer is the original internal, admitted native plan.
// Identity-free eligibility, footprint, dependencies and limits are complete
// before append. This phase privately binds actual assigned identities/assets;
// it cannot publish, acquire collection locks, or alter Entries/Effects.
// On error original Custody must retain every partial resource independently.
// NativeAssignedFailureCustody is implemented by the SAME original sparse
// custody. It records failure against that already admitted sparse ledger
// before lower private aliases end. result.State is borrowed during this call;
// lower releases its owned edge afterward. Keep no whole EngineVersion here.
// This is infallible O(1), with no destructor,
// allocation or collection mutex; every earlier partial asset is already owned.
type NativeAssignedFailureCustody interface {
	RetainAssignedNativeFailure(NativeFinalizedPublication, error)
}

type NativePublicationFinalizer interface {
	FinalizeNativePublication(CommandWALPublishContext) (NativeFinalizedPublication, error)
}

type NativeFinalizedPublication struct {
	State       NativeSemanticState
	Projections []rootpublication.ParticipantProjection
}

// NativeAllocationReceipt is an independently admitted accounting authority.
// It has no native state/resource backpointer and is completed after final use.
type NativeAllocationReceipt interface{ Close() }

// NativeSemanticAllocator is the SAME original budget authority. Retaining it
// keeps no capture, state, Snapshot, cache or ancestor EngineVersion alive.
type NativeSemanticAllocator interface {
	Limits() memtable.COWLimits
	AdmitNativeBytes(uint64) (*memtable.COWExternalLease, error)
	// AcquireRetention enrolls existing storage-owned metadata in this SAME
	// budget. The enrollment carries no engine, capture or cache backpointer.
	AcquireRetention(uint64) (retainedalloc.Lease, error)
	NewNativeCOWWriter(sourceOwnerBytes uint64) (*memtable.COWWriter, error)
}

// NativeSemanticCapture borrows a single complete immutable cut until Close.
type NativeSemanticCapture interface {
	State() NativeSemanticState
	Snapshot() *Snapshot
	LogicalSequence() uint64
	AdmitNativeBytes(uint64) (NativeAllocationReceipt, error)
	NativeAllocator() NativeSemanticAllocator
	// NativeMaintenanceAllocator is the SAME reserved physical-progress budget,
	// used for build/successor metadata, never native producer source occupancy.
	NativeMaintenanceAllocator() NativeSemanticAllocator
	Close() error
}

// NativePublicationResult distinguishes transfer, canonical assignment, logical
// installation, WAL completion and exact physical sealing. Consumed transfers
// the original descriptor custody even when a later assigned phase fails.
type NativePublicationResult struct {
	Consumed, Assigned, Uncertain, LogicalInstalled, WALCompleted, WALStable bool
	AssignedLSN, LogicalSequence                                             uint64
	Seal                                                                     rootpublication.PhysicalSealReceipt
}

// NativeColumnStoreCompaction carries only an operation's scalar completion.
// It grants its caller no access to captured state or physical build authority.
type NativeColumnStoreCompaction interface {
	CompleteColumnStoreCompaction(systemRoot uint64)
}

// NativeColumnStoreCompactionState is an optional operation of the fixed
// ORIGINAL captured engine state. Like PrepareBuild it uses only that state's
// own ledger and the original group's synchronous borrowed capabilities.
type NativeColumnStoreCompactionState interface {
	PrepareColumnStoreCompaction(NativeColumnStoreCompaction, *RootPublicationBuildGroup, *Snapshot, *StableResourceCaptureLease) error
}

// NativeSemanticBridge is fixed once at Open to the backend's SAME private COW
// owner. It introduces neither a participant registry nor a publication lane.
type NativeSemanticBridge interface {
	CompactNativeColumnStore(context.Context, NativeColumnStoreCompaction) error
	Checkpoint() error
	PrepareNativeProducer() error
	CaptureNativeCut() (NativeSemanticCapture, error)
	BeginNativeConditionalTxn() (NativeConditionalTxn, error)
	BeginPreparedNativeConditionalTxn() (NativeConditionalTxn, error)
	WriteNativePublication(*NativePreparedPublication, *CommandWALIntent, bool) (NativePublicationResult, error)
}

func (db *DB) BindNativeSemanticBridge(owner *transactionowner.Owner, bridge NativeSemanticBridge) error {
	if db == nil || owner == nil || db.privateCOWOwner != owner || bridge == nil || db.nativeSemanticBridge != nil {
		return ErrNativeSemanticUnsupported
	}
	db.nativeSemanticBridge = bridge
	return nil
}
func (db *DB) HasNativeSemanticBridge() bool { return db != nil && db.nativeSemanticBridge != nil }

// SyncNativeSemanticPrefix seals and synchronizes the original cache owner's
// captured complete prefix. It does not acquire a legacy command publisher or
// grant backend aliases authority over native accepted state.
func (db *DB) SyncNativeSemanticPrefix() error {
	if db == nil {
		return ErrClosed
	}
	if !db.nativeSemanticActive.Load() || !db.HasNativeSemanticBridge() {
		return ErrNativeSemanticUnsupported
	}
	if err := db.ActivateNativeSemanticOwner(); err != nil {
		return err
	}
	return db.nativeSemanticBridge.Checkpoint()
}

// CompactNativeColumnStore uses the original checkpoint owner and covered
// handoff. No command identity is allocated for physical maintenance.
func (db *DB) CompactNativeColumnStore(ctx context.Context, operation NativeColumnStoreCompaction) error {
	if db == nil {
		return ErrClosed
	}
	if err := db.CheckStorageMaintenanceReady(); err != nil {
		return err
	}
	if operation == nil || !db.nativeSemanticActive.Load() || !db.HasNativeSemanticBridge() {
		return ErrNativeSemanticUnsupported
	}
	if err := db.ActivateNativeSemanticOwner(); err != nil {
		return err
	}
	return db.nativeSemanticBridge.CompactNativeColumnStore(ctx, operation)
}

func (db *DB) CaptureNativeSemanticState() (NativeSemanticCapture, error) {
	if !db.HasNativeSemanticBridge() {
		return nil, ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.CaptureNativeCut()
}
func (db *DB) BeginNativeConditionalTxn() (NativeConditionalTxn, error) {
	if !db.HasNativeSemanticBridge() {
		return nil, ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.BeginNativeConditionalTxn()
}

// PrepareNativeProducer runs maintenance before collection locks or captures.
// It reserves no capacity and grants no publication authority.
func (db *DB) PrepareNativeProducer() error {
	if !db.HasNativeSemanticBridge() {
		return ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.PrepareNativeProducer()
}

// BeginPreparedNativeConditionalTxn is the opening-only adapter for consumers
// that prepared before acquiring their own mutation locks.
func (db *DB) BeginPreparedNativeConditionalTxn() (NativeConditionalTxn, error) {
	if !db.HasNativeSemanticBridge() {
		return nil, ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.BeginPreparedNativeConditionalTxn()
}
func (db *DB) WriteNativePublication(native *NativePreparedPublication, intent *CommandWALIntent, sync bool) (NativePublicationResult, error) {
	if !db.HasNativeSemanticBridge() {
		return NativePublicationResult{}, ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.WriteNativePublication(native, intent, sync)
}

// ValidateNativeCommandIntent keeps unmigrated command kinds fail-closed using
// the SAME replay registration. No handler or preparation executes under P.
func (db *DB) ValidateNativeCommandIntent(owner *transactionowner.Owner, intent *CommandWALIntent) error {
	if err := db.PreflightPrivateCOWCommand(owner); err != nil {
		return err
	}
	if intent == nil || intent.AssignedLSN() != 0 {
		return ErrCommandWALRejected
	}
	if participants := intent.inner.durablePrefixGroup; len(participants) != 0 {
		if len(participants) < 2 || len(participants) > commitlog.MaxCompositeMutationMembersV1 || db.resolvedProfile != ProfileCommandWALDurable || intent.inner.fromReplay ||
			intent.inner.kind != 0 || len(intent.inner.payload) != 0 {
			return ErrCommandWALRejected
		}
		for i, participant := range participants {
			if participant == nil || len(participant.inner.durablePrefixGroup) != 0 || participant.inner.fromReplay {
				return ErrCommandWALRejected
			}
			for _, prior := range participants[:i] {
				if prior == participant {
					return ErrCommandWALRejected
				}
			}
			if err := db.ValidateNativeCommandIntent(owner, participant); err != nil {
				return err
			}
		}
		return nil
	}
	registration, ok := lookupCommandWALReplayHandler(intent.inner.kind)
	if !ok || registration.nativePrepare == nil {
		return ErrNativeSemanticUnsupported
	}
	return nil
}

// NativePhysicalReader resolves only an already captured physical root. The
// original capture must remain live through the call; concurrent Close on that
// same capture is excluded by its enclosing read/transaction scope. A true
// copyValue returns exact-length caller-owned inline bytes; false is a metadata
// probe and returns no Value. Key is omitted for exact point lookup. No physical scratch
// escapes, and dictionary/codec eligibility remains the native bounded contract.
type NativePhysicalReader interface {
	NativeEntryAtRoot(root uint64, key []byte, copyValue bool) (node.LeafEntry, error)
}

// NativeAdmittedPhysicalReader is an optional synchronous point companion.
// It preserves NativePhysicalReader implementations. admitOutput receives the
// already rounded inline allocation charge before independent copy-out under
// the original basis lock and any existing cache-slot lease. It must not reenter
// that owner. Key and borrowed cache/decoder bytes never escape; pointer and
// tombstone metadata require no inline output admission. The enclosing scope
// retains this capture and excludes concurrent Close through the entire call.
type NativeAdmittedPhysicalReader interface {
	NativeEntryAtRootAdmitted(root uint64, key []byte, copyValue bool, admitOutput func(uint64) error) (node.LeafEntry, error)
}

// NativePhysicalLeafReader decodes an outer leaf through the capture's exact
// retained basis and original admitted physical workspace. dst must contain
// caller-admitted page storage. Successful output aliases only dst, never shared
// decoder/cache backing; it remains valid after later calls until the caller
// reuses dst. The enclosing scope retains this capture and excludes its Close
// during the call. Record inspection and native codec policy apply on every
// read; a cache hit requires original full-record checksum verification bound
// to the exact pinned file identity.
type NativePhysicalLeafReader interface {
	NativeLeafPage(ptr page.LeafLogPtr, dst []byte) ([]byte, error)
}

// NativePhysicalValueReader decodes an already captured pointer using that
// capture's exact retained basis and registered files. It returns exact-length,
// independent output before releasing shared scratch. Dictionary-free codecs
// use the maintained bounded reader. Trained codecs additionally require the
// pure dictionary provider's independently owned, verified definition; no codec
// slot may borrow a caller's capture or a global-cache dictionary alias.
//
// admitOutput is nil for caller-owned public results. The existing internal
// retained consumer may supply only its synchronous original-budget admission:
// no application callback, capture, publication or scope lock acquisition. It
// runs before the independent output allocation, under the basis read mutex.
// As with NativePhysicalReader, the enclosing scope excludes same-capture Close.
type NativePhysicalValueReader interface {
	NativeValueAtPointer(ptr page.ValuePtr, admitOutput func(uint64) error) ([]byte, error)
}
