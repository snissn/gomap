package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"github.com/snissn/gomap/TreeDB/internal/txnhistory"
)

var ErrNativeSemanticUnsupported = errors.New("treedb: command kind has no native semantic preparation")

// NativeSemanticBuildRetryableRefusal proves that PrepareBuild refused before
// mutating its original build ledger, allocating assets, or beginning fallible
// materialization. A capacity error after those effects is terminal instead.
// This classification carries no resource or failure custody of its own.
type NativeSemanticBuildRetryableRefusal struct{ Cause error }

func (e NativeSemanticBuildRetryableRefusal) Error() string { return e.Cause.Error() }
func (e NativeSemanticBuildRetryableRefusal) Unwrap() error { return e.Cause }

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
	// All fallible allocation precedes the logical installation latch.
	PrepareHandoff(latest NativeSemanticState, accepted *Snapshot) (NativeSemanticState, error)
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
	AdmitNativeBytes(uint64) (*memtable.COWExternalLease, error)
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

// NativeSemanticBridge is fixed once at Open to the backend's SAME private COW
// owner. It introduces neither a participant registry nor a publication lane.
type NativeSemanticBridge interface {
	CaptureNativeCut() (NativeSemanticCapture, error)
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
func (db *DB) CaptureNativeSemanticState() (NativeSemanticCapture, error) {
	if !db.HasNativeSemanticBridge() {
		return nil, ErrNativeSemanticUnsupported
	}
	return db.nativeSemanticBridge.CaptureNativeCut()
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
	registration, ok := lookupCommandWALReplayHandler(intent.inner.kind)
	if !ok || registration.nativePrepare == nil {
		return ErrNativeSemanticUnsupported
	}
	return nil
}
