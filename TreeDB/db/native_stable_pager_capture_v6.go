package db

import (
	"crypto/sha256"
	"os"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var nativeDataIndexDigestV6 = sha256.Sum256([]byte("treedb/index-file/v1"))
var nativePrimaryIndexDigestV6 = sha256.Sum256([]byte("treedb/primary-bank/v1"))

// Exactly two of these original capture frames live inside the selected
// producer: DATA and PRIMARY. No independent retention pool is introduced.
// The containing producer admits these controls; the separately allocated
// original Snapshot and callback backing retain their own actual charges.
type nativeStablePagerCaptureV6 struct {
	snapshot                                                                    *Snapshot
	operations                                                                  *stablePagerOwnedOperations
	token                                                                       *rootpublication.StableResourceToken
	namespace                                                                   *rootpublication.StableNamespaceToken
	construction                                                                rootpublication.StableIndexTokenConstructionWorkV6
	spec                                                                        rootpublication.StableResourceSpec
	identity                                                                    rootpublication.StableIdentity
	registry                                                                    *rootpublication.IdentityPinRegistry
	namespaceParent                                                             *os.File
	namespaceParentMetadata                                                     *retainedalloc.Owner
	namespaceParentCharge                                                       uint64
	pendingProof                                                                *rootpublication.StableNamespaceCreationProof
	proofRelease                                                                rootpublication.NamespaceProofReleaseWork
	proofReleaseBound                                                           bool
	metadata                                                                    *retainedalloc.Owner
	wrapperCharge                                                               uint64
	ownerRelease                                                                primaryArenaReleaseWork
	phase, releasePhase, namespacePhase                                         uint8
	observed, ownerReleaseBound, builderOperationsConsumed, constructorDetached bool
}

func (operations *stablePagerOwnedOperations) SupportsReleaseWithWorkV6(w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || operations.native == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*operations))) {
		return false, nil
	}
	operations.completionMu.Lock()
	defer operations.completionMu.Unlock()
	return operations.supportsNativeReleaseLockedV6()
}

func (operations *stablePagerOwnedOperations) supportsNativeReleaseLockedV6() (bool, error) {
	s := operations.native
	if s == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	if operations.snapshotCompleted {
		return true, nil
	}
	snapshot := operations.snapshot
	if snapshot == nil || snapshot != s.snapshot || !snapshot.nativeMVCC.workOwned || snapshot.db == nil || snapshot.idx == nil {
		return false, retainedalloc.ErrWorkRequired
	}
	db, idx := snapshot.db, snapshot.idx
	db.idxMu.Lock()
	idx.wrapperMu.Lock()
	eligible := db.idx.Load() == idx && idx.refs.Load() > 0 && !idx.physicalCompleted && !idx.closeCompleted && idx.wrapperBorrowCountV6() != 0 && idx.pager.SupportsBoundedPhysicalClose()
	idx.wrapperMu.Unlock()
	db.idxMu.Unlock()
	if !eligible {
		return false, retainedalloc.ErrWorkRequired
	}
	return true, nil
}

// The SAME original token supplies consumed. A last release keeps its original
// Snapshot, counter, owner and cleanup custody until all actual consumers end.
func (operations *stablePagerOwnedOperations) ReleaseWithWorkV6(consumed *bool, w *iterator.OrdinalScanWork) (*retainedalloc.Owner, uint64, bool, error) {
	if w == nil || consumed == nil {
		return nil, 0, false, retainedalloc.ErrWorkRequired
	}
	operations.completionMu.Lock()
	defer operations.completionMu.Unlock()
	s := operations.native
	if s == nil {
		return nil, 0, false, retainedalloc.ErrWorkRequired
	}
	if !*consumed {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*operations))+2*uint64(unsafe.Sizeof(*s))) {
			return nil, 0, false, nil
		}
		if ready, err := operations.supportsNativeReleaseLockedV6(); !ready || err != nil {
			return nil, 0, false, err
		}
		n := operations.refs.Add(-1)
		if n < 0 {
			panic("unbalanced native pager callback backing")
		}
		*consumed = true
		if n != 0 {
			return nil, 0, true, nil
		}
		operations.disposalClaimed = true
	}
	if operations.refs.Load() != 0 {
		return nil, 0, true, nil
	}
	if s.releasePhase == 0 {
		snapshot := operations.snapshot
		if snapshot == nil {
			return nil, 0, false, rootpublication.ErrResourceOwnership
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*snapshot))) {
			return nil, 0, false, nil
		}
		snapshot.iteratorMu.Lock()
		if snapshot.stablePagerCompletion != nil && snapshot.stablePagerCompletion != operations {
			snapshot.iteratorMu.Unlock()
			return nil, 0, false, rootpublication.ErrResourceOwnership
		}
		// This SAME callback remains sole completion/counter owner. Prevent
		// the native Snapshot finalizer from invoking the ordinary callback.
		snapshot.stablePagerCompletion = nil
		snapshot.stableIndexCaptureTransferred = true
		snapshot.iteratorMu.Unlock()
		s.releasePhase = 1
	}
	if s.releasePhase == 1 {
		ready, err := operations.snapshot.CloseNativeMVCCSnapshotStepV6(w)
		if !ready || err != nil {
			return nil, 0, false, err
		}
		operations.snapshot = nil
		s.snapshot = nil
		if operations.counter != nil {
			operations.counter.Add(-1)
			operations.counter = nil
		}
		operations.transferred.Store(false)
		operations.snapshotCompleted = true
		s.releasePhase = 2
	}
	if s.releasePhase == 2 {
		if operations.owner != nil && !s.ownerReleaseBound {
			ready, err := s.ownerRelease.Bind(operations.owner, w)
			if !ready || err != nil {
				return nil, 0, false, err
			}
			operations.owner = nil
			s.ownerReleaseBound = true
		}
		if s.ownerReleaseBound {
			ready, _, err := s.ownerRelease.ContinueWork(w)
			if !ready || err != nil {
				return nil, 0, false, err
			}
		}
		s.releasePhase = 3
	}
	if s.releasePhase == 3 {
		completion, ready, err := operations.originalCustody.CompleteWithWorkV6(w)
		if !ready || err != nil {
			return nil, 0, false, err
		}
		operations.originalCustody = nil
		completion.Complete()
		s.releasePhase = 4
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*operations))+uint64(len(operations.syncPath))) {
		return nil, 0, false, nil
	}
	owner, charge := operations.metadata, operations.charge+s.wrapperCharge
	if owner != s.metadata {
		return nil, 0, false, rootpublication.ErrResourceOwnership
	}
	operations.pager, operations.native, operations.metadata = nil, nil, nil
	operations.syncPath, operations.charge = "", 0
	operations.disposalConsumed.Store(true)
	s.wrapperCharge, s.metadata, s.operations = 0, nil, nil
	s.releasePhase = 5
	// The original disposing parent clears its operations alias before refund.
	return owner, charge, true, nil
}

// step constructs one original maintenance capture. The root/sequence check is
// repeated on every turn before accepting a new capture effect; no lock lives
// across a return. DATA and PRIMARY remain the ordinary producer's two operands.
func (s *nativeStablePagerCaptureV6) step(c *NativeMVCCCurrentDeleteV6, primary bool, w *iterator.OrdinalScanWork) (bool, error) {
	db := c.db
	idx := db.rootPublication.idx
	for {
		db.rootReuseMu.RLock()
		db.mu.RLock()
		if db.closing.Load() || db.idx.Load() != idx || db.meta.CommitSeq != c.rootPreparation.expectedSequence || db.vacuumCutoverInProgress.Load() {
			db.mu.RUnlock()
			db.rootReuseMu.RUnlock()
			return false, errDurableRootCandidateStale
		}
		ready, err := s.stepLocked(c, primary, w)
		db.mu.RUnlock()
		db.rootReuseMu.RUnlock()
		if !ready || err != nil {
			return false, err
		}
		if s.phase == 8 {
			return true, nil
		}
	}
}

func (s *nativeStablePagerCaptureV6) stepLocked(c *NativeMVCCCurrentDeleteV6, primary bool, w *iterator.OrdinalScanWork) (bool, error) {
	db, idx := c.db, c.db.rootPublication.idx
	metadata := idx.primary.MetadataOwner()
	p, name, lane, digest := idx.pager, indexFileName, "db/index", nativeDataIndexDigestV6
	if primary {
		p, name, lane, digest = idx.primary.Pager(), primaryIndexFileName, "db/primary", nativePrimaryIndexDigestV6
	}
	switch s.phase {
	case 0:
		if !w.Reserve(3, 2*uint64(unsafe.Sizeof(Snapshot{}))+2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(*idx))) {
			return false, nil
		}
		db.idxMu.Lock()
		idx.wrapperMu.Lock()
		if db.idx.Load() != idx || idx.refs.Load() <= 0 || idx.physicalCompleted || idx.closeCompleted || !idx.pager.SupportsBoundedPhysicalClose() || !idx.primary.Pager().SupportsBoundedPhysicalClose() {
			idx.wrapperMu.Unlock()
			db.idxMu.Unlock()
			return false, ErrNativeMVCCSnapshotWork
		}
		charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Snapshot{})))
		if err := metadata.AddPending(charge); err != nil {
			idx.wrapperMu.Unlock()
			db.idxMu.Unlock()
			return false, err
		}
		s.snapshot = db.newDurableCandidateStableIndexSnapshotV1(idx)
		s.snapshot.nativeMVCC = nativeMVCCSnapshotStateV6{workOwned: true, capturePhase: 4}
		s.metadata, s.wrapperCharge = metadata, charge
		idx.wrapperMu.Unlock()
		db.idxMu.Unlock()
		s.phase = 1
	case 1:
		mutex := &idx.stableNamespaceMu
		parent, proof := &idx.stableNamespaceParent, &idx.stableNamespaceProof
		if primary {
			mutex = &idx.primaryOwner.namespaceMu
			parent, proof = &idx.primaryOwner.namespaceParent, &idx.primaryOwner.namespaceProof
		}
		mutex.Lock()
		if *proof == nil {
			if *parent != nil {
				mutex.Unlock()
				return false, rootpublication.ErrResourceOwnership
			}
			if s.namespaceParent == nil {
				fileBytes, err := rootpublication.StableFileMetadataCharge(uint64(len(db.dir)))
				if err != nil {
					mutex.Unlock()
					return false, err
				}
				if !w.Reserve(2, 2*fileBytes+2*uint64(len(db.dir))) {
					mutex.Unlock()
					return false, nil
				}
				if err := metadata.AddPending(fileBytes); err != nil {
					mutex.Unlock()
					return false, err
				}
				f, err := os.Open(strings.Clone(db.dir))
				if err != nil {
					metadata.RemovePending(fileBytes)
					mutex.Unlock()
					return false, err
				}
				s.namespaceParent, s.namespaceParentCharge, s.namespaceParentMetadata = f, fileBytes, metadata
			}
			var ready bool
			err := p.WithStableResourceFile(func(file *os.File) error {
				var err error
				s.pendingProof, ready, err = rootpublication.NewOwnedStableNamespaceCreationProofWithWorkV6(s.namespaceParent, file, name, metadata, w)
				return err
			})
			if !ready || err != nil {
				mutex.Unlock()
				return false, err
			}
			*parent, *proof = s.namespaceParent, s.pendingProof
			if primary {
				idx.primaryOwner.namespaceParentCharge = s.namespaceParentCharge
			} else {
				idx.stableNamespaceParentCharge = s.namespaceParentCharge
				idx.stableNamespaceMetadata = metadata
			}
			s.namespaceParent, s.namespaceParentCharge, s.pendingProof, s.namespaceParentMetadata = nil, 0, nil, nil
		}
		if !w.Reserve(1, 256) {
			mutex.Unlock()
			return false, nil
		}
		generation, err := rootpublication.StableNamespaceParentGeneration(*parent)
		if err != nil {
			mutex.Unlock()
			return false, err
		}
		ns, ready, err := (*proof).BindWithMetadataWithWorkV6(*parent, generation, name, name, metadata, w)
		mutex.Unlock()
		if !ready || err != nil {
			return false, err
		}
		s.namespace = ns
		s.phase = 2
	case 2:
		if !w.Reserve(3, 2*uint64(unsafe.Sizeof(s.spec))+2*uint64(unsafe.Sizeof(s.identity))+256) {
			return false, nil
		}
		err := p.WithStableResourceFile(func(file *os.File) error {
			info, err := file.Stat()
			if err != nil {
				return err
			}
			identity, err := rootpublication.StableIdentityFromFile(file)
			if err != nil {
				return err
			}
			s.identity = identity
			s.spec = rootpublication.StableResourceSpec{MetadataOwner: metadata, Kind: rootpublication.ResourceIndex, LogicalLane: lane, ResourceID: name, Digest: digest, Reachability: rootpublication.ReachabilityIndexFile, File: file, Generation: idx.id, DiagnosticPath: name, Frontier: rootpublication.DurableFrontier{Bytes: uint64(info.Size())}, Namespace: s.namespace, PinRegistry: db.StableResourceIdentityPinRegistry()}
			return nil
		})
		if err != nil {
			return false, err
		}
		s.phase = 3
	case 3:
		ready, err := db.StableResourceIdentityPinRegistry().ObserveWithWorkV6(s.identity, w)
		if !ready || err != nil {
			return false, err
		}
		s.registry = db.StableResourceIdentityPinRegistry()
		s.observed, s.phase = true, 4
	case 4:
		if !w.Reserve(6, 2*uint64(unsafe.Sizeof(stablePagerOwnedOperations{}))+2*uint64(unsafe.Sizeof(rootpublication.OriginalCleanupCustody{}))+2*uint64(len(s.spec.File.Name()))+2*uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))) {
			return false, nil
		}
		operations, err := newStablePagerOwnedOperations(s.snapshot, p, metadata, s.spec.File.Name())
		if err != nil {
			return false, err
		}
		operations.native = s
		s.operations, s.spec.OwnedOperations = operations, operations
		s.phase = 5
	case 5:
		token, ready, err := s.construction.Step(s.spec, w)
		if !ready || err != nil {
			return false, err
		}
		// Preserve the exact original successful transfer protocol. All later
		// physical completion remains with this SAME Snapshot/callback pair.
		s.snapshot.iteratorMu.Lock()
		s.operations.transferred.Store(true)
		s.snapshot.stablePagerCompletion = s.operations
		s.snapshot.stableIndexCaptureTransferred = true
		s.snapshot.iteratorMu.Unlock()
		s.token = token
		s.phase = 6
	case 6:
		ready, err := db.StableResourceIdentityPinRegistry().UnobserveWithWorkV6(s.identity, w)
		if !ready || err != nil {
			return false, err
		}
		s.observed = false
		s.phase = 7
	case 7:
		// Drop the original constructor's independent callback and namespace
		// edges. The accepted token owns its own exact edges thereafter.
		if s.operations != nil {
			_, _, ready, err := s.operations.ReleaseWithWorkV6(&s.builderOperationsConsumed, w)
			if !ready || err != nil {
				return false, err
			}
			s.operations = nil
		}
		if s.namespace != nil {
			completion, ready, err := s.namespace.ReleaseWithWorkV6(&s.namespacePhase, w)
			if !ready || err != nil {
				return false, err
			}
			s.namespace = nil
			completion.Complete()
		}
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(s.spec))) {
			return false, nil
		}
		_, ready, err := s.construction.DetachV6(w)
		if !ready || err != nil {
			return false, err
		}
		s.spec = rootpublication.StableResourceSpec{}
		s.constructorDetached = true
		s.phase = 8
	case 8:
		return true, nil
	default:
		return false, rootpublication.ErrResourceOwnership
	}
	return true, nil
}

func (s *nativeStablePagerCaptureV6) closeStep(w *iterator.OrdinalScanWork) (bool, error) {
	if !s.constructorDetached {
		token, ready, err := s.construction.DetachV6(w)
		if !ready || err != nil {
			return false, err
		}
		if s.token != nil && token != nil && s.token != token {
			return false, rootpublication.ErrResourceOwnership
		}
		if s.token == nil {
			s.token = token
		}
		s.constructorDetached = true
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(s.spec))) {
		return false, nil
	}
	s.spec = rootpublication.StableResourceSpec{}
	// A token allocated before its callback Retain has no callback edge yet.
	// The original builder can therefore be the last owner: propagate its
	// actual Snapshot/callback charge, clearing aliases before the refund.
	if s.operations != nil && s.token != nil {
		owner, charge, ready, err := s.operations.ReleaseWithWorkV6(&s.builderOperationsConsumed, w)
		if !ready || err != nil {
			return false, err
		}
		s.operations = nil
		if owner != nil {
			owner.RemovePending(charge)
		}
	}
	if s.token != nil {
		completion, ready, err := s.token.ReleaseWithWorkV6(w)
		if !ready || err != nil {
			return false, err
		}
		s.token = nil
		completion.Complete()
	}
	if s.operations != nil {
		owner, charge, ready, err := s.operations.ReleaseWithWorkV6(&s.builderOperationsConsumed, w)
		if !ready || err != nil {
			return false, err
		}
		s.operations = nil
		if owner != nil {
			owner.RemovePending(charge)
		}
	}
	if s.snapshot != nil {
		ready, err := s.snapshot.CloseNativeMVCCSnapshotStepV6(w)
		if !ready || err != nil {
			return false, err
		}
		s.snapshot = nil
		owner, charge := s.metadata, s.wrapperCharge
		s.metadata, s.wrapperCharge = nil, 0
		if owner != nil {
			owner.RemovePending(charge)
		}
	}
	if s.namespace != nil {
		completion, ready, err := s.namespace.ReleaseWithWorkV6(&s.namespacePhase, w)
		if !ready || err != nil {
			return false, err
		}
		s.namespace = nil
		completion.Complete()
	}
	if s.pendingProof != nil && !s.proofReleaseBound {
		ready, err := s.proofRelease.Bind(s.pendingProof, w)
		if !ready || err != nil {
			return false, err
		}
		s.pendingProof = nil
		s.proofReleaseBound = true
	}
	if s.proofReleaseBound {
		ready, err := s.proofRelease.ContinueWork(w)
		if !ready || err != nil {
			return false, err
		}
	}
	if s.namespaceParent != nil {
		if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s.namespaceParent))+128) {
			return false, nil
		}
		if err := s.namespaceParent.Close(); err != nil {
			return false, err
		}
		s.namespaceParent = nil
		owner, charge := s.namespaceParentMetadata, s.namespaceParentCharge
		s.namespaceParentCharge, s.namespaceParentMetadata = 0, nil
		if owner != nil {
			owner.RemovePending(charge)
		}
	}
	if s.observed {
		// The original Snapshot or callback may now be scrubbed. The SAME
		// producer registry is retained by the original selected capture.
		ready, err := s.registry.UnobserveWithWorkV6(s.identity, w)
		if !ready || err != nil {
			return false, err
		}
		s.observed = false
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	*s = nativeStablePagerCaptureV6{}
	return true, nil
}
