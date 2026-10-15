package db

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
	"unsafe"
)

// NativeMVCCFrameReaderV6 is a consumer of the SAME retained Snapshot. The
// enclosing native cursor admits this containing wrapper and all Leaf backings
// before construction and retains the Snapshot through phased consumer close.
// It creates no file, registry, cache, publication or retention authority.
type NativeMVCCFrameReaderV6 struct {
	Snapshot         *Snapshot
	Leaf             *valuelog.COWLeafProjectionReader
	ref              page.ChildRef
	dst, image       []byte
	copied           int
	phase            uint8
	active, external bool
}

func (r *NativeMVCCFrameReaderV6) BeginProjectionFrame(ref page.ChildRef, dst []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))+uint64(unsafe.Sizeof(ref))) {
		return false, nil
	}
	if r.active || r.Snapshot == nil || len(dst) != page.PageSize {
		return false, tree.ErrProjectionReader
	}
	external := false
	if ref.IsLeafLog() {
		// Admit the actual existing Set lookup and registered File observation.
		// The passed codec borrow must use this exact producer-owned file.
		if !w.Reserve(2, 256) {
			return false, nil
		}
		file, _, err := r.Snapshot.PinnedValueLogFile(ref.Log.ValueLogFileID())
		if err != nil {
			return false, err
		}
		if r.Leaf == nil {
			return false, tree.ErrProjectionReader
		}
		r.Leaf.File, r.Leaf.FileID = file, ref.Log.FileID
		ready, err := r.Leaf.BeginProjectionFrame(ref, dst, w)
		if !ready || err != nil {
			return ready, err
		}
		external = true
	} else if !ref.IsPage() || ref.Page < 2 || ref.Log != (page.LogRecordRef{}) {
		return false, tree.ErrProjectionReader
	}
	r.ref, r.dst, r.copied, r.phase, r.active, r.external = ref, dst, 0, 1, true, external
	return true, nil
}

func (r *NativeMVCCFrameReaderV6) StepProjectionFrame(w *iterator.OrdinalScanWork) ([]byte, bool, error) {
	if !r.active || r.Snapshot == nil {
		return nil, false, tree.ErrProjectionReader
	}
	if r.external {
		image, ready, err := r.Leaf.StepProjectionFrame(w)
		if ready && err == nil {
			r.active = false
		}
		return image, ready, err
	}
	if r.phase == 1 {
		if w == nil || !w.Reserve(1, 128) {
			return nil, false, nil
		}
		if err := r.Snapshot.beginRead(); err != nil {
			return nil, false, err
		}
		if r.Snapshot.idx == nil || r.Snapshot.idx.pager == nil {
			r.Snapshot.endRead()
			return nil, false, ErrClosed
		}
		image, ready, err := r.Snapshot.idx.pager.GetWithWork(r.ref.Page, w)
		r.Snapshot.endRead()
		if !ready || err != nil {
			return nil, ready, err
		}
		if len(image) != page.PageSize {
			return nil, false, tree.ErrProjectionReader
		}
		r.image, r.phase = image, 2
	}
	if r.phase == 2 {
		if w == nil || w.Bytes > w.ByteLimit {
			return nil, false, nil
		}
		amount := int(min(uint64(page.PageSize-r.copied), (w.ByteLimit-w.Bytes)/2))
		if amount == 0 || !w.Reserve(1, 2*uint64(amount)) {
			return nil, false, nil
		}
		copy(r.dst[r.copied:r.copied+amount], r.image[r.copied:r.copied+amount])
		r.copied += amount
		if r.copied != page.PageSize {
			return nil, false, nil
		}
		r.phase = 3
	}
	if !w.Reserve(1, 64) {
		return nil, false, nil
	}
	image := r.dst
	r.image, r.active, r.phase = nil, false, 0
	return image, true, nil
}

func (r *NativeMVCCFrameReaderV6) CancelProjectionFrame(w *iterator.OrdinalScanWork) (bool, error) {
	if r.external && r.Leaf != nil {
		ready, err := r.Leaf.CancelProjectionFrame(w)
		if !ready || err != nil {
			return ready, err
		}
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*r))) {
		return false, nil
	}
	r.ref, r.dst, r.image, r.copied, r.phase, r.active, r.external = page.ChildRef{}, nil, nil, 0, 0, false, false
	return true, nil
}

// NewNativeMVCCProjectionV6 borrows this original Snapshot's exact Tree. The
// original cursor's retained allocation admission precedes this constructor;
// ChargedProjection then prices its actual constructor before allocation.
// cert must be the retained immutable producer certificate supplied by capture.
func (s *Snapshot) NewNativeMVCCProjectionV6(cert *primaryarena.ReadRootCertificateV6, start, end []byte, reader *NativeMVCCFrameReaderV6, w *iterator.OrdinalScanWork) (*tree.ChargedProjection, error) {
	if s == nil || reader == nil || reader.Snapshot != s {
		return nil, tree.ErrProjectionOwner
	}
	if w == nil || !w.Reserve(1, uint64(unsafe.Sizeof(*s))) {
		return nil, tree.ErrProjectionPending
	}
	if err := s.beginRead(); err != nil {
		return nil, err
	}
	defer s.endRead()
	if s.primaryRoot != nil && (cert == nil || cert.Arena() != s.primaryRoot.arena || cert.Reference() != s.primaryRoot.ref) {
		return nil, tree.ErrProjectionOwner
	}
	return s.tree.NewChargedProjection(cert, start, end, reader, w)
}

// ValidateNativeMVCCSelectionV6 binds actual selected physical bytes to this
// SAME retained Snapshot. It confers no discard-floor or publication authority.
func (s *Snapshot) ValidateNativeMVCCSelectionV6(selection tree.NativeMVCCSelectionV6, w *iterator.OrdinalScanWork) (bool, error) {
	if s == nil || w == nil || !w.Reserve(1, 128) {
		return false, nil
	}
	if err := s.beginRead(); err != nil {
		return false, err
	}
	defer s.endRead()
	return s.tree.MatchesNativeMVCCSelectionV6(selection, w)
}

// NativeMVCCSelectionSourceV6 borrows an actual producer-owned composite cut.
// It validates and returns that cut's stationary selected winner, including its
// original revision. The caller retains the producer until transaction cleanup;
// a borrowed backend Snapshot alone cannot witness an overlay winner.
type NativeMVCCSelectionSourceV6 interface {
	NativeMVCCSelectedRecordV6(*Snapshot, *iterator.OrdinalScanWork) (node.LeafEntry, bool, error)
}

// NativeMVCCCurrentDeleteV6 is the original caller's admitted construction
// continuation. The Snapshot and physical selection remain stationary until
// the same transaction is either published or physically cancelled. Preparing
// its unsealed constructor does not select a current root, mutate the floor,
// install output, or report a delete ACK.
type NativeMVCCCurrentDeleteV6 struct {
	db                     *DB
	snapshot               *Snapshot
	selection              tree.NativeMVCCSelectionV6
	source                 NativeMVCCSelectionSourceV6
	record                 node.LeafEntry
	records                []node.LeafEntry
	request                mvccadmission.Request
	authorityScope         nativeAuthorityScopeV6
	rootPreparation        nativeMVCCRootPreparationV6
	allocatorPreparation   nativeMVCCAllocatorPreparationV6
	candidatePreparation   nativeMVCCCandidatePreparationV6
	sealPreparation        nativeMVCCSealPreparationV6
	transaction            *rootpublication.DurableRootTransaction
	output                 *zipper.MaterializedWriteWork
	outputSequence         uint64
	outputInput            zipper.MaterializedInputV6
	inputWork              *zipper.MaterializedInputWorkV6
	outputInputReady       bool
	outputAdoptionReady    bool
	outputDiagnostic       bool
	outputScope            NativeMVCCPrivateOutputScopeV6
	outputReader           *NativeMVCCFrameReaderV6
	release                rootpublication.PrimaryConstructionReleaseWork
	publication            rootpublication.NativePublicationV6
	publicationCoordinator *rootpublication.Coordinator
	publicationBound       bool
	phase                  uint8
}

func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCCurrentDeleteV6(s *Snapshot, selection tree.NativeMVCCSelectionV6, request mvccadmission.Request, w *iterator.OrdinalScanWork) (bool, error) {
	return c.bindNativeMVCCCurrentDeleteV6(s, selection, nil, request, w)
}

// BindNativeMVCCCompositeDeleteV6 retains no new Snapshot edge: source owns the
// SAME composite cut and its basis. Its original producer validates the winner
// before constructor creation, and the Request is checked again at current cut.
func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCCompositeDeleteV6(s *Snapshot, source NativeMVCCSelectionSourceV6, request mvccadmission.Request, w *iterator.OrdinalScanWork) (bool, error) {
	if source == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	return c.bindNativeMVCCCurrentDeleteV6(s, tree.NativeMVCCSelectionV6{}, source, request, w)
}

func (c *NativeMVCCCurrentDeleteV6) bindNativeMVCCCurrentDeleteV6(s *Snapshot, selection tree.NativeMVCCSelectionV6, source NativeMVCCSelectionSourceV6, request mvccadmission.Request, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || s == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))+uint64(unsafe.Sizeof(selection))) {
		return false, nil
	}
	if c.phase != 0 || c.snapshot != nil || s.db == nil ||
		(source == nil && (!s.nativeMVCC.workOwned || s.nativeMVCC.capturePhase != 4)) {
		return false, ErrNativeMVCCSnapshotWork
	}
	c.db, c.snapshot, c.selection, c.source, c.request, c.phase = s.db, s, selection, source, request, 1
	return true, nil
}

// BindNativeMVCCDeleteBatchV6 borrows the cursor's preadmitted verified vector.
// Its last row is the stationary original producer selection used to create
// this SAME constructor. The caller retains every payload until checked Finish.
func (c *NativeMVCCCurrentDeleteV6) BindNativeMVCCDeleteBatchV6(records []node.LeafEntry, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 3 || len(records) == 0 {
		return false, ErrNativeMVCCSnapshotWork
	}
	last := records[len(records)-1]
	if !w.Reserve(1, uint64(len(last.Key))+64) {
		return false, nil
	}
	if !bytes.Equal(last.Key, c.record.Key) || last.Revision != c.record.Revision || last.Flags != c.record.Flags || last.ValuePtr != c.record.ValuePtr {
		return false, rootpublication.ErrDurableRootOwnership
	}
	c.records = records
	return true, nil
}

// StepNativeMVCCCurrentDeleteV6 validates the selected immutable physical
// operand and creates the SAME producer transaction. It deliberately leaves
// the directory unsealed so legal future writes may complete at prepared
// returns. Fresh composition belongs to the later current publication cut.
func (c *NativeMVCCCurrentDeleteV6) StepNativeMVCCCurrentDeleteV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.db == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if c.phase == 3 {
		return c.transaction.PreparePrimaryPromotionStorageV6(w)
	}
	if c.phase == 4 {
		return true, nil
	}
	if c.phase == 1 {
		var ready bool
		var err error
		if c.source != nil {
			c.record, ready, err = c.source.NativeMVCCSelectedRecordV6(c.snapshot, w)
		} else {
			ready, err = c.snapshot.ValidateNativeMVCCSelectionV6(c.selection, w)
			if ready && err == nil {
				c.record = c.selection.Record()
			}
		}
		if !ready || err != nil {
			return ready, err
		}
		ready, err = c.request.Validate(w)
		if !ready || err != nil {
			return ready, err
		}
		c.phase = 2
	}
	if c.phase != 2 {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+128) {
		return false, nil
	}
	if !c.db.writeMu.TryLock() {
		return false, nil
	}
	defer c.db.writeMu.Unlock()
	if !c.db.durablePublishMu.TryLock() {
		return false, nil
	}
	defer c.db.durablePublishMu.Unlock()
	if c.db.closing.Load() || c.db.idx.Load() != c.snapshot.idx || c.snapshot.primaryRoot == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	arena := c.snapshot.primaryRoot.arena
	if !arena.Pager().SupportsBoundedPhysicalClose() {
		return false, ErrNativeMVCCSnapshotWork
	}
	original, ready, err := rootpublication.NewNativeMVCCPrimaryConstructionTransactionV6(arena, w)
	if original != nil {
		var ok bool
		c.transaction, ok = original.(*rootpublication.DurableRootTransaction)
		if !ok {
			return false, rootpublication.ErrDurableRootOwnership
		}
		c.phase = 3
	}
	if !ready || err != nil {
		return ready, err
	}
	return c.transaction.PreparePrimaryPromotionStorageV6(w)
}

// composeNativeMVCCCurrentDeleteLockedV6 uses the actual current immutable
// directory under the original write/durable cut. The retained selected DATA
// proof remains authoritative under the SAME Request; the base operand and
// sequence copied into output come from current, never the older selection.
// The caller must consume this sealed transaction in that SAME cut or abort
// it; this function does not publish or commit admission by itself.
func (c *NativeMVCCCurrentDeleteV6) composeNativeMVCCCurrentDeleteLockedV6(scratch []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.phase != 3 || c.transaction == nil || len(scratch) != page.PageSize {
		return false, ErrNativeMVCCSnapshotWork
	}
	if !w.Reserve(2, 2*uint64(unsafe.Sizeof(*c))+2*uint64(unsafe.Sizeof(node.PrimaryDirectoryEntry{}))+128) {
		return false, nil
	}
	db := c.db
	if db.closing.Load() || db.idx.Load() != c.snapshot.idx || db.meta.MaxEntryRevision == ^uint64(0) {
		return false, ErrNativeMVCCSnapshotWork
	}
	current := db.primaryCurrentRootV5()
	if current == nil || current.arena != c.snapshot.primaryRoot.arena || current.certificate == nil || current.certificate.Reference() != current.ref {
		return false, ErrNativeMVCCSnapshotWork
	}
	// Fresh physical CRC/digest and bank checks precede any derived base/key span.
	cert, ready, err := current.arena.BorrowReadRootCertificateV6(current.ref, w)
	if !ready || err != nil {
		return ready, err
	}
	if cert != current.certificate {
		return false, rootpublication.ErrDurableRootOwnership
	}
	record := c.record
	if c.source != nil {
		selected, ready, err := c.source.NativeMVCCSelectedRecordV6(c.snapshot, w)
		if !ready || err != nil {
			return ready, err
		}
		if !bytes.Equal(selected.Key, record.Key) || selected.Revision != record.Revision || selected.Flags != record.Flags || selected.ValuePtr != record.ValuePtr {
			return false, rootpublication.ErrDurableRootOwnership
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(mvccadmission.Cut{}))+2*uint64(unsafe.Sizeof(mvccadmission.Authority{}))) {
		return false, nil
	}
	cut := db.mvccAdmission.Begin()
	defer cut.Abort()
	ready, err = cut.ObservePrune(c.request, record.Key, w)
	if !ready || err != nil {
		return ready, err
	}
	base, sequence := cert.Directory().Base()
	entry := node.PrimaryDirectoryEntry{Key: record.Key, Kind: node.PrimaryAbsence, Revision: page.EntryRevision(db.meta.MaxEntryRevision + 1)}
	ready, err = c.transaction.SealPrimaryCurrentAbsenceV6(current.ref, entry, base, sequence, record.Revision, scratch, w)
	if ready && err == nil {
		c.phase = 4
	}
	return ready, err
}

// CloseNativeMVCCCurrentDeleteStepV6 physically cancels the SAME untransferred
// constructor through its preborn external receipt. Its Snapshot borrow is
// cleared only after actual transaction cleanup, and the enclosing cursor
// retains both containing allocations until its own finalizer completes.
func (c *NativeMVCCCurrentDeleteV6) CloseNativeMVCCCurrentDeleteStepV6(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || w == nil {
		return false, ErrNativeMVCCSnapshotWork
	}
	if c.phase == 6 {
		c.outputReader = nil
		return c.completeNativePublicationBindingV6(w)
	}
	if ready, err := c.sealPreparation.closeStep(w); !ready || err != nil {
		return false, err
	}
	if ready, err := c.candidatePreparation.closeStep(w); !ready || err != nil {
		return false, err
	}
	if ready, err := c.allocatorPreparation.closeStep(w); !ready || err != nil {
		return false, err
	}
	if ready, err := c.rootPreparation.closeStep(w); !ready || err != nil {
		return false, err
	}
	if c.outputReader != nil {
		ready, err := c.outputReader.CancelProjectionFrame(w)
		if !ready || err != nil {
			return false, err
		}
		c.outputReader = nil
	}
	if c.output != nil {
		// Explicit synchronous diagnostic teardown only. The bounded installed-page
		// cancellation continuation remains a reported refusal, never hidden here.
		cleanupWork := w
		if c.outputDiagnostic {
			cleanupWork = nil
		}
		ready, err := c.output.Cancel(cleanupWork)
		if !ready || err != nil {
			return false, err
		}
		owner, charge, done, completeErr := c.output.CompleteWithWorkV6(cleanupWork)
		if !done || completeErr != nil {
			return false, completeErr
		}
		c.output = nil
		owner.RemovePending(charge)
	}
	if ready, err := c.closeMaterializedInputWorkV6(w); !ready || err != nil {
		return false, err
	}

	if c.outputScope != nil {
		var lease *memtable.COWExternalLease
		var err error
		if step, ok := c.outputScope.(NativeMVCCPrivateOutputStepScopeV6); ok {
			var ready bool
			ready, lease, err = step.ClosePrivateNativeOutputStepV6(w)
			if !ready || err != nil {
				return false, err
			}
		} else {
			lease, err = c.outputScope.ClosePrivateNativeOutputV6()
			if err != nil {
				return false, err
			}
		}
		c.outputScope = nil
		if lease != nil {
			lease.Close()
		}
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*c))) {
		return false, nil
	}
	if c.transaction == nil && c.phase != 5 {
		c.db, c.snapshot, c.selection, c.request, c.phase = nil, nil, tree.NativeMVCCSelectionV6{}, mvccadmission.Request{}, 6
		c.source, c.record, c.records = nil, node.LeafEntry{}, nil
		return c.completeNativePublicationBindingV6(w)
	}
	if c.phase != 5 {
		ready, err := c.release.Bind(c.transaction, w)
		if !ready || err != nil {
			return ready, err
		}
		c.transaction = nil // The external receipt now owns the only disposing alias.
		c.phase = 5
	}
	ready, err := c.release.ContinueWork(w)
	if ready && err == nil {
		c.transaction, c.db, c.snapshot = nil, nil, nil
		c.selection, c.request, c.phase = tree.NativeMVCCSelectionV6{}, mvccadmission.Request{}, 6
		c.source, c.record, c.records = nil, node.LeafEntry{}, nil
		return c.completeNativePublicationBindingV6(w)
	}
	return ready, err
}

func (c *NativeMVCCCurrentDeleteV6) MaintenanceCurrentDeleteScalarsForTest() (selected, prepared, cut bool, phase uint8) {
	if c == nil {
		return
	}
	return c.phase >= 2 && c.phase < 5, c.phase >= 3 && c.phase < 5, c.phase == 4, c.phase
}
