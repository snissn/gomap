package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/tree"
)

// PruneCursor owns the original Request and all physical preparation across
// bounded public calls. Close drives these same cleanup stages, never a second
// snapshot, registry, transaction or accounting authority.
type PruneCursor struct {
	mu                                        sync.Mutex
	store                                     *Store
	db                                        *treedb.DB
	mode                                      CommitMode
	batchSize                                 int
	recordLimit, byteLimit                    uint64
	retention                                 *memtable.COWExternalLease
	retainedBytes                             uint64
	currentWork                               *iterator.OrdinalScanWork
	capture                                   treedb.NativeMVCCCaptureV6
	captureBegun                              bool
	projection                                *tree.ChargedProjection
	projectionAdmitted                        bool
	reader                                    backenddb.NativeMVCCFrameReaderV6
	value                                     valuelog.COWLeafContinuation
	codec                                     valuelog.COWZSTDContinuation
	template                                  valuelog.COWTemplateContinuation
	leaf                                      valuelog.COWLeafProjectionReader
	leafInput, leafRaw                        [64 << 10]byte
	namespace                                 [16]byte
	namespaceSize                             int
	input, raw                                [64 << 10]byte
	logical, previous, selected               [mvcckey.MaxEncodedKeySize]byte
	logicalSize, previousSize, selectedSize   int
	selectedBytes                             uint64
	selectedPending, haveAnchor, recordActive bool
	timestamp                                 uint64
	record                                    node.LeafEntry
	requestCaptured                           bool
	request                                   mvccadmission.Request
	preparation                               *backenddb.NativeMVCCCurrentDeleteV6
	prepared                                  backenddb.NativeMVCCCurrentDeleteV6
	loadingFloor, integrityDone               bool
	phase, closePhase                         uint8
	closed, complete                          bool
	failure                                   error
}

func (s *Store) pruneNativeV6(options PruneOptions) (stats PruneStats, err error) {
	if s == nil || s.db == nil {
		return stats, storageError("prune versions", treedb.ErrClosed)
	}
	if options.Mode != CommitRelaxed && options.Mode != CommitDurable {
		return stats, ErrInvalidCommitMode
	}
	if options.WorkRecords == 0 || options.WorkBytes == 0 {
		return stats, &iterator.OrdinalUnitTooLarge{Records: 1, Bytes: 1}
	}
	w := iterator.OrdinalScanWork{RecordLimit: options.WorkRecords, ByteLimit: options.WorkBytes}
	defer func() { stats.WorkRecords, stats.WorkBytes = w.Records, w.Bytes }()
	c := options.Cursor
	if c == nil {
		d, ok := s.db.(*treedb.DB)
		if !ok {
			return stats, ErrSnapshotUnavailable
		}
		budget, ready, e := d.NativeMVCCBudgetV6(&w)
		if !ready || e != nil {
			return stats, e
		}
		size := uint64(unsafe.Sizeof(PruneCursor{}))
		if !w.Reserve(1, size) {
			return stats, nil
		}
		admitted, ready, e := budget.AcquireRetentionWork(retainedalloc.AllocationCharge(size), &w)
		if !ready || e != nil {
			return stats, e
		}
		lease := admitted.(*memtable.COWExternalLease)
		c = &PruneCursor{store: s, db: d, mode: options.Mode, batchSize: options.BatchSize, recordLimit: options.WorkRecords, byteLimit: options.WorkBytes, retention: lease, retainedBytes: retainedalloc.AllocationCharge(size), loadingFloor: true}
		c.codec.Owner, c.codec.Retention = c, c
		c.value.ZSTD, c.value.Template = &c.codec, &c.template
		c.namespaceSize = len(mvcckey.AppendNamespaceLower(c.namespace[:0]))
		c.leaf = valuelog.COWLeafProjectionReader{Input: c.leafInput[:], Raw: c.leafRaw[:], Limits: valuelog.COWReadLimits{MaxRecordBytes: uint64(len(c.leafInput)), MaxRawBytes: uint64(len(c.leafRaw)), MaxValueBytes: 4096}, VerifyCRC: true, ZSTD: &c.codec, Template: &c.template}
		c.reader.Leaf = &c.leaf
	}
	stats.Cursor = c
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.store != s || c.mode != options.Mode || c.batchSize != options.BatchSize || c.recordLimit != options.WorkRecords || c.byteLimit != options.WorkBytes {
		return stats, mvccadmission.ErrRequestInvalid
	}
	if c.closed {
		return stats, treedb.ErrClosed
	}
	if c.failure != nil {
		return stats, c.failure
	}
	c.currentWork = &w
	defer func() {
		c.currentWork = nil
		if err != nil {
			c.failure = err
		}
	}()
	// Only this call owns maintenance admission. Foreground Store writes remain
	// legal between every bounded return; no original mutex escapes the call.
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	err = c.step(&stats, &w)
	if err != nil {
		err = fmt.Errorf("native prune phase %d: %w", c.phase, err)
	}
	stats.Complete = c.complete
	return stats, err
}

func (c *PruneCursor) RetainCodecBytes(n uint64) bool {
	if n > ^uint64(0)-c.retainedBytes {
		return false
	}
	ready, err := c.retention.ResizeWork(c.retainedBytes+n, c.currentWork)
	if ready && err == nil {
		c.retainedBytes += n
		return true
	}
	return false
}
func (c *PruneCursor) ReleaseCodecBytes(n uint64) {
	if n > c.retainedBytes {
		panic("native MVCC codec retention underflow")
	}
	// The codec has already admitted and detached this concrete backing before
	// this synchronous scalar refund. No user callback or physical edge exists.
	if err := c.retention.Resize(c.retainedBytes - n); err != nil {
		panic(err)
	}
	c.retainedBytes -= n
}

func (c *PruneCursor) step(stats *PruneStats, w *iterator.OrdinalScanWork) error {
	for {
		switch c.phase {
		case 0:
			c.captureBegun = true
			snapshot, ready, err := c.db.StepNativeMVCCOwnedCaptureV6(&c.capture, w)
			if !ready || err != nil {
				return err
			}
			c.reader.Snapshot = snapshot
			c.phase = 1
		case 1:
			if !c.projectionAdmitted {
				if !w.Reserve(1, 64) {
					return nil
				}
				charge := tree.ChargedProjectionRetainedCharge()
				ready, err := c.retention.ResizeWork(c.retainedBytes+charge, w)
				if !ready || err != nil {
					return err
				}
				c.retainedBytes += charge
				c.projectionAdmitted = true
			}
			// Constructor byte/record refusal is a pause before the allocation.
			if w.Records >= w.RecordLimit || tree.ChargedProjectionRetainedCharge()+256 > w.ByteLimit-w.Bytes {
				return nil
			}
			certificate, err := c.reader.Snapshot.PrimaryReadRootCertificateV6()
			if err != nil {
				return err
			}
			projection, err := c.reader.Snapshot.NewNativeMVCCProjectionV6(certificate, nil, nil, &c.reader, w)
			if err != nil {
				return err
			}
			c.projection = projection
			c.phase = 2
		case 2:
			if !w.Reserve(1, 64) {
				return nil
			}
			if err := c.capture.BeginNativeProjectionV6(c.projection, tree.ProjectionSeekGE, discardFloorKey, w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 3
		case 3:
			ready, err := c.capture.StepNativeProjectionV6(c.projection, w)
			if !ready || err != nil {
				return err
			}
			c.phase = 4
		case 4:
			if !w.Reserve(1, 256) {
				return nil
			}
			record, valid, err := c.capture.NativeProjectionRecordV6(c.projection, w)
			if errors.Is(err, cowbtree.ErrWork) {
				return nil
			}
			if err != nil {
				return err
			}
			if !valid || !bytes.Equal(record.Key, discardFloorKey) {
				c.complete = true
				c.loadingFloor = false
				return nil
			}
			c.record = record
			if record.Flags&node.FlagPointer != 0 {
				c.phase = 8
			} else {
				c.phase = 19
			}

		case 5:
			if !w.Reserve(1, 64) {
				return nil
			}
			if err := c.capture.BeginNativeProjectionV6(c.projection, tree.ProjectionSeekGE, c.namespace[:c.namespaceSize], w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 6
		case 6:
			ready, err := c.capture.StepNativeProjectionV6(c.projection, w)
			if !ready || err != nil {
				return err
			}
			c.phase = 7
		case 7:
			if !w.Reserve(1, 256) {
				return nil
			}
			record, valid, err := c.capture.NativeProjectionRecordV6(c.projection, w)
			if errors.Is(err, cowbtree.ErrWork) {
				return nil
			}
			if err != nil {
				return err
			}
			if !valid || !mvcckey.InNamespace(record.Key) {
				c.integrityDone = true
				c.phase = 12
				continue
			}
			if !w.Reserve(1, 3*uint64(len(record.Key))) {
				return nil
			}
			logical, ts, err := mvcckey.DecodeAppend(c.logical[:0], record.Key)
			if err != nil {
				return errors.Join(ErrMalformedRecord, err)
			}
			c.logicalSize, c.timestamp, c.record = len(logical), ts, record
			if record.Flags&node.FlagPointer != 0 {
				c.phase = 8
			} else {
				c.phase = 10
			}
		case 8:
			if !w.Reserve(1, 128) {
				return nil
			}
			f, found, err := c.capture.NativePinnedValueLogFileV6(c.record.ValuePtr.FileID, w)
			if !found || err != nil {
				return err
			}
			ready, err := c.value.BeginValue(f, c.record.ValuePtr, valuelog.COWReadLimits{MaxRecordBytes: uint64(len(c.input)), MaxRawBytes: uint64(len(c.raw)), MaxValueBytes: uint64(len(c.raw))}, c.input[:], c.raw[:], true, w)
			if !ready || err != nil {
				return err
			}
			c.phase = 9
		case 9:
			record, ready, err := c.value.StepWithCodec(w)
			if !ready || err != nil {
				return err
			}
			c.record.Value = record
			if c.loadingFloor {
				c.phase = 19
			} else {
				c.phase = 10
			}
		case 10:
			if !w.Reserve(1, 2*uint64(c.logicalSize)+uint64(len(c.record.Value))+uint64(len(c.record.Key))+128) {
				return nil
			}
			state, err := validateRecord(c.record.Value)
			if err != nil {
				return err
			}
			same := c.previousSize == c.logicalSize && bytes.Equal(c.previous[:c.previousSize], c.logical[:c.logicalSize])
			if !same {
				copy(c.previous[:], c.logical[:c.logicalSize])
				c.previousSize = c.logicalSize
				c.haveAnchor = false
			}
			stats.Visited++
			size := uint64(len(c.record.Key) + len(c.record.Value))
			if c.timestamp > c.request.Floor() {
				stats.Skipped++
				stats.Retained++
				stats.RetainedBytes += size
			} else if c.haveAnchor {
				if !c.selectedPending {
					copy(c.selected[:], c.record.Key)
					c.selectedSize = len(c.record.Key)
					c.selectedBytes = size
					c.selectedPending = true
				}
			} else {
				c.haveAnchor = true
				// Tombstone anchor disposal follows all of its older physical records.
				// Selection of that last anchor is a separate current-output iteration.
				_ = state
				stats.Retained++
				stats.RetainedBytes += size
			}
			c.record = node.LeafEntry{}
			c.phase = 11
		case 11:
			if !w.Reserve(1, 64) {
				return nil
			}
			if err := c.capture.BeginNativeProjectionV6(c.projection, tree.ProjectionNext, nil, w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 6
		case 12:
			if !c.selectedPending {
				c.complete = true
				return nil
			}
			if !w.Reserve(1, uint64(c.selectedSize)+64) {
				return nil
			}
			if err := c.capture.BeginNativeProjectionV6(c.projection, tree.ProjectionSeekGE, c.selected[:c.selectedSize], w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 13
		case 13:
			ready, err := c.capture.StepNativeProjectionV6(c.projection, w)
			if !ready || err != nil {
				return err
			}
			c.phase = 14
		case 14:
			var ready bool
			var err error
			if c.capture.IsCompositeCOWV6() {
				var record node.LeafEntry
				record, ready, err = c.capture.NativeMVCCSelectedRecordV6(c.reader.Snapshot, w)
				if !ready || err != nil {
					return err
				}
				if !w.Reserve(1, uint64(c.selectedSize)+64) {
					return nil
				}
				if !bytes.Equal(record.Key, c.selected[:c.selectedSize]) {
					return tree.ErrProjectionOwner
				}
				ready, err = c.prepared.BindNativeMVCCCompositeDeleteV6(c.reader.Snapshot, &c.capture, c.request, w)
			} else {
				var selection tree.NativeMVCCSelectionV6
				selection, ready, err = c.projection.NativeMVCCSelectionV6(w)
				if !ready || err != nil {
					return err
				}
				if !w.Reserve(1, uint64(c.selectedSize)+64) {
					return nil
				}
				if !bytes.Equal(selection.Record().Key, c.selected[:c.selectedSize]) {
					return tree.ErrProjectionOwner
				}
				ready, err = c.prepared.BindNativeMVCCCurrentDeleteV6(c.reader.Snapshot, selection, c.request, w)
			}
			if !ready || err != nil {
				return err
			}
			c.preparation = &c.prepared
			c.phase = 15
		case 15:
			ready, err := c.preparation.StepNativeMVCCCurrentDeleteV6(w)
			if !ready || err != nil {
				return err
			}
			c.phase = 16
			// A prepared return must permit legal intervening writes. Current-root
			// composition has not run and the immutable Request remains unchanged.
			return nil
		case 19:
			if !w.Reserve(1, uint64(len(c.record.Value))+128) {
				return nil
			}
			if len(c.record.Value) != 9 || c.record.Value[0] != discardFloorRecordV1 {
				return ErrMalformedRecord
			}
			floor := binary.BigEndian.Uint64(c.record.Value[1:])
			if floor == 0 {
				return ErrMalformedRecord
			}
			request, ready, err := c.store.admission.CaptureRequest(floor, w)
			if !ready || err != nil {
				return err
			}
			c.request, c.requestCaptured, c.loadingFloor = request, true, false
			c.record = node.LeafEntry{}
			c.phase = 5
		case 16:
			return rootpublication.ErrNativeMVCCRootWorkRequiredV6
		default:
			return nil
		}
	}
}

// Close is the ordinary unlimited driver of the SAME physical cleanup phases.
// On hard refusal every original operand and its admission remain reachable in
// this returned cursor; repeated Close reports the first cause without replay.
func (c *PruneCursor) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	lease, err := c.closeLockedV6()
	c.currentWork = nil
	c.mu.Unlock()
	c = nil
	if lease != nil {
		lease.Close()
	}
	return err
}

func (c *PruneCursor) closeLockedV6() (retainedalloc.Lease, error) {
	if c.closed {
		return nil, nil
	}
	w := iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	c.currentWork = &w
	for {
		switch c.closePhase {
		case 0:
			if c.preparation != nil {
				ready, err := c.preparation.CloseNativeMVCCCurrentDeleteStepV6(&w)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
				c.preparation = nil
			}
			c.closePhase = 1
		case 1:
			if c.projection != nil {
				ready, err := c.projection.CloseStep(&w)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
				c.projection = nil
			}
			c.closePhase = 2
		case 2:
			ready, err := c.value.CancelWithCodec(&w)
			if err != nil {
				return nil, err
			}
			if !ready {
				continue
			}
			c.record = node.LeafEntry{}
			c.reader = backenddb.NativeMVCCFrameReaderV6{}
			c.closePhase = 3
		case 3:
			if c.captureBegun {
				ready, err := c.capture.CloseNativeMVCCOwnedCaptureV6(&w)
				if err != nil {
					return nil, err
				}
				if !ready {
					continue
				}
			}
			c.closePhase = 4
		case 4:
			c.store, c.db = nil, nil
			c.request = mvccadmission.Request{}
			c.codec = valuelog.COWZSTDContinuation{}
			c.closed = true
			lease := c.retention
			c.retention = nil
			return lease, nil
		}
	}
}
