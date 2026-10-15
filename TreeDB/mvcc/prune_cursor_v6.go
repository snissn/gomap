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
	mu                                                  sync.Mutex
	store                                               *Store
	db                                                  *treedb.DB
	mode                                                CommitMode
	batchSize                                           int
	recordLimit, byteLimit                              uint64
	retention                                           *memtable.COWExternalLease
	retainedBytes                                       uint64
	currentWork                                         *iterator.OrdinalScanWork
	capture                                             treedb.NativeMVCCCaptureV6
	captureBegun                                        bool
	projection                                          *tree.ChargedProjection
	projectionAdmitted                                  bool
	reader                                              backenddb.NativeMVCCFrameReaderV6
	outputReader                                        backenddb.NativeMVCCFrameReaderV6
	outputLeaf                                          valuelog.COWLeafProjectionReader
	outputInput, outputRaw                              [64 << 10]byte
	value                                               valuelog.COWLeafContinuation
	codec                                               valuelog.COWZSTDContinuation
	template                                            valuelog.COWTemplateContinuation
	leaf                                                valuelog.COWLeafProjectionReader
	leafInput, leafRaw                                  [64 << 10]byte
	namespace                                           [16]byte
	namespaceSize                                       int
	input, raw                                          [64 << 10]byte
	logical, previous, selected                         [mvcckey.MaxEncodedKeySize]byte
	logicalSize, previousSize, selectedSize             int
	selectedBytes                                       uint64
	batchEntries                                        []node.LeafEntry
	batchBytes, batchPayloadCharge                      uint64
	batchClear                                          int
	selectionBuffered, batchFlushing, flushBeforeAnchor bool
	selectedPending, haveAnchor, recordActive           bool
	anchorKey, resumeKey                                [mvcckey.MaxEncodedKeySize]byte
	anchorSize, resumeSize                              int
	anchorBytes                                         uint64
	anchorTombstone, resumeAfterAnchor                  bool
	namespaceEnd                                        bool
	timestamp                                           uint64
	record                                              node.LeafEntry
	requestCaptured                                     bool
	request                                             mvccadmission.Request
	preparation                                         *backenddb.NativeMVCCCurrentDeleteV6
	prepared                                            backenddb.NativeMVCCCurrentDeleteV6
	loadingFloor                                        bool
	// integrityDone certifies the floor, retained anchor and exact selected
	// key/value integrity for the selected batch, not global namespace validation.
	integrityDone     bool
	phase, closePhase uint8
	closed, complete  bool
	failure           error
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
	if options.BatchSize <= 0 {
		options.BatchSize = 256
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
		c.outputLeaf = valuelog.COWLeafProjectionReader{Input: c.outputInput[:], Raw: c.outputRaw[:], Limits: valuelog.COWReadLimits{MaxRecordBytes: uint64(len(c.outputInput)), MaxRawBytes: uint64(len(c.outputRaw)), MaxValueBytes: 4096}, VerifyCRC: true, ZSTD: &c.codec, Template: &c.template}
		c.outputReader.Leaf = &c.outputLeaf
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

// beginBatchFlushV6 repositions the SAME retained projection on its last
// verified selection, so the original constructor binds a real producer row.
func (c *PruneCursor) beginBatchFlushV6() {
	last := c.batchEntries[len(c.batchEntries)-1]
	copy(c.selected[:], last.Key)
	c.selectedSize = len(last.Key)
	c.selectedPending, c.integrityDone, c.batchFlushing = true, true, true
	c.phase = 12
}

func (c *PruneCursor) step(stats *PruneStats, w *iterator.OrdinalScanWork) error {
	if c.complete {
		return nil
	}
	if c.batchEntries == nil {
		size := uint64(unsafe.Sizeof(node.LeafEntry{}))
		if uint64(c.batchSize) > ^uint64(0)/size {
			return &iterator.OrdinalUnitTooLarge{Records: 1, Bytes: ^uint64(0)}
		}
		size *= uint64(c.batchSize)
		if size > w.ByteLimit {
			return &iterator.OrdinalUnitTooLarge{Records: 1, Bytes: size}
		}
		if !w.Reserve(1, size) {
			return nil
		}
		charge := retainedalloc.AllocationCharge(size)
		if charge > ^uint64(0)-c.retainedBytes {
			return &iterator.OrdinalUnitTooLarge{Records: 1, Bytes: charge}
		}
		ready, err := c.retention.ResizeWork(c.retainedBytes+charge, w)
		if !ready || err != nil {
			return err
		}
		c.retainedBytes += charge
		c.batchEntries = make([]node.LeafEntry, 0, c.batchSize)
	}
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
				if charge > ^uint64(0)-c.retainedBytes {
					return &iterator.OrdinalUnitTooLarge{Records: 1, Bytes: charge}
				}
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
			if err := c.projection.Begin(tree.ProjectionSeekGE, discardFloorKey, w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 3
		case 3:
			ready, err := c.projection.Step(w)
			if !ready || err != nil {
				return err
			}
			c.phase = 4
		case 4:
			if !w.Reserve(1, 256) {
				return nil
			}
			record, valid, err := c.projection.Record(w)
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
			if err := c.projection.Begin(tree.ProjectionSeekGE, c.namespace[:c.namespaceSize], w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 6
		case 6:
			ready, err := c.projection.Step(w)
			if !ready || err != nil {
				return err
			}
			c.phase = 7
		case 7:
			if !w.Reserve(1, 256) {
				return nil
			}
			record, valid, err := c.projection.Record(w)
			if errors.Is(err, cowbtree.ErrWork) {
				return nil
			}
			if err != nil {
				return err
			}
			if !valid || !mvcckey.InNamespace(record.Key) {
				c.namespaceEnd = true
				if len(c.batchEntries) != 0 {
					c.flushBeforeAnchor = c.anchorTombstone
					c.beginBatchFlushV6()
					continue
				}
				if c.anchorTombstone {
					if !w.Reserve(1, uint64(c.anchorSize)+128) {
						return nil
					}
					copy(c.selected[:], c.anchorKey[:c.anchorSize])
					c.selectedSize, c.selectedBytes = c.anchorSize, c.anchorBytes
					c.selectedPending, c.integrityDone = true, true
					c.resumeAfterAnchor, c.namespaceEnd = true, true
					c.phase = 12
					continue
				}
				c.complete = true
				return nil
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
			f, _, err := c.reader.Snapshot.PinnedValueLogFile(c.record.ValuePtr.FileID)
			if err != nil {
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
			if !w.Reserve(1, 2*uint64(c.logicalSize)+uint64(len(c.record.Value))+3*uint64(len(c.record.Key))+uint64(c.anchorSize)+128) {
				return nil
			}
			state, err := validateRecord(c.record.Value)
			if err != nil {
				return err
			}
			same := c.previousSize == c.logicalSize && bytes.Equal(c.previous[:c.previousSize], c.logical[:c.logicalSize])
			if !same && c.anchorTombstone {
				// Older versions must be fenced and ACKed before
				// their tombstone anchor disappears. Flush any retained batch first.
				// Keep the next logical key as a seek boundary, not a second scan.
				copy(c.resumeKey[:], c.record.Key)
				c.resumeSize = len(c.record.Key)
				if len(c.batchEntries) != 0 {
					c.flushBeforeAnchor = true
					c.record = node.LeafEntry{}
					c.beginBatchFlushV6()
					continue
				}
				copy(c.selected[:], c.anchorKey[:c.anchorSize])
				c.selectedSize, c.selectedBytes = c.anchorSize, c.anchorBytes
				c.selectedPending, c.integrityDone = true, true
				c.resumeAfterAnchor = true
				c.record = node.LeafEntry{}
				c.phase = 12
				continue
			}
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
				// The retained newest-at-floor anchor and this exact record have
				// passed key/envelope/CRC validation. Select while the original
				// projection is stationary; unrelated later keys are not prerequisites.
				copy(c.selected[:], c.record.Key)
				c.selectedSize, c.selectedBytes = len(c.record.Key), size
				c.selectedPending, c.integrityDone = true, true
				c.record = node.LeafEntry{}
				c.phase = 14
				continue
			} else {
				c.haveAnchor = true
				c.anchorTombstone = state == Tombstone
				if c.anchorTombstone {
					copy(c.anchorKey[:], c.record.Key)
					c.anchorSize, c.anchorBytes = len(c.record.Key), size
				} else {
					stats.Retained++
					stats.RetainedBytes += size
				}
			}
			c.record = node.LeafEntry{}
			c.phase = 11
		case 11:
			if !w.Reserve(1, 64) {
				return nil
			}
			if err := c.projection.Begin(tree.ProjectionNext, nil, w); err != nil {
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
			if err := c.projection.Begin(tree.ProjectionSeekGE, c.selected[:c.selectedSize], w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 13
		case 13:
			ready, err := c.projection.Step(w)
			if !ready || err != nil {
				return err
			}
			c.phase = 14
		case 14:
			var selectedRecord node.LeafEntry
			var ready bool
			var err error
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
			selectedRecord = selection.Record()
			if !c.selectionBuffered && !c.batchFlushing {
				value := selectedRecord.Value
				if selectedRecord.Flags&node.FlagPointer != 0 {
					value = nil
				}
				n := uint64(len(selectedRecord.Key) + len(value))
				if !w.Reserve(1, n*2+uint64(unsafe.Sizeof(selectedRecord))) {
					return nil
				}
				charge := retainedalloc.AllocationCharge(uint64(len(selectedRecord.Key)))
				if len(value) != 0 {
					charge += retainedalloc.AllocationCharge(uint64(len(value)))
				}
				ready, err := c.retention.ResizeWork(c.retainedBytes+charge, w)
				if !ready || err != nil {
					return err
				}
				c.retainedBytes += charge
				selectedRecord.Key = bytes.Clone(selectedRecord.Key)
				selectedRecord.Value = bytes.Clone(value)
				c.batchEntries = append(c.batchEntries, selectedRecord)
				c.batchPayloadCharge += charge
				c.batchBytes += c.selectedBytes
				c.selectionBuffered = true
			}
			if len(c.batchEntries) < c.batchSize && !c.batchFlushing && !c.resumeAfterAnchor {
				c.selectionBuffered, c.selectedPending, c.integrityDone = false, false, false
				c.phase = 11
				continue
			}
			ready, err = c.prepared.BindNativeMVCCCurrentDeleteV6(c.reader.Snapshot, selection, c.request, w)

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
			ready, err = c.preparation.BindNativeMVCCDeleteBatchV6(c.batchEntries, w)
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
			c.preparation.BindNativeMVCCOutputReaderV6(&c.outputReader)
			ready, err := c.preparation.StepNativeMVCCBackendMaterializedOutputV6(false, w)
			if !ready || err != nil {
				return err
			}
			c.phase = 24
		case 24:
			ready, err := c.preparation.StepNativeMVCCBackendRootV6(w)
			if !ready || err != nil {
				return err
			}
			// A sealed private directory still confers no visible/durable/physical
			// completion. Private COW/member/seal preparation remains required.
			c.phase = 25
		case 25:
			ready, err := c.preparation.StepNativeMVCCAllocatorV6(w)
			if !ready || err != nil {
				return err
			}
			c.phase = 26
		case 26:
			ready, err := c.preparation.StepNativeMVCCCandidateV6(w)
			if !ready || err != nil {
				return err
			}
			ready, err = c.preparation.StepNativeMVCCSealPreparationV6(w)
			if !ready || err != nil {
				return err
			}
			return rootpublication.ErrNativeMVCCRootWorkRequiredV6
		case 22:
			stats.Pruned += uint64(len(c.batchEntries))
			stats.PrunedBytes += c.batchBytes
			stats.Batches++
			// ACK covers this exact finished transaction only. The retained scan
			// still owns later history and may subsequently report a real error.
			c.phase = 20
			return nil
		case 20:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(c.prepared))+128) {
				return nil
			}
			if c.preparation != nil {
				ready, err := c.preparation.CloseNativeMVCCCurrentDeleteStepV6(w)
				if !ready || err != nil {
					return err
				}
				c.preparation = nil
				c.prepared = backenddb.NativeMVCCCurrentDeleteV6{}
			}
			for c.batchClear < len(c.batchEntries) {
				if !w.Reserve(1, uint64(unsafe.Sizeof(node.LeafEntry{}))) {
					return nil
				}
				c.batchEntries[c.batchClear] = node.LeafEntry{}
				c.batchClear++
			}
			if !w.Reserve(1, 64) {
				return nil
			}
			c.batchEntries = c.batchEntries[:0]
			c.ReleaseCodecBytes(c.batchPayloadCharge)
			c.batchPayloadCharge, c.batchBytes, c.batchClear = 0, 0, 0
			c.selectionBuffered, c.batchFlushing = false, false
			c.selectedPending, c.integrityDone = false, false
			c.selectedSize, c.selectedBytes = 0, 0
			if c.flushBeforeAnchor {
				c.flushBeforeAnchor = false
				copy(c.selected[:], c.anchorKey[:c.anchorSize])
				c.selectedSize, c.selectedBytes = c.anchorSize, c.anchorBytes
				c.selectedPending, c.integrityDone, c.resumeAfterAnchor = true, true, true
				c.phase = 12
				continue
			}
			if c.resumeAfterAnchor {
				c.resumeAfterAnchor, c.anchorTombstone, c.haveAnchor = false, false, false
				c.anchorSize, c.anchorBytes = 0, 0
				if c.namespaceEnd {
					c.complete = true
					return nil
				}
				c.phase = 21
			} else if c.namespaceEnd {
				c.complete = true
				return nil
			} else {
				c.phase = 11
			}
		case 21:
			if !w.Reserve(1, uint64(c.resumeSize)+64) {
				return nil
			}
			if err := c.projection.Begin(tree.ProjectionSeekGE, c.resumeKey[:c.resumeSize], w); err != nil {
				if errors.Is(err, cowbtree.ErrWork) {
					return nil
				}
				return err
			}
			c.phase = 6
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
	w := iterator.OrdinalScanWork{RecordLimit: c.recordLimit, ByteLimit: c.byteLimit}
	c.currentWork = &w
	for {
		w.Records, w.Bytes = 0, 0
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
				c.prepared = backenddb.NativeMVCCCurrentDeleteV6{}
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
			ready, err = c.outputReader.CancelProjectionFrame(&w)
			if err != nil {
				return nil, err
			}
			if !ready {
				continue
			}
			c.outputReader = backenddb.NativeMVCCFrameReaderV6{}
			c.outputLeaf = valuelog.COWLeafProjectionReader{}
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
			clear(c.batchEntries)
			c.batchEntries = nil
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
