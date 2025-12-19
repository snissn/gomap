package hashdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

const (
	slabKeyLenControl = ^uint64(0)

	// FlagControl marks slab records used for batch begin/commit markers.
	FlagControl = 0x40

	controlBatchBegin  = 1
	controlBatchCommit = 2
)

// ApplyBatch applies a set of operations atomically (all-or-nothing) in the in-memory index.
// It is not guaranteed durable on power loss; use ApplyBatchSync for a durable commit.
func (h *DB) ApplyBatch(ops []BatchOp) error {
	return h.applyBatch(ops, false)
}

// ApplyBatchSync applies a set of operations atomically and fsyncs the slab value log so
// the full batch survives a crash/power loss.
func (h *DB) ApplyBatchSync(ops []BatchOp) error {
	return h.applyBatch(ops, true)
}

type preparedBatchOp struct {
	op        BatchOp
	val       []byte
	valFlags  uint8
	recordLen int
}

type appliedRecord struct {
	op      BatchOp
	slabOff SlabOffset
}

func (h *DB) applyBatch(ops []BatchOp, sync bool) error {
	if len(ops) == 0 {
		return nil
	}

	batchID := h.batchSeq + 1
	h.batchSeq = batchID

	prepared := make([]preparedBatchOp, 0, len(ops))
	totalBytes := 0

	beginRecLen := 16 + 9 // header + (type byte + batchID)
	commitRecLen := 16 + 9
	totalBytes += beginRecLen + commitRecLen

	for _, op := range ops {
		if len(op.Key) == 0 {
			return fmt.Errorf("apply batch: empty key not supported")
		}
		switch op.Type {
		case BatchOpPut:
			val := op.Value
			var flags uint8
			if compressed, ok := compressValueIfEnabled(h.compressionEnabled, val); ok {
				val = compressed
				flags |= FlagCompressed
			}
			recLen := 16 + len(op.Key) + len(val)
			prepared = append(prepared, preparedBatchOp{
				op:        op,
				val:       val,
				valFlags:  flags,
				recordLen: recLen,
			})
			totalBytes += recLen
		case BatchOpDelete:
			recLen := 16 + len(op.Key)
			prepared = append(prepared, preparedBatchOp{
				op:        op,
				recordLen: recLen,
			})
			totalBytes += recLen
		default:
			return fmt.Errorf("apply batch: unknown op type %d", op.Type)
		}
	}

	maxSegmentSize := atomic.LoadInt64(&MaxSegmentSize)
	if int64(totalBytes) > maxSegmentSize {
		return fmt.Errorf("apply batch: batch too large (%d bytes) for max segment size (%d bytes)", totalBytes, maxSegmentSize)
	}

	prevSegmentID := h.activeSegmentID
	if h.activeSegmentSize+int64(totalBytes) > maxSegmentSize {
		if err := h.rotateSlabSegment(); err != nil {
			return err
		}
	}

	f := h.slabFiles[h.activeSegmentID]
	if f == nil {
		return fmt.Errorf("apply batch: missing slab-%d", h.activeSegmentID)
	}

	startOffset := *h.slabOffset
	currOffset := startOffset

	buf := make([]byte, 0, totalBytes)
	var header [16]byte

	// Begin marker.
	binary.LittleEndian.PutUint64(header[:8], slabKeyLenControl)
	binary.LittleEndian.PutUint64(header[8:], packLength(9, FlagControl))
	buf = append(buf, header[:]...)
	buf = append(buf, controlBatchBegin)
	var idScratch [8]byte
	binary.LittleEndian.PutUint64(idScratch[:], batchID)
	buf = append(buf, idScratch[:]...)
	currOffset += SlabOffset(beginRecLen)

	applied := make([]appliedRecord, 0, len(prepared))

	for _, p := range prepared {
		switch p.op.Type {
		case BatchOpPut:
			recordOffset := currOffset

			binary.LittleEndian.PutUint64(header[:8], uint64(len(p.op.Key)))
			binary.LittleEndian.PutUint64(header[8:], packLength(uint64(len(p.val)), p.valFlags))
			buf = append(buf, header[:]...)
			buf = append(buf, p.op.Key...)
			buf = append(buf, p.val...)

			applied = append(applied, appliedRecord{op: p.op, slabOff: recordOffset})
			currOffset += SlabOffset(p.recordLen)
		case BatchOpDelete:
			recordOffset := currOffset

			binary.LittleEndian.PutUint64(header[:8], uint64(len(p.op.Key)))
			binary.LittleEndian.PutUint64(header[8:], ^uint64(0))
			buf = append(buf, header[:]...)
			buf = append(buf, p.op.Key...)

			applied = append(applied, appliedRecord{op: p.op, slabOff: recordOffset})
			currOffset += SlabOffset(p.recordLen)
		default:
			return fmt.Errorf("apply batch: unknown op type %d", p.op.Type)
		}
	}

	// Commit marker.
	binary.LittleEndian.PutUint64(header[:8], slabKeyLenControl)
	binary.LittleEndian.PutUint64(header[8:], packLength(9, FlagControl))
	buf = append(buf, header[:]...)
	buf = append(buf, controlBatchCommit)
	buf = append(buf, idScratch[:]...)

	if _, err := writeAll(f, buf); err != nil {
		return err
	}
	h.activeSegmentSize += int64(len(buf))
	*h.slabOffset = currOffset + SlabOffset(commitRecLen)

	if sync {
		if err := h.syncActiveSegment(prevSegmentID); err != nil {
			return err
		}
	}

	// Apply to index. If this fails, force a rebuild from the slab log so the
	// in-process state is consistent with the committed batch.
	if err := h.applyBatchToIndex(applied); err != nil {
		_ = h.Recover()
		return err
	}

	return nil
}

func (h *DB) rotateSlabSegment() error {
	h.activeSegmentID++
	newFilename := fmt.Sprintf("%s/slab-%d", h.dir, h.activeSegmentID)
	newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if h.slabFiles == nil {
		h.slabFiles = make(map[uint16]*os.File)
	}
	h.slabFiles[h.activeSegmentID] = newF
	*h.slabOffset = SlabOffset(uint64(h.activeSegmentID) << OffsetBits)
	h.activeSegmentSize = 0
	return nil
}

func (h *DB) applyBatchToIndex(records []appliedRecord) error {
	puts := 0

	for _, r := range records {
		switch r.op.Type {
		case BatchOpPut:
			if err := h.addBucket(r.op.Key, Key{slabOffset: r.slabOff, hash: hash(r.op.Key)}); err != nil {
				return err
			}
			puts++
		case BatchOpDelete:
			if _, err := h.deleteWithoutLog(r.op.Key, hash(r.op.Key)); err != nil {
				return err
			}
		default:
			return fmt.Errorf("apply batch: unknown op type %d", r.op.Type)
		}
	}

	if h.rehashInProgress {
		steps := uint64(puts) * rehashBucketsPerWrite
		if err := h.rehashStep(steps); err != nil {
			return err
		}
	}
	return nil
}

func (h *DB) deleteWithoutLog(key []byte, keyHash Hash) (bool, error) {
	foundNew := false

	if len(h.keys) > 0 && h.capacity > 0 {
		idx, found, err := h.probeIndexWithHash(h.keys, h.controls, h.capacity, key, keyHash)
		if err != nil {
			return false, err
		}
		if found {
			h.keys[idx].slabOffset = Tombstone
			h.setDeleted(idx)
			*h.count -= 1
			foundNew = true
		}
	}

	if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
		idx, found, err := h.probeIndexWithHash(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key, keyHash)
		if err != nil {
			return false, err
		}
		if found {
			h.rehashOldKeys[idx].slabOffset = Tombstone
		}
	}

	return foundNew, nil
}

func writeAll(w io.Writer, b []byte) (int, error) {
	total := 0
	for len(b) > 0 {
		n, err := w.Write(b)
		total += n
		b = b[n:]
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrShortWrite
		}
	}
	return total, nil
}
