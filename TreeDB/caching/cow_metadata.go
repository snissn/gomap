package caching

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

// validateCOWCanonicalEntries runs inside the backend's borrowed pre-frame
// metadata authority. It never retains payload/lookup, reads a buffered pointer
// independently, or treats a self-contained RID as lacking a live dependency.
func validateCOWCanonicalEntries(payload []byte, entries []batch.Entry, lookup func(page.ValuePtr) (uint64, bool)) error {
	index := 0
	err := commitlog.ScanRawKVBatchPayloadWithRevision(payload, func(op commitlog.RawKVOp, key, value []byte, revision uint64) error {
		if index >= len(entries) {
			return fmt.Errorf("COW canonical operation overflow")
		}
		e := &entries[index]
		index++
		return validateCOWCanonicalEntry(index-1, e, op, key, value, revision, lookup)
	})
	if err != nil {
		return err
	}
	if index != len(entries) {
		return fmt.Errorf("COW canonical operation count mismatch: %d/%d", index, len(entries))
	}
	return nil
}

func validateCOWCanonicalEntry(index int, e *batch.Entry, op commitlog.RawKVOp, key, value []byte, revision uint64, lookup func(page.ValuePtr) (uint64, bool)) error {
	if !bytes.Equal(key, e.Key) || revision != uint64(e.Revision) {
		return fmt.Errorf("COW canonical key/revision mismatch at %d", index)
	}
	if e.Type == batch.OpDelete {
		if op != commitlog.RawKVOpDelete {
			return fmt.Errorf("COW canonical delete mismatch at %d", index)
		}
		return nil
	}
	if e.Type != batch.OpPut {
		return ErrCOWUnsupported
	}
	if !e.IsPtr {
		if op != commitlog.RawKVOpSet || !bytes.Equal(value, e.Value) {
			return fmt.Errorf("COW canonical inline mismatch at %d", index)
		}
		return nil
	}
	if (op != commitlog.RawKVOpSetRID && op != commitlog.RawKVOpSetMaterializedRID) || lookup == nil || len(value) < 8 {
		return fmt.Errorf("COW canonical pointer operation mismatch at %d", index)
	}
	rid, ok := lookup(e.ValuePtr)
	if !ok || rid == 0 || rid != binary.LittleEndian.Uint64(value[:8]) {
		return fmt.Errorf("COW canonical RID mismatch at %d", index)
	}
	if op == commitlog.RawKVOpSetMaterializedRID && !bytes.Equal(value[8:], e.Value) {
		return fmt.Errorf("COW canonical materialized value mismatch at %d", index)
	}
	return nil
}
