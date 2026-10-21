package caching

import (
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// StageLogicalMVCC encodes directly into this native batch's existing admitted
// immutable arena. The returned byte count is canonical point ingress, not a
// separate command payload. Original Input qualification, preparation, native
// revisions, WAL encoding and accepted publication remain unchanged.
func (b *Batch) StageLogicalMVCC(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput) (int, error) {
	if b == nil || b.cowState == nil {
		return 0, ErrCOWUnsupported
	}
	if b.cowState.err != nil {
		return 0, b.cowState.err
	}
	if b.closed {
		return 0, ErrBatchClosed
	}
	if len(mutations) == 0 {
		return 0, nil
	}
	if !input.Matches(mutations) {
		return 0, ErrCOWUnsupported
	}
	const maxInt = int(^uint(0) >> 1)
	if len(mutations) > maxInt-len(b.entries) {
		return 0, memtable.ErrCOWCapacity
	}
	total := 0
	for _, m := range mutations {
		n, err := mvcckey.EncodedLen(m.Key)
		if err != nil {
			return 0, err
		}
		valueBytes := 1
		if !m.Delete {
			if len(m.Value) == maxInt {
				return 0, memtable.ErrCOWCapacity
			}
			valueBytes += len(m.Value)
		}
		if n > maxInt-total || valueBytes > maxInt-total-n {
			return 0, memtable.ErrCOWCapacity
		}
		total += n + valueBytes
	}
	if b.size > maxInt-total {
		return 0, memtable.ErrCOWCapacity
	}
	b.cowReserve(len(b.entries) + len(mutations))
	if b.cowState.err != nil {
		return 0, b.cowState.err
	}
	owned, err := b.cowCopyStorage(total)
	if err != nil {
		return 0, err
	}
	encoded := owned[:0]
	for i, m := range mutations {
		start := len(encoded)
		encoded, err = mvcckey.Append(encoded, m.Key, m.Timestamp)
		if err != nil {
			return 0, err
		}
		end := len(encoded)
		if m.Delete {
			encoded = append(encoded, 0x02)
		} else {
			encoded = append(encoded, 0x01)
			encoded = append(encoded, m.Value...)
		}
		key, value := encoded[start:end:end], encoded[end:len(encoded):len(encoded)]
		physicalInput := input.BindPhysical(i, key)
		if !physicalInput.Present() {
			return 0, ErrCOWUnsupported
		}
		// These bytes already belong to cowCopyStorage. They are neither a
		// caller view nor a second copy and have exactly the batch lifetime.
		b.entries = append(b.entries, batch.Entry{Type: batch.OpPut, Key: key, Value: value, MVCCInput: physicalInput})
		b.noteEntryAppend()
	}
	b.cowState.shardAssignmentsReady = false
	b.size += total
	return total, nil
}
