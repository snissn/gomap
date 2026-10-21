package treedb

import (
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

var errLogicalMVCCInput = errors.New("treedb: logical MVCC operands do not match admitted input")

// SupportsLogicalMVCC identifies the optional logical-history data plane. The
// original COW cache still owns preparation, durability and publication.
func (db *DB) SupportsLogicalMVCC() bool {
	cached, _, err := db.captureReadOwners()
	return err == nil && cached != nil && cached.SupportsLogicalMVCC()
}

// CommitLogicalMVCC publishes one Store-admitted group through the ordinary
// native batch producer. Application timestamps remain distinct from native
// entry revisions and from the publication sequence. Delete creates a logical
// MVCC tombstone, rather than deleting a retained physical version.
//
// The initial projection deliberately uses the canonical physical command
// format. Its admission input follows each owned physical key into the same
// accepted publication cut; this method grants no independent authority.
func (db *DB) CommitLogicalMVCC(mutations []mvccadmission.LogicalMutation, input mvccadmission.LogicalInput, syncWrite bool) (err error) {
	if err := db.beginPublicOperation(); err != nil {
		return err
	}
	if db.cached == nil || !db.cached.SupportsLogicalMVCC() {
		db.lifecycleMu.RUnlock()
		return caching.ErrCOWUnsupported
	}
	if len(mutations) == 0 {
		db.lifecycleMu.RUnlock()
		return nil
	}
	if !input.Matches(mutations) {
		db.lifecycleMu.RUnlock()
		return errLogicalMVCCInput
	}

	// The producer owns this temporary encoding, so admit its concrete backing
	// before allocation. One arena holds every key and envelope unchanged until
	// batch Close has cleared all input aliases. The ordinary batch separately
	// admits and owns the bytes that cross its publication boundary.
	encodedBytes := 0
	const maxInt = int(^uint(0) >> 1)
	for i := range mutations {
		m := &mutations[i]
		keyBytes, keyErr := mvcckey.EncodedLen(m.Key)
		if keyErr != nil {
			db.lifecycleMu.RUnlock()
			return fmt.Errorf("logical MVCC operand %d: %w", i, keyErr)
		}
		valueBytes := 1
		if !m.Delete {
			if len(m.Value) == maxInt {
				db.lifecycleMu.RUnlock()
				return memtable.ErrCOWCapacity
			}
			valueBytes += len(m.Value)
		}
		if keyBytes > maxInt-encodedBytes || valueBytes > maxInt-encodedBytes-keyBytes {
			db.lifecycleMu.RUnlock()
			return memtable.ErrCOWCapacity
		}
		encodedBytes += keyBytes + valueBytes
	}
	lease, leaseErr := db.cached.AcquireCOWAllocation(memtable.COWAllocationCharge(uint64(encodedBytes)))
	if leaseErr != nil {
		db.lifecycleMu.RUnlock()
		return leaseErr
	}
	var b Batch
	if len(mutations) > 1 {
		b = db.cached.NewBatchWithSize(len(mutations))
		if db.commandWALCached {
			b = newCommandWALPublicBatch(db, b, len(mutations))
		}
	}
	// Batch Write takes its own public admission. As with ordinary COW point
	// writes, release this read lock before entering that boundary.
	db.lifecycleMu.RUnlock()
	var encoded []byte
	defer func() {
		if b != nil {
			err = errors.Join(err, b.Close())
		}
		encoded = nil
		lease.Close()
	}()
	var qualified interface {
		SetWithMVCCInput([]byte, []byte, mvccadmission.Input) error
	}
	if b != nil {
		if _, err := b.GetByteSize(); err != nil {
			return err
		}
		var ok bool
		qualified, ok = b.(interface {
			SetWithMVCCInput([]byte, []byte, mvccadmission.Input) error
		})
		if !ok {
			return caching.ErrCOWUnsupported
		}
	}
	encoded = make([]byte, 0, encodedBytes)
	for i := range mutations {
		m := &mutations[i]
		keyStart := len(encoded)
		encoded, err = mvcckey.Append(encoded, m.Key, m.Timestamp)
		if err != nil {
			return err
		}
		keyEnd := len(encoded)
		if m.Delete {
			encoded = append(encoded, 0x02)
		} else {
			encoded = append(encoded, 0x01)
			encoded = append(encoded, m.Value...)
		}
		physical := encoded[keyStart:keyEnd:keyEnd]
		record := encoded[keyEnd:len(encoded):len(encoded)]
		physicalInput := input.BindPhysical(i, physical)
		if !physicalInput.Present() {
			return errLogicalMVCCInput
		}
		if b == nil {
			return db.SetPointWithMVCCInput(physical, record, syncWrite, physicalInput)
		}
		if err := qualified.SetWithMVCCInput(physical, record, physicalInput); err != nil {
			return err
		}
	}
	if syncWrite {
		return b.WriteSync()
	}
	return b.Write()
}
