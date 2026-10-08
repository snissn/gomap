package iterator

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"unsafe"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/node"
)

// InlineRecordSelection is a call-local value selector. No selector, iterator,
// callback, or underlying page owner is retained in the returned records.
// An unknown key ends both passes, preserving the existing manifest scan.
type InlineRecordSelection struct {
	Start         []byte
	RequiredExact []byte // independently seek one required record before the scan
	Exact         [2][]byte
	Prefixes      [8][]byte
}

func (s InlineRecordSelection) matches(key []byte) bool {
	for _, exact := range s.Exact {
		if exact != nil && bytes.Equal(key, exact) {
			return true
		}
	}
	for _, prefix := range s.Prefixes {
		if len(prefix) != 0 && bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

type OwnedInlineRecord struct{ Key, Value []byte }
type InlineRecordAccount interface{ ReserveStableMetadata(uint64) error }

var ErrInlineRecordRequired = errors.New("iterator: selected records must be inline")
var ErrInlineRecordChanged = errors.New("iterator: pinned selected records changed during owned copy")
var ErrInlineRecordMissing = errors.New("iterator: required exact inline record missing")
var ErrInlineRecordLimit = errors.New("iterator: unsupported owned record backing")

func hashInlineRecord(d *xxhash.Digest, key, value []byte) {
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(key)))
	_, _ = d.Write(size[:])
	_, _ = d.Write(key)
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = d.Write(size[:])
	_, _ = d.Write(value)
}

// CopyOwnedInlineRecords is the sole two-pass inline copier for ordinary and
// selected owned reads. It reserves all actual byte-copy classes and the exact
// record-array capacity before the first allocation. The iterator remains the
// caller's synchronous loan; it is never returned or retained here.
func CopyOwnedInlineRecords(it UnsafeIterator, selection InlineRecordSelection, account InlineRecordAccount) (records []OwnedInlineRecord, retErr error) {
	if it == nil {
		return nil, ErrInlineRecordLimit
	}
	complete := false
	defer func() {
		if !complete {
			ClearOwnedInlineRecords(records)
			records = nil
		}
	}()
	count := 0
	var charge uint64
	var expected xxhash.Digest
	expected.Reset()
	for phase := 0; phase < 2; phase++ {
		if phase == 0 {
			if selection.RequiredExact == nil {
				continue
			}
			it.Seek(selection.RequiredExact)
			if !it.Valid() {
				if err := it.Error(); err != nil {
					return nil, err
				}
				return nil, ErrInlineRecordMissing
			}
		} else {
			it.Seek(selection.Start)
		}
		for it.Valid() {
			key := it.UnsafeKey()
			if phase == 0 {
				if !bytes.Equal(key, selection.RequiredExact) || it.IsDeleted() {
					return nil, ErrInlineRecordMissing
				}
			} else if !selection.matches(key) {
				break
			}
			if it.IsDeleted() {
				it.Next()
				continue
			}
			value, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 {
				return nil, ErrInlineRecordRequired
			}
			if count == math.MaxInt {
				return nil, ErrInlineRecordLimit
			}
			count++
			hashInlineRecord(&expected, key, value)
			if account != nil {
				for _, raw := range [...]uint64{uint64(len(key)), uint64(len(value))} {
					n, err := allocclass.ClassBytes(raw, false)
					if err != nil || n > math.MaxUint64-charge {
						return nil, ErrInlineRecordLimit
					}
					charge += n
				}
			}
			if phase == 0 {
				break
			}
			it.Next()
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if account != nil {
		element := uint64(unsafe.Sizeof(OwnedInlineRecord{}))
		if uint64(count) > math.MaxUint64/element {
			return nil, ErrInlineRecordLimit
		}
		n, err := allocclass.ClassBytes(uint64(count)*element, true)
		if err != nil || n > math.MaxUint64-charge {
			return nil, ErrInlineRecordLimit
		}
		if err := account.ReserveStableMetadata(charge + n); err != nil {
			return nil, err
		}
	}
	remaining := charge
	var actual xxhash.Digest
	actual.Reset()
	records = make([]OwnedInlineRecord, 0, count)
	for phase := 0; phase < 2; phase++ {
		if phase == 0 {
			if selection.RequiredExact == nil {
				continue
			}
			it.Seek(selection.RequiredExact)
			if !it.Valid() {
				return records, ErrInlineRecordChanged
			}
		} else {
			it.Seek(selection.Start)
		}
		for it.Valid() {
			key := it.UnsafeKey()
			if phase == 0 {
				if !bytes.Equal(key, selection.RequiredExact) || it.IsDeleted() {
					return records, ErrInlineRecordChanged
				}
			} else if !selection.matches(key) {
				break
			}
			if it.IsDeleted() {
				it.Next()
				continue
			}
			value, _, flags := it.UnsafeEntry()
			if flags&node.FlagPointer != 0 || len(records) == count {
				return records, ErrInlineRecordChanged
			}
			if account != nil {
				for _, raw := range [...]uint64{uint64(len(key)), uint64(len(value))} {
					n, err := allocclass.ClassBytes(raw, false)
					if err != nil || n > remaining {
						return records, ErrInlineRecordChanged
					}
					remaining -= n
				}
			}
			hashInlineRecord(&actual, key, value)
			records = append(records, OwnedInlineRecord{Key: bytes.Clone(key), Value: bytes.Clone(value)})
			if phase == 0 {
				break
			}
			it.Next()
		}
	}
	if err := it.Error(); err != nil {
		return records, err
	}
	if len(records) != count || expected.Sum64() != actual.Sum64() || account != nil && remaining != 0 {
		return records, ErrInlineRecordChanged
	}
	complete = true
	return records, nil
}

// ClearOwnedInlineRecords discharges copied payload aliases as well as array
// entries on an abandoned result. Cumulative operation debit is never refunded.
func ClearOwnedInlineRecords(records []OwnedInlineRecord) {
	for i := range records {
		clear(records[i].Key)
		clear(records[i].Value)
		records[i] = OwnedInlineRecord{}
	}
}
