package collections

import (
	"bytes"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// borrowedNamedDomain uses this read's one retained complete cut. Callers hold
// r.mu and use its original scratch and pointer/leaf readers; it never recaptures.
func (r *NativeJSONRead) borrowedNamedDomain(name string) (nativeDomainRead, error) {
	if r.capture == nil || r.closing || r.failure != nil {
		return nativeDomainRead{}, backenddb.ErrClosed
	}
	if r.collection == nil {
		return nativeDomainRead{}, tree.ErrKeyNotFound
	}
	domain, ok := r.collection.domains.Get(name)
	if !ok {
		return nativeDomainRead{}, tree.ErrKeyNotFound
	}
	return newNativeDomainRead(domain, r.capture.Snapshot())
}

func (r *NativeJSONRead) namedDomainGet(name string, key []byte) ([]byte, error) {
	return r.namedDomainGetAdmitted(name, key, nil)
}
func (r *NativeJSONRead) namedDomainGetAdmitted(name string, key []byte, admit func(uint64) error) ([]byte, error) {
	if r == nil {
		return nil, backenddb.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	domain, err := r.borrowedNamedDomain(name)
	if err != nil {
		return nil, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return nil, err
	}
	entry, err := domain.getEntry(key, scratch, r.readLeaf)
	if err != nil {
		return nil, err
	}
	return r.namedDomainValueAdmitted(entry, admit)
}

func (r *NativeJSONRead) namedDomainValue(entry node.LeafEntry) ([]byte, error) {
	return r.namedDomainValueAdmitted(entry, nil)
}
func (r *NativeJSONRead) namedDomainValueAdmitted(entry node.LeafEntry, admit func(uint64) error) ([]byte, error) {
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		return r.readValue(entry.ValuePtr, admit)
	}
	if admit != nil {
		if err := admit(memtable.COWAllocationCharge(uint64(len(entry.Value)))); err != nil {
			return nil, err
		}
	}
	return bytes.Clone(entry.Value), nil
}

// namedDomainSeek returns independent caller-owned bytes. Bounds follow the
// maintained [start,end) source convention in both directions. Tombstones and
// shadowed rows are resolved by the same source merge used for ordinary reads.
func (r *NativeJSONRead) namedDomainSeek(name string, start, end []byte, reverse bool) ([]byte, []byte, bool, error) {
	return r.namedDomainSeekAdmitted(name, start, end, reverse, nil)
}
func (r *NativeJSONRead) namedDomainSeekAdmitted(name string, start, end []byte, reverse bool, admit func(uint64) error) (key, value []byte, found bool, resultErr error) {
	if r == nil {
		return nil, nil, false, backenddb.ErrClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	domain, err := r.borrowedNamedDomain(name)
	if err != nil {
		return nil, nil, false, err
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		return nil, nil, false, err
	}
	lease, err := r.capture.NativeAllocator().AdmitNativeBytes(memtable.COWAllocationCharge(page.PageSize + 1))
	if err != nil {
		return nil, nil, false, err
	}
	cursor := make([]byte, page.PageSize+1)
	// Finish this operation before returning its temporary scratch receipt.
	key, value, found, resultErr = func() ([]byte, []byte, bool, error) {
		for {
			entry, ok, err := domain.seek(start, end, scratch, r.readLeaf, reverse, nil)
			if err != nil || !ok {
				return nil, nil, false, err
			}
			if entry.Flags&node.FlagTombstone == 0 {
				if admit != nil {
					if err := admit(memtable.COWAllocationCharge(uint64(len(entry.Key) + 1))); err != nil {
						return nil, nil, false, err
					}
				}
				value, err := r.namedDomainValueAdmitted(entry, admit)
				if err != nil {
					return nil, nil, false, err
				}
				return bytes.Clone(entry.Key), value, true, nil
			}
			if len(entry.Key) >= len(cursor) {
				return nil, nil, false, memtable.ErrCOWCapacity
			}
			n := copy(cursor, entry.Key)
			if reverse {
				end = cursor[:n]
			} else {
				cursor[n] = 0
				start = cursor[:n+1]
			}
		}
	}()
	cursor = nil
	lease.Close()
	return key, value, found, resultErr
}
