package collections

import (
	"hash/maphash"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// localIDIndex is operation-private, not an identity authority. Linear probing
// follows rootpublication's maintained registry table, but it has no owner,
// refund or shared lifetime: every allocation remains on the original read's
// cumulative admission until that read closes. Hashes only select probes.
type localIDCell struct {
	key     string
	ordinal int
	status  uint8
}

type localIDIndex struct {
	cells       []localIDCell
	count, used int
	admit       func(uint64) error
	seed        maphash.Seed
}

func localIDCapacity(n int) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if n < 0 || n > maxInt/2-1 {
		return 0, memtable.ErrCOWCapacity
	}
	capacity := 8
	for capacity < 2*(n+1) {
		if capacity > maxInt/2 {
			return 0, memtable.ErrCOWCapacity
		}
		capacity *= 2
	}
	return capacity, nil
}

func localIDCellsCharge(capacity int) (uint64, error) {
	width := uint64(unsafe.Sizeof(localIDCell{}))
	if capacity < 0 || uint64(capacity) > uint64(int(^uint(0)>>1))/width {
		return 0, memtable.ErrCOWCapacity
	}
	return memtable.COWAllocationCharge(uint64(capacity) * width), nil
}

func localIDIndexCharge(n int) (uint64, error) {
	capacity, err := localIDCapacity(n)
	if err != nil {
		return 0, err
	}
	cells, err := localIDCellsCharge(capacity)
	header := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(localIDIndex{})))
	if err != nil || cells > ^uint64(0)-header {
		return 0, memtable.ErrCOWCapacity
	}
	return cells + header, nil
}

func newLocalIDIndex(hint int, admit func(uint64) error) (*localIDIndex, error) {
	charge, err := localIDIndexCharge(hint)
	if err != nil {
		return nil, err
	}
	if admit != nil {
		if err = admit(charge); err != nil {
			return nil, err
		}
	}
	capacity, _ := localIDCapacity(hint)
	return &localIDIndex{cells: make([]localIDCell, capacity), admit: admit, seed: maphash.MakeSeed()}, nil
}

func (t *localIDIndex) lookup(key string) (int, bool) {
	if t == nil {
		return 0, false
	}
	return t.lookupHash(key, maphash.String(t.seed, key))
}
func (t *localIDIndex) lookupHash(key string, hash uint64) (int, bool) {
	if t == nil || len(t.cells) == 0 {
		return 0, false
	}
	mask := len(t.cells) - 1
	for i := int(hash) & mask; ; i = (i + 1) & mask {
		cell := &t.cells[i]
		if cell.status == 0 {
			return 0, false
		}
		if cell.status == 1 && cell.key == key {
			return cell.ordinal, true
		}
	}
}

func (t *localIDIndex) insert(key string, ordinal int) {
	t.insertHash(key, ordinal, maphash.String(t.seed, key))
}
func (t *localIDIndex) insertHash(key string, ordinal int, hash uint64) {
	mask, tomb := len(t.cells)-1, -1
	for i := int(hash) & mask; ; i = (i + 1) & mask {
		cell := &t.cells[i]
		if cell.status == 1 && cell.key == key {
			cell.ordinal = ordinal
			return
		}
		if cell.status == 2 && tomb < 0 {
			tomb = i
		}
		if cell.status == 0 {
			if tomb >= 0 {
				i = tomb
			} else {
				t.used++
			}
			t.cells[i] = localIDCell{key: key, ordinal: ordinal, status: 1}
			t.count++
			return
		}
	}
}

// prepare admits complete replacement backing AND payload before allocating.
// Old backing remains charged even after rehash; there is no early GC refund.
// Grow before the next insertion would leave fewer than half the cells empty.
func (t *localIDIndex) prepare(payload uint64) error {
	capacity := len(t.cells)
	replace := t.used+1 > capacity/2
	charge := uint64(0)
	if replace {
		if t.count+1 > capacity/2 {
			if capacity > int(^uint(0)>>1)/2 {
				return memtable.ErrCOWCapacity
			}
			capacity *= 2
		}
		var err error
		charge, err = localIDCellsCharge(capacity)
		if err != nil {
			return err
		}
	}
	if payload > ^uint64(0)-charge {
		return memtable.ErrCOWCapacity
	}
	if t.admit != nil {
		if err := t.admit(charge + payload); err != nil {
			return err
		}
	}
	if replace {
		old := t.cells
		t.cells = make([]localIDCell, capacity)
		t.count = 0
		t.used = 0
		for i := range old {
			if old[i].status == 1 {
				t.insert(old[i].key, old[i].ordinal)
			}
		}
		clear(old)
	}
	return nil
}

// setBorrowed accepts only keys whose backing dominates this operation's
// index lifetime (for example its admitted typed accumulator/record array).
func (t *localIDIndex) setBorrowed(key string, ordinal int) error {
	if _, ok := t.lookup(key); ok {
		t.insert(key, ordinal)
		return nil
	}
	if err := t.prepare(0); err != nil {
		return err
	}
	t.insert(key, ordinal)
	return nil
}

func (t *localIDIndex) setCopy(id []byte, ordinal int) error {
	_, err := t.copyKey(id, ordinal)
	return err
}
func (t *localIDIndex) copyKey(id []byte, ordinal int) (string, error) {
	mask := len(t.cells) - 1
	for i := int(maphash.String(t.seed, unsafeStringFromBytes(id))) & mask; ; i = (i + 1) & mask {
		cell := &t.cells[i]
		if cell.status == 0 {
			break
		}
		if cell.status == 1 && cell.key == unsafeStringFromBytes(id) {
			return cell.key, nil
		}
	}
	if err := t.prepare(memtable.COWAllocationCharge(uint64(len(id)))); err != nil {
		return "", err
	}
	key := string(id)
	t.insert(key, ordinal)
	return key, nil
}

func admitLocalIDArray(admit func(uint64) error, n int, width uint64) error {
	if n < 0 || width == 0 || uint64(n) > uint64(int(^uint(0)>>1))/width {
		return memtable.ErrCOWCapacity
	}
	if admit != nil {
		return admit(memtable.COWAllocationCharge(uint64(n) * width))
	}
	return nil
}

// The concrete adapter preserves legacy map semantics and nil versus empty.
// all invokes its callback synchronously through a direct concrete call; no
// callback/iterator is retained on a read, runtime, index or shared owner.
type hybridScalarIDs struct {
	legacy hybridScalarAllowSet
	native *localIDIndex
	// Operation-local construction or immutable independently owned entry only.
	forwardPlan *nativeScalarForwardPlan
}

func legacyScalarIDs(s hybridScalarAllowSet) hybridScalarIDs { return hybridScalarIDs{legacy: s} }
func nativeScalarIDs(s *localIDIndex) hybridScalarIDs        { return hybridScalarIDs{native: s} }
func (s hybridScalarIDs) len() int {
	if s.native != nil {
		return s.native.count
	}
	return len(s.legacy)
}
func (s hybridScalarIDs) present() bool { return s.native != nil || s.legacy != nil }
func (s hybridScalarIDs) contains(key string) bool {
	if s.native != nil {
		_, ok := s.native.lookup(key)
		return ok
	}
	_, ok := s.legacy[key]
	return ok
}
func (s hybridScalarIDs) all(yield func(string) bool) {
	if s.native != nil {
		for i := range s.native.cells {
			cell := &s.native.cells[i]
			if cell.status == 1 && !yield(cell.key) {
				return
			}
		}
		return
	}
	for key := range s.legacy {
		if !yield(key) {
			return
		}
	}
}
func (s hybridScalarIDs) delete(key string) {
	if s.forwardPlan != nil {
		panic("collections: mutate scalar-forward selection")
	}
	if s.native == nil {
		delete(s.legacy, key)
		return
	}
	t := s.native
	mask := len(t.cells) - 1
	for i := int(maphash.String(t.seed, key)) & mask; ; i = (i + 1) & mask {
		cell := &t.cells[i]
		if cell.status == 0 {
			return
		}
		if cell.status == 1 && cell.key == key {
			*cell = localIDCell{status: 2}
			t.count--
			return
		}
	}
}
