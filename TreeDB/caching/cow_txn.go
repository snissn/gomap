package caching

import (
	"bytes"
	"errors"
	"slices"
	"sort"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
)

type cowTxnRange struct{ start, end []byte }

func cloneCOWTxnBound(bound []byte) []byte {
	if bound == nil {
		return nil
	}
	owned := make([]byte, len(bound))
	copy(owned, bound)
	return owned
}

type cowTxnState struct {
	writeIndex    map[string]int
	writeIndexCap int
	lease         *memtable.COWExternalLease
	charged       uint64
	write         func(*Batch, bool) error
	ranges        []cowTxnRange
	rangeCharge   uint64
	iterators     *cowTxnIterator
}

func (db *DB) InitOwnedCOWTxn(tx *ConditionalTxn, owner *transactionowner.Owner, write func(*Batch, bool) error) error {
	if db == nil || db.cow == nil || owner == nil || db.cow.txnOwner != owner {
		return backenddb.ErrConditionalTxnUnsupported
	}
	if tx == nil || (!tx.closed && (tx.db != nil || tx.batch != nil)) {
		return backenddb.ErrConditionalTxnClosed
	}
	provider, ok := db.backend.(interface {
		PreflightPrivateCOWCommand(*transactionowner.Owner) error
	})
	if !ok {
		return backenddb.ErrConditionalTxnUnsupported
	}
	if err := provider.PreflightPrivateCOWCommand(owner); err != nil {
		return err
	}
	if err := db.beginDirectWrite(); err != nil {
		return err
	}
	defer db.writeMu.RUnlock()
	c := db.cow
	c.writerMu.Lock()
	defer c.writerMu.Unlock()
	if err := c.ensureTxnJournal(); err != nil {
		return err
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowTxnState{})))
	lease, err := c.budget.AcquireExternal(charge)
	if err != nil {
		return err
	}
	snap, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		lease.Close()
		return err
	}
	b := db.NewBatch()
	if b.cowState.err != nil {
		batchErr := b.cowState.err
		closeErr := errors.Join(b.Close(), snap.Close())
		lease.Close()
		return errors.Join(batchErr, closeErr)
	}
	*tx = ConditionalTxn{db: db, batch: b, snap: snap, startSeq: snap.cowCut.logicalSeq, cow: &cowTxnState{lease: lease, charged: charge, write: write}}
	tx.reads = tx.inlineReads[:0]
	tx.keyArena = tx.inlineKeys[:0]
	b.cowState.txn = tx
	db.conditionalTxnStarted.Add(1)
	db.conditionalActiveTxnCount.Add(1)
	return nil
}
func (tx *ConditionalTxn) COWTransaction() bool { return tx != nil && tx.cow != nil && !tx.closed }
func (tx *ConditionalTxn) cowAdmit(bytes uint64) error {
	if tx.cow == nil {
		return nil
	}
	if bytes > ^uint64(0)-tx.cow.charged {
		return memtable.ErrCOWCapacity
	}
	if err := tx.cow.lease.Resize(tx.cow.charged + bytes); err != nil {
		return err
	}
	tx.cow.charged += bytes
	return nil
}
func (tx *ConditionalTxn) ensureMutation() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.cow != nil && tx.cow.iterators != nil {
		return ErrTxnIteratorOpen
	}
	return nil
}
func (tx *ConditionalTxn) cowCommit(sync bool) (err error) {
	defer func() {
		tx.db.observeConditionalTxnCommit(len(tx.reads), err)
		if closeErr := tx.finish(); err == nil {
			err = closeErr
		}
	}()
	if len(tx.batch.entries) == 0 {
		// Read-only views serialize at capture. Explicit sync retains its boundary.
		if sync {
			return tx.cow.write(tx.batch, true)
		}
		return nil
	}
	return tx.cow.write(tx.batch, sync)
}

// Iterator records the complete requested interval before exposing any result.
// The opening iterator and immutable staged effects merge lazily; early close
// or seek does not narrow the predicate dependency.
func (tx *ConditionalTxn) Iterator(start, end []byte) (publiciterator.Iterator, error) {
	return tx.cowIterator(start, end, false)
}
func (tx *ConditionalTxn) ReverseIterator(start, end []byte) (publiciterator.Iterator, error) {
	return tx.cowIterator(start, end, true)
}
func (tx *ConditionalTxn) cowIterator(start, end []byte, reverse bool) (publiciterator.Iterator, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	if tx.cow == nil {
		return nil, backenddb.ErrConditionalTxnUnsupported
	}
	n := len(tx.batch.entries)
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowTxnIterator{}))) + memtable.COWAllocationCharge(uint64(n)*uint64(unsafe.Sizeof(batch.Entry{}))) + memtable.COWAllocationCharge(uint64(len(start))) + memtable.COWAllocationCharge(uint64(len(end)))
	lease, err := tx.db.cow.budget.AcquireExternal(charge)
	if err != nil {
		return nil, err
	}
	defer func() {
		if lease != nil {
			lease.Close()
		}
	}()
	if err := tx.cowRecordRange(start, end); err != nil {
		return nil, err
	}
	r := cowTxnRange{start: cloneCOWTxnBound(start), end: cloneCOWTxnBound(end)}
	staged := make([]batch.Entry, n)
	copy(staged, tx.batch.entries)
	// Stable sort retains original operation order, so the final effect wins.
	slices.SortStableFunc(staged, func(a, b batch.Entry) int { return bytes.Compare(a.Key, b.Key) })
	out := staged[:0]
	for _, e := range staged {
		if len(out) > 0 && bytes.Equal(out[len(out)-1].Key, e.Key) {
			out[len(out)-1] = e
		} else {
			out = append(out, e)
		}
	}
	staged = out
	var base publiciterator.Iterator
	if reverse {
		base, err = tx.snap.ReverseIterator(r.start, r.end)
	} else {
		base, err = tx.snap.Iterator(r.start, r.end)
	}
	if err != nil {
		return nil, err
	}
	it := &cowTxnIterator{tx: tx, base: base, staged: staged, start: r.start, end: r.end, reverse: reverse, lease: lease, next: tx.cow.iterators}
	if it.next != nil {
		it.next.previous = it
	}
	tx.cow.iterators = it
	lease = nil
	it.Seek(nil)
	return it, nil
}

// Predicate storage lasts until transaction finish, separately from child
// traversal storage. Covered opens allocate no new predicate; coalescing drops
// unused bound/backing aliases before refunding their exact retained capacity.
func (tx *ConditionalTxn) cowRecordRange(start, end []byte) error {
	if batch.IsDeleteRangeNoop(start, end) {
		return nil
	}
	c := tx.cow
	for _, old := range c.ranges {
		if bytes.Compare(old.start, start) <= 0 && (old.end == nil || (end != nil && bytes.Compare(old.end, end) >= 0)) {
			return nil
		}
	}
	newCap := cap(c.ranges)
	var backing uint64
	if len(c.ranges)+1 > newCap {
		newCap = max(1, 2*newCap)
		backing = memtable.COWAllocationCharge(uint64(newCap) * uint64(unsafe.Sizeof(cowTxnRange{})))
	}
	reserved := backing + memtable.COWAllocationCharge(uint64(len(start))) + memtable.COWAllocationCharge(uint64(len(end)))
	if err := tx.cowAdmit(reserved); err != nil {
		return err
	}
	if backing != 0 {
		old := c.ranges
		c.ranges = make([]cowTxnRange, len(old), newCap)
		copy(c.ranges, old)
		clear(old)
	}
	c.recordRange(cowTxnRange{start: cloneCOWTxnBound(start), end: cloneCOWTxnBound(end)})
	retained := memtable.COWAllocationCharge(uint64(cap(c.ranges)) * uint64(unsafe.Sizeof(cowTxnRange{})))
	for _, r := range c.ranges {
		retained += memtable.COWAllocationCharge(uint64(cap(r.start))) + memtable.COWAllocationCharge(uint64(cap(r.end)))
	}
	refund := c.rangeCharge + reserved - retained
	if refund != 0 {
		if err := c.lease.Resize(c.charged - refund); err != nil {
			return err
		}
		c.charged -= refund
	}
	c.rangeCharge = retained
	return nil
}

// recordRange keeps one union of requested predicates. Bounds are owned by the
// transaction; iterator traversal retains its own original interval. Nil end
// denotes infinity, while a nonnil empty end remains an exclusive empty bound.
func (c *cowTxnState) recordRange(r cowTxnRange) {
	if r.end != nil && bytes.Compare(r.start, r.end) >= 0 {
		return // The requested key domain is mathematically empty.
	}
	for i := 0; i < len(c.ranges); {
		old := c.ranges[i]
		if (old.end != nil && bytes.Compare(old.end, r.start) < 0) ||
			(r.end != nil && bytes.Compare(r.end, old.start) < 0) {
			i++
			continue
		}
		if old.start == nil || (r.start != nil && bytes.Compare(old.start, r.start) < 0) {
			r.start = old.start
		}
		if old.end == nil || (r.end != nil && bytes.Compare(old.end, r.end) > 0) {
			r.end = old.end
		}
		copy(c.ranges[i:], c.ranges[i+1:])
		c.ranges[len(c.ranges)-1] = cowTxnRange{}
		c.ranges = c.ranges[:len(c.ranges)-1]
		// A newly extended interval can overlap an earlier disjoint interval.
		i = 0
	}
	c.ranges = append(c.ranges, r)
}

type cowTxnIterator struct {
	lease                            *memtable.COWExternalLease
	previous, next                   *cowTxnIterator
	tx                               *ConditionalTxn
	base                             publiciterator.Iterator
	staged                           []batch.Entry
	start, end, key, value           []byte
	pos                              int
	fromBase, closed, valid, reverse bool
	err                              error
}

func (it *cowTxnIterator) alive() bool { return !it.closed && it.tx != nil && !it.tx.closed }
func (it *cowTxnIterator) Valid() bool { return it.alive() && it.valid && it.err == nil }
func (it *cowTxnIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.key
}
func (it *cowTxnIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.value
}
func (it *cowTxnIterator) KeyCopy(dst []byte) []byte   { return append(dst[:0], it.Key()...) }
func (it *cowTxnIterator) ValueCopy(dst []byte) []byte { return append(dst[:0], it.Value()...) }
func (it *cowTxnIterator) Error() error {
	if !it.alive() {
		return backenddb.ErrConditionalTxnClosed
	}
	return errors.Join(it.err, it.base.Error())
}
func (it *cowTxnIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.key = nil
	it.value = nil
	if it.previous != nil {
		it.previous.next = it.next
	} else {
		it.tx.cow.iterators = it.next
	}
	if it.next != nil {
		it.next.previous = it.previous
	}
	it.previous, it.next = nil, nil
	it.staged = nil
	it.start = nil
	it.end = nil
	err := it.base.Close()
	it.base = nil
	it.tx = nil
	it.lease.Close()
	it.lease = nil
	return err
}
func (it *cowTxnIterator) advanceStaged() {
	if it.reverse {
		it.pos--
	} else {
		it.pos++
	}
}
func (it *cowTxnIterator) Next() {
	if !it.Valid() {
		return
	}
	if it.fromBase {
		it.base.Next()
	} else {
		it.advanceStaged()
	}
	it.choose()
}
func (it *cowTxnIterator) Seek(key []byte) {
	if !it.alive() {
		return
	}
	it.base.Seek(key)
	if it.reverse {
		if key == nil {
			it.pos = len(it.staged) - 1
		} else {
			it.pos = sort.Search(len(it.staged), func(i int) bool { return bytes.Compare(it.staged[i].Key, key) > 0 }) - 1
		}
	} else {
		it.pos = sort.Search(len(it.staged), func(i int) bool { return bytes.Compare(it.staged[i].Key, key) >= 0 })
	}
	it.choose()
}
func (it *cowTxnIterator) choose() {
	it.key = nil
	it.value = nil
	it.valid = false
	for {
		for it.pos >= 0 && it.pos < len(it.staged) {
			k := it.staged[it.pos].Key
			if (it.start == nil || bytes.Compare(k, it.start) >= 0) && (it.end == nil || bytes.Compare(k, it.end) < 0) {
				break
			}
			it.advanceStaged()
		}
		has := it.pos >= 0 && it.pos < len(it.staged)
		if !has && !it.base.Valid() {
			return
		}
		useStage := has
		if has && it.base.Valid() {
			cmp := bytes.Compare(it.staged[it.pos].Key, it.base.Key())
			if cmp == 0 {
				it.base.Next()
			} else {
				useStage = (cmp < 0 && !it.reverse) || (cmp > 0 && it.reverse)
			}
		}
		if useStage {
			e := it.staged[it.pos]
			if e.Type == batch.OpDelete {
				it.advanceStaged()
				continue
			}
			it.key = e.Key
			it.valid = true
			it.value = e.Value
			it.fromBase = false
			return
		}
		it.key = it.base.Key()
		it.valid = true
		it.value = it.base.Value()
		it.fromBase = true
		return
	}
}

// DeleteRange expands against the same private merged view and closes its
// internal iterator before staging tombstones. The existing finite batch cap
// bounds expansion; failure leaves the prior staged effects intact.
func (tx *ConditionalTxn) DeleteRange(start, end []byte) error {
	if err := tx.ensureMutation(); err != nil {
		return err
	}
	if tx.cow == nil {
		return backenddb.ErrConditionalTxnUnsupported
	}
	if batch.IsDeleteRangeNoop(start, end) {
		return nil
	}
	it, err := tx.Iterator(start, end)
	if err != nil {
		return err
	}
	var keys [][]byte
	total := 0
	for it.Valid() {
		if err := tx.cowAdmit(memtable.COWAllocationCharge(uint64(len(it.Key()))) + 128); err != nil {
			it.Close()
			return err
		}
		k := it.KeyCopy(nil)
		total += len(k)
		if len(keys) >= 4096 || total > 1<<20 {
			it.Close()
			return ErrBatchDeleteRangeTooLarge
		}
		keys = append(keys, k)
		it.Next()
	}
	err = it.Error()
	closeErr := it.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// Any staging failure terminally invalidates the transaction, preventing a
	// caller from accidentally committing an incomplete expanded deletion.
	for _, k := range keys {
		if err = tx.Delete(k); err != nil {
			tx.finish()
			return err
		}
	}
	return nil
}

func (tx *ConditionalTxn) recordCOWStagedWrite() {
	if tx.cow == nil {
		return
	}
	n := len(tx.batch.entries)
	if tx.cow.writeIndex == nil && n < 16 {
		return
	}
	if tx.cow.writeIndex == nil {
		tx.cow.writeIndex = make(map[string]int, 2*n)
		tx.cow.writeIndexCap = 2 * n
		for i, e := range tx.batch.entries {
			tx.cow.writeIndex[conditionalBytesString(e.Key)] = i
		}
	} else if n > 0 {
		tx.cow.writeIndex[conditionalBytesString(tx.batch.entries[n-1].Key)] = n - 1
	}
}
func (tx *ConditionalTxn) admitCOWStagedWrite() error {
	if tx.cow == nil {
		return nil
	}
	n := len(tx.batch.entries) + 1
	if n >= 16 && (tx.cow.writeIndex == nil || n > tx.cow.writeIndexCap) {
		if err := tx.cowAdmit(memtable.COWAllocationCharge(uint64(n) * 256)); err != nil {
			return err
		}
		tx.cow.writeIndexCap = 2 * n
	}
	return nil
}
