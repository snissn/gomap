package caching

import (
	"errors"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	publiciterator "github.com/snissn/gomap/TreeDB/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWTxnCanceledPublicationReleasesStagingBeforeNextWriter(t *testing.T) {
	db, c := cowCutFixture(t)
	c.txnOwner = transactionowner.New()
	if err := c.ensureTxnJournal(); err != nil {
		t.Fatal(err)
	}
	backend := db.backend.(*backenddb.DB)
	first := &Batch{db: db, cowState: &cowBatchState{inPublication: true}, entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("canceled"), Value: []byte("private"), Revision: 23}}}
	injected := errors.New("refused before append")
	c.writerMu.Lock()
	err := backend.FinalizeRawKVEntryScanForCachedPublication(first.PrepareExternalCommandWALPublication, func([]byte, func(page.ValuePtr) (uint64, bool)) error { return injected }, first.Replay, 1)
	if !errors.Is(err, injected) || first.cowState.prepared == nil {
		c.writerMu.Unlock()
		t.Fatalf("wrong refusal stage: %v", err)
	}
	stale := first.cowState.prepared.mutation
	canceled := first.cancelCOWPublication()
	c.writerMu.Unlock()
	if stale.Valid() {
		canceled.drainCancelled()
		t.Fatal("canceled command retained shared staging after writer release")
	}
	ready, retired := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		second := &Batch{db: db, cowState: &cowBatchState{inPublication: true}, entries: []batch.Entry{{Type: batch.OpPut, Key: []byte("next"), Value: []byte("accepted"), Revision: 24}}}
		c.writerMu.Lock()
		err := backend.FinalizeRawKVEntryScanForCachedPublication(second.PrepareExternalCommandWALPublication, second.finalizeCOWPublication, second.Replay, 1)
		close(ready)
		<-retired
		if err != nil {
			p := second.cancelCOWPublication()
			c.writerMu.Unlock()
			if p != nil {
				p.drainCancelled()
			}
			result <- err
			return
		}
		p := second.cowState.prepared
		if !p.mutation.Valid() {
			err = errors.New("old retirement consumed next writer preparation")
		}
		p.cut.next.logicalSeq = c.cut.logicalSeq + 1
		c.journal.Install(p.mutation, p.cut.next.logicalSeq)
		p.mutation = txnMutationOwner{}
		c.updateTxnStats()
		old := p.cut.publish()
		c.writerMu.Unlock()
		if old != nil {
			old.drain()
		}
		p.scratch.Close()
		result <- err
	}()
	<-ready
	canceled.drainCancelled()
	stale.Cancel()
	close(retired)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if c.cut.logicalSeq != 1 || c.journal.PointChanged(0, []byte("canceled")) || !c.journal.PointChanged(0, []byte("next")) {
		t.Fatal("cancellation or successor publication changed logical history")
	}
	snapshot := db.AcquireSnapshot()
	defer snapshot.Close()
	if value, err := snapshot.Get([]byte("next")); err != nil || string(value) != "accepted" {
		t.Fatalf("next writer value=%q err=%v", value, err)
	}
}

type cowTxnCloseSentinel struct {
	publiciterator.Iterator
	err    error
	closes *int
}

func (c cowTxnCloseSentinel) Close() error {
	(*c.closes)++
	return errors.Join(c.Iterator.Close(), c.err)
}

func TestCOWTxnFinishReturnsEveryChildCloseErrorExactlyOnce(t *testing.T) {
	db, c := cowCutFixture(t)
	owner := transactionowner.New()
	if err := db.backend.(*backenddb.DB).BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	c.txnOwner = owner
	if err := c.ensureTxnJournal(); err != nil {
		t.Fatal(err)
	}
	before := c.budget.Stats()
	tx := new(ConditionalTxn)
	if err := db.InitOwnedCOWTxn(tx, owner, nil); err != nil {
		t.Fatal(err)
	}
	first, second := errors.New("first child close"), errors.New("second child close")
	closes := 0
	for _, sentinel := range []error{first, second} {
		it, err := tx.Iterator(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		child := it.(*cowTxnIterator)
		child.base = cowTxnCloseSentinel{Iterator: child.base, err: sentinel, closes: &closes}
	}
	if err := tx.Close(); !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("child cleanup errors=%v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatal(err)
	}
	if closes != 2 {
		t.Fatalf("child Close calls=%d", closes)
	}
	after := c.budget.Stats()
	if before.TotalBytes != after.TotalBytes || before.ReservedBytes != after.ReservedBytes || before.ExternalLeases != after.ExternalLeases || before.Views != after.Views {
		t.Fatalf("child cleanup leaked: before=%+v after=%+v", before, after)
	}
}

func TestCOWTxnPartialInitRefusalReleasesBatchAdmission(t *testing.T) {
	db, c := cowCutFixture(t)
	owner := transactionowner.New()
	if err := db.backend.(*backenddb.DB).BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	c.txnOwner = owner
	if err := c.ensureTxnJournal(); err != nil {
		t.Fatal(err)
	}
	base := c.budget.Stats()
	empty := db.newCOWBatch(0)
	ownerBytes := c.budget.Stats().ReservedBytes - base.ReservedBytes
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
	full := db.NewBatch()
	if full.cowState.err != nil {
		t.Fatal(full.cowState.err)
	}
	reserveBytes := c.budget.Stats().ReservedBytes - base.ReservedBytes - ownerBytes
	if err := full.Close(); err != nil {
		t.Fatal(err)
	}
	leaseBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	stateBytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowTxnState{})))
	headroom := stateBytes + leaseBytes + ownerBytes + reserveBytes/2
	available := c.budget.Limits().MaxInFlightBytes - c.budget.Stats().ReservedBytes
	filler, err := c.budget.AcquireExternal(available - headroom - leaseBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	before := c.budget.Stats()
	// Prove the intended partial admission: state and batch wrapper fit, while
	// NewBatch's default backing reserve refuses after acquiring its wrapper.
	state, err := c.budget.AcquireExternal(stateBytes)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	probe := db.NewBatch()
	if probe == &cowDeniedBatch || !errors.Is(probe.cowState.err, memtable.ErrCOWCapacity) {
		t.Fatalf("wrong refusal stage: denied=%t err=%v", probe == &cowDeniedBatch, probe.cowState.err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	state.Close()
	for n := 0; n < 32; n++ {
		if err := db.InitOwnedCOWTxn(new(ConditionalTxn), owner, nil); !errors.Is(err, memtable.ErrCOWCapacity) {
			t.Fatalf("init%d=%v", n, err)
		}
		after := c.budget.Stats()
		if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.ExternalLeases != before.ExternalLeases || after.Views != before.Views {
			t.Fatalf("partial init%d retained admission: before=%+v after=%+v", n, before, after)
		}
	}
}

func TestConditionalTxnUnsupportedPublicOwnerIsImmutableAndBareAPIUnchanged(t *testing.T) {
	for _, private := range []bool{false, true} {
		opts := Options{RelaxedSync: true, AllowUnsafe: true, ExternalCommandWAL: true}
		if private {
			opts.TransactionOwner = transactionowner.New()
		}
		db, err := Open(t.TempDir(), newBlockingWriteBackend(), opts)
		if err != nil {
			t.Fatal(err)
		}
		if db.conditionalTxnUnsupported != private {
			t.Fatal("wrong immutable eligibility")
		}
		tx, err := db.NewConditionalTxn()
		if private {
			if tx != nil || !errors.Is(err, backenddb.ErrConditionalTxnUnsupported) {
				t.Fatalf("private=%v %v", tx, err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
		}
		db.conditionalRecordCommittedEntries([]batch.Entry{{Key: []byte("key")}}, nil)
		if db.conditionalSeq.Load() != 1 {
			t.Fatal("logical clock depended on transaction activity")
		}
		if (db.conditionalOracle.history == nil) != private {
			t.Fatal("unused history or missing supported history")
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
