package caching

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Observe the original independent receipt before its actual final refund. The
// same receipt is used by production cancellation after the containing batch's
// cut alias has ended; no replacement owner or failure-time keeper is created.
func TestCOWCancelledPreparationDetachesBeforeOriginalCompletion(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	prepared := cowCutPrepare(t, c, "private")
	originalLease := prepared.lease
	next := prepared.next
	prepared.cancel()
	c.writerMu.Unlock()
	refund, err := prepared.drainCancelledWithCompletion()
	if err != nil || refund != originalLease {
		t.Fatalf("lost original completion: receipt=%p original=%p err=%v", refund, originalLease, err)
	}
	if prepared.next != nil || prepared.opening != nil || prepared.native != nil || prepared.cache != nil || prepared.prepared != nil || prepared.retired != nil || prepared.lease != nil {
		t.Fatal("original private owner aliases survived until containing completion")
	}
	if next.basis != nil || next.shards != nil || next.frozen != nil || next.sources != nil || next.domains != nil || next.lease != nil || next.cache != nil || next.semanticRootSet != nil || next.frozenSemanticRootSet != nil {
		t.Fatal("original private cut aliases survived until containing completion")
	}
	before := c.budget.Stats()
	prepared, next = nil, nil
	refund.Close()
	after := c.budget.Stats()
	if after.ExternalLeases+1 != before.ExternalLeases || after.ExternalBytes >= before.ExternalBytes {
		t.Fatalf("original containing receipt not completed once: before=%+v after=%+v", before, after)
	}
	refund.Close()
	if c.budget.Stats() != after {
		t.Fatal("original containing receipt replayed")
	}
}

func TestCOWBatchScratchDetachesBorrowedAliasesBeforeCompletion(t *testing.T) {
	_, c := cowCutFixture(t)
	lease, err := c.budget.AcquireExternal(8192)
	if err != nil {
		t.Fatal(err)
	}
	p := &cowBatchPreparation{scratch: lease, cut: &cowPreparedCut{}, files: [][]uint32{{1}}, resources: []*cowLiveResource{{}}, dictionaries: [][]uint64{{2}}}
	refund := p.detachScratch()
	if refund != lease || p.cut != nil || p.scratch != nil || p.files != nil || p.resources != nil || p.dictionaries != nil || p.mutation.Valid() {
		t.Fatal("containing scratch receipt detached before borrowed aliases")
	}
	before := c.budget.Stats()
	p = nil
	refund.Close()
	if after := c.budget.Stats(); after.ExternalLeases+1 != before.ExternalLeases || after.ExternalBytes >= before.ExternalBytes {
		t.Fatal("original scratch receipt not completed")
	}
}

// A real Snapshot completion registration refusal leaves the SAME exact failed
// basis charged through its independently admitted original registry custody.
// The cleared private wrapper and successful neighbors finish exactly once.
func TestCOWCancelledPreparationKeepsOriginalFailedBasisAndCharge(t *testing.T) {
	db, c := cowCutFixture(t)
	backend := db.backend.(*backenddb.DB)
	var original *cowBackendBasis
	snapshot, err := backend.AcquireSnapshotWithAllocationAdmission(func(s backenddb.SnapshotAllocationSizes) error {
		var e error
		original, e = admitCOWBackendBasis(c.budget, s)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := &cowBasisCompletionReceipt{}
	if err := snapshot.AdoptSnapshotCleanupCompletion(receipt); err != nil {
		t.Fatal(err)
	}
	if err := original.adoptSnapshot(snapshot); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("actual registration refusal missing: %v", err)
	}
	beforePreparation := c.budget.Stats().ExternalLeases
	c.writerMu.Lock()
	p := cowCutPrepare(t, c, "private")
	oldBasis := p.next.basis
	p.next.basis = original
	p.cancel()
	c.writerMu.Unlock()
	if err := oldBasis.release(); err != nil {
		t.Fatal(err)
	}
	originalLease, originalNext := p.lease, p.next
	refund, err := p.drainCancelledWithCompletion()
	if refund != originalLease || !errors.Is(err, rootpublication.ErrResourceOwnership) || p.lease != nil || p.next != nil || originalNext.basis != nil || original.snapshot != snapshot || original.lease == nil || original.originalCustody == nil || receipt.calls != 1 {
		t.Fatalf("lost SAME failed original basis/preparation: refund=%p err=%v calls=%d", refund, err, receipt.calls)
	}
	if originalNext.shards != nil || originalNext.frozen != nil || originalNext.sources != nil || originalNext.domains != nil || p.opening != nil || p.prepared != nil || p.retired != nil || p.cache != nil {
		t.Fatal("unrelated private raw neighbors survived failed original basis cleanup")
	}
	p, originalNext = nil, nil
	refund.Close()
	before := c.budget.Stats()
	if before.ExternalLeases != beforePreparation {
		t.Fatalf("independent failed basis charge changed: leases=%d before=%d", before.ExternalLeases, beforePreparation)
	}
	if err := snapshot.Close(); err != nil || receipt.calls != 1 || c.budget.Stats() != before {
		t.Fatal("observing original Snapshot repeated cleanup or refunded failure debt")
	}
}

// Use original generation callbacks to observe the actual physical destruction
// boundary, while its original history charge is still present.
func TestCOWCutDetachesRawTablesBeforeGenerationCleanup(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "frozen"}[frozen], func(t *testing.T) {
			budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			writer, err := memtable.NewCOWWriter(budget)
			if err != nil {
				t.Fatal(err)
			}
			staged, err := writer.Prepare(nil, memtable.COWPrepareOptions{ResourceSlots: 1, ResourceBytes: 512})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := budget.AcquireExternal(8192)
			if err != nil {
				t.Fatal(err)
			}
			cache := &cowCache{budget: budget}
			cache.activeCuts.Store(1)
			cut := &cowReadCut{refs: 1, cache: cache, lease: lease}
			var originalTables []cowTable
			calls := 0
			if err := staged.AttachResources([]memtable.COWResourceID{{Kind: 1, ID: 9}}, func() {
				calls++
				if cut.shards != nil || cut.frozen != nil || cut.sources != nil || cut.domains != nil {
					t.Error("cut projections survived into original generation destruction")
				}
				for _, table := range originalTables {
					if table.root != nil || table.resources != nil {
						t.Error("table root/resolver alias survived original generation destruction")
					}
				}
				if budget.Stats().Generations != 1 {
					t.Error("original generation charge refunded before actual callback")
				}
			}); err != nil {
				t.Fatal(err)
			}
			root := staged.Publish()
			staged = nil
			originalTables = []cowTable{{root: root, resources: &cowGenerationResources{}}}
			root = nil
			if frozen {
				cut.frozen = originalTables
				cut.shards = make([]cowTable, 1)
			} else {
				cut.shards = originalTables
			}
			cut.buildDomains()
			retired := writer.Close()
			writer = nil
			retired.Drain()
			if err := cut.drain(); err != nil {
				t.Fatal(err)
			}
			cut, originalTables = nil, nil
			budget.Close()
			if calls != 1 || budget.Stats().TotalBytes != 0 {
				t.Fatalf("original cleanup/refund mismatch: calls=%d stats=%+v", calls, budget.Stats())
			}
		})
	}
}

func TestCOWCancelledTablesDetachBeforeOriginalResourceCleanup(t *testing.T) {
	_, c := cowCutFixture(t)
	c.writerMu.Lock()
	p, err := c.prepare([][]memtable.COWMutation{{{Key: []byte("private"), Value: []byte("value")}}, nil}, []memtable.COWPrepareOptions{{ResourceSlots: 1, ResourceBytes: 512}, {}})
	if err != nil {
		t.Fatal(err)
	}
	next := p.next
	calls := 0
	if err := p.prepared[0].AttachResources([]memtable.COWResourceID{{Kind: 1, ID: 83}}, func() {
		calls++
		if next.shards != nil || next.sources != nil || next.domains != nil || p.prepared != nil {
			t.Error("cancelled private aliases survived original resource destruction")
		}
		if c.budget.Stats().DeferredBytes == 0 {
			t.Error("original resource charge ended before callback")
		}
	}); err != nil {
		t.Fatal(err)
	}
	p.cancel()
	if next.shards[0].root != nil || next.shards[0].resources != nil || p.prepared[0] != nil {
		t.Fatal("original staging alias survived Cancel refund")
	}
	c.writerMu.Unlock()
	if err := p.drainCancelled(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("original cancellation destructor calls=%d", calls)
	}
}
