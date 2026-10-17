package collections

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

func TestNativeSparseCustodyReleasesOnlyOriginalPlannerEdges(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	work, err := newNativeMutationCustody(1, 0, budget)
	if err != nil {
		t.Fatal(err)
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("value"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	work.adoptDomain(domain)
	admit := func(bytes uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(bytes) }
	directory := nativeTestDomainDirectory(t, nil, domain, admit)
	record := work.ReleaseNativeCustody()
	if record != &work.cleanup {
		t.Fatal("replacement custody cleanup")
	}
	completion, err := record.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if work.domains != nil || work.collections != nil || work.refund != nil || record.Work != nil {
		t.Fatal("planner graph escaped into refund")
	}
	work = nil
	record = nil
	completion.Complete()
	if got, _ := domain.pending.Get([]byte("key")); got.Value != "value" {
		t.Fatal("accepted planner release lost engine domain edge", got)
	}
	domain = nil
	budget.Close()
	done, err := finishNativeDomainDirectory(&directory)
	if err != nil {
		t.Fatal(err)
	}
	done.Complete()
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

func TestNativeSparseAssignedFailureKeepsExactOriginalPlanAndFirstError(t *testing.T) {
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	work, err := newNativeMutationCustody(1, 0, budget)
	if err != nil {
		t.Fatal(err)
	}
	domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("accepted"), Flags: node.FlagInline}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	work.adoptDomain(domain)
	original := errors.New("assigned native private asset preparation failed")
	work.RetainAssignedNativeFailure(backenddb.NativeFinalizedPublication{}, original)
	work.RetainAssignedNativeFailure(backenddb.NativeFinalizedPublication{}, errors.New("later observer"))
	before := budget.Stats()
	budget.Close()
	record := work.ReleaseNativeCustody()
	for i := 0; i < 2; i++ {
		completion, err := record.Finish()
		if err != original || completion.Refund != nil {
			t.Fatal("assigned ambiguity lost original failure", err)
		}
	}
	if record.Work != work || work.domains[0] != domain || domain.pending == nil || work.refund == nil {
		t.Fatal("assigned ambiguity dropped exact original custody")
	}
	if got, _ := domain.pending.Get([]byte("key")); got.Value != "accepted" {
		t.Fatal(got)
	}
	after := budget.Stats()
	if after.TotalBytes != before.TotalBytes || after.HistoryBytes != before.HistoryBytes || after.ReservedBytes != before.ReservedBytes {
		t.Fatal("assigned failed plans refunded", before, after)
	}
}
