package rootpublication

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestNamespaceProofReleaseActualHandlesManyCapturesAndFutureAdd(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
	if err != nil {
		t.Fatal(err)
	}
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = owner.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}

	if err = owner.AddPending(16); err != nil {
		t.Fatal(err)
	}
	held := proof
	if err = proof.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	proof = nil
	if held.parent != nil || held.persistence != nil || held.metadata != nil || !held.released.Load() {
		t.Fatal("actual disposal/aliases incomplete")
	}
	if err = held.ReleaseWithError(); err != nil {
		t.Fatal("terminal replay", err)
	}
	owner.RemovePending(16)
	for i, e := range captures {
		e.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("capture admission leaked")
		}
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != 0 {
		t.Fatal("completed proof retained admission")
	}
}
