package collections

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type nativeRolloverAdmissionControl struct {
	*memtable.COWBudget
	remaining int
	err       error
}

func (a *nativeRolloverAdmissionControl) AdmitNativeBytes(n uint64) (*memtable.COWExternalLease, error) {
	if a.remaining == 0 {
		return nil, a.err
	}
	a.remaining--
	return a.COWBudget.AdmitNativeBytes(n)
}

func nativeTestFinishEngine(t *testing.T, slot **nativeEngineVersion) {
	t.Helper()
	e := *slot
	w := e.Release()
	*slot = nil
	if w == nil {
		return
	}
	done, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	e, w = nil, nil
	done.Complete()
}

func TestNativeEngineRolloverPartialAdmissionOwnsExactSparseWork(t *testing.T) {
	// Refuse each successive original allocation. Every private partial result
	// must own enough cleanup to restore the opening engine's exact accounting.
	for stop := 0; stop < 40; stop++ {
		budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		admit := func(n uint64) (cowbtree.OwnedMapLease, error) { return budget.AdmitNativeBytes(n) }
		schema, err := newNativeSchemaState(&CollectionMeta{Name: "collection"}, admit)
		if err != nil {
			t.Fatal(err)
		}
		domain, err := prepareNativeDomainUpdate(nil, "primary", 0, 1, 0, []memtable.COWMutation{{Key: []byte("key"), Value: []byte("old")}}, budget)
		if err != nil {
			t.Fatal(err)
		}
		base := nativeTestEngine(t, budget, nil, domain, schema)
		domain, schema = nil, nil
		refusal := errors.New("original rollover allocation refused")
		control := &nativeRolloverAdmissionControl{COWBudget: budget, remaining: stop, err: refusal}
		// Configure the private fixture before using it as an opening cut.
		base.maintenance = control
		before := budget.Stats()
		next, err := prepareNativeEngineRollover(base)
		if err != nil && err != refusal {
			t.Fatal(err)
		}
		if stop == 0 && next != nil {
			t.Fatal("preallocation refusal created an owner")
		}
		if next != nil {
			if next == base {
				t.Fatal("active pending work did not rotate")
			}
			if err == nil {
				c, _ := next.collections.Get(1)
				d, _ := c.domains.Get("primary")
				if d.pending != nil || d.captured == nil {
					t.Fatal("incomplete whole successor")
				}
				if row, ok := d.captured.get([]byte("key")); !ok || row.Value != "old" {
					t.Fatal(row, ok)
				}
				c, d = nil, nil
			}
			nativeTestFinishEngine(t, &next)
		}
		after := budget.Stats()
		if after.TotalBytes != before.TotalBytes || after.ReservedBytes != before.ReservedBytes || after.Sources != before.Sources {
			t.Fatalf("stage %d changed original accounting: before=%+v after=%+v", stop, before, after)
		}
		c, _ := base.collections.Get(1)
		d, _ := c.domains.Get("primary")
		if row, ok := d.pending.Get([]byte("key")); !ok || row.Value != "old" {
			t.Fatal(row, ok)
		}
		budget.Close()
		c, d = nil, nil
		nativeTestFinishEngine(t, &base)
		if budget.Stats().TotalBytes != 0 {
			t.Fatal(budget.Stats())
		}
		if err == nil {
			return
		}
	}
	t.Fatal("never reached complete rollover")
}
