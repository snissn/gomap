package memtable

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
)

func TestCOWPrepareReconcilesOnlyUnusedNodeReservation(t *testing.T) {
	for _, shape := range []string{"raw", "ordered", "mixed"} {
		t.Run(shape, func(t *testing.T) {
			budget, err := NewCOWBudget(DefaultCOWLimits())
			if err != nil {
				t.Fatal(err)
			}
			var writer *COWWriter
			if shape == "raw" {
				writer, err = NewCOWWriter(budget)
			} else {
				writer, err = NewCOWOrderedWriter(budget)
			}
			if err != nil {
				t.Fatal(err)
			}
			var entries []COWMutation
			for i := range 256 {
				if shape != "raw" {
					entries = append(entries, cowOrderedLifecycleMutation(t, "shared", uint64(i+1), "value"))
				}
				if shape != "ordered" {
					entries = append(entries, COWMutation{Key: []byte(fmt.Sprintf("raw/%08d", i)), Value: []byte("value"), Flags: node.FlagInline})
				}
			}
			upper, err := writer.Estimate(entries, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			before := budget.Stats()
			prepared, err := writer.Prepare(entries, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			charge := prepared.Charge()
			actualNodes := prepared.private.AllocationCharge()
			if prepared.ordered != nil {
				actualNodes += prepared.ordered.nodeAllocationCharge()
			}
			if charge.Nodes != actualNodes || charge.Nodes >= upper.Nodes {
				t.Fatalf("node receipt not reconciled: actual=%d charge=%+v upper=%+v", actualNodes, charge, upper)
			}
			if charge.Payload != upper.Payload || charge.Wrappers != upper.Wrappers || charge.Resources != upper.Resources || charge.Extra != upper.Extra || charge.Temporary != upper.Temporary {
				t.Fatalf("reconciliation changed non-node reservation: charge=%+v upper=%+v", charge, upper)
			}
			after := budget.Stats()
			if after.TotalBytes-before.TotalBytes != charge.Total() || after.ReservedBytes-before.ReservedBytes != charge.Total() {
				t.Fatalf("pending accounting does not match receipt: before=%+v after=%+v charge=%+v", before, after, charge)
			}
			retirement := prepared.Cancel()
			retirement.Drain()
			if got := budget.Stats(); got.TotalBytes != before.TotalBytes || got.ReservedBytes != before.ReservedBytes || got.HistoryBytes != before.HistoryBytes {
				t.Fatalf("cancel did not refund original remainder exactly: got=%+v before=%+v", got, before)
			}
			prepared, err = writer.Prepare(entries, COWPrepareOptions{})
			if err != nil {
				t.Fatal(err)
			}
			charge = prepared.Charge()
			root := prepared.Publish()
			if got := budget.Stats(); got.HistoryBytes-before.HistoryBytes != charge.History() || got.ReservedBytes != before.ReservedBytes {
				t.Fatalf("publication did not transfer reconciled receipt: got=%+v before=%+v charge=%+v", got, before, charge)
			}
			if root.Len() != len(entries) {
				t.Fatalf("published versions=%d want=%d", root.Len(), len(entries))
			}
			cowTestRelease(root)
			cowTestClose(writer)
			budget.Close()
			if got := budget.Stats(); got.TotalBytes != 0 {
				t.Fatalf("final receipt retained: %+v", got)
			}
		})
	}
}

func TestCOWNodeReconciliationRejectsOverrunWithoutPartialRefund(t *testing.T) {
	budget, writer := cowTestWriter(t, DefaultCOWLimits())
	old := cowTestPublish(t, writer, []COWMutation{{Key: []byte("key"), Value: []byte("old")}})
	prepared, err := writer.Prepare([]COWMutation{{Key: []byte("key"), Value: []byte("new")}}, COWPrepareOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before, original := budget.Stats(), prepared.Charge()
	writer.mu.Lock()
	err = prepared.reconcileNodes(original.Nodes + 1)
	writer.mu.Unlock()
	if !errors.Is(err, ErrCOWCapacity) || prepared.Charge() != original || budget.Stats() != before {
		t.Fatalf("node overrun changed reservation: charge=%+v original=%+v stats=%+v before=%+v err=%v", prepared.Charge(), original, budget.Stats(), before, err)
	}
	retirement := prepared.Cancel()
	retirement.Drain()
	if got, ok := old.Get([]byte("key")); !ok || got.Value != "old" {
		t.Fatal("refused/cancelled preparation changed held root")
	}
	next := cowTestPublish(t, writer, []COWMutation{{Key: []byte("key"), Value: []byte("accepted")}})
	if got, ok := next.Get([]byte("key")); !ok || got.Value != "accepted" {
		t.Fatal("refusal consumed later admission")
	}
	cowTestRelease(next)
	cowTestRelease(old)
	cowTestClose(writer)
	budget.Close()
	if got := budget.Stats(); got.TotalBytes != 0 {
		t.Fatalf("overrun lifecycle leaked: %+v", got)
	}
}
