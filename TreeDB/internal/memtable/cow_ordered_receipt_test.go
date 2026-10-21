package memtable

import "testing"

func TestCOWOrderedNodeReceiptIncludesDiscardedHistoryMaps(t *testing.T) {
	// Three numeric trees are created. The first two are discarded when their
	// histories shrink to singleton, including a remove/reinsert of the entire
	// primary entry. None may disappear from the preparation's allocation bill.
	entries := []COWMutation{
		cowOrderedStorageMutation(t, "shared", 1),
		cowOrderedStorageMutation(t, "shared", 2),
		cowOrderedStorageMutation(t, "shared", 1),
		cowOrderedStorageMutation(t, "shared", 3),
		cowOrderedStorageMutation(t, "shared", 2),
		cowOrderedStorageMutation(t, "shared", 3),
		cowOrderedStorageMutation(t, "shared", 4),
		cowOrderedStorageMutation(t, "shared", 5),
		cowOrderedStorageMutation(t, "shared", 6),
	}
	entries[2].Remove, entries[4].Remove, entries[5].Remove = true, true, true

	// Independently retain each numeric header through its removal. The receipt
	// remains readable even though the history itself no longer points at it.
	h := &cowOrderedHistory{}
	var expected uint64
	for _, entry := range entries {
		_, ts, _ := cowOrderedPrefix(entry.Key)
		if entry.Remove {
			numeric := h.versions
			h.remove(ts)
			if numeric != nil && h.versions == nil {
				expected += numeric.AllocationCharge()
			}
		} else {
			h.set(ts, cowOrderedValue{key: string(entry.Key), value: cowEncodeValue(entry)})
		}
	}
	if h.versions == nil {
		t.Fatal("oracle did not leave the final numeric tree live")
	}
	expected += h.versions.AllocationCharge()

	basis := newCOWOrderedTree()
	private := basis.fork()
	private.applyFrom(basis, entries)
	if private.historyNodeCharge != expected || expected == 0 {
		t.Fatalf("numeric receipt=%d want all allocated maps=%d", private.historyNodeCharge, expected)
	}
	if got := private.nodeAllocationCharge(); got != private.keys.AllocationCharge()+expected {
		t.Fatalf("combined receipt=%d primary=%d numeric=%d", got, private.keys.AllocationCharge(), expected)
	}
	sealed := private.immutableCopy()
	if sealed.nodeAllocationCharge() != 0 || sealed.fork().nodeAllocationCharge() != 0 || private.fork().nodeAllocationCharge() != 0 {
		t.Fatal("a reader or a new preparation inherited an earlier allocation receipt")
	}
	if private.nodeAllocationCharge() == 0 {
		t.Fatal("sealing erased the original preparation's receipt")
	}
}

func TestCOWOrderedNodeReceiptSaturates(t *testing.T) {
	private := newCOWOrderedTree()
	private.addHistoryNodeCharge(^uint64(0) - 1)
	private.addHistoryNodeCharge(2)
	if got := private.nodeAllocationCharge(); got != ^uint64(0) {
		t.Fatalf("overflow receipt=%d", got)
	}
}
