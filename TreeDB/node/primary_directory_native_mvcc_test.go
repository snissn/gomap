package node

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestPrimaryDirectoryReplacementAdmitsActualOperands(t *testing.T) {
	base, _ := primaryTestOperand(2, []byte("base"), []byte("value"), 1, false)
	entries := make([]PrimaryDirectoryEntry, 3)
	for i, key := range []string{"a", "c", "e"} {
		op, _ := primaryTestOperand(uint64(10+i), []byte(key), []byte("value"), 2, false)
		entries[i] = PrimaryDirectoryEntry{Key: []byte(key), Operand: op, Revision: 2, Kind: PrimaryPut, ClassSlot: uint16(i)}
	}
	image := make([]byte, page.PageSize)
	if err := EncodePrimaryDirectory(image, 30, 1, base, entries); err != nil {
		t.Fatal(err)
	}
	view, err := DecodePrimaryDirectory(image)
	if err != nil {
		t.Fatal(err)
	}
	replacement := PrimaryDirectoryEntry{Key: []byte("a"), Revision: 3, Kind: PrimaryAbsence, ClassSlot: 0}
	dst := bytes.Repeat([]byte{0xac}, page.PageSize)
	before := bytes.Clone(dst)
	probes := 0
	_, ready, err := view.ReplacePrimaryComponentWithVisit(dst, 31, replacement, func(observed PrimaryDirectoryEntry) bool {
		probes++
		if probes == 1 && !bytes.Equal(observed.Key, []byte("c")) {
			t.Fatal("wrong actual first operand", string(observed.Key))
		}
		return probes < 2
	})
	if ready || err != nil || probes != 2 || !bytes.Equal(dst, before) {
		t.Fatalf("refused effect: ready=%v err=%v probes=%d", ready, err, probes)
	}
	next, ready, err := view.ReplacePrimaryComponentWithVisit(dst, 31, replacement, func(PrimaryDirectoryEntry) bool { return true })
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	decoded, err := DecodePrimaryDirectory(dst)
	if err != nil {
		t.Fatal(err)
	}
	if next.Count() != view.Count() || decoded.Count() != view.Count() {
		t.Fatal("changed class count")
	}
	got, found := decoded.Search(replacement.Key)
	if !found || !got.InlineAbsence() || got.ClassSlot != 0 || got.Revision != 3 {
		t.Fatal("wrong exact replacement", got)
	}
	for i := 1; i < len(entries); i++ {
		got, _ := decoded.ClassEntry(uint16(i))
		if !bytes.Equal(got.Key, entries[i].Key) || got.Operand != entries[i].Operand || got.Revision != entries[i].Revision || got.Kind != entries[i].Kind {
			t.Fatal("unrelated class changed", i)
		}
	}
	ordinary := make([]byte, page.PageSize)
	if _, err := view.ReplacePrimaryComponent(ordinary, 31, replacement); err != nil || !bytes.Equal(ordinary, dst) {
		t.Fatal("ordinary delegation changed bytes", err)
	}
}
