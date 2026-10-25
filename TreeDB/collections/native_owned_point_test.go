package collections

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Real captured-domain points must reuse an already validated private leaf,
// rather than repeat physical traversal for every key in that same leaf.
func TestNativeOwnedPointCapturedLeaf(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	for i := 0; i < 64; i++ {
		id := []byte(fmt.Sprintf("d-%04d", i))
		if _, err := c.Insert(id, []byte(fmt.Sprintf(`{"content":"policy","tenant":"one","n":%d}`, i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	r, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if !r.domain.hasBase {
		t.Fatal("fixture did not publish a physical primary root")
	}
	scratch, err := r.physicalScratch()
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	var last page.LeafLogPtr
	readLeaf := func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
		reads++
		last = ptr
		return r.readLeaf(ptr, dst)
	}
	for _, i := range []int{0, 1, 2, 1, 0} {
		id := []byte(fmt.Sprintf("d-%04d", i))
		entry, err := r.domain.baseEntry(id, scratch, readLeaf)
		if err != nil || !bytes.Equal(entry.Key, id) {
			t.Fatalf("captured physical point %q: key=%q err=%v", id, entry.Key, err)
		}
		got := entry.Value
		if entry.Flags&node.FlagPointer != 0 {
			got, err = r.readValue(entry.ValuePtr, nil)
		}
		if err != nil || !bytes.Contains(got, []byte(`"content":"policy"`)) {
			t.Fatalf("captured physical value %q: %q err=%v", id, got, err)
		}
	}
	if reads != 1 {
		t.Fatalf("same captured leaf traversed %d times; want 1", reads)
	}
	// Text catalog points reconstruct this domain by value on every call.
	// Reuse follows the captured identity, never the temporary Tree address.
	scratch.point.Invalidate()
	reads = 0
	for _, key := range []string{"d-0000", "d-0001", "d-0002", "d-0000"} {
		domain, err := r.borrowedNamedDomain("primary")
		if err != nil {
			t.Fatal(err)
		}
		entry, err := domain.baseEntry([]byte(key), scratch, readLeaf)
		if err != nil || string(entry.Key) != key {
			t.Fatalf("reconstructed captured domain %q: key=%q err=%v", key, entry.Key, err)
		}
	}
	if reads != 1 {
		t.Fatalf("reconstructed domain traversed same leaf %d times; want1", reads)
	}
	// A projection has disjoint private storage. Its callback must not discard
	// the point view, whereas a successor using the point arrays must do so.
	var foreign [page.PageSize]byte
	if _, err := r.readLeaf(last, foreign[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := r.domain.baseEntry([]byte("d-0001"), scratch, readLeaf); err != nil || reads != 1 {
		t.Fatalf("disjoint reader discarded private point: reads=%d err=%v", reads, err)
	}
	if _, found, err := r.domain.seekGE([]byte("d-0002"), nil, scratch, readLeaf); err != nil || !found {
		t.Fatalf("interleaved successor: found=%v err=%v", found, err)
	}
	before := reads
	entry, err := r.domain.baseEntry([]byte("d-0000"), scratch, readLeaf)
	if err != nil || !bytes.Equal(entry.Key, []byte("d-0000")) || reads != before+1 {
		t.Fatalf("successor did not invalidate point: key=%q reads=%d before=%d err=%v", entry.Key, reads, before, err)
	}
	if _, err := r.domain.baseEntry([]byte("d-0001"), scratch, readLeaf); err != nil || reads != before+1 {
		t.Fatalf("point did not recover after successor: reads=%d err=%v", reads, err)
	}
}
