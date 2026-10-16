package primaryarena

import (
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestPrimaryPrivateClaimWrapperAdmissionAndTerminalRelease(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.SetMetadataDecoder(func(Class, []byte) ([]MetadataEdge, error) { return nil, nil })
	r := testClaim(t, a, Component) // Reuse isolates wrapper admission from pager growth.
	if _, err := a.Drop(r, nil); err != nil {
		t.Fatal(err)
	}
	testDrain(t, a)
	before := a.MetadataOwner().Bytes()
	c, ready, err := a.PrepareClaim(Component, nil)
	if !ready || err != nil {
		t.Fatal(err)
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(Claim{})))
	if got := a.MetadataOwner().Bytes(); got != before+charge {
		t.Fatalf("private claim wrapper unadmitted: got %d want %d", got, before+charge)
	}
	if _, done, err := c.Step(nil); !done || err != nil {
		t.Fatal(err)
	}
	if a.MetadataOwner().Bytes() != before || c.arena != nil || c.chunk != nil || c.growth != nil {
		t.Fatal("installed claim retained private aliases/charge")
	}
	if _, done, err := c.Step(nil); !done || err != nil {
		t.Fatal("terminal claim diagnostic changed")
	}
	if a.MetadataOwner().Bytes() != before {
		t.Fatal("terminal claim refunded twice")
	}
	pair := testBundle(t, a)
	for _, ref := range []Ref{pair.Directory, pair.Record} {
		if _, err := a.Drop(ref, nil); err != nil {
			t.Fatal(err)
		}
	}
	testDrain(t, a)
	before = a.MetadataOwner().Bytes()
	b, ready, err := a.PrepareBundleClaim(nil)
	if !ready || err != nil {
		t.Fatal(err)
	}
	charge = retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(BundleClaim{})))
	if a.MetadataOwner().Bytes() != before+charge {
		t.Fatal("private pair wrapper unadmitted")
	}
	if done, err := b.Cancel(nil); !done || err != nil {
		t.Fatal(err)
	}
	if a.MetadataOwner().Bytes() != before || b.arena != nil || b.chunk != nil || b.growth != nil {
		t.Fatal("canceled pair retained private aliases/charge")
	}
	if done, err := b.Cancel(nil); !done || err != nil || a.MetadataOwner().Bytes() != before {
		t.Fatal("cancel refunded twice")
	}
}
