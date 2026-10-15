package rootpublication

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// A record's two owning edges remain borrowed across recursive child decoding.
// Validation-only dependency capacity must not be retained by that small result.
func TestOwnedPrimaryMetadataScratchKeepsOnlyActualEdgesV5(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.SetMetadataDecoder(PrimaryBankMetadataEdgesV5); err != nil {
		t.Fatal(err)
	}
	record, banks := metadataSlot(t, a, 2)
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)
	var retained []primaryarena.MetadataEdgeScratch
	defer func() {
		for _, s := range retained {
			s.Close()
		}
	}()
	for _, ref := range []primaryarena.Ref{record, banks[2]} {
		class, _, ok, err := a.Identity(ref, nil)
		if !ok || err != nil {
			t.Fatal(err)
		}
		image, err := a.Get(ref.PageID)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := PrimaryBankMetadataEdgesV5(class, image)
		if err != nil {
			t.Fatal(err)
		}
		before := owner.Bytes()
		s, err := PrimaryBankMetadataEdgesOwnedV5(class, image, owner)
		if err != nil {
			t.Fatal(err)
		}
		retained = append(retained, s)
		if !reflect.DeepEqual(s.Edges(), expected) {
			t.Fatal("canonical owning edges changed")
		}
		if used := owner.Bytes() - before; used > page.PageSize {
			t.Fatalf("small metadata class %v retains %d bytes for %d edges; exceeds one page", class, used, len(expected))
		}
	}
	parent := append([]primaryarena.MetadataEdge(nil), retained[0].Edges()...)
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetPageID(primaryarena.Namespace | 100)
	n.SetType(page.PageTypeInternal)
	for i := 0; i < 4; i++ {
		if err := n.AddInternalChild([]byte{byte('a' + i)}, primaryarena.Namespace|uint64(101+i)); err != nil {
			t.Fatal(err)
		}
	}
	n.UpdateChecksum()
	expected, err := PrimaryBankMetadataEdgesV5(primaryarena.Dependency, image)
	if err != nil {
		t.Fatal(err)
	}
	baseline := owner.Bytes()
	child, err := PrimaryBankMetadataEdgesOwnedV5(primaryarena.Dependency, image, owner)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(child.Edges(), expected) || len(expected) != 4 {
		t.Fatal("dependency children truncated")
	}
	if !reflect.DeepEqual(parent, retained[0].Edges()) {
		t.Fatal("recursive child decode overwrote retained parent edges")
	}
	child.Close()
	child.Close()
	if owner.Bytes() != baseline {
		t.Fatal("dependency scratch charge not refunded once")
	}
	image[len(image)-1] ^= 1
	if s, err := PrimaryBankMetadataEdgesOwnedV5(primaryarena.Dependency, image, owner); err == nil || s != nil || owner.Bytes() != baseline {
		t.Fatal("corrupt dependency accepted or admission leaked")
	}
	for _, s := range retained {
		s.Close()
		s.Close()
	}
	if owner.Bytes() != 0 {
		t.Fatal("parent scratch survived final use")
	}
	retained = nil
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}
