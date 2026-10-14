package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestOriginalWriterClosePhysicalRefusalRetainsOperand(t *testing.T) {
	pins := rootpublication.NewIdentityPinRegistry()
	before := pins.MetadataOwner().Bytes()
	w, err := NewWriterWithStableResourcePinRegistry(filepath.Join(t.TempDir(), "value-l0-000001.log"), 1, pins)
	if err != nil {
		t.Fatal(err)
	}
	original := w.f
	if err = original.Close(); err != nil {
		t.Fatal(err)
	}
	first := w.Close()
	if !errors.Is(first, os.ErrClosed) {
		t.Fatalf("actual final close refusal=%v", first)
	}
	if w.f != original {
		t.Fatal("failed actual file operand lost")
	}
	if w.stableParent != nil {
		t.Fatal("successful parent constituent remained an owning alias")
	}
	if pins.MetadataOwner().Bytes() <= before {
		t.Fatal("failed Writer containing/file capacity refunded")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 32; j++ {
				if next := w.Close(); next != first {
					t.Errorf("Close replay changed original first result: %v", next)
				}
				if !errors.Is(first, os.ErrClosed) {
					t.Error("lost physical cause")
				}
			}
		}()
	}
	wg.Wait()
}
