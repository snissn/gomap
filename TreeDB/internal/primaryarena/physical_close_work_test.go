package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"path/filepath"
	"testing"
)

type closeBudget struct{ bytes uint64 }
type closeLease struct {
	budget *closeBudget
	bytes  uint64
}

func (b *closeBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	b.bytes += n
	return &closeLease{b, n}, nil
}
func (l *closeLease) Resize(n uint64) error {
	l.budget.bytes = l.budget.bytes - l.bytes + n
	l.bytes = n
	return nil
}
func (l *closeLease) Close() { l.budget.bytes -= l.bytes; l.bytes = 0 }

func TestArenaPhysicalCloseOriginalCaptureRemains(t *testing.T) {
	a, err := Open(filepath.Join(t.TempDir(), "arena.db"))
	if err != nil {
		t.Fatal(err)
	}
	b := &closeBudget{}
	e, err := a.MetadataOwner().Enroll(b)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if !a.MetadataOwner().PhysicalClosed() || b.bytes == 0 {
		t.Fatal("physical cleanup erased held capture metadata")
	}
	e.Close()
	if b.bytes != 0 || a.MetadataOwner().Bytes() != 0 {
		t.Fatal("last actual capture did not refund")
	}
}
