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
func TestOriginalWriterOrdinaryPredecessorRefusalStopsRotation(t *testing.T) {
	pins := rootpublication.NewIdentityPinRegistry()
	dir := t.TempDir()
	w, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "value-l0-000001.log"), 1, pins)
	if err != nil {
		t.Fatal(err)
	}
	originalParent := w.stableParent
	if err = originalParent.Close(); err != nil {
		t.Fatal(err)
	}
	first := w.RotateToWithSync(filepath.Join(dir, "value-l0-000002.log"), 2, false)
	if !errors.Is(first, os.ErrClosed) || !RotationInstalled(first) {
		t.Fatalf("original parent refusal after install=%v", first)
	}
	if w.failedPredecessor.parent != originalParent || !w.failedPredecessor.failed {
		t.Fatal("failed predecessor custody lost")
	}
	if _, err = w.Append(0, nil, 1, []byte("cannot append after destructive refusal")); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("append after refusal=%v", err)
	}
	if err = w.RotateToWithSync(filepath.Join(dir, "value-l0-000003.log"), 3, false); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("rotation after refusal=%v", err)
	}
	terminal := w.Close()
	if terminal != w.closeErr || !errors.Is(terminal, os.ErrClosed) {
		t.Fatalf("terminal first receipt=%v", terminal)
	}
	if w.failedPredecessor.parent != originalParent {
		t.Fatal("terminal close retried/forgot refused original predecessor")
	}
	if w.Close() != terminal {
		t.Fatal("terminal close replay changed receipt")
	}
	if _, err = os.Stat(filepath.Join(dir, "value-l0-000003.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed-stop rotation had a physical effect")
	}
}
func TestOriginalWriterSuccessfulCapacityDrains(t *testing.T) {
	pins := rootpublication.NewIdentityPinRegistry()
	dir := t.TempDir()
	warmup, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "value-l0-000000.log"), 0, pins)
	if err != nil {
		t.Fatal(err)
	}
	if err = warmup.Close(); err != nil {
		t.Fatal(err)
	}
	// Registry table capacity survives successful observations by design.
	before := pins.MetadataOwner().Bytes()
	w, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "value-l0-000001.log"), 1, pins)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint32(2); id <= 4; id++ {
		if err = w.RotateToWithSync(filepath.Join(dir, "value-l0-00000"+string(rune('0'+id))+".log"), id, false); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := pins.MetadataOwner().Bytes(); got != before {
		t.Fatalf("actual successful Writer capacity=%d baseline=%d", got, before)
	}
}

func TestOriginalWriterPartialConstructionRefusalRetainsOperand(t *testing.T) {
	pins := rootpublication.NewIdentityPinRegistry()
	dir := t.TempDir()
	partial, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = partial.Close(); err != nil {
		t.Fatal(err)
	}
	originalOpen := openStableValueLogParent
	captureCause := errors.New("original partial parent capture")
	openStableValueLogParent = func(string) (*os.File, error) { return partial, captureCause }
	defer func() { openStableValueLogParent = originalOpen }()
	w, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "value-l0-000001.log"), 1, pins)
	if w != nil {
		t.Fatal("construction published after physical cleanup refusal")
	}
	var receipt *WriterCleanupFailure
	if !errors.As(err, &receipt) || !errors.Is(err, os.ErrClosed) || !errors.Is(err, captureCause) {
		t.Fatalf("partial original receipt=%v", err)
	}
	original := receipt.writer
	if original == nil || original.stableParent != partial || original.f != nil {
		t.Fatal("partial containing original operands not retained truthfully")
	}
	if original.Close() != receipt {
		t.Fatal("partial constructor cleanup replayed")
	}
	if original.metadataCharge == 0 {
		t.Fatal("partial original capacity refunded")
	}
}
