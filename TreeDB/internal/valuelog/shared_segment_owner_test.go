package valuelog

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

func TestStableOuterRawCapturesReuseSegmentHandleAndNamespace(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000001.vlog")
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(path, 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	registration := StableResourceRegistration{LogicalLane: "leaf", Generation: 1, DiagnosticPath: "leaf/000001.vlog", Reachability: rootpublication.ReachabilityOuterLeafRawPointer, ParentGeneration: 1, NamespaceOperation: rootpublication.NamespaceCreate}
	first, err := writer.StableResourceToken(registration)
	if err != nil {
		writer.Close()
		t.Fatal(err)
	}
	owner := writer.stableSegmentOwner
	if owner == nil {
		first.Release()
		writer.Close()
		t.Skip("pinned constructor census unavailable on this target")
	}
	second, err := writer.StableResourceToken(registration)
	if err != nil {
		first.Release()
		writer.Close()
		t.Fatal(err)
	}
	if writer.stableSegmentOwner != owner || first.Namespace() != second.Namespace() {
		t.Fatal("repeated capture duplicated namespace backing")
	}
	var firstFD, secondFD uintptr
	if err = first.WithPinnedFile(func(f *os.File) error { firstFD = f.Fd(); return nil }); err != nil {
		t.Fatal(err)
	}
	if err = second.WithPinnedFile(func(f *os.File) error { secondFD = f.Fd(); return nil }); err != nil {
		t.Fatal(err)
	}
	if firstFD != secondFD || registry.PinCount(first.Identity()) != 2 {
		t.Fatal("capture duplicated handle or lost independent deletion pins")
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if writer.stableSegmentOwner != nil {
		t.Fatal("Writer.Close retained cache ownership")
	}
	if err = second.WithPinnedFile(func(f *os.File) error { _, e := f.Stat(); return e }); err != nil {
		t.Fatalf("writer Close revoked actual captured handle: %v", err)
	}
	first.Release()
	if registry.PinCount(second.Identity()) != 1 {
		t.Fatal("first capture terminal revoked second")
	}
	second.Release()
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		t.Fatal("final capture leaked registry identity")
	}
}
func TestStableRotationAttemptDebitIsCumulativeAndPendingIsNotBirth(t *testing.T) {
	var bytes uint64
	metadata, err := NewFiniteStableMetadata(4, 2, func(n uint64) error { bytes += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.AdmitRotationAttempt(); err != nil {
		t.Fatal(err)
	}
	// A failed actual Open still consumes this admitted attempt.
	if _, err = os.Open(filepath.Join(t.TempDir(), "absent")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if err = metadata.AdmitRotationAttempt(); err != nil {
		t.Fatal(err)
	}
	if err = metadata.AdmitRotationAttempt(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatal("failed attempt credit was refunded")
	}
	if metadata.rotations != 2 || bytes == 0 {
		t.Fatal("attempt accounting lost cumulative births")
	}
	if err = metadata.Close(); err != nil {
		t.Fatal(err)
	}
	if err = metadata.AdmitRotationAttempt(); !errors.Is(err, ErrFiniteWriterLoan) {
		t.Fatal("closed owner admitted new attempt")
	}
}
