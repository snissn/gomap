package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableOuterLeafProducerFamilyApplyReuseRotationAndFailure(t *testing.T) {
	if !rootpublication.StableNamespaceCreationSupported() {
		t.Skip("creation certificates unavailable")
	}
	dir := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	writer, err := NewWriterWithStableResourcePinRegistry(filepath.Join(dir, "000001.vlog"), 1, registry)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	epoch, err := writer.StableNamespaceParentGeneration()
	if err != nil {
		t.Fatal(err)
	}
	reg := StableResourceRegistration{Kind: rootpublication.ResourceOuterLeafLog, LogicalLane: "outer-leaf-0", Generation: 1, DiagnosticPath: "leaf/000001.vlog", Reachability: rootpublication.ReachabilityOuterLeafRawPointer, ParentGeneration: epoch, NamespaceOperation: rootpublication.NamespaceCreate, PinRegistry: registry}
	original := bindRetainedValueLogStableNamespaceCreationProof
	defer func() { bindRetainedValueLogStableNamespaceCreationProof = original }()
	injected := errors.New("bind failed")
	bindRetainedValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, path string) (*rootpublication.StableNamespaceToken, error) {
		return nil, injected
	}
	if token, family, err := writer.StableOuterLeafResourceTokenForApply(reg, nil); token != nil || family != nil || !errors.Is(err, injected) {
		token.Release()
		family.Release()
		t.Fatalf("failed family=%v", err)
	}
	if registry.Stats().ActivePins != 0 {
		t.Fatal("failed constructor leaked pins")
	}
	binds := 0
	bindRetainedValueLogStableNamespaceCreationProof = func(proof *rootpublication.StableNamespaceCreationProof, parent *os.File, generation uint64, name, path string) (*rootpublication.StableNamespaceToken, error) {
		binds++
		return original(proof, parent, generation, name, path)
	}
	if _, err := writer.Append(0, nil, 1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	first, family, err := writer.StableOuterLeafResourceTokenForApply(reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	defer family.Release()
	firstEnd := first.Frontier().Bytes
	if _, err := writer.Append(0, nil, 2, []byte("second")); err != nil {
		t.Fatal(err)
	}
	second, reused, err := writer.StableOuterLeafResourceTokenForApply(reg, family)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if reused != family || binds != 1 || first.Frontier().Bytes != firstEnd || second.Frontier().Bytes <= firstEnd {
		t.Fatal("same writer did not reuse exact family with immutable flushed frontiers")
	}
	public, err := writer.StableResourceToken(reg)
	if err != nil {
		t.Fatal(err)
	}
	public.Release()
	if binds != 2 {
		t.Fatal("public capture unexpectedly reused private family")
	}
	if err := writer.RotateToWithSync(filepath.Join(dir, "000002.vlog"), 2, true); err != nil {
		t.Fatal(err)
	}
	reg.Generation, reg.DiagnosticPath = 2, "leaf/000002.vlog"
	third, successor, err := writer.StableOuterLeafResourceTokenForApply(reg, family)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	defer successor.Release()
	if successor == family || third.Identity() == first.Identity() {
		t.Fatal("rotation reused old physical authority")
	}
	family.Release()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if token, _, err := writer.StableOuterLeafResourceTokenForApply(reg, successor); token != nil || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		token.Release()
		t.Fatalf("closed writer=%v", err)
	}
	successor.Release()
	if err := third.WithPinnedFile(func(file *os.File) error { _, err := file.Stat(); return err }); err != nil {
		t.Fatalf("successor output lost handle: %v", err)
	}
	first.Release()
	second.Release()
	third.Release()
	if registry.Stats().ActivePins != 0 {
		t.Fatal("last output release leaked pins")
	}
}
