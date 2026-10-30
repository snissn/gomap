package rootpublication

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestKnownLinkNamespaceMetadataAdmission(t *testing.T) {
	if !StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace unavailable")
	}
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	const name = "segment.tca"
	child, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	identity, err := StableIdentityFromFile(child)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := registry.Unobserve(identity); err != nil {
			t.Errorf("unobserve: %v", err)
		}
	}()
	pin, err := registry.Pin(identity)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	if _, err := registry.BeginDelete(identity); !errors.Is(err, ErrResourcePinned) {
		t.Fatalf("delete while pinned: %v", err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	spec := StableNamespaceSpec{Parent: parent, LinkedResource: child, ParentGeneration: 1, Operation: NamespaceCreate, NewName: name, DiagnosticPath: "column-assets/segments"}
	if token, err := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(spec, nil); token != nil || !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil owner: token=%v err=%v", token, err)
	}
	if token, err := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(spec, &owner); token != nil || !errors.Is(err, ErrNamespaceUnstable) {
		t.Fatalf("unknown link: token=%v err=%v", token, err)
	}
	if owner.Bytes() != baseline {
		t.Fatal("unknown link changed accounting")
	}
	if err := parent.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := registry.RememberStableNamespaceLink(parent, child, name); err != nil {
		t.Fatal(err)
	}
	pinsBefore := registry.Stats().ActivePins
	budget.cap = baseline
	if token, err := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(spec, &owner); token != nil || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("capacity refusal: token=%v err=%v", token, err)
	}
	if owner.Bytes() != baseline || registry.Stats().ActivePins != pinsBefore {
		t.Fatal("refusal changed accounting or physical pin custody")
	}
	budget.cap = 1 << 20
	token, err := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(spec, &owner)
	if err != nil {
		t.Fatal(err)
	}
	if token.metadata != &owner || !token.metadataScoped || owner.Bytes() <= baseline {
		token.Release()
		t.Fatal("namespace not charged to requested owner")
	}
	if err := token.validateStable(); err != nil {
		token.Release()
		t.Fatal(err)
	}
	if err := token.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != baseline {
		t.Fatalf("namespace retained charge: baseline=%d current=%d", baseline, owner.Bytes())
	}
	if err := registry.ForgetStableNamespaceLink(parent, child, name); err != nil {
		t.Fatal(err)
	}
	if token, err := registry.NewStableNamespaceTokenForKnownLinkWithMetadata(spec, &owner); token != nil || !errors.Is(err, ErrNamespaceUnstable) {
		if token != nil {
			token.Release()
		}
		t.Fatalf("forgotten link: token=%v err=%v", token, err)
	}
	if owner.Bytes() != baseline || registry.Stats().ActivePins != pinsBefore {
		t.Fatal("forgotten link changed custody")
	}
}
