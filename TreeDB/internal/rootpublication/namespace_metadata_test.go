package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestPrimaryNamespaceMetadataRefusalAndIndependentBinding(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline
	if proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner); !errors.Is(err, retainedalloc.ErrCapacity) || proof != nil {
		t.Fatalf("proof refusal=%v %v", proof, err)
	}
	if owner.Bytes() != baseline {
		t.Fatal("proof refusal retained capacity")
	}
	budget.cap = 1 << 20
	proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
	if err != nil {
		t.Fatal(err)
	}
	proofBytes := owner.Bytes()
	budget.cap = proofBytes
	if token, err := proof.Bind(parent, 1, "primary", "primary"); !errors.Is(err, retainedalloc.ErrCapacity) || token != nil {
		t.Fatalf("binding refusal=%v %v", token, err)
	}
	if owner.Bytes() != proofBytes || proof.parent == nil {
		t.Fatal("binding refusal consumed proof")
	}
	budget.cap = 1 << 20
	token, err := proof.Bind(parent, 1, "primary", "primary")
	if err != nil {
		t.Fatal(err)
	}
	if err = token.retain(); err != nil {
		t.Fatal(err)
	}
	afterBind := owner.Bytes()
	if err = proof.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if proof.parent != nil || proof.metadata != nil || proof.name != "" || owner.Bytes() >= afterBind {
		t.Fatal("proof did not retire its own exact storage")
	}
	if err = token.validateStable(); err != nil {
		t.Fatal("binding borrowed released proof", err)
	}
	clone, err := token.cloneStable()
	if err != nil {
		t.Fatal(err)
	}
	afterClone := owner.Bytes()
	token.Release() // retained physical token edge prevents public release
	if owner.Bytes() != afterClone {
		t.Fatal("caller release stole retained token")
	}
	token.release()
	if token.parent != nil || token.metadata != nil {
		t.Fatal("last token edge did not clear owned storage")
	}
	if err = clone.validateStable(); err != nil {
		t.Fatal("clone borrowed original namespace storage", err)
	}
	clone.Release()
	clone.Release()
	proof.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("owned proof/clone bytes leaked=%d baseline=%d", owner.Bytes(), baseline)
	}
	enrollment.Close()
	if budget.bytes != 0 {
		t.Fatal("successful borrowing did not detach")
	}
}

func TestPrimaryNamespaceFailedSyncUnwindsOwnedBacking(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	proof, err := newStableNamespaceCreationProofWithMetadata(parent, child, "primary", unsupportedNamespaceAdapter{}, &owner)
	if !errors.Is(err, ErrNamespacePersistenceUnsupported) || proof != nil {
		t.Fatalf("failed sync=%v %v", proof, err)
	}
	if owner.Bytes() != baseline {
		t.Fatal("failed creation sync retained disposed storage")
	}
	if _, err = child.Write([]byte("persistent")); err != nil {
		t.Fatal("unwind closed caller's child", err)
	}
}

// Readers of selected token/proof storage join its existing lifetime lock;
// successful cleanup must not race borrowed names or the adapter/handle.
func TestPrimaryNamespaceMetadataConcurrentCleanup(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	proof, err := NewOwnedStableNamespaceCreationProof(parent, child, "primary", &owner)
	if err != nil {
		t.Fatal(err)
	}
	token, err := proof.Bind(parent, 1, "primary", "primary")
	if err != nil {
		t.Fatal(err)
	}
	clone, err := token.cloneStable()
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				_ = token.validateStable()
				_ = token.validateLinkedResource(proof.childID)
				_ = token.compatible(clone)
				_ = clone.compatible(token)
				bound, e := proof.Bind(parent, 1, "primary", "primary")
				if e == nil {
					bound.Release()
				}
			}
		}()
	}
	close(start)
	token.Release()
	proof.Release()
	wg.Wait()
	clone.Release()
	if err := token.validateStable(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("released owned token=%v", err)
	}
	if owner.Bytes() != 0 {
		t.Fatalf("cleanup retained %d", owner.Bytes())
	}
}

// The original construction proof and newly admitted binding have independent
// storage ownership. Reusing its completed fence must not borrow its names.
func TestNamespaceExistingProofSuppliedMetadataIndependentNoResync(t *testing.T) {
	dir := t.TempDir()
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	child, err := os.Create(filepath.Join(dir, "segment"))
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	proof, err := NewStableNamespaceCreationProof(parent, child, "segment")
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline
	if token, e := proof.BindWithMetadata(parent, 7, "segment", "value_vlog", &owner); !errors.Is(e, retainedalloc.ErrCapacity) || token != nil {
		t.Fatalf("refusal=%v %v", token, e)
	}
	if owner.Bytes() != baseline || proof.parent == nil {
		t.Fatal("refusal consumed storage")
	}
	budget.cap = 1 << 20
	token, err := proof.BindWithMetadata(parent, 7, "segment", "value_vlog", &owner)
	if err != nil {
		t.Fatal(err)
	}
	if token.metadata != &owner || token.syncs.Load() != proof.syncs.Load() || token.syncs.Load() != 1 {
		t.Fatal("binding did not reuse original completed fence")
	}
	if err = proof.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = token.validateStable(); err != nil {
		t.Fatal("binding borrowed original proof", err)
	}
	clone, err := token.cloneStable()
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
	if err = clone.validateStable(); err != nil {
		t.Fatal("clone borrowed released binding", err)
	}
	clone.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("binding retained %d baseline %d", owner.Bytes(), baseline)
	}
}
