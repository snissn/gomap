package db

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type originalClosureAdmissionBudget struct {
	mu    sync.Mutex
	limit uint64
}
type originalClosureAdmissionLease struct {
	budget *originalClosureAdmissionBudget
}

func (b *originalClosureAdmissionBudget) setLimit(limit uint64) {
	b.mu.Lock()
	b.limit = limit
	b.mu.Unlock()
}
func (b *originalClosureAdmissionBudget) AcquireRetention(bytes uint64) (retainedalloc.Lease, error) {
	l := &originalClosureAdmissionLease{budget: b}
	if err := l.Resize(bytes); err != nil {
		return nil, err
	}
	return l, nil
}
func (l *originalClosureAdmissionLease) Resize(bytes uint64) error {
	l.budget.mu.Lock()
	defer l.budget.mu.Unlock()
	if bytes > l.budget.limit {
		return retainedalloc.ErrCapacity
	}
	return nil
}
func (l *originalClosureAdmissionLease) Close() {}

func TestRecoverableRootSetOriginalContainingAdmissionRefusal(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// Capture the genuine production snapshot before the selected containing
	// allocation. The failure budget then distinguishes the original RootSet.
	stable := database.acquireStableSnapshotWithMaintenanceLockHeld()
	if stable == nil {
		t.Fatal("actual stable snapshot capture failed")
	}
	defer stable.Close()
	owner := database.StableResourceIdentityPinRegistry().MetadataOwner()
	budget := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.setLimit(before)
	set, retry, err := database.tryCaptureRecoverableRootSet(stable)
	budget.setLimit(math.MaxUint64)
	if set != nil {
		set.Release()
	}
	if set != nil || retry || !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("original containing refusal: set=%p retry=%t err=%v", set, retry, err)
	}
	if got := owner.Bytes(); got != before {
		t.Fatalf("refused original containing allocation changed charge: %d -> %d", before, got)
	}
}

func TestLeafGenerationPackAuthorityOriginalCloseRefusalRetainsCharge(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	owner := database.StableResourceIdentityPinRegistry().MetadataOwner()
	before := owner.Bytes()
	stagingRoot := t.TempDir()
	stagingDir := filepath.Join(stagingRoot, "leaf_vlog")
	if err := os.Mkdir(stagingDir, 0700); err != nil {
		t.Fatal(err)
	}
	authority, err := newLeafGenerationPackPromotionAuthority(database, stagingRoot, stagingDir, LeafLogDirPath(database.dir))
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		if authority != nil || !errors.Is(err, rootpublication.ErrNamespacePersistenceUnsupported) || owner.Bytes() != before || database.backgroundError() != nil {
			t.Fatalf("unsupported promotion must refuse before original custody: authority=%p err=%v charge=%d", authority, err, owner.Bytes())
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	failedParent := authority.stagingParent
	if err := failedParent.Close(); err != nil {
		t.Fatal(err)
	}
	// The actual original final Close now refuses. Its preborn receipt must keep
	// the original parent and containing charge reachable through this DB.
	authority.release()
	first := database.backgroundError()
	if !errors.Is(first, os.ErrClosed) {
		t.Fatalf("original final-Close cause not DB-rooted: %v", first)
	}
	if authority.stagingParent != failedParent {
		t.Fatal("failed original physical parent alias discarded")
	}
	if got := owner.Bytes(); got <= before {
		t.Fatalf("failed original authority capacity refunded: before=%d after=%d", before, got)
	}
	authority.release()
	if again := database.backgroundError(); again != first {
		t.Fatalf("repeat release replaced first DB receipt: %v -> %v", first, again)
	}
	if owner.PhysicalClosed() {
		t.Fatal("failed containing owner reported physical completion")
	}
}

func TestRecoverableRootSetCaptureBackingRefundsAfterOriginalCompletion(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	owner := database.StableResourceIdentityPinRegistry().MetadataOwner()
	before := owner.Bytes()
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() <= before {
		t.Fatal("actual capture retained no producer capacity")
	}
	set.Release()
	if got := owner.Bytes(); got != before {
		t.Fatalf("actual original completion failed to refund capture backing: before=%d after=%d", before, got)
	}
}
