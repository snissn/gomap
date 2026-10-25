package db

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func TestPrivateCOWLeafPackFinalizerRoleAndHeldSource(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, d, leafLog, 1024)
	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	// This owning guard fixture isolates the already-active finalizer policy;
	// it is not evidence of public native activation. The cache test separately
	// executes actual WAL-on append/install/ack and equivalent-basis adoption.
	d.nativeSemanticActive.Store(true)
	before, ok := d.StateToken()
	if !ok {
		t.Fatal("missing original token")
	}
	opts := LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Force: true, Sync: true}
	for _, invalid := range []*transactionowner.Owner{nil, transactionowner.New()} {
		if _, err := d.LeafGenerationPackRunOnceForPrivateCOW(invalid, context.Background(), opts); !errors.Is(err, ErrNativeSemanticUnsupported) {
			t.Fatalf("wrong original owner: %v", err)
		}
	}
	if _, err := d.LeafGenerationPackRunOnce(context.Background(), opts); !errors.Is(err, ErrNativeSemanticUnsupported) {
		t.Fatalf("unowned active finalizer unexpectedly admitted: %v", err)
	}
	afterRejected, _ := d.StateToken()
	if afterRejected.CommitSeq != before.CommitSeq || afterRejected.RootPageID != before.RootPageID || afterRejected.SystemRootPageID != before.SystemRootPageID || afterRejected.AppliedCommandLSN != before.AppliedCommandLSN || afterRejected.MaxEntryRevision != before.MaxEntryRevision {
		t.Fatal("rejected role published logical state")
	}
	held := d.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing held source snapshot")
	}
	defer held.Close()
	result, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	if err != nil || !result.Ran || result.Pack.BytesCopied == 0 {
		t.Fatalf("named owner did not reach physical finalizer: %+v %v", result, err)
	}
	after, _ := d.StateToken()
	if after.CommitSeq <= before.CommitSeq || after.AppliedCommandLSN != before.AppliedCommandLSN || after.MaxEntryRevision != before.MaxEntryRevision {
		t.Fatalf("physical rewrite frontier changed: before%+v after%+v", before, after)
	}
	expectLeafGenerationValue(t, d, leafGenerationKey("pack-concurrent", 768), 'a')
	got, err := held.Get(leafGenerationKey("pack-concurrent", 768))
	if err != nil || len(got) == 0 || got[0] != 'a' {
		t.Fatalf("held source unreadable: %q %v", got, err)
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("pinned original source disappeared: %v", err)
	}
}

func TestPrivateCOWLeafPackPreMetaFailureKeepsExactRetry(t *testing.T) {
	d, leafLog, _ := openLeafGenerationPackTestDB(t)
	candidate := prepareLeafGenerationPackTestCandidate(t, d, leafLog, 1024)
	owner := transactionowner.New()
	if err := d.BindPrivateCOWOwner(owner); err != nil {
		t.Fatal(err)
	}
	d.nativeSemanticActive.Store(true)
	before, _ := d.StateToken()
	failure := errors.New("private COW leaf pack before metadata")
	unregister := registerLeafGenerationPackPublishHook(func(event leafGenerationPackPublishEvent) error {
		if event.Phase == leafGenerationPackBeforeMetaWrite {
			return failure
		}
		return nil
	})
	opts := LeafGenerationPackFromPlanOptions{MaxGenerations: 1, Force: true, Sync: true}
	_, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	unregister()
	if !errors.Is(err, failure) || d.publicationPoisoned.Load() {
		t.Fatalf("pre-meta outcome: %v poison%t", err, d.publicationPoisoned.Load())
	}
	after, _ := d.StateToken()
	if after.CommitSeq != before.CommitSeq || after.RootPageID != before.RootPageID || after.SystemRootPageID != before.SystemRootPageID {
		t.Fatal("failed private finalizer advanced logical root")
	}
	if _, err := os.Stat(candidate.sourcePath); err != nil {
		t.Fatalf("source lost on failed private rewrite: %v", err)
	}
	result, err := d.LeafGenerationPackRunOnceForPrivateCOW(owner, context.Background(), opts)
	if err != nil || !result.Ran || result.Pack.BytesCopied == 0 {
		t.Fatalf("exact candidate not retryable: %+v %v", result, err)
	}
	expectLeafGenerationValue(t, d, leafGenerationKey("pack-concurrent", 768), 'a')
}
