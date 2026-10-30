package db

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPrimarySealAllocationV6ReleaseKeepsContainingChargeUntilCompletion(t *testing.T) {
	var owner retainedalloc.Owner
	charge := primarySealMetadataV6(0)
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	allocation := &primarySealAllocationV6{}
	seal := &allocation.seal
	seal.primary = &allocation.primary
	seal.metadata, seal.metadataCharge = &owner, charge
	// Model an already completed primary disposal. The outer release must still
	// own and account for the entire containing allocation until it returns.
	allocation.primary.disposalAttempted = true
	completion, ready, err := releaseRootPublicationSealV6(seal)
	if err != nil || !ready {
		t.Fatalf("release ready=%v err=%v", ready, err)
	}
	if seal.primary != nil || seal.metadata != nil || seal.metadataCharge != 0 {
		t.Fatal("completed seal retained ownership aliases")
	}
	if got := owner.Bytes(); got != charge {
		t.Fatalf("premature refund: %d want %d", got, charge)
	}
	seal, allocation = nil, nil
	completion.complete()
	if got := owner.Bytes(); got != 0 {
		t.Fatalf("completion retained %d bytes", got)
	}
}

func TestPrimarySealAllocationV6FailureRetainsOriginalContainingOwner(t *testing.T) {
	var owner retainedalloc.Owner
	charge := primarySealMetadataV6(0)
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	allocation := &primarySealAllocationV6{}
	seal := &allocation.seal
	seal.primary = &allocation.primary
	seal.metadata, seal.metadataCharge = &owner, charge
	// Missing prepared authority refuses primary disposal. The original parent
	// must retain both its interior child pointer and its full pending charge.
	_, ready, err := releaseRootPublicationSealV6(seal)
	if ready || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("release ready=%v err=%v", ready, err)
	}
	if seal.primary != &allocation.primary || seal.metadata != &owner || seal.metadataCharge != charge {
		t.Fatal("failure lost original allocation ownership")
	}
	if seal.disposalFailure.payload != seal {
		t.Fatal("failure replaced original receipt")
	}
	_, againReady, againErr := releaseRootPublicationSealV6(seal)
	if againReady || againErr != err {
		t.Fatal("repeated release changed failure receipt")
	}
	if got := owner.Bytes(); got != charge {
		t.Fatalf("failed release refunded charge: %d want %d", got, charge)
	}
}
