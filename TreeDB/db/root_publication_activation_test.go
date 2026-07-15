package db

import (
	"errors"
	"testing"
)

func TestActivatedRootPublicationTripsFormerDirectPublisher(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	idx := database.idx.Load()
	if idx == nil {
		t.Fatal("missing active index")
	}
	_, err = database.publishDurableRootV1(idx, database.meta, nil, nil)
	if !errors.Is(err, errDirectDurableRootPublisherDisabledV1) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("former direct publisher error=%v, want disabled tripwire and ErrRecoveryRequired", err)
	}
}
