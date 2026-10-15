package db

import (
	"bytes"
	"testing"
)

func TestPrimaryPromotionSharesImmutableLogicalRootV6(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.Set([]byte("key"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("missing original logical reader")
	}
	defer old.Close()
	logical := old.primaryRoot.ref
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if got := d.durableRoot.primary.records[d.durableRoot.slot]; got != logical {
		t.Fatalf("slot owns duplicate promotion RAM image: slot=%+v original=%+v", got, logical)
	}
	if got, err := old.Get([]byte("key")); err != nil || !bytes.Equal(got, []byte("before")) {
		t.Fatal("old reader", err, got)
	}
}
