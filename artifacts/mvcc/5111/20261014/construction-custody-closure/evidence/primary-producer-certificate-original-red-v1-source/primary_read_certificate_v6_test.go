package db

import (
	"context"
	"testing"
)

func TestPrimaryProducerCertificateV6ActualImmutableCuts(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	check := func(s *Snapshot) {
		t.Helper()
		c, e := s.PrimaryReadRootCertificateV6()
		if e != nil || c == nil {
			t.Fatalf("actual retained producer certificate missing: %v", e)
		}
		if c != s.primaryRoot.certificate || c.Arena() != s.primaryRoot.arena || c.Reference() != s.primaryRoot.ref {
			t.Fatal("certificate did not bind the same retained root")
		}
	}
	initial := database.AcquireSnapshot()
	if initial == nil {
		t.Fatal("initial capture")
	}
	check(initial)
	if err = initial.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if err = database.SetSync([]byte("key"), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	old := database.AcquireSnapshot()
	if old == nil {
		t.Fatal("old capture")
	}
	defer old.Close()
	check(old)
	c, _ := old.PrimaryReadRootCertificateV6()
	for i := 0; i < 4; i++ {
		if err = database.SetSync([]byte("key"), []byte{byte(i + 4)}); err != nil {
			t.Fatal(err)
		}
	}
	check(old)
	retained, err := old.PrimaryReadRootCertificateV6()
	if err != nil || retained != c {
		t.Fatal("old cut borrowed a successor certificate")
	}
	roots, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range roots.Roots() {
		s := roots.AcquireSnapshotForRoot(root)
		if s == nil {
			t.Fatalf("captured exact root unavailable: %d", root.CommitSeq)
		}
		check(s)
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	roots.Release()
	if err = old.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("recovered capture")
	}
	check(s)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
}
