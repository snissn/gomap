package db

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestPrimaryProducerCertificateV6ActualImmutableCuts(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
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
		before := s.primaryRoot.arena.Counters()
		c, e := s.PrimaryReadRootCertificateV6()
		if e != nil || c == nil {
			t.Fatalf("actual retained producer certificate missing: %v", e)
		}
		if c != s.primaryRoot.certificate || c.Arena() != s.primaryRoot.arena || c.Reference() != s.primaryRoot.ref {
			t.Fatal("certificate did not bind the same retained root")
		}
		if after := s.primaryRoot.arena.Counters(); after != before {
			t.Fatal("consumer accessor rediscovered or retained the bank")
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

func TestPrimaryProducerCertificateV6BindingAndFreshImage(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("actual capture")
	}
	defer s.Close()
	cert, err := s.PrimaryReadRootCertificateV6()
	if err != nil || cert == nil {
		t.Fatal(err)
	}
	original := s.primaryRoot
	mismatch := *original
	mismatch.ref.Incarnation++
	s.primaryRoot = &mismatch
	if _, err = s.PrimaryReadRootCertificateV6(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("accepted wrong incarnation", err)
	}
	s.primaryRoot = original
	image := cert.Image()
	image[16] ^= 1
	if page.VerifyChecksumNonMutating(image) {
		t.Fatal("actual certificate image corruption did not reach integrity operand")
	}
	// The accessor grants only tuple custody. A fresh producer integrity action
	// must reject the SAME corrupted image; bounded tree consumers phase this work.
	if _, ready, err := original.arena.BorrowReadRootCertificateV6(original.ref, nil); ready || !errors.Is(err, primaryarena.ErrFormat) {
		t.Fatal("producer integrity accepted corrupted image", ready, err)
	}
	image[16] ^= 1
	if !page.VerifyChecksumNonMutating(image) {
		t.Fatal("restore actual owned image")
	}
}

func TestPrimaryProducerCertificateV5DeliberateNil(t *testing.T) {
	dir := t.TempDir()
	createSavedPrimaryFormatV5(t, dir, false)
	database, err := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	s := database.AcquireSnapshot()
	if s == nil {
		t.Fatal("actual legacy capture")
	}
	defer s.Close()
	if s.primaryRoot == nil || s.primaryRoot.arena.CapsuleFormatV6() {
		t.Fatal("fixture did not select actual V5 root")
	}
	if cert, err := s.PrimaryReadRootCertificateV6(); cert != nil || err != nil {
		t.Fatal("forged V6 certificate on V5", cert, err)
	}
}
