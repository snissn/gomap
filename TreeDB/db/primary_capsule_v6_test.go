package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryCapsuleOrdinaryAuthorityAndExactFallbackV6(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	db, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { db.Close() }()
	a := db.idx.Load().primary
	if a == nil || !a.CapsuleFormatV6() {
		t.Fatal("unchanged NoWALFast producer did not select capsules")
	}
	var meta [2][]byte
	for i := range meta {
		v, e := db.idx.Load().pager.Get(uint64(i))
		if e != nil {
			t.Fatal(e)
		}
		meta[i] = append([]byte(nil), v...)
	}
	if e = db.SetSync([]byte("first"), []byte("one")); e != nil {
		t.Fatal(e)
	}
	prior := db.State().CommitSeq
	old := db.AcquireSnapshot()
	defer old.Close()
	if e = db.SetSync([]byte("second"), []byte("two")); e != nil {
		t.Fatal(e)
	}
	for i := range meta {
		v, e := db.idx.Load().pager.Get(uint64(i))
		if e != nil || !bytes.Equal(v, meta[i]) {
			t.Fatal("capsule route rewrote DATA META")
		}
	}
	newest := db.durableRoot.slot
	image := db.durableRoot.primary.capsuleImages[newest]
	view, e := rootpublication.DecodePrimaryCapsuleV6(image, newest, a.UUID())
	if e != nil {
		t.Fatal(e)
	}
	if view.Current().Record.CommitSeq != db.State().CommitSeq || view.Parent().Record.CommitSeq != prior {
		t.Fatal("complete current/one-hop parent sequence")
	}
	if got, e := old.Get([]byte("first")); e != nil || string(got) != "one" {
		t.Fatalf("copied reader %q %v", got, e)
	}
	if got, e := old.Get([]byte("second")); !errors.Is(e, tree.ErrKeyNotFound) || got != nil {
		t.Fatalf("old reader exposed future %q %v", got, e)
	}
	old.Close()
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	dataBeforeCorruption, readErr := os.ReadFile(filepath.Join(dir, primaryIndexFileName))
	if readErr != nil {
		t.Fatal(readErr)
	}
	for slot := uint64(0); slot < 2; slot++ {
		start := (2 + 3*slot) * page.PageSize
		actual, decodeErr := rootpublication.DecodePrimaryCapsuleV6(dataBeforeCorruption[start:start+rootpublication.PrimaryCapsuleSizeV6], slot, a.UUID())
		if decodeErr != nil {
			t.Logf("pre-corruption slot%d decode=%v", slot, decodeErr)
		} else {
			t.Logf("pre-corruption slot%d commit%d parent%d newest%d prior%d", slot, actual.Current().Record.CommitSeq, actual.Parent().Record.CommitSeq, newest, prior)
		}
	}
	f, e := os.OpenFile(filepath.Join(dir, primaryIndexFileName), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	// Flip the actual digest byte: a fixed replacement may equal its input.
	corruptionOffset := (2+3*newest)*page.PageSize + 48
	corrupted := dataBeforeCorruption[corruptionOffset] ^ 0xff
	if _, e = f.WriteAt([]byte{corrupted}, int64(corruptionOffset)); e != nil {
		t.Fatal(e)
	}
	if e = f.Sync(); e != nil {
		t.Fatal(e)
	}
	f.Close()
	db, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	if got := db.State().CommitSeq; got != prior {
		t.Fatalf("fallback commit=%d want%d", got, prior)
	}
	if got, e := db.Get([]byte("second")); e != nil || got != nil {
		t.Fatalf("older slot exposed newest %q %v", got, e)
	}
	if got, e := db.Get([]byte("first")); e != nil || string(got) != "one" {
		t.Fatalf("older first %q %v", got, e)
	}
	if e = db.SetSync([]byte("after"), []byte("recovered")); e != nil {
		t.Fatal(e)
	}
}

func TestPrimaryCapsulePostInstallFenceFailureRetainsCustodyV6(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	db, e := Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.SetSync([]byte("first"), []byte("one")); e != nil {
		t.Fatal(e)
	}
	before := db.durableRoot
	old := db.AcquireSnapshot()
	defer old.Close()
	budget, e := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer budget.Close()
	owner := old.primaryRoot.arena.MetadataOwner()
	enrollment, e := retainedalloc.EnrollPair(owner, db.valueLogIdentityPins.MetadataOwner(), budget)
	if e != nil {
		t.Fatal(e)
	}
	if e = old.AdoptPrimaryMetadataEnrollment(enrollment); e != nil {
		t.Fatal(e)
	}
	db.testFailSyncMeta.Store(true)
	e = db.SetSync([]byte("second"), []byte("two"))
	db.testFailSyncMeta.Store(false)
	if !errors.Is(e, errTestSyncMetaFailpoint) || !errors.Is(e, ErrRecoveryRequired) {
		t.Fatalf("post-install failure=%v", e)
	}
	if !db.publicationPoisoned.Load() || db.durableRoot.meta != before.meta {
		t.Fatal("uncertain capsule reported durable")
	}

	physical, e := os.ReadFile(filepath.Join(dir, primaryIndexFileName))
	if e != nil {
		t.Fatal(e)
	}
	for _, write := range []func([]byte, []byte) error{db.Set, db.SetSync} {
		if e = write([]byte("after-uncertain"), []byte("refused")); !errors.Is(e, ErrRecoveryRequired) {
			t.Fatalf("poisoned writer admitted: %v", e)
		}
		after, e := os.ReadFile(filepath.Join(dir, primaryIndexFileName))
		if e != nil {
			t.Fatal(e)
		}
		const start = 2 * page.PageSize
		const end = 8 * page.PageSize
		if !bytes.Equal(physical[start:end], after[start:end]) {
			t.Fatal("poisoned writer overwrote fixed capsule")
		}
	}
	if got, e := old.Get([]byte("first")); e != nil || string(got) != "one" {
		t.Fatalf("failure lost reader custody %q %v", got, e)
	}
	if _, e = db.idx.Load().primary.Get(before.primary.records[before.slot].PageID); e != nil {
		t.Fatal("failure dropped prior slot")
	}
	// Installed bytes may survive a failed fence. Recovery must accept only one
	// complete actual slot; it must never use a DATA META or private candidate.
	old.Close()
	_ = db.Close()
	if got := budget.Stats().ExternalBytes; got != 0 {
		t.Fatalf("terminal uncertain promotion retained governor bytes=%d owner=%d", got, owner.Bytes())
	}
	db, e = Open(opts)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if got, e := db.Get([]byte("first")); e != nil || string(got) != "one" {
		t.Fatalf("recovery lost durable predecessor %q %v", got, e)
	}
}

func TestPrimaryCapsuleCapturedRecoverableReadRootsSurviveSlotReuseV6(t *testing.T) {
	db, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	for i := 0; i < 3; i++ {
		if e = db.SetSync([]byte("key"), []byte(fmt.Sprintf("value%d", i))); e != nil {
			t.Fatal(e)
		}
	}
	set, e := db.CaptureRecoverableRootSet(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer set.Release()
	var snapshots []*Snapshot
	var values [][]byte
	for _, root := range set.Roots() {
		if !root.Durable {
			continue
		}
		snap := set.AcquireSnapshotForRoot(root)
		if snap == nil {
			t.Fatalf("actual copied root %d unavailable", root.CommitSeq)
		}
		snapshots = append(snapshots, snap)
		defer snap.Close()
		v, e := snap.Get([]byte("key"))
		if e != nil {
			t.Fatal(e)
		}
		values = append(values, append([]byte(nil), v...))
	}
	if len(snapshots) < 3 {
		t.Fatalf("complete A/B plus independent parent roots=%d", len(snapshots))
	}
	for i := 0; i < 6; i++ {
		if e = db.SetSync([]byte("key"), []byte(fmt.Sprintf("future%d", i))); e != nil {
			t.Fatal(e)
		}
	}
	for i, snap := range snapshots {
		got, e := snap.Get([]byte("key"))
		if e != nil || !bytes.Equal(got, values[i]) {
			t.Fatalf("retained root%d changed %q want%q %v", i, got, values[i], e)
		}
	}
}

// Containment for the missing joint namespace protocol, not the canonical
// successful-vacuum or crash-recovery qualification.
func TestPrimaryCapsuleVacuumCutRefusesBeforeNamespaceMutationV6(t *testing.T) {
	dir := t.TempDir()
	db, e := Open(Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.SetSync([]byte("key"), []byte("value")); e != nil {
		t.Fatal(e)
	}
	data, e := os.ReadFile(filepath.Join(dir, "index.db"))
	if e != nil {
		t.Fatal(e)
	}
	primary, e := os.ReadFile(filepath.Join(dir, primaryIndexFileName))
	if e != nil {
		t.Fatal(e)
	}
	cut, e := db.CapturePhysicalSnapshotCutV1(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer cut.Close()
	seq := db.State().CommitSeq
	if e = db.VacuumIndexOnline(context.Background()); !errors.Is(e, rootpublication.ErrResourcePinned) {
		t.Fatalf("vacuum %v", e)
	}
	after, e := os.ReadFile(filepath.Join(dir, "index.db"))
	if e != nil || !bytes.Equal(data, after) {
		t.Fatal("refusal mutated DATA")
	}
	after, e = os.ReadFile(filepath.Join(dir, primaryIndexFileName))
	if e != nil || !bytes.Equal(primary, after) {
		t.Fatal("refusal mutated PRIMARY")
	}
	if db.State().CommitSeq != seq {
		t.Fatal("refusal published replacement")
	}
	if e = db.SetSync([]byte("after"), []byte("still-live")); e != nil {
		t.Fatal(e)
	}
}
