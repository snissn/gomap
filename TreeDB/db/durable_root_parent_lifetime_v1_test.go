package db

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func parentLifetimeFixtureV1(t *testing.T) *DB {
	t.Helper()
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.SetSync([]byte("retained"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		publishNeutralMetadata4627(t, database)
	}
	validateBothMetadataSlots4627(t, database)
	return database
}

func TestDurableRootParentLifetimeRefusesConflictingBindingV1(t *testing.T) {
	database := parentLifetimeFixtureV1(t)
	original := database.durableRoot
	target := 1 - original.slot
	for _, name := range []string{"parent-digest", "parent-page", "parent-commit", "slot-commit", "record-alias", "partial-slot"} {
		t.Run(name, func(t *testing.T) {
			corrupted := original
			switch name {
			case "parent-digest":
				corrupted.slotRecord[original.slot].ParentRecordDigest[0] ^= 1
			case "parent-page":
				corrupted.slotRecord[original.slot].ParentRecordPageID++
			case "parent-commit":
				corrupted.slotRecord[original.slot].ParentCommitSeq--
			case "slot-commit":
				corrupted.slotCommit[target]++
			case "record-alias":
				corrupted.slotMeta[original.slot].RootRecordPageID = corrupted.slotMeta[target].RootRecordPageID
			case "partial-slot":
				corrupted.slotCommit[target] = 0
			}
			corrupted.record = corrupted.slotRecord[original.slot]
			corrupted.meta = corrupted.slotMeta[original.slot]
			database.durableRoot = corrupted
			defer func() { database.durableRoot = original }()
			before := database.idx.Load().allocator.COWPrepareProfileV1()
			pages := database.idx.Load().pager.PageCount()
			builds := database.durableRootManifestBuildCount.Load()
			next := page.MetaPageBody{CommitSeq: original.record.CommitSeq + 1, UserRootPageID: original.record.UserRootPageID, SystemRootPageID: original.record.SystemRootPageID}
			candidate, err := database.prepareDurableRootCandidateV1(database.idx.Load(), next, nil, nil, false)
			if err == nil || candidate != nil {
				t.Fatalf("conflicting binding prepared candidate=%v err=%v", candidate, err)
			}
			if after := database.idx.Load().allocator.COWPrepareProfileV1(); after != before {
				t.Fatalf("refusal changed allocator profile: before=%+v after=%+v", before, after)
			}
			if database.idx.Load().pager.PageCount() != pages || database.durableRootManifestBuildCount.Load() != builds || database.durableRoot.pending != original.pending {
				t.Fatal("refusal crossed candidate preparation boundary")
			}
		})
	}
}

func TestDurableRootParentLifetimePreSealRetryFallbackV1(t *testing.T) {
	database := parentLifetimeFixtureV1(t)
	original := database.durableRoot
	target := 1 - original.slot
	horizon, err := durableRootOverwrittenRecordHorizonV1(original, target)
	if err != nil || horizon != original.record.CommitSeq {
		t.Fatalf("root horizon=%d err=%v", horizon, err)
	}
	parentID := original.slotRecord[target].ParentRecordPageID
	parentImage, err := database.idx.Load().pager.ReadPage(parentID)
	if err != nil {
		t.Fatal(err)
	}
	parentImage = bytes.Clone(parentImage)
	held := database.AcquireSnapshot()
	defer held.Close()
	injected := errors.New("parent lifetime pre-seal retry")
	observed := false
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point != durabilitycut.BeforePublicationSealWrite {
			return nil
		}
		observed = true
		source := database.idx.Load().pager
		for slot := uint64(0); slot < 2; slot++ {
			meta, err := readDurableMetaSlotV1(source, slot)
			if err != nil {
				return err
			}
			selected, err := validateDurableMetaCandidateV1(source, source.PageCount(), durableMetaCandidateV1{slot: slot, meta: meta}, nil)
			selected.closeOwnedFreelistV1()
			if err != nil {
				return fmt.Errorf("pre-seal slot%d: %w", slot, err)
			}
		}
		after, err := source.ReadPage(parentID)
		if err != nil || !bytes.Equal(parentImage, after) {
			return fmt.Errorf("pre-seal parent overwritten: %v", err)
		}
		return injected
	})
	err = database.SetSync([]byte("retained"), []byte("new"))
	restore()
	if !observed || !errors.Is(err, injected) {
		t.Fatalf("cut=%t error=%v", observed, err)
	}
	database.rootPublication.mu.Lock()
	pending := database.rootPublication.activeSeal
	database.rootPublication.mu.Unlock()
	if pending == nil {
		t.Fatal("failed queued seal not retained for retry")
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("retry: %v", err)
	}
	database.rootPublication.mu.Lock()
	retainedSeal := database.rootPublication.activeSeal
	database.rootPublication.mu.Unlock()
	if retainedSeal != nil || database.durableRoot.meta.RootRecordPageID != pending.meta.RootRecordPageID {
		t.Fatal("retry did not publish retained exact candidate")
	}
	validateBothMetadataSlots4627(t, database)
	if got, err := held.Get([]byte("retained")); err != nil || string(got) != "old" {
		t.Fatalf("held value=%q err=%v", got, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	// Independently prune a decoded copy at the successor horizon. The manifest
	// retires at its own slot commit, while its root record must survive as parent.
	generation, err := freelist.LoadGenerationV1(database.idx.Load().pager, database.durableRoot.record.Freelist)
	if err != nil {
		t.Fatal(err)
	}
	transaction := freelist.NewFreelistTxn(generation, freelist.NewReservationLedger())
	capability, err := freelist.NewReuseCapability(horizon, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	transaction.PruneWithCapability(capability)
	pruned, err := transaction.Materialize(generation.GenerationID() + 1000)
	if err != nil {
		t.Fatal(err)
	}
	manifestID := original.slotRecord[target].Manifest.FirstPageID
	if !pruned.Allocatable(manifestID) || pruned.Allocatable(original.slotMeta[target].RootRecordPageID) {
		t.Fatalf("manifest/root retirement horizons fused: manifest%d free=%t root%d free=%t", manifestID, pruned.Allocatable(manifestID), original.slotMeta[target].RootRecordPageID, pruned.Allocatable(original.slotMeta[target].RootRecordPageID))
	}
	dir := database.dir
	active := database.metaPageID
	fallback := database.durableRoot.slotRecord[1-active]
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	validateBothMetadataSlots4627(t, reopened)
	if got, err := reopened.Get([]byte("retained")); err != nil || string(got) != "new" {
		t.Fatalf("reopen=%q err=%v", got, err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	corruptIndexPageByte(t, dir, active)
	recovered, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if recovered.State().CommitSeq != fallback.CommitSeq {
		t.Fatalf("fallback commit=%d want%d", recovered.State().CommitSeq, fallback.CommitSeq)
	}
	if got, err := recovered.Get([]byte("retained")); err != nil || string(got) != "old" {
		t.Fatalf("fallback=%q err=%v", got, err)
	}
}
