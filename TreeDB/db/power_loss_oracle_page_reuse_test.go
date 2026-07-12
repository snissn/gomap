package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/page"
)

// TestPowerLossOracleCounterexampleRecoverablePageReuse is the stable witness
// for graveyard/freelist reuse before the older recoverable root is displaced.
// It uses package-local access only to identify actual live and reused page IDs;
// every mutation and the crash-image reopen use exported production methods.
func TestPowerLossOracleCounterexampleRecoverablePageReuse(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Dir:                    dir,
		ChunkSize:              64 * 1024,
		Durability:             DurabilityWALOffRelaxed,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
		FreelistRegionRadius:   -1,
	}
	d, err := Open(opts)
	if err != nil {
		t.Fatalf("open actual reuse fixture: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = d.Close()
		}
	}()

	const keys = 5000
	writeGeneration := func(tag byte, sync bool) error {
		b := d.NewBatch().(*Batch)
		defer b.Close()
		value := bytes.Repeat([]byte{tag}, 32)
		for i := 0; i < keys; i++ {
			key := []byte(fmt.Sprintf("reuse/%04d", i))
			if err := b.Set(key, value); err != nil {
				return err
			}
		}
		if sync {
			return b.WriteSync()
		}
		return b.Write()
	}

	if err := writeGeneration('a', true); err != nil {
		t.Fatalf("write stable generation: %v", err)
	}
	oldState := d.State()
	if oldState == nil || oldState.RootPageID == 0 {
		t.Fatalf("missing stable root: %+v", oldState)
	}
	oldPages := collectRootPageIDs(t, d, oldState.RootPageID)
	oldLive := make(map[uint64]struct{}, len(oldPages))
	for _, id := range oldPages {
		oldLive[id] = struct{}{}
	}
	if len(oldLive) < 2 {
		t.Fatalf("stable generation has too few live pages: root=%d pages=%v", oldState.RootPageID, oldPages)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("capture stable generation: %v", err)
	}

	// Generation 2 retires generation 1. Generation 3 advances KeepRecent=1
	// far enough for synchronous pruning to put those actual page IDs on the
	// freelist. Neither relaxed commit changes the model's stable image.
	if err := writeGeneration('b', false); err != nil {
		t.Fatalf("write retirement generation: %v", err)
	}
	if err := writeGeneration('c', false); err != nil {
		t.Fatalf("write prune generation: %v", err)
	}
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing current index generation")
	}
	beforeReuse := idx.allocator.Counters()
	if beforeReuse.FreeIDs == 0 {
		t.Fatalf("actual prune exposed no reusable pages: counters=%+v", beforeReuse)
	}

	cutErr := errors.New("power-loss-oracle: stop after actual reuse meta write")
	var snapshot *powerlossoracle.Model
	var meta durabilitycut.Event
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaWrite {
			meta = event
			snapshot = model.Clone()
			return cutErr
		}
		return nil
	})
	err = writeGeneration('d', false)
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		t.Fatalf("actual reuse generation did not stop at AfterMetaWrite: err=%v", err)
	}
	afterReuse := idx.allocator.Counters()
	if afterReuse.ReuseAllocPages <= beforeReuse.ReuseAllocPages {
		t.Fatalf("actual generation reused no freelist pages: before=%+v after=%+v", beforeReuse, afterReuse)
	}

	indexPath, err := filepath.Rel(dir, meta.Path)
	if err != nil {
		t.Fatalf("relative index path: %v", err)
	}
	changed, err := snapshot.ChangedRanges(indexPath)
	if err != nil {
		t.Fatalf("changed index ranges: %v", err)
	}
	changedOldPage := func(pageID uint64) bool {
		start := int64(pageID) * int64(page.PageSize)
		end := start + int64(page.PageSize)
		for _, r := range changed {
			if r.Offset < end && r.Offset+r.Length > start {
				return true
			}
		}
		return false
	}
	var reusedPage uint64
	for _, id := range oldPages {
		if changedOldPage(id) {
			reusedPage = id
			break
		}
	}
	if reusedPage == 0 {
		t.Fatalf("freelist reuse did not overwrite an old-live page: root=%d old_pages=%d changed_ranges=%d before=%+v after=%+v", oldState.RootPageID, len(oldPages), len(changed), beforeReuse, afterReuse)
	}

	// Model the physically permitted writeback of the actual reused page while
	// keeping the older synced meta page. The resulting image must never read as
	// the complete older generation.
	pageOffset := int64(reusedPage) * int64(page.PageSize)
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "older-meta-live-page-reuse",
		Point:      powerlossoracle.AfterMetaWrite,
		Occurrence: 1,
		Model:      snapshot,
		OldPageWrites: []powerlossoracle.DirtyResource{{
			Kind:   powerlossoracle.ResourceIndex,
			ID:     "first-reused-old-live-page",
			Path:   filepath.ToSlash(indexPath),
			Ranges: []powerlossoracle.ByteRange{{Offset: pageOffset, Length: int64(page.PageSize)}},
		}},
		RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantOldPageReuse},
		ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{
			powerlossoracle.VariantSyncedOnly:   powerlossoracle.ExpectedOldRoot,
			powerlossoracle.VariantOldPageReuse: powerlossoracle.ExpectedCorruption,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ledgerData, err := os.ReadFile(filepath.Join("..", "testdata", "power_loss_counterexamples.json"))
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := powerlossoracle.ParseCounterexampleLedger(ledgerData)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := powerlossoracle.BindCounterexampleWitnesses(ledger, "TestPowerLossOracleCounterexampleRecoverablePageReuse", variants)
	if err != nil {
		t.Fatal(err)
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			crashDir := t.TempDir()
			if err := variant.Model.MaterializeStable(crashDir); err != nil {
				t.Fatalf("materialize stable-only image: %v", err)
			}
			reopenOpts := opts
			reopenOpts.Dir = crashDir
			reopenOpts.ReadOnly = true
			reopened, openErr := Open(reopenOpts)
			if openErr != nil {
				t.Fatalf("public db.Open rejected %s image without an allowed typed sentinel: %v", variant.Family, openErr)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Errorf("close reopened DB: %v", err)
				}
			}()
			entry, known := bound[variant.ID]
			validate := func(observation powerlossoracle.VariantObservation) {
				if known {
					if err := powerlossoracle.ValidateVariantObservation(variant, observation, &entry); err != nil {
						t.Fatal(err)
					}
					return
				}
				if err := powerlossoracle.ValidateVariantObservation(variant, observation, nil); err != nil {
					t.Fatal(err)
				}
			}
			if variant.Family == powerlossoracle.VariantSyncedOnly {
				opened := reopened.State()
				if opened == nil || opened.CommitSeq != oldState.CommitSeq || opened.AppliedCommandLSN != oldState.AppliedCommandLSN || opened.RootPageID != oldState.RootPageID {
					t.Fatalf("synced old root state=%+v want commit=%d applied=%d root=%d", opened, oldState.CommitSeq, oldState.AppliedCommandLSN, oldState.RootPageID)
				}
				validate(powerlossoracle.VariantObservation{Opened: true, Result: powerlossoracle.ExpectedOldRoot})
				return
			}
			if variant.Family != powerlossoracle.VariantOldPageReuse {
				t.Fatalf("unclassified generated family %s", variant.Family)
			}
			opened := reopened.State()
			if opened == nil {
				t.Fatal("public db.Open returned no state")
			}
			generation := powerLossDBCompleteGeneration(oldState.CommitSeq, oldState.AppliedCommandLSN)
			generation.LivePages = append([]uint64(nil), oldPages...)
			scenario := powerlossoracle.Scenario{
				Name:                 "actual-recoverable-page-reuse",
				Cut:                  powerlossoracle.AfterMetaWrite,
				Generations:          []powerlossoracle.Generation{generation},
				LatestSealedSequence: oldState.CommitSeq,
				SelectedSequence:     opened.CommitSeq,
				OpenedSequence:       opened.CommitSeq,
				OpenedAppliedLSN:     opened.AppliedCommandLSN,
				ReusedPages:          []uint64{reusedPage},
				ReopenAttempted:      true,
			}
			if err := powerlossoracle.RequireViolation(scenario.Validate(), powerlossoracle.InvariantRecoverablePageReused); err != nil {
				t.Fatalf("successful public Open did not produce recoverable-page-reused diagnosis: %v", err)
			}
			validate(powerlossoracle.VariantObservation{Opened: true, Result: powerlossoracle.ExpectedCorruption, NamedInvariant: powerlossoracle.InvariantRecoverablePageReused})
		})
	}
	t.Logf("adversarial crash images: cut=%s count=%d family_coverage=%v", coverage.CutID, len(variants), coverage.ByFamily)
}

func powerLossDBCompleteGeneration(sequence, applied uint64) powerlossoracle.Generation {
	kinds := []powerlossoracle.ResourceKind{
		powerlossoracle.ResourceIndex,
		powerlossoracle.ResourceFreelist,
		powerlossoracle.ResourceValueLog,
		powerlossoracle.ResourceOuterLeaf,
		powerlossoracle.ResourceAuxiliary,
		powerlossoracle.ResourceDirectory,
		powerlossoracle.ResourceSeal,
		powerlossoracle.ResourceCommandWAL,
	}
	resources := make([]powerlossoracle.Resource, 0, len(kinds))
	for _, kind := range kinds {
		resources = append(resources, powerlossoracle.Resource{Kind: kind, ID: string(kind), Stable: true, Live: true})
	}
	return powerlossoracle.Generation{Sequence: sequence, Recoverable: true, Resources: resources, AppliedLSN: applied}
}
