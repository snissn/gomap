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

// TestPowerLossOracleCounterexampleRecoverablePageReuse retains the historical
// old-page-reuse witness after the two-slot horizon fix. It proves that pages
// from generation A are not reused while A remains selectable, then advances
// both durable slots and proves that reusing an A page cannot affect the newer
// stable generation C. Package-local access identifies actual live and reused
// page IDs; every database mutation and crash-image reopen uses exported
// production methods. The witness advances until the allocator actually
// selects an A page instead of assuming a fixed generation-count horizon.
func TestPowerLossOracleCounterexampleRecoverablePageReuse(t *testing.T) {
	if expected := os.Getenv("TREEDB_POWERLOSS_PROFILE"); expected != "" && expected != "no_wal_fast" {
		t.Fatalf("TREEDB_POWERLOSS_PROFILE=%q does not match exercised profile no_wal_fast", expected)
	}
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
	retiredState := d.State()
	if retiredState == nil || retiredState.RootPageID == 0 {
		t.Fatalf("missing first durable root: %+v", retiredState)
	}
	oldPages := collectRootPageIDs(t, d, retiredState.RootPageID)
	oldLive := make(map[uint64]struct{}, len(oldPages))
	for _, id := range oldPages {
		oldLive[id] = struct{}{}
	}
	if len(oldLive) < 2 {
		t.Fatalf("first durable generation has too few live pages: root=%d pages=%v", retiredState.RootPageID, oldPages)
	}

	// B retires A but leaves A in the alternate slot. C displaces A and D
	// transfers its now-horizon-safe pages into the sealed freelist generation
	// that E may consume.
	if err := writeGeneration('b', true); err != nil {
		t.Fatalf("write retirement generation: %v", err)
	}
	if err := writeGeneration('c', true); err != nil {
		t.Fatalf("write horizon-advance generation: %v", err)
	}
	if err := writeGeneration('d', true); err != nil {
		t.Fatalf("write retired-page-transfer generation: %v", err)
	}
	stableState := d.State()
	if stableState == nil || stableState.CommitSeq <= retiredState.CommitSeq || stableState.RootPageID == retiredState.RootPageID {
		t.Fatalf("durable horizon did not advance: retired=%+v stable=%+v", retiredState, stableState)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatalf("capture post-horizon stable generation: %v", err)
	}
	idx := d.idx.Load()
	if idx == nil {
		t.Fatal("missing current index generation")
	}
	beforeReuse := idx.allocator.Counters()
	if beforeReuse.FreeIDs == 0 {
		t.Fatalf("advanced horizon exposed no reusable pages: counters=%+v", beforeReuse)
	}

	cutErr := errors.New("power-loss-oracle: stop before actual reuse index sync")
	var snapshot *powerlossoracle.Model
	var reusedPage uint64
	var indexPath string
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.BeforeIndexDataSync {
			candidate := model.Clone()
			rel, err := filepath.Rel(dir, event.Path)
			if err != nil {
				return fmt.Errorf("relative index path: %w", err)
			}
			changed, err := candidate.ChangedRanges(rel)
			if err != nil {
				return fmt.Errorf("changed index ranges: %w", err)
			}
			for _, pageID := range oldPages {
				start := int64(pageID) * int64(page.PageSize)
				end := start + int64(page.PageSize)
				for _, r := range changed {
					if r.Offset < end && r.Offset+r.Length > start {
						indexPath = rel
						reusedPage = pageID
						snapshot = candidate
						return cutErr
					}
				}
			}
		}
		return nil
	})
	// no_wal_fast ordinary writes may acknowledge below the admission bound
	// without forcing stable publication. Use explicit-sync boundaries until a
	// region-hinted allocation actually overwrites an A page. Every completed
	// iteration becomes the stable old-root image for the next attempted cut.
	const maxReuseWitnessGenerations = 16
	for generation := 0; generation < maxReuseWitnessGenerations; generation++ {
		priorStable := d.State()
		err = writeGeneration(byte('e'+generation), true)
		if errors.Is(err, cutErr) {
			stableState = priorStable
			break
		}
		if err != nil {
			restore()
			t.Fatalf("post-horizon reuse generation %d: %v", generation+1, err)
		}
		stableState = d.State()
	}
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		t.Fatalf("no displaced-generation page was selected within %d explicit-sync generations: err=%v old_pages=%v before=%+v after=%+v", maxReuseWitnessGenerations, err, oldPages, beforeReuse, idx.allocator.Counters())
	}
	afterReuse := idx.allocator.Counters()
	if afterReuse.ReuseAllocPages <= beforeReuse.ReuseAllocPages {
		t.Fatalf("post-horizon generation reused no freelist pages: before=%+v after=%+v", beforeReuse, afterReuse)
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
	if reusedPage == 0 || !changedOldPage(reusedPage) {
		t.Fatalf("safe freelist reuse did not overwrite a displaced-generation page: root=%d old_pages=%v changed=%v index=%q before=%+v after=%+v", retiredState.RootPageID, oldPages, changed, indexPath, beforeReuse, afterReuse)
	}

	// Model the physically permitted writeback of the actual reused A page while
	// keeping stable generation C and both of its metas. Because A was displaced
	// before reuse, neither the synced-only image nor this partial writeback may
	// change the selected durable root.
	pageOffset := int64(reusedPage) * int64(page.PageSize)
	variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "older-meta-live-page-reuse",
		Point:      powerlossoracle.BeforeIndexDataSync,
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
			powerlossoracle.VariantOldPageReuse: powerlossoracle.ExpectedOldRoot,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ledgerPath := filepath.Join("..", "testdata", "power_loss_counterexamples.json")
	if path := os.Getenv("TREEDB_POWERLOSS_COUNTEREXAMPLE_LEDGER"); path != "" {
		ledgerPath = path
	}
	ledgerData, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := powerlossoracle.ParseCounterexampleLedger(ledgerData)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := powerlossoracle.BindCounterexampleWitnesses(ledger, t.Name(), variants)
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
			releaseIdentities, err := variant.Model.InstallStableIdentityOverrides(crashDir)
			if err != nil {
				t.Fatalf("install stable identity model: %v", err)
			}
			defer releaseIdentities()
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
			if variant.Family != powerlossoracle.VariantSyncedOnly && variant.Family != powerlossoracle.VariantOldPageReuse {
				t.Fatalf("unclassified generated family %s", variant.Family)
			}
			opened := reopened.State()
			if opened == nil || opened.CommitSeq != stableState.CommitSeq || opened.AppliedCommandLSN != stableState.AppliedCommandLSN || opened.RootPageID != stableState.RootPageID {
				t.Fatalf("stable root changed after %s image: state=%+v want commit=%d applied=%d root=%d", variant.Family, opened, stableState.CommitSeq, stableState.AppliedCommandLSN, stableState.RootPageID)
			}
			validate(powerlossoracle.VariantObservation{Opened: true, Result: powerlossoracle.ExpectedOldRoot})
		})
	}
	t.Logf("adversarial crash images: cut=%s count=%d family_coverage=%v", coverage.CutID, len(variants), coverage.ByFamily)
}
