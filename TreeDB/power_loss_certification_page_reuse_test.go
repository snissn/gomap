package treedb_test

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

// TestPowerLossCertificationRecoverablePageReusePublicReopen complements the
// package-local page-ID witness with retained normal-public-open evidence. The
// backend test proves which displaced generation supplied the reused page;
// this test independently requires the production reuse counter to advance,
// persists the entire physically changed pre-index-sync image, and proves that
// public recovery still selects the older dependency-closed root.
func TestPowerLossCertificationRecoverablePageReusePublicReopen(t *testing.T) {
	requirePowerLossProfile(t, "no_wal_fast")
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	opts.ChunkSize = 64 * 1024
	opts.KeepRecent = 1
	opts.FreelistRegionRadius = -1
	opts.IndexOuterLeavesInValueLog = false
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = closeBackend()
		}
	}()

	const keys = 5000
	writeGeneration := func(tag byte) error {
		batch := backend.NewBatch()
		defer batch.Close()
		value := bytes.Repeat([]byte{tag}, 32)
		for i := 0; i < keys; i++ {
			if err := batch.Set([]byte(fmt.Sprintf("reuse/%04d", i)), value); err != nil {
				return err
			}
		}
		return batch.WriteSync()
	}
	for _, tag := range []byte{'a', 'b', 'c', 'd'} {
		if err := writeGeneration(tag); err != nil {
			t.Fatalf("write generation %q: %v", tag, err)
		}
	}
	stableState := backend.State()
	if stableState == nil || stableState.CommitSeq == 0 {
		t.Fatalf("missing stable generation: %+v", stableState)
	}
	model, err := powerlossoracle.Capture(dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss-certification: stop before reused-page index sync")
	var snapshot *powerlossoracle.Model
	var indexPath string
	beforeIndexSyncs := 0
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(dir, event); err != nil {
			return err
		}
		if event.Point != durabilitycut.BeforeIndexDataSync {
			return nil
		}
		if beforeIndexSyncs == 0 {
			beforeIndexSyncs++
			return nil
		}
		snapshot = model.Clone()
		rel, err := filepath.Rel(dir, event.Path)
		if err != nil {
			return err
		}
		indexPath = filepath.ToSlash(rel)
		return cutErr
	})
	if err := writeGeneration('e'); err != nil {
		restore()
		t.Fatalf("write first post-horizon generation: %v", err)
	}
	stableState = backend.State()
	beforeReuse := certificationStatUint64(t, backend.Stats(), "treedb.freelist.reuse_alloc_pages_total")
	err = writeGeneration('f')
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil || indexPath == "" {
		t.Fatalf("pre-index-sync reuse cut err=%v snapshot=%t path=%q", err, snapshot != nil, indexPath)
	}
	afterReuse := certificationStatUint64(t, backend.Stats(), "treedb.freelist.reuse_alloc_pages_total")
	if afterReuse <= beforeReuse {
		t.Fatalf("cut generation reused no freelist page: before=%d after=%d", beforeReuse, afterReuse)
	}
	changed, err := snapshot.ChangedRanges(indexPath)
	if err != nil || len(changed) == 0 {
		t.Fatalf("changed index ranges=%v err=%v", changed, err)
	}

	variants, _, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{
		ID:         "older-meta-live-page-reuse",
		Point:      powerlossoracle.BeforeIndexDataSync,
		Occurrence: 1,
		Model:      snapshot,
		OldPageWrites: []powerlossoracle.DirtyResource{{
			Kind: powerlossoracle.ResourceIndex, ID: "first-reused-old-live-page", Path: indexPath, Ranges: changed,
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
	ledger := loadPowerLossCounterexampleLedger(t)
	var retained powerlossoracle.CounterexampleLedgerEntry
	for _, entry := range ledger.Entries {
		if entry.ID == "older-meta-live-page-reused" {
			retained = entry
			break
		}
	}
	if retained.ID == "" {
		t.Fatal("counterexample ledger omitted older-meta-live-page-reused")
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	variants, err = powerlossoracle.SelectReplayVariant(variants, selector)
	if err != nil {
		t.Fatal(err)
	}
	if err := closeBackend(); err != nil && !errors.Is(err, cutErr) {
		t.Logf("close after injected pre-index-sync cut: %v", err)
	}
	closed = true

	for _, variant := range variants {
		variant := variant
		t.Run(variant.ID, func(t *testing.T) {
			result, reopened, closeReopened, err := powerlossreopen.Stable(variant.Model, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if result.Rejected {
				t.Fatalf("public Open rejected reused-page image: %v", result.Err)
			}
			if reopened == nil {
				t.Fatal("public Open returned no database")
			}
			if result.CommitSeq != stableState.CommitSeq || result.AppliedLSN != stableState.AppliedCommandLSN {
				_ = closeReopened()
				t.Fatalf("recovered state=(commit=%d applied=%d) want=(%d,%d)", result.CommitSeq, result.AppliedLSN, stableState.CommitSeq, stableState.AppliedCommandLSN)
			}
			if err := closeReopened(); err != nil {
				t.Fatal(err)
			}
			var entry *powerlossoracle.CounterexampleLedgerEntry
			if variant.ID == retained.VariantID {
				entry = &retained
			}
			if variant.Family == powerlossoracle.VariantOldPageReuse && entry == nil {
				t.Fatalf("selected reused-page variant %q is not retained", variant.ID)
			}
			if err := powerlossoracle.ValidateVariantObservation(variant, powerlossoracle.VariantObservation{Opened: true, Result: powerlossoracle.ExpectedOldRoot}, entry); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func certificationStatUint64(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(stats[key], 10, 64)
	if err != nil {
		t.Fatalf("parse stat %s=%q: %v", key, stats[key], err)
	}
	return value
}
