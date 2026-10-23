package treedb

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// These tests materialize only modeled stable bytes/names. Unlike the separate
// subprocess tests, process-visible dirty pages do not survive by assumption.
type externalStableCut struct {
	name  string
	model *powerlossoracle.Model
	new   bool
}

func externalPowerlossObserve(observer durabilitycut.Observer) func() {
	return sync.OnceFunc(durabilitycut.Install(observer))
}

func externalStableReopen(t *testing.T, model *powerlossoracle.Model, opts Options, config ExternalRecoveryConfig, cert ExternalRecoveryCheckpoint, value []byte, previous *ExternalRecoveryCheckpoint, previousValue []byte, absent ...string) {
	t.Helper()
	dir := t.TempDir() // Empty: MaterializeStable does not erase preexisting files.
	if err := model.MaterializeStable(dir); err != nil {
		t.Fatal(err)
	}
	release, err := model.InstallStableIdentityOverrides(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir = dir
	var previousRoot backenddb.RetainedPhysicalRootV1
	var previousPath string
	for attempt := 0; attempt < 2; attempt++ {
		o, err := OpenExternalRecovery(opts, config)
		if err != nil {
			t.Fatalf("stable public reopen %d: %v", attempt, err)
		}
		got, foundCert := o.RecoveredCheckpoint()
		if !foundCert || got.Generation != cert.Generation || got.Digest != cert.Digest || !bytes.Equal(got.Metadata, cert.Metadata) {
			_ = o.Close()
			t.Fatalf("selected certificate=%+v want generation=%d digest=%s", got, cert.Generation, cert.Digest)
		}
		actual, readErr := o.DB().Get([]byte("key"))
		if readErr != nil || !bytes.Equal(actual, value) {
			_ = o.Close()
			t.Fatalf("stable value bytes=%d want=%d err=%v", len(actual), len(value), readErr)
		}
		for _, key := range absent {
			got, err := o.DB().Get([]byte(key))
			if err != nil || got != nil {
				_ = o.Close()
				t.Fatalf("uncertified key %q survived: bytes=%d err=%v", key, len(got), err)
			}
		}
		if previous != nil {
			prior, priorErr := o.readCertificate(got.PreviousGeneration, got.PreviousDigest)
			if priorErr != nil || prior.Digest != previous.Digest {
				_ = o.Close()
				t.Fatalf("previous certificate lost: %v", priorErr)
			}
			var err error
			previousRoot, err = o.readRetainedRoot(prior)
			if err != nil {
				_ = o.Close()
				t.Fatalf("previous physical descriptor lost: %v", err)
			}
			previousPath, err = o.dataPath(prior.DataDirectory)
			if err != nil {
				_ = o.Close()
				t.Fatal(err)
			}
			if prior.DataDirectory == got.DataDirectory {
				roots, err := o.db.backend.CaptureRecoverableRootSet(context.Background())
				if err != nil {
					_ = o.Close()
					t.Fatal(err)
				}
				found := map[uint64]bool{}
				for _, root := range roots.Roots() {
					found[root.CommitSeq] = true
				}
				roots.Release()
				if !found[prior.Root.CommitSeq] || !found[got.Root.CommitSeq] {
					_ = o.Close()
					t.Fatalf("current/previous physical root union missing: %v", found)
				}
			}
		}
		if err := o.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if previous != nil {
		// Descriptor parsing alone cannot prove the old namespace's pointer
		// closure. Select and read that exact physical root after owner Close.
		if err := backenddb.RestoreRetainedPhysicalRootsV1(previousPath, previousRoot, nil); err != nil {
			t.Fatal(err)
		}
		physical := opts
		physical.Dir, physical.DisableSideStores, physical.DisableBackgroundPrune = previousPath, true, true
		db, err := backenddb.Open(physical)
		if err != nil {
			t.Fatalf("previous physical reopen: %v", err)
		}
		got, readErr := db.Get([]byte("key"))
		closeErr := db.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(got, previousValue) {
			t.Fatalf("previous physical value bytes=%d want=%d read=%v close=%v", len(got), len(previousValue), readErr, closeErr)
		}
	}
}

func externalPowerlossOptions(t *testing.T, outer bool) Options {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	opts := OptionsFor(ProfileNoWALFast, dir)
	opts.MemtableMode = "cow_ordered"
	opts.ValueLog.PointerThreshold, opts.ValueLog.ForcePointers = 1, true
	opts.IndexOuterLeavesInValueLog = outer
	return opts
}

func TestExternalRecoveryStableCheckpointPublication(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for _, outer := range []bool{false, true} {
		t.Run(fmt.Sprint("outer=", outer), func(t *testing.T) {
			opts := externalPowerlossOptions(t, outer)
			config := ExternalRecoveryConfig{Identity: "stable-checkpoint"}
			o, err := OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = o.Close() }()
			oldValue, newValue := bytes.Repeat([]byte("old"), 4096), bytes.Repeat([]byte("new"), 4096)
			externalTestSet(t, o, "key", oldValue)
			old, err := o.Checkpoint(context.Background(), []byte("old"))
			if err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var cuts []externalStableCut
			var renamed *powerlossoracle.Model
			restore := externalPowerlossObserve(func(event durabilitycut.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Namespace == durabilitycut.NamespaceRename && event.NewPath == filepath.Join(opts.Dir, "CURRENT") {
					renamed = model.Clone()
					cuts = append(cuts, externalStableCut{"current-renamed-unsynced", renamed, false})
				}
				return nil
			})
			defer restore()
			o.testCut = func(stage string) error {
				mu.Lock()
				defer mu.Unlock()
				if err := model.Overlay(opts.Dir); err != nil {
					return err
				}
				cuts = append(cuts, externalStableCut{stage, model.Clone(), stage == "current-stable"})
				return nil
			}
			externalTestSet(t, o, "key", newValue)
			newCert, err := o.Checkpoint(context.Background(), []byte("new"))
			restore()
			o.testCut = nil
			if err != nil {
				t.Fatal(err)
			}
			if len(cuts) != 4 || renamed == nil {
				t.Fatalf("missing publication cuts: count=%d current-rename=%v", len(cuts), renamed != nil)
			}
			// The JSON bytes were synced before rename. Selective writeback of
			// the CURRENT name may choose new; retaining its old name chooses old.
			families := []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantDataWithoutNamespace, powerlossoracle.VariantNamespaceWithoutData, powerlossoracle.VariantFullWriteback}
			expected := map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{}
			for _, family := range families {
				expected[family] = powerlossoracle.ExpectedOldRoot
			}
			expected[powerlossoracle.VariantNamespaceWithoutData] = powerlossoracle.ExpectedNewRoot
			expected[powerlossoracle.VariantFullWriteback] = powerlossoracle.ExpectedNewRoot
			variants, coverage, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{ID: "external-current", Point: durabilitycut.BeforeNewFileDirectorySync, Model: renamed, Dependencies: []powerlossoracle.DirtyResource{{Kind: powerlossoracle.ResourceAuxiliary, ID: "current", Path: "CURRENT", NewName: true, NamespaceDirs: []string{"."}}}, RequiredFamilies: families, ExpectedByFamily: expected})
			if err != nil || coverage.Generated != 4 {
				t.Fatalf("selective CURRENT coverage=%+v err=%v", coverage, err)
			}
			for _, variant := range variants {
				cuts = append(cuts, externalStableCut{string(variant.Family), variant.Model, variant.Expected == powerlossoracle.ExpectedNewRoot})
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			for _, cut := range cuts {
				t.Run(cut.name, func(t *testing.T) {
					if cut.new {
						externalStableReopen(t, cut.model, opts, config, newCert, newValue, &old, oldValue)
					} else {
						externalStableReopen(t, cut.model, opts, config, old, oldValue, nil, nil)
					}
				})
			}
		})
	}
}

func TestExternalRecoveryStableRestoreMetaSlots(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	opts := externalPowerlossOptions(t, true)
	config := ExternalRecoveryConfig{Identity: "stable-restore"}
	o, err := OpenExternalRecovery(opts, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Close() }()
	oldValue, certifiedValue := bytes.Repeat([]byte("first"), 2048), bytes.Repeat([]byte("certified"), 2048)
	externalTestSet(t, o, "key", oldValue)
	old, err := o.Checkpoint(context.Background(), []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	externalTestSet(t, o, "key", certifiedValue)
	cert, err := o.Checkpoint(context.Background(), []byte("certified"))
	if err != nil {
		t.Fatal(err)
	}
	// Advance both ordinary slots past the certificate. Otherwise one restore
	// write can be identical to its old bytes and has no meaningful torn range.
	for i := 0; i < 3; i++ {
		externalTestSet(t, o, "key", []byte(fmt.Sprint("uncertified suffix ", i)))
		if err := o.DB().Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var cuts []externalStableCut
	var tornCount int
	restore := externalPowerlossObserve(func(event durabilitycut.Event) error {
		mu.Lock()
		defer mu.Unlock()
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.StablePath == nil || event.Path != filepath.Join(opts.Dir, "working", "index.db") {
			return nil
		}
		switch event.Point {
		case durabilitycut.AfterMetaWrite:
			cuts = append(cuts, externalStableCut{fmt.Sprintf("after-slot-%d", event.Offset/int64(page.PageSize)), model.Clone(), true})
			ranges, err := model.ChangedRanges("working/index.db")
			if err != nil {
				return err
			}
			for _, changed := range ranges {
				if changed.Length < 2 || changed.Offset < event.Offset || changed.Offset+changed.Length > event.Offset+event.Length {
					continue
				}
				length := min(changed.Length, int64(16))
				resource := powerlossoracle.DirtyResource{Kind: powerlossoracle.ResourceIndex, ID: "restore-meta", Path: "working/index.db", Ranges: []powerlossoracle.ByteRange{{Offset: event.Offset, Length: event.Length}}, Torn: []powerlossoracle.TornBoundary{{ID: "changed-prefix", Format: powerlossoracle.FormatMeta, Offset: changed.Offset, Length: length, Persisted: 1}}}
				variants, _, err := powerlossoracle.GenerateVariants(powerlossoracle.CutSpec{ID: "external-restore", Point: event.Point, Occurrence: int(event.Offset / int64(page.PageSize)), Model: model, TargetMeta: &resource, RequiredFamilies: []powerlossoracle.VariantFamily{powerlossoracle.VariantSyncedOnly, powerlossoracle.VariantTargetMetaOnly, powerlossoracle.VariantTornFormat}, ExpectedByFamily: map[powerlossoracle.VariantFamily]powerlossoracle.ExpectedResult{powerlossoracle.VariantSyncedOnly: powerlossoracle.ExpectedNewRoot, powerlossoracle.VariantTargetMetaOnly: powerlossoracle.ExpectedNewRoot, powerlossoracle.VariantFullWriteback: powerlossoracle.ExpectedNewRoot, powerlossoracle.VariantTornFormat: powerlossoracle.ExpectedNewRoot}})
				if err != nil {
					return err
				}
				for _, variant := range variants {
					cuts = append(cuts, externalStableCut{fmt.Sprintf("slot-%d-%s", event.Offset/int64(page.PageSize), variant.Family), variant.Model, true})
					if variant.Family == powerlossoracle.VariantTornFormat {
						tornCount++
					}
				}
				break
			}
		case durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync:
			cuts = append(cuts, externalStableCut{string(event.Point), model.Clone(), true})
		}
		return nil
	})
	defer restore()
	o, err = OpenExternalRecovery(opts, config)
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if len(cuts) != 12 || tornCount != 2 {
		t.Fatalf("restore coverage cuts=%d want12 torn=%d want2", len(cuts), tornCount)
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			externalStableReopen(t, cut.model, opts, config, cert, certifiedValue, &old, oldValue)
		})
	}
}

func TestExternalRecoveryStableTransferPublication(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	archive, source, want := externalTransferFixture(t)
	expectedFiles := make(map[string]durabilitycut.Resource)
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(header.Name, "db/") {
			continue
		}
		rel := strings.TrimPrefix(header.Name, "db/")
		resource := durabilitycut.ResourceAuxiliary
		switch {
		case rel == "index.db":
			resource = durabilitycut.ResourceIndex
		case strings.HasPrefix(rel, "value_vlog/"):
			resource = durabilitycut.ResourceValueLog
		case strings.HasPrefix(rel, "leaf_vlog/"):
			resource = durabilitycut.ResourceOuterLeaf
		}
		expectedFiles[rel] = resource
	}
	syncedFiles := make(map[string]rootpublication.StableIdentity)
	opts := externalPowerlossOptions(t, false)
	config := ExternalRecoveryConfig{Identity: "stable-receiver"}
	o, err := OpenExternalRecovery(opts, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = o.Close() }()
	oldValue := []byte("receiver-before-install")
	externalTestSet(t, o, "key", oldValue)
	old, err := o.Checkpoint(context.Background(), []byte("old-receiver-progress"))
	if err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var cuts []externalStableCut
	var directoriesRenamed int
	restore := externalPowerlossObserve(func(event durabilitycut.Event) error {
		// The independent source fixture may still run its ordinary maintenance.
		if event.Root != "" {
			rel, err := filepath.Rel(opts.Dir, event.Root)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return nil
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyFileSync && strings.Contains(event.Path, ".install-") {
			rel, err := filepath.Rel(opts.Dir, event.Path)
			if err != nil {
				return err
			}
			parts := strings.SplitN(filepath.ToSlash(rel), "/", 2)
			if len(parts) != 2 || !strings.HasPrefix(parts[0], ".install-") || event.StablePath == nil {
				return fmt.Errorf("extraction sync lacks exact open file: %+v", event)
			}
			kind, ok := expectedFiles[parts[1]]
			if !ok || kind != event.Resource {
				return fmt.Errorf("unexpected extraction resource %q kind=%s", parts[1], event.Resource)
			}
			identity, err := rootpublication.StableIdentityFromFile(event.StablePath)
			if err != nil {
				return err
			}
			if _, duplicate := syncedFiles[parts[1]]; duplicate {
				return fmt.Errorf("duplicate extraction sync %q", parts[1])
			}
			for name, previous := range syncedFiles {
				if rootpublication.SamePhysicalIdentity(identity, previous) {
					return fmt.Errorf("extracted names %q and %q share an inode", name, parts[1])
				}
			}
			opened, err := event.StablePath.Stat()
			if err != nil {
				return err
			}
			named, err := os.Stat(event.Path)
			if err != nil || !opened.Mode().IsRegular() || !os.SameFile(opened, named) {
				return fmt.Errorf("extraction sync used a different physical file: %q: %v", parts[1], err)
			}
			syncedFiles[parts[1]] = identity
		}
		if event.Namespace == durabilitycut.NamespaceRename && strings.HasPrefix(filepath.Base(event.OldPath), ".install-") {
			directoriesRenamed++
			cuts = append(cuts, externalStableCut{"install-renamed-unsynced", model.Clone(), false})
		}
		if event.Namespace == durabilitycut.NamespaceRename && event.NewPath == filepath.Join(opts.Dir, "CURRENT") {
			cuts = append(cuts, externalStableCut{"current-renamed-unsynced", model.Clone(), false})
			early := model.Clone()
			if err := early.SyncDir("."); err != nil {
				return err
			}
			cuts = append(cuts, externalStableCut{"current-name-selective-writeback", early, true})
		}
		return nil
	})
	defer restore()
	o.testCut = func(stage string) error {
		mu.Lock()
		defer mu.Unlock()
		cuts = append(cuts, externalStableCut{stage, model.Clone(), stage == "current-stable"})
		return nil
	}
	authorized := false
	installed, err := o.InstallCheckpoint(context.Background(), bytes.NewReader(archive), func(cert ExternalRecoveryCheckpoint) ([]byte, error) {
		if cert.Digest != source.Digest {
			return nil, fmt.Errorf("source authority changed")
		}
		mu.Lock()
		cuts = append(cuts, externalStableCut{"before-acceptance-return", model.Clone(), false})
		mu.Unlock()
		authorized = true
		return []byte("receiver-accepted-progress"), nil
	})
	restore()
	o.testCut = nil
	if err != nil {
		t.Fatal(err)
	}
	if !authorized || len(expectedFiles) < 3 || len(syncedFiles) != len(expectedFiles) || directoriesRenamed != 1 || len(cuts) != 8 {
		t.Fatalf("missing install cuts: authorized=%v extracted=%d expected=%d renames=%d cuts=%d", authorized, len(syncedFiles), len(expectedFiles), directoriesRenamed, len(cuts))
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			if cut.new {
				externalStableReopen(t, cut.model, opts, config, installed, want, &old, oldValue, "suffix")
			} else {
				externalStableReopen(t, cut.model, opts, config, old, oldValue, nil, nil, "suffix")
			}
		})
	}
	// The positive oracle must depend on the pointer closure. Deliberately
	// persist deletion of the installed value namespace and require public
	// recovery/read to reject the resulting incomplete image.
	broken := cuts[len(cuts)-1].model.Clone()
	valueDir := filepath.ToSlash(filepath.Join(installed.DataDirectory, "value_vlog"))
	if err := broken.Unlink(valueDir); err != nil {
		t.Fatal(err)
	}
	if err := broken.SyncDir(installed.DataDirectory); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := broken.MaterializeStable(dir); err != nil {
		t.Fatal(err)
	}
	release, err := broken.InstallStableIdentityOverrides(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	opts.Dir = dir
	if missing, err := OpenExternalRecovery(opts, config); err == nil {
		got, readErr := missing.DB().Get([]byte("key"))
		closeErr := missing.Close()
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if readErr == nil {
			t.Fatalf("missing forced-pointer storage returned success: value bytes=%d", len(got))
		}
	}
}

func TestExternalRecoveryStablePublicationSyncFailure(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeNewFileDirectorySync, durabilitycut.AfterNewFileDirectorySync} {
		t.Run(string(point), func(t *testing.T) {
			opts := externalPowerlossOptions(t, false)
			config := ExternalRecoveryConfig{Identity: "publication-sync-failure"}
			o, err := OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = o.Close() }()
			oldValue, newValue := []byte("old-certificate"), []byte("candidate-certificate")
			externalTestSet(t, o, "key", oldValue)
			old, err := o.Checkpoint(context.Background(), oldValue)
			if err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("CURRENT parent sync failed")
			var mu sync.Mutex
			renamed, hit := false, false
			release := externalPowerlossObserve(func(event durabilitycut.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Namespace == durabilitycut.NamespaceRename && event.NewPath == filepath.Join(opts.Dir, "CURRENT") {
					renamed = true
				}
				if renamed && event.Point == point && event.Path == opts.Dir {
					hit = true
					return injected
				}
				return nil
			})
			defer release()
			externalTestSet(t, o, "key", newValue)
			if _, err := o.Checkpoint(context.Background(), newValue); !errors.Is(err, ErrExternalRecovery) || !strings.Contains(err.Error(), injected.Error()) {
				t.Fatalf("checkpoint failure=%v", err)
			}
			release()
			if !hit {
				t.Fatal("CURRENT parent barrier not reached")
			}
			called := false
			if err := o.Apply(func() error { called = true; return nil }); !errors.Is(err, ErrExternalRecovery) || called {
				t.Fatalf("ambiguous owner accepted Apply: called=%v err=%v", called, err)
			}
			if _, err := o.Checkpoint(context.Background(), nil); !errors.Is(err, ErrExternalRecovery) {
				t.Fatalf("ambiguous owner accepted Checkpoint: %v", err)
			}
			candidate, found, err := o.readCurrent()
			if err != nil || !found || candidate.Generation != old.Generation+1 {
				t.Fatalf("renamed certificate missing: %+v found=%v err=%v", candidate, found, err)
			}
			stable := model.Clone()
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			if point == durabilitycut.AfterNewFileDirectorySync {
				externalStableReopen(t, stable, opts, config, candidate, newValue, &old, oldValue)
			} else {
				externalStableReopen(t, stable, opts, config, old, oldValue, nil, nil)
			}
		})
	}
}

func TestExternalRecoveryStableRestoreSyncFailure(t *testing.T) {
	requireExternalRecoveryPlatform(t)
	for _, point := range []durabilitycut.Point{durabilitycut.BeforeMetaSync, durabilitycut.AfterMetaSync} {
		t.Run(string(point), func(t *testing.T) {
			opts := externalPowerlossOptions(t, true)
			config := ExternalRecoveryConfig{Identity: "restore-sync-failure"}
			o, err := OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			value := bytes.Repeat([]byte("certified"), 1024)
			externalTestSet(t, o, "key", value)
			cert, err := o.Checkpoint(context.Background(), value)
			if err != nil {
				_ = o.Close()
				t.Fatal(err)
			}
			externalTestSet(t, o, "key", []byte("uncertified suffix"))
			if err := o.DB().Checkpoint(); err != nil {
				_ = o.Close()
				t.Fatal(err)
			}
			if err := o.Close(); err != nil {
				t.Fatal(err)
			}
			model, err := powerlossoracle.Capture(opts.Dir)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("restore meta sync failed")
			var mu sync.Mutex
			hit := false
			release := externalPowerlossObserve(func(event durabilitycut.Event) error {
				mu.Lock()
				defer mu.Unlock()
				if err := model.Observe(opts.Dir, event); err != nil {
					return err
				}
				if event.Point == point && event.Path == filepath.Join(opts.Dir, cert.DataDirectory, "index.db") {
					hit = true
					if event.StablePath == nil {
						return errors.New("meta sync lacks physical handle")
					}
					return injected
				}
				return nil
			})
			defer release()
			failed, err := OpenExternalRecovery(opts, config)
			release()
			if failed != nil {
				_ = failed.Close()
				t.Fatal("restore failure returned an owner")
			}
			if !errors.Is(err, injected) || !hit {
				t.Fatalf("restore failure not propagated: hit=%v err=%v", hit, err)
			}
			externalStableReopen(t, model.Clone(), opts, config, cert, value, nil, nil)
			// The failed Open must also release ownership of the original namespace.
			reopened, err := OpenExternalRecovery(opts, config)
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
