package treedb_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

type freshLayoutNamespaceCase struct {
	variantID         string
	disableSideStores bool
	partial           bool
	point             durabilitycut.Point
	path              func(parent, database string) string
	wantOccurrence    int
	wantStableDirs    []string
}

func TestPowerLossCertificationFreshCompositeBeforeOuterParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-before-outer-parent-sync",
		point:          durabilitycut.BeforeNewFileDirectorySync,
		path:           func(parent, _ string) string { return parent },
		wantOccurrence: 1,
		wantStableDirs: []string{"."},
	})
}

func TestPowerLossCertificationFreshCompositeBeforeRootSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-before-root-sync",
		point:          durabilitycut.BeforeNewFileDirectorySync,
		path:           func(_, database string) string { return database },
		wantOccurrence: 0,
		wantStableDirs: []string{"."},
	})
}

func TestPowerLossCertificationFreshCompositeAfterRootSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-after-root-sync",
		point:          durabilitycut.AfterNewFileDirectorySync,
		path:           func(_, database string) string { return database },
		wantOccurrence: 0,
		wantStableDirs: []string{"."},
	})
}

func TestPowerLossCertificationFreshCompositeAfterOuterParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-after-outer-parent-sync",
		point:          durabilitycut.AfterNewFileDirectorySync,
		path:           func(parent, _ string) string { return parent },
		wantOccurrence: 1,
		wantStableDirs: []string{".", "db", "db/dictdb", "db/maindb"},
	})
}

func TestPowerLossCertificationFreshCompositeBeforeMainDBParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-before-maindb-parent-sync",
		point:          durabilitycut.BeforeNewFileDirectorySync,
		path:           func(_, database string) string { return filepath.Join(database, "maindb") },
		wantOccurrence: 5,
		wantStableDirs: []string{".", "db", "db/dictdb", "db/dictdb/column_assets", "db/dictdb/leaf_vlog", "db/dictdb/value_vlog", "db/dictdb/wal", "db/maindb"},
	})
}

func TestPowerLossCertificationFreshCompositeAfterMainDBParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "fresh-layout-composite-after-maindb-parent-sync",
		point:          durabilitycut.AfterNewFileDirectorySync,
		path:           func(_, database string) string { return filepath.Join(database, "maindb") },
		wantOccurrence: 5,
		wantStableDirs: []string{".", "db", "db/dictdb", "db/dictdb/column_assets", "db/dictdb/leaf_vlog", "db/dictdb/value_vlog", "db/dictdb/wal", "db/maindb", "db/maindb/column_assets", "db/maindb/leaf_vlog", "db/maindb/value_vlog", "db/maindb/wal"},
	})
}

func TestPowerLossCertificationPartialCompositeAfterOuterParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:      "partial-layout-composite-after-outer-parent-sync",
		partial:        true,
		point:          durabilitycut.AfterNewFileDirectorySync,
		path:           func(parent, _ string) string { return parent },
		wantOccurrence: 1,
		wantStableDirs: []string{".", "db", "db/dictdb", "db/maindb"},
	})
}

func TestPowerLossCertificationFreshFlatBeforeOuterParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:         "fresh-layout-flat-before-outer-parent-sync",
		disableSideStores: true,
		point:             durabilitycut.BeforeNewFileDirectorySync,
		path:              func(parent, _ string) string { return parent },
		wantOccurrence:    0,
		wantStableDirs:    []string{"."},
	})
}

func TestPowerLossCertificationFreshFlatAfterOuterParentSync(t *testing.T) {
	testPowerLossCertificationFreshLayout(t, freshLayoutNamespaceCase{
		variantID:         "fresh-layout-flat-after-outer-parent-sync",
		disableSideStores: true,
		point:             durabilitycut.AfterNewFileDirectorySync,
		path:              func(parent, _ string) string { return parent },
		wantOccurrence:    0,
		wantStableDirs:    []string{".", "db"},
	})
}

func testPowerLossCertificationFreshLayout(t *testing.T, testCase freshLayoutNamespaceCase) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("modeled generic parent-directory barriers are unavailable on Windows")
	}
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	databaseDir := filepath.Join(parent, "db")
	model, err := powerlossoracle.Capture(parent)
	if err != nil {
		t.Fatal(err)
	}
	if testCase.partial {
		if err := os.MkdirAll(filepath.Join(databaseDir, "maindb"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, databaseDir)
	opts.DisableSideStores = testCase.disableSideStores
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1

	cutErr := errors.New("power-loss-certification: fresh-layout namespace cut")
	wantPath := filepath.Clean(testCase.path(parent, databaseDir))
	cutTriggered := false
	pointOccurrence := 0
	targetOccurrence := -1
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Path != "" && !pathWithinRoot(parent, event.Path) {
			// Writable open conservatively repairs ancestors above the modeled
			// parent. That parent was captured as the stable oracle root, so its
			// own ancestor barriers are outside this crash image.
			return nil
		}
		if err := model.Observe(parent, event); err != nil {
			return err
		}
		occurrence := -1
		if event.Point == testCase.point {
			occurrence = pointOccurrence
			pointOccurrence++
		}
		if !cutTriggered && event.Resource == durabilitycut.ResourceAuxiliary && event.Point == testCase.point && filepath.Clean(event.Path) == wantPath {
			cutTriggered = true
			targetOccurrence = occurrence
			return cutErr
		}
		return nil
	})
	database, openErr := treedb.Open(opts)
	restore()
	if database != nil {
		_ = database.Close()
	}
	if !cutTriggered || targetOccurrence < 0 || !errors.Is(openErr, cutErr) {
		t.Fatalf("fresh-layout Open error=%v cutTriggered=%t want injected cut at %s %q", openErr, cutTriggered, testCase.point, wantPath)
	}
	if targetOccurrence != testCase.wantOccurrence {
		t.Fatalf("fresh-layout target occurrence=%d want frozen address occurrence=%d", targetOccurrence, testCase.wantOccurrence)
	}
	wantCutID := fmt.Sprintf("cut/%s/%s/%03d", testCase.variantID, testCase.point, targetOccurrence)
	t.Logf("fresh-layout evidence address=%s", wantCutID)
	if selector != (powerlossoracle.ReplaySelector{}) {
		if selector.CutID != wantCutID || selector.VariantID != testCase.variantID || selector.Seed != powerLossOracleSeed {
			t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, wantCutID, testCase.variantID, powerLossOracleSeed)
		}
	}
	stableImage := t.TempDir()
	if err := model.MaterializeStable(stableImage); err != nil {
		t.Fatal(err)
	}
	if got, err := relativeDirectories(stableImage); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(got, testCase.wantStableDirs) {
		t.Fatalf("stable namespace directories=%v want=%v", got, testCase.wantStableDirs)
	}

	readOnly := os.Getenv(powerlossoracle.EnvEvidenceReopenMode) == powerlossoracle.EvidenceReopenReadOnly
	if readOnly {
		// These cuts intentionally precede initialization proof. A stable image
		// may contain no database root or only an empty/partial layout, so normal
		// recovery is the writable public Open path.
		t.Skip("fresh-layout initialization cuts require read-write public reopen")
	}
	result, reopened, closeReopened, err := powerlossreopen.StableChild(model, "db", opts, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rejected || reopened == nil {
		t.Fatalf("public Open rejected modeled fresh-layout image: %+v", result)
	}
	if err := reopened.SetSync([]byte("fresh-layout/recovered"), []byte(testCase.variantID)); err != nil {
		_ = closeReopened()
		t.Fatalf("recovered database is not durably writable: %v", err)
	}
	if got, err := reopened.Get([]byte("fresh-layout/recovered")); err != nil || string(got) != testCase.variantID {
		_ = closeReopened()
		t.Fatalf("recovered value=%q err=%v want=%q", got, err, testCase.variantID)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

func relativeDirectories(root string) ([]string, error) {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		dirs = append(dirs, filepath.ToSlash(relative))
		return nil
	})
	return dirs, err
}

func pathWithinRoot(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func TestPowerLossCertificationProvenLayoutDoesNotRewriteNamespace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("generic parent-directory barrier control is Unix-specific")
	}
	parent := t.TempDir()
	databaseDir := filepath.Join(parent, "db")
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, databaseDir)
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	database, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SetSync([]byte("proven-layout"), []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(parent)
	if err != nil {
		t.Fatal(err)
	}

	var layoutBarriers int
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(parent, event); err != nil {
			return err
		}
		if event.Resource == durabilitycut.ResourceAuxiliary && (event.Point == durabilitycut.BeforeNewFileDirectorySync || event.Point == durabilitycut.AfterNewFileDirectorySync) {
			layoutBarriers++
		}
		return nil
	})
	reopened, err := treedb.Open(opts)
	if err == nil {
		err = reopened.Close()
	}
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if layoutBarriers != 0 {
		t.Fatalf("proven steady-state layout emitted %d creation barriers, want no rewrite", layoutBarriers)
	}
}
