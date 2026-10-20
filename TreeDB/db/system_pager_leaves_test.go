package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestSystemPagerLeavesRejectUnmarkedNamespacesBeforeRecovery(t *testing.T) {
	for _, name := range []string{indexFileName, indexNewFileName, indexBakFileName} {
		for _, ignore := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignore=%t", name, ignore), func(t *testing.T) {
				dir := t.TempDir()
				// This is a recognized older format, not an unknown-feature error.
				if err := writeFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureFreelistPatriciaV2}}); err != nil {
					t.Fatal(err)
				}
				image := bytes.Repeat([]byte{0xa5}, page.PageSize)
				if err := os.WriteFile(filepath.Join(dir, name), image, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, indexReadyFileName), []byte("ready\n"), 0600); err != nil {
					t.Fatal(err)
				}
				before := make(map[string][]byte)
				entries, err := os.ReadDir(dir)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					before[entry.Name()], err = os.ReadFile(filepath.Join(dir, entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
				}
				for _, open := range []func(Options) (*DB, error){Open, openReadOnly, openReadOnlyNoLock} {
					database, err := open(Options{Dir: dir, ReadOnly: true, IgnoreFormatConfig: ignore})
					if database != nil {
						_ = database.Close()
					}
					if !errors.Is(err, ErrLegacyFormatRebuildRequired) || !strings.Contains(err.Error(), RequiredFeatureSystemPagerLeavesV1) {
						t.Fatalf("unmarked %s open: %v", name, err)
					}
				}
				if database, err := Open(Options{Dir: dir, IgnoreFormatConfig: ignore}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
					if database != nil {
						_ = database.Close()
					}
					t.Fatalf("read-write open: %v", err)
				}
				for label, operation := range map[string]func() error{
					"swap":   func() error { return recoverIndexSwap(dir) },
					"vacuum": func() error { return VacuumIndexOffline(Options{Dir: dir, IgnoreFormatConfig: ignore}) },
					"rewrite": func() error {
						_, err := ValueLogRewriteOffline(Options{Dir: dir, IgnoreFormatConfig: ignore})
						return err
					},
					"retrofit": func() error { return SaveFormatConfig(dir, formatConfigFromOptions(Options{Dir: dir})) },
				} {
					if err := operation(); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
						t.Fatalf("%s accepted old namespace: %v", label, err)
					}
				}
				after, err := os.ReadDir(dir)
				if err != nil || len(after) != len(before) {
					t.Fatalf("namespace changed: %v", err)
				}
				for _, entry := range after {
					got, err := os.ReadFile(filepath.Join(dir, entry.Name()))
					want, ok := before[entry.Name()]
					if err != nil || !ok || !bytes.Equal(got, want) {
						t.Fatalf("refused file changed: %s: %v", entry.Name(), err)
					}
				}
			})
		}
	}
}

func TestSystemPagerLeavesFreshMarkerCannotBeRemoved(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	cfg, ok, err := LoadFormatConfig(dir)
	if err != nil || !ok || !cfg.RequiresSystemPagerLeavesV1() {
		t.Fatalf("fresh marker: %+v %t %v", cfg, ok, err)
	}
	before, err := os.ReadFile(formatConfigPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	features := cfg.RequiredFeatures[:0]
	for _, feature := range cfg.RequiredFeatures {
		if feature != RequiredFeatureSystemPagerLeavesV1 {
			features = append(features, feature)
		}
	}
	cfg.RequiredFeatures = features
	if err := SaveFormatConfig(dir, cfg); !errors.Is(err, ErrLegacyFormatRebuildRequired) || !strings.Contains(err.Error(), RequiredFeatureSystemPagerLeavesV1) {
		t.Fatalf("removed required marker: %v", err)
	}
	after, err := os.ReadFile(formatConfigPath(dir))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refused save changed config: %v", err)
	}
}

func TestSystemPagerLeavesComposeWithLogDATAAndMaintenance(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, DisableBackgroundPrune: true}
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			_ = database.Close()
		}
	}()
	b := database.NewBatch()
	for i := 0; i < 512; i++ {
		if err := b.Set([]byte(fmt.Sprintf("user/%04d", i)), []byte("persistent user value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	expectedSystemValue := ""
	check := func(label string) {
		t.Helper()
		snap := database.AcquireSnapshot()
		if snap == nil {
			t.Fatal("missing snapshot")
		}
		defer snap.Close()
		state := snap.State()
		internalPages := 0
		if err := leafrefscan.Walk(context.Background(), state.SystemRootPageID, snap.idx.pager.Get, func(_ uint64, n node.Node) error {
			if n.Type() == page.PageTypeInternal {
				internalPages++
			}
			return nil
		}, func(page.LeafLogPtr) error {
			return errors.New("SYSTEM child escaped pager namespace")
		}); err != nil {
			t.Fatalf("%s system closure: %v", label, err)
		}
		if internalPages == 0 {
			t.Fatalf("%s did not exercise SYSTEM child pages", label)
		}
		for _, i := range []int{0, 255, 511} {
			want := fmt.Sprintf("value-%04d", i)
			if i == 0 {
				want = expectedSystemValue
			}
			entry, err := snap.GetEntryAtRoot(state.SystemRootPageID, []byte(fmt.Sprintf("sys/%04d", i)))
			if err != nil || string(entry.Value) != want {
				t.Fatalf("%s system %d: %q want %q: %v", label, i, entry.Value, want, err)
			}
		}
		userLeaves := 0
		if err := leafrefscan.Walk(context.Background(), state.RootPageID, snap.idx.pager.Get, nil, func(page.LeafLogPtr) error { userLeaves++; return nil }); err != nil {
			t.Fatal(err)
		}
		if userLeaves == 0 {
			t.Fatalf("%s did not activate user log DATA", label)
		}
		for _, i := range []int{0, 255, 511} {
			got, err := snap.Get([]byte(fmt.Sprintf("user/%04d", i)))
			if err != nil || string(got) != "persistent user value" {
				t.Fatalf("%s user %d: %q %v", label, i, got, err)
			}
		}
	}
	publish := func(value string) {
		t.Helper()
		kvs := systemRangeKVs(512, map[int]string{0: value})
		if _, err := database.PublishSystemRootIterator(mustFrozenSystemMemtable(t, kvs...).NewIterator(nil, nil)); err != nil {
			t.Fatal(err)
		}
		expectedSystemValue = value
		check(value)
	}
	publish("old")
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("missing held snapshot")
	}
	defer func() {
		if held != nil {
			_ = held.Close()
		}
	}()
	heldRoot := held.State().SystemRootPageID
	publish("current")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	beforeVacuum, beforeIndex := database.state.Load(), database.idx.Load()
	vacuumErr := database.VacuumIndexOnline(context.Background())
	if runtime.GOOS == "windows" {
		if !errors.Is(vacuumErr, ErrVacuumUnsupported) || database.state.Load() != beforeVacuum || database.idx.Load() != beforeIndex {
			t.Fatalf("unsupported online vacuum changed state or returned %v", vacuumErr)
		}
	} else if vacuumErr != nil {
		t.Fatal(vacuumErr)
	}
	check("online-vacuum")
	publish("after-online")
	old, err := held.GetEntryAtRoot(heldRoot, []byte("sys/0000"))
	if err != nil || string(old.Value) != "old" {
		t.Fatalf("held system: %q %v", old.Value, err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	if err := VacuumIndexOffline(opts); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	check("offline-reopen")
	publish("after-offline")
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = nil
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	check("final-reopen")
	snap := database.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing final snapshot")
	}
	entry, err := snap.GetEntryAtRoot(snap.State().SystemRootPageID, []byte("sys/0000"))
	got := string(entry.Value)
	closeErr := snap.Close()
	if err != nil || closeErr != nil || got != "after-offline" {
		t.Fatalf("reopen system: %q %v %v", got, err, closeErr)
	}
}
