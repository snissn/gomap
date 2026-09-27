package db

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDependencyDirectoryV2RequiredFeatureNewStoreAndReopen(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		dir := t.TempDir()
		cfg := FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}
		if err := SaveFormatConfig(dir, cfg); err != nil {
			t.Fatal(err)
		}
		database, err := Open(Options{Dir: dir, IgnoreFormatConfig: ignore})
		if err != nil {
			t.Fatal(err)
		}
		if database.durableRoot.record.Directory.RootPageID == 0 {
			t.Fatal("required directory was not initialized")
		}
		batch := database.NewBatch()
		if err := batch.Set([]byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
		if err := batch.WriteSync(); err != nil {
			t.Fatal(err)
		}
		if err := batch.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database, err = Open(Options{Dir: dir, IgnoreFormatConfig: ignore})
		if err != nil {
			t.Fatal(err)
		}
		got, err := database.Get([]byte("key"))
		if err != nil || !bytes.Equal(got, []byte("value")) {
			t.Fatalf("reopen: %q %v", got, err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		for _, open := range []func(Options) (*DB, error){Open, openReadOnlyNoLock} {
			reader, err := open(Options{Dir: dir, ReadOnly: true, IgnoreFormatConfig: ignore})
			if err != nil {
				t.Fatal(err)
			}
			got, err := reader.Get([]byte("key"))
			if err != nil || !bytes.Equal(got, []byte("value")) {
				t.Fatalf("read-only reopen: %q %v", got, err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := SaveFormatConfig(dir, FormatConfig{}); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
			t.Fatalf("feature removal: %v", err)
		}
	}
}

func TestDependencyDirectoryV2RequiredFeatureRefusesV1Activation(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	batch := database.NewBatch()
	if err := batch.Set([]byte("populated"), []byte("V1")); err != nil {
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, indexFileName))
	if err != nil {
		t.Fatal(err)
	}
	cfg := FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}
	if err := SaveFormatConfig(dir, cfg); !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("V1 activation: %v", err)
	}
	// An externally edited format file must not turn V1 recovery into migration.
	if err := writeFormatConfig(dir, cfg); err != nil {
		t.Fatal(err)
	}
	for _, ignore := range []bool{false, true} {
		database, err := Open(Options{Dir: dir, IgnoreFormatConfig: ignore})
		if database != nil {
			database.Close()
		}
		if !errors.Is(err, ErrLegacyFormatRebuildRequired) {
			t.Fatalf("V1 open ignore=%v: %v", ignore, err)
		}
	}
	after, err := os.ReadFile(filepath.Join(dir, indexFileName))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("refusal changed V1 index: %v", err)
	}
}

func TestDependencyDirectoryV2UnknownRequiredFeaturePrecedesStorageDecode(t *testing.T) {
	dir := t.TempDir()
	if err := writeFormatConfig(dir, FormatConfig{RequiredFeatures: []string{"dependency_directory_future"}}); err != nil {
		t.Fatal(err)
	}
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(walDir, "commit-l0-000001.log")
	wal := []byte("malformed WAL must not be decoded")
	if err := os.WriteFile(walPath, wal, 0600); err != nil {
		t.Fatal(err)
	}
	index := []byte("malformed index must not be decoded")
	if err := os.WriteFile(filepath.Join(dir, indexFileName), index, 0600); err != nil {
		t.Fatal(err)
	}
	for _, ignore := range []bool{false, true} {
		database, err := Open(Options{Dir: dir, IgnoreFormatConfig: ignore})
		if database != nil {
			database.Close()
		}
		if !errors.Is(err, ErrUnsupportedRequiredFeature) {
			t.Fatalf("gate ignore=%v: %v", ignore, err)
		}
	}
	if database, err := openReadOnlyNoLock(Options{Dir: dir, ReadOnly: true}); !errors.Is(err, ErrUnsupportedRequiredFeature) {
		if database != nil {
			database.Close()
		}
		t.Fatalf("no-lock feature gate: %v", err)
	}
	walAfter, err := os.ReadFile(walPath)
	if err != nil || !bytes.Equal(wal, walAfter) {
		t.Fatalf("gate mutated WAL: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, indexFileName))
	if err != nil || !bytes.Equal(index, got) {
		t.Fatalf("gate mutated storage: %v", err)
	}
}

func TestDependencyDirectoryV2RequiredFeatureRefusesDirtyWAL(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(walDir, "commit-l0-000001.log")
	before := []byte("dirty WAL")
	if err := os.WriteFile(walPath, before, 0600); err != nil {
		t.Fatal(err)
	}
	if err := SaveFormatConfig(dir, FormatConfig{RequiredFeatures: []string{RequiredFeatureDependencyDirectoryV2}}); !errors.Is(err, ErrCommandWALDirtyActivation) {
		t.Fatalf("dirty activation: %v", err)
	}
	after, err := os.ReadFile(walPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("activation changed WAL: %v", err)
	}
	if _, err := os.Stat(formatConfigPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("activation wrote feature file: %v", err)
	}
}
