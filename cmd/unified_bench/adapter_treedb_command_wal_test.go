package main

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/kvstore"
)

func TestTreeDBBackendCommandWALVariantPersistsFeatureAndAppendsTypedFrames(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbCommandWALStatsScan = true

	dir := t.TempDir()
	db, err := NewTreeDBBackendCommandWAL(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackendCommandWAL: %v", err)
	}
	if got := db.Name(); got != "TreeDB (backend command_wal_v1)" {
		t.Fatalf("Name=%q, want explicit command WAL variant name", got)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	sp, ok := db.(interface{ Stats() map[string]string })
	if !ok {
		t.Fatalf("%T does not expose Stats", db)
	}
	stats := sp.Stats()
	for key, want := range map[string]string{
		"treedb.command_wal.enabled":          "true",
		"treedb.command_wal.required_feature": "true",
		"treedb.applied_command_lsn":          "1",
		"treedb.command_wal.frames":           "1",
		"treedb.command_wal.typed_segments":   "1",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stats[%q]=%q, want %q (stats=%#v)", key, got, want, stats)
		}
	}
	required, err := treedbdb.CommandWALRequiredFeatureEnabled(dir)
	if err != nil {
		t.Fatalf("CommandWALRequiredFeatureEnabled: %v", err)
	}
	if !required {
		t.Fatal("command WAL variant did not persist command_wal_v1 required feature")
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatal("format config missing")
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("command WAL backend disabled requested IndexOuterLeavesInValueLog")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closedStats := sp.Stats()
	if got := closedStats["treedb.command_wal.frames"]; got != "1" {
		t.Fatalf("closed stats command WAL frames=%q, want cached final stats (stats=%#v)", got, closedStats)
	}
	postClose, ok := db.(interface {
		Get([]byte) ([]byte, error)
		Set([]byte, []byte) error
		Delete([]byte) error
		SetSync([]byte, []byte) error
		DeleteSync([]byte) error
		Has([]byte) (bool, error)
		Print() error
		Checkpoint() error
		Iterator([]byte, []byte) (kvstore.Iterator, error)
		ReverseIterator([]byte, []byte) (kvstore.Iterator, error)
		NewBatch() (kvstore.Batch, error)
		NewBatchWithSize(int) (kvstore.Batch, error)
		AcquireReadSnapshot() (kvstore.ReadSnapshot, error)
	})
	if !ok {
		t.Fatalf("%T does not expose post-close test surface", db)
	}
	if _, err := postClose.Get([]byte("k")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Get after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.Set([]byte("k"), []byte("v")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Set after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.Delete([]byte("k")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Delete after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.SetSync([]byte("k"), []byte("v")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("SetSync after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.DeleteSync([]byte("k")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("DeleteSync after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.Has([]byte("k")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Has after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.Print(); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Print after Close error=%v, want ErrClosed", err)
	}
	if err := postClose.Checkpoint(); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Checkpoint after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.Iterator(nil, nil); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Iterator after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.ReverseIterator(nil, nil); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("ReverseIterator after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.NewBatch(); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("NewBatch after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.NewBatchWithSize(1); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("NewBatchWithSize after Close error=%v, want ErrClosed", err)
	}
	if _, err := postClose.AcquireReadSnapshot(); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("AcquireReadSnapshot after Close error=%v, want ErrClosed", err)
	}
}

func TestTreeDBPublicCommandWALVariantUsesCachedCommandWALPath(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbCommandWALStatsScan = true

	dir := t.TempDir()
	db, err := NewTreeDBPublicCommandWAL(dir)
	if err != nil {
		t.Fatalf("NewTreeDBPublicCommandWAL: %v", err)
	}
	if got := db.Name(); got != "TreeDB (public cached command_wal_v1)" {
		_ = db.Close()
		t.Fatalf("Name=%q, want explicit public cached command WAL variant name", got)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		_ = db.Close()
		t.Fatalf("Set: %v", err)
	}
	sp, ok := db.(interface{ Stats() map[string]string })
	if !ok {
		_ = db.Close()
		t.Fatalf("%T does not expose Stats", db)
	}
	stats := sp.Stats()
	for key, want := range map[string]string{
		"treedb.write_path.mode":              "command_wal_cached",
		"treedb.command_wal.enabled":          "true",
		"treedb.command_wal.required_feature": "true",
		// An uncontended durable cached write syncs its mutation frame directly;
		// concurrent writes use the grouped durable-prefix path.
		"treedb.command_wal.frames":         "1",
		"treedb.command_wal.typed_segments": "1",
	} {
		if got := stats[key]; got != want {
			_ = db.Close()
			t.Fatalf("stats[%q]=%q, want %q (stats=%#v)", key, got, want, stats)
		}
	}
	checkpointer, ok := db.(interface{ Checkpoint() error })
	if !ok {
		_ = db.Close()
		t.Fatalf("%T does not expose Checkpoint", db)
	}
	if err := checkpointer.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	stats = sp.Stats()
	if got := stats["treedb.applied_command_lsn"]; got != "1" {
		_ = db.Close()
		t.Fatalf("applied_command_lsn=%q, want directly synced mutation after checkpoint (stats=%#v)", got, stats)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopen, err := treedb.Open(treedb.Options{Dir: dir, CommandWALStatsScan: true})
	if err != nil {
		t.Fatalf("reopen public command WAL variant dir: %v", err)
	}
	defer func() { _ = reopen.Close() }()
	reopenStats := reopen.Stats()
	if got := reopenStats["treedb.write_path.mode"]; got != "command_wal_cached" {
		t.Fatalf("reopen write_path.mode=%q, want command_wal_cached (stats=%#v)", got, reopenStats)
	}
	if got := reopenStats["treedb.command_wal.required_feature"]; got != "true" {
		t.Fatalf("reopen required_feature=%q, want true (stats=%#v)", got, reopenStats)
	}
}

func TestTreeDBBackendCloseSerializesStatsAndOperations(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbCommandWALStatsScan = true

	db, err := NewTreeDBBackendCommandWAL(t.TempDir())
	if err != nil {
		t.Fatalf("NewTreeDBBackendCommandWAL: %v", err)
	}
	adapter, ok := db.(*treeDBBackendAdapter)
	if !ok {
		_ = db.Close()
		t.Fatalf("backend type=%T, want *treeDBBackendAdapter", db)
	}
	if err := adapter.Set([]byte("seed"), []byte("value")); err != nil {
		_ = adapter.Close()
		t.Fatalf("Set seed: %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < 25; j++ {
				if _, err := adapter.Get([]byte("seed")); err != nil && !errors.Is(err, treedbdb.ErrClosed) {
					errs <- fmt.Errorf("worker %d Get: %w", worker, err)
					return
				}
				_ = adapter.Stats()
			}
		}(i)
	}
	close(start)
	closeErr := adapter.Close()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatalf("Close: %v", closeErr)
	}
	if _, err := adapter.Get([]byte("seed")); !errors.Is(err, treedbdb.ErrClosed) {
		t.Fatalf("Get after Close error=%v, want ErrClosed", err)
	}
	if stats := adapter.Stats(); stats["treedb.command_wal.enabled"] != "true" {
		t.Fatalf("Stats after Close lost final command WAL stats: %#v", stats)
	}
}

func TestTreeDBBackendPreservesOuterLeavesFlag(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeavesInVlog = true

	dir := t.TempDir()
	db, err := NewTreeDBBackend(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackend: %v", err)
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set with outer leaves enabled: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close backend: %v", err)
	}
	cfg, ok, err := treedbdb.LoadFormatConfig(dir)
	if err != nil {
		t.Fatalf("LoadFormatConfig: %v", err)
	}
	if !ok {
		t.Fatal("format config missing")
	}
	if !cfg.IndexOuterLeavesInValueLog {
		t.Fatalf("legacy backend disabled IndexOuterLeavesInValueLog")
	}
}

func TestTreeDBBackendInstallsLeafLogForPersistedOuterLeavesFormat(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()

	dir := t.TempDir()
	if err := treedbdb.SaveFormatConfig(dir, treedbdb.FormatConfig{IndexOuterLeavesInValueLog: true}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	db, err := NewTreeDBBackend(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackend: %v", err)
	}
	adapter, ok := db.(*treeDBBackendAdapter)
	if !ok {
		_ = db.Close()
		t.Fatalf("backend type=%T, want *treeDBBackendAdapter", db)
	}
	if adapter.leafLog == nil {
		_ = db.Close()
		t.Fatalf("backend did not install standalone leaf page log for persisted outer leaves format")
	}
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		_ = db.Close()
		t.Fatalf("Set with persisted outer leaves format: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close backend: %v", err)
	}
}

func TestTreeDBBackendDoesNotInstallLeafLogWhenPersistedInlineFormatOverridesFlag(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeavesInVlog = true

	dir := t.TempDir()
	if err := treedbdb.SaveFormatConfig(dir, treedbdb.FormatConfig{IndexOuterLeavesInValueLog: false}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	db, err := NewTreeDBBackend(dir)
	if err != nil {
		t.Fatalf("NewTreeDBBackend: %v", err)
	}
	defer db.Close()
	adapter, ok := db.(*treeDBBackendAdapter)
	if !ok {
		t.Fatalf("backend type=%T, want *treeDBBackendAdapter", db)
	}
	if adapter.leafLog != nil {
		t.Fatalf("backend installed standalone leaf page log despite persisted inline-leaf format")
	}
}

func TestTreeDBBackendCommandWALForcesWALOnWhenProfileDisablesWAL(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbDisableWAL = true

	db, err := NewTreeDBBackendCommandWAL(t.TempDir())
	if err != nil {
		t.Fatalf("NewTreeDBBackendCommandWAL with disabled WAL flag: %v", err)
	}
	defer db.Close()
	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

func TestTreeDBCommandWALAliasResolvesToBackendVariant(t *testing.T) {
	factory, err := GetDBFactory("treedb_command_wal")
	if err != nil {
		t.Fatalf("GetDBFactory alias: %v", err)
	}
	if factory == nil {
		t.Fatal("GetDBFactory returned nil factory")
	}
}
