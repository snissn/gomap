package db

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestConditionalTxnRejectsExistingReadOverwrite(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	value, revision, err := tx.GetVersioned([]byte("guard"))
	if err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	if !bytes.Equal(value, []byte("before")) || revision == page.LegacyEntryRevision {
		t.Fatalf("txn read=(%q,%d), want before/non-legacy", value, revision)
	}
	if err := d.SetSync([]byte("guard"), []byte("after")); err != nil {
		t.Fatalf("concurrent guard write: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
	if got, err := d.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("target after rejected commit=(%q,%v), want missing", got, err)
	}
}

func TestConditionalTxnRejectsVisibleWriteAfterAcceptedDurabilityError(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}

	d.testFailWriteMeta.Store(true)
	writeErr := d.SetSync([]byte("guard"), []byte("visible-before-durable"))
	d.testFailWriteMeta.Store(false)
	if !errors.Is(writeErr, errTestWriteMetaFailpoint) {
		t.Fatalf("accepted write error=%v, want retryable meta failpoint", writeErr)
	}
	if got, getErr := d.Get([]byte("guard")); getErr != nil || string(got) != "visible-before-durable" {
		t.Fatalf("visible guard after accepted error=(%q,%v)", got, getErr)
	}
	if err := tx.Set([]byte("target"), []byte("stale")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
	if got, err := d.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("target after rejected commit=(%q,%v), want missing", got, err)
	}
}

func TestConditionalTxnRejectsDeleteConflict(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	if err := d.DeleteSync([]byte("guard")); err != nil {
		t.Fatalf("concurrent guard delete: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnRejectsRangeDeleteConflict(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	batch := d.NewBatch().(*Batch)
	if err := batch.DeleteRange([]byte("g"), []byte("h")); err != nil {
		_ = batch.Close()
		t.Fatalf("concurrent range delete: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		t.Fatalf("concurrent range delete write: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("range delete batch Close: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnRejectsAbsentReadInsertDeleteCycle(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	value, revision, err := tx.GetVersioned([]byte("missing"))
	if err != nil {
		t.Fatalf("txn GetVersioned missing: %v", err)
	}
	if value != nil || revision != page.LegacyEntryRevision {
		t.Fatalf("txn missing read=(%q,%d), want nil/legacy", value, revision)
	}
	if err := d.SetSync([]byte("missing"), []byte("temporary")); err != nil {
		t.Fatalf("concurrent insert: %v", err)
	}
	if err := d.DeleteSync([]byte("missing")); err != nil {
		t.Fatalf("concurrent delete: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnRejectsExternalSnapshotRevisionBeforeTxn(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()
	_, revision, err := snap.GetVersionedAppend([]byte("guard"), nil)
	if err != nil {
		t.Fatalf("snapshot GetVersionedAppend guard: %v", err)
	}
	if err := d.SetSync([]byte("guard"), []byte("after")); err != nil {
		t.Fatalf("concurrent guard write: %v", err)
	}

	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()
	if err := tx.RequireReadVersion([]byte("guard"), revision, true); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn RequireReadVersion error=%v, want ErrConcurrentModification", err)
	}
}

func TestConditionalTxnAllowsDisjointConcurrentWrite(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	if err := d.SetSync([]byte("other"), []byte("concurrent")); err != nil {
		t.Fatalf("concurrent disjoint write: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.CommitSync(); err != nil {
		t.Fatalf("txn CommitSync: %v", err)
	}
	assertDBValue(t, d, "other", "concurrent")
	assertDBValue(t, d, "target", "value")
}

func TestConditionalTxnReadsStagedWritesBeforeSnapshot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("seed k: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()

	const stagedRevision page.EntryRevision = 12345
	if err := tx.SetWithRevision([]byte("k"), []byte("staged"), stagedRevision); err != nil {
		t.Fatalf("tx.SetWithRevision: %v", err)
	}
	value, revision, err := tx.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("tx.GetVersioned staged: %v", err)
	}
	if !bytes.Equal(value, []byte("staged")) || revision != stagedRevision {
		t.Fatalf("staged GetVersioned=(%q,%d), want staged/%d", value, revision, stagedRevision)
	}
	has, err := tx.Has([]byte("k"))
	if err != nil {
		t.Fatalf("tx.Has staged: %v", err)
	}
	if !has {
		t.Fatalf("tx.Has staged=false, want true")
	}
	if got := len(tx.reads); got != 0 {
		t.Fatalf("staged-only reads recorded %d read preconditions, want 0", got)
	}
	if err := tx.CommitSync(); err != nil {
		t.Fatalf("tx.CommitSync: %v", err)
	}
	assertDBValue(t, d, "k", "staged")
}

func TestConditionalTxnReadsStagedDeleteBeforeSnapshot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("k"), []byte("before")); err != nil {
		t.Fatalf("seed k: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()

	if err := tx.Delete([]byte("k")); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}
	value, revision, err := tx.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("tx.GetVersioned staged delete: %v", err)
	}
	if value != nil || revision != page.LegacyEntryRevision {
		t.Fatalf("staged delete GetVersioned=(%q,%d), want nil/legacy", value, revision)
	}
	has, err := tx.Has([]byte("k"))
	if err != nil {
		t.Fatalf("tx.Has staged delete: %v", err)
	}
	if has {
		t.Fatalf("tx.Has staged delete=true, want false")
	}
	if got := len(tx.reads); got != 0 {
		t.Fatalf("staged-delete-only reads recorded %d read preconditions, want 0", got)
	}
	if err := tx.CommitSync(); err != nil {
		t.Fatalf("tx.CommitSync: %v", err)
	}
	if got, err := d.Get([]byte("k")); err != nil || got != nil {
		t.Fatalf("k after staged delete=(%q,%v), want missing", got, err)
	}
}

func TestConditionalTxnRejectsDirectRootPublishChangingReadKey(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}

	newRoot := buildConditionalTxnManualRoot(t, d, "guard", "after")
	if err := d.ForceCommit(newRoot); err != nil {
		t.Fatalf("direct Commit: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
	if got, err := d.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("target after rejected direct-root conflict=(%q,%v), want missing", got, err)
	}
}

func TestConditionalTxnAllowsDirectSameRootPublish(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	state := d.State()
	if state == nil {
		t.Fatalf("missing DB state")
	}
	if err := d.ForceCommit(state.RootPageID); err != nil {
		t.Fatalf("same-root Commit: %v", err)
	}
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.CommitSync(); err != nil {
		t.Fatalf("txn CommitSync: %v", err)
	}
	assertDBValue(t, d, "target", "value")
}

func TestConditionalTxnStatsExposeOracleAndCleanup(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	stats := d.Stats()
	if got := stats["treedb.conditional_txn.active"]; got != "1" {
		t.Fatalf("active stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_txn.started_total"]; got != "1" {
		t.Fatalf("started stats=%q, want 1", got)
	}

	if err := d.SetSync([]byte("guard"), []byte("after")); err != nil {
		t.Fatalf("concurrent guard write: %v", err)
	}
	stats = d.Stats()
	if got := stats["treedb.conditional_oracle.retained_points"]; got != "1" {
		t.Fatalf("retained point stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_oracle.recorded_points_total"]; got != "1" {
		t.Fatalf("recorded point stats=%q, want 1", got)
	}

	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn Commit error=%v, want ErrConcurrentModification", err)
	}
	stats = d.Stats()
	if got := stats["treedb.conditional_txn.active"]; got != "0" {
		t.Fatalf("active stats after conflict=%q, want 0", got)
	}
	if got := stats["treedb.conditional_txn.closed_total"]; got != "1" {
		t.Fatalf("closed stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_txn.commit_attempts_total"]; got != "1" {
		t.Fatalf("commit attempts stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_txn.conflicts_total"]; got != "1" {
		t.Fatalf("conflict stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_txn.read_set.max"]; got != "1" {
		t.Fatalf("read-set max stats=%q, want 1", got)
	}
	if got := stats["treedb.conditional_oracle.retained_points"]; got != "0" {
		t.Fatalf("retained point stats after cleanup=%q, want 0", got)
	}
}

func TestConditionalTxnReserveReadSetDeduplicatesEarlyReads(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("value")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	defer tx.Close()

	tx.ReserveReadSet(conditionalTxnReadMapThreshold)
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("first GetVersioned: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("second GetVersioned: %v", err)
	}
	if got := len(tx.reads); got != 1 {
		t.Fatalf("read set size=%d, want 1", got)
	}
}

func TestConditionalTxnConflictDoesNotReserveCommandWALLSN(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	d := openCommandWALDB(t, dir)
	defer d.Close()

	if err := d.SetSync([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("seed guard: %v", err)
	}
	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	if err := d.SetSync([]byte("guard"), []byte("after")); err != nil {
		t.Fatalf("concurrent guard write: %v", err)
	}
	nextBeforeConflict := d.CommandWALNextLSN()
	if err := tx.Set([]byte("target"), []byte("value")); err != nil {
		t.Fatalf("txn Set target: %v", err)
	}
	if err := tx.CommitSync(); !errors.Is(err, ErrConcurrentModification) {
		t.Fatalf("txn CommitSync error=%v, want ErrConcurrentModification", err)
	}
	if nextAfterConflict := d.CommandWALNextLSN(); nextAfterConflict != nextBeforeConflict {
		t.Fatalf("CommandWALNextLSN changed on conflict: before=%d after=%d", nextBeforeConflict, nextAfterConflict)
	}
	if got, err := d.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("target after rejected command-WAL commit=(%q,%v), want missing", got, err)
	}
}

func TestConditionalTxnCommandWALReplayMatchesLiveRevisionContract(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	d := openCommandWALDB(t, dir)

	tx, err := d.NewConditionalTxn()
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if _, _, err := tx.GetVersioned([]byte("guard")); err != nil {
		_ = d.Close()
		t.Fatalf("txn GetVersioned guard: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("value")); err != nil {
		_ = d.Close()
		t.Fatalf("txn Set k: %v", err)
	}
	if err := tx.CommitSync(); err != nil {
		_ = d.Close()
		t.Fatalf("txn CommitSync: %v", err)
	}
	if got := d.Stats()["treedb.conditional_txn.command_wal_payloads_total"]; got != "1" {
		_ = d.Close()
		t.Fatalf("conditional command-WAL payload stats=%q, want 1", got)
	}

	value, revision, err := d.GetVersioned([]byte("k"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("GetVersioned live: %v", err)
	}
	if !bytes.Equal(value, []byte("value")) || revision == page.LegacyEntryRevision {
		_ = d.Close()
		t.Fatalf("live GetVersioned=(%q,%d), want value/non-legacy", value, revision)
	}

	r, err := commitlog.NewReader(filepath.Join(WALDirPath(dir), "commit-l0-000001.log"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("NewReader: %v", err)
	}
	env, err := r.ReadCommandFrame()
	if err != nil {
		_ = r.Close()
		_ = d.Close()
		t.Fatalf("ReadCommandFrame: %v", err)
	}
	var visits int
	var commandRevision uint64
	if err := commitlog.ScanRawKVBatchPayloadWithRevision(env.Payload, func(op commitlog.RawKVOp, key, value []byte, gotRevision uint64) error {
		visits++
		if op != commitlog.RawKVOpSet || !bytes.Equal(key, []byte("k")) || !bytes.Equal(value, []byte("value")) {
			return fmt.Errorf("command op=%v key=%q value=%q, want set k/value", op, key, value)
		}
		commandRevision = gotRevision
		return nil
	}); err != nil {
		_ = r.Close()
		_ = d.Close()
		t.Fatalf("ScanRawKVBatchPayloadWithRevision: %v", err)
	}
	if err := r.Close(); err != nil {
		_ = d.Close()
		t.Fatalf("reader Close: %v", err)
	}
	if visits != 1 || commandRevision != uint64(revision) {
		_ = d.Close()
		t.Fatalf("command visits=%d revision=%d, want 1/%d", visits, commandRevision, revision)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close before reopen: %v", err)
	}

	reopen := openCommandWALDB(t, dir)
	defer reopen.Close()
	value, reopenedRevision, err := reopen.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if !bytes.Equal(value, []byte("value")) || reopenedRevision != revision {
		t.Fatalf("reopen GetVersioned=(%q,%d), want value/%d", value, reopenedRevision, revision)
	}
}

func TestConditionalTxnCloseAndReuse(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	tx, err := d.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if err := tx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tx.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrConditionalTxnClosed) {
		t.Fatalf("Set after Close error=%v, want ErrConditionalTxnClosed", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrConditionalTxnClosed) {
		t.Fatalf("Commit after Close error=%v, want ErrConditionalTxnClosed", err)
	}
}

func buildConditionalTxnManualRoot(t *testing.T, d *DB, key, value string) uint64 {
	t.Helper()
	idx := d.idx.Load()
	if idx == nil {
		t.Fatalf("missing index")
	}
	d.mu.RLock()
	rootID := d.meta.UserRootPageID
	baseSeq := d.meta.CommitSeq
	regID := idx.registry.Register(baseSeq)
	d.mu.RUnlock()
	defer idx.registry.Unregister(regID)

	batch := d.NewBatch().(*Batch)
	if err := batch.Set([]byte(key), []byte(value)); err != nil {
		_ = batch.Close()
		t.Fatalf("manual root Set: %v", err)
	}
	newRoot, _, _, err := idx.zipper.Apply(rootID, batch.batch)
	if closeErr := batch.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("manual root Apply: %v", err)
	}
	if newRoot == rootID {
		t.Fatalf("manual root unchanged: %d", newRoot)
	}
	return newRoot
}
