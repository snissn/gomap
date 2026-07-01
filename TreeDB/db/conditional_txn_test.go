package db

import (
	"bytes"
	"errors"
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
			t.Fatalf("command op=%v key=%q value=%q, want set k/value", op, key, value)
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
