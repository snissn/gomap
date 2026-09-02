package kvstoreadapter

import (
	"bytes"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

var errDownstreamConflict = errors.New("downstream adapter: conflict")

type conditionalProofAdapter struct {
	db *treedb.DB
}

func (a conditionalProofAdapter) readCacheToken(key []byte) ([]byte, treedb.EntryRevision, error) {
	return a.db.GetVersioned(key)
}

func (a conditionalProofAdapter) mapCommitError(err error) error {
	if errors.Is(err, treedb.ErrConcurrentModification) {
		return errDownstreamConflict
	}
	return err
}

func TestAdapterReadinessUsesNativeRevisionsAndConditionalConflicts(t *testing.T) {
	t.Parallel()

	opened, err := Open(OpenConfig{
		ParentDir:      t.TempDir(),
		Name:           "application",
		AdapterName:    "TreeDB Conditional Proof",
		DefaultProfile: treedb.ProfileNoWALFast,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = opened.DB.Close() }()
	if opened.Options.CommandWAL {
		t.Fatalf("profile opened command WAL; current public cached command-WAL conditionals should be tested separately")
	}

	proof := conditionalProofAdapter{db: opened.DB}
	if err := opened.KV.Set([]byte("guard"), []byte("before")); err != nil {
		t.Fatalf("adapter Set guard: %v", err)
	}
	value, token, err := proof.readCacheToken([]byte("guard"))
	if err != nil {
		t.Fatalf("readCacheToken: %v", err)
	}
	if !bytes.Equal(value, []byte("before")) || token == treedb.LegacyEntryRevision {
		t.Fatalf("readCacheToken=(%q,%d), want before/non-legacy token", value, token)
	}

	tx, err := opened.DB.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn: %v", err)
	}
	if got, gotToken, err := tx.GetVersioned([]byte("guard")); err != nil {
		_ = tx.Close()
		t.Fatalf("tx.GetVersioned guard: %v", err)
	} else if !bytes.Equal(got, value) || gotToken != token {
		_ = tx.Close()
		t.Fatalf("tx.GetVersioned=(%q,%d), want cached token (%q,%d)", got, gotToken, value, token)
	}
	if err := tx.Set([]byte("guard"), []byte("after")); err != nil {
		_ = tx.Close()
		t.Fatalf("tx.Set guard: %v", err)
	}
	if err := proof.mapCommitError(tx.Commit()); err != nil {
		t.Fatalf("tx.Commit mapped error: %v", err)
	}
	after, afterToken, err := proof.readCacheToken([]byte("guard"))
	if err != nil {
		t.Fatalf("readCacheToken after commit: %v", err)
	}
	if !bytes.Equal(after, []byte("after")) || afterToken <= token {
		t.Fatalf("after token=(%q,%d), want after with newer token than %d", after, afterToken, token)
	}

	conflictTx, err := opened.DB.NewConditionalTxn()
	if err != nil {
		t.Fatalf("NewConditionalTxn conflict: %v", err)
	}
	if _, _, err := conflictTx.GetVersioned([]byte("guard")); err != nil {
		_ = conflictTx.Close()
		t.Fatalf("conflict tx GetVersioned: %v", err)
	}
	if err := opened.KV.Set([]byte("guard"), []byte("outside")); err != nil {
		_ = conflictTx.Close()
		t.Fatalf("adapter outside Set: %v", err)
	}
	if err := conflictTx.Set([]byte("target"), []byte("inside")); err != nil {
		_ = conflictTx.Close()
		t.Fatalf("conflict tx Set target: %v", err)
	}
	if err := proof.mapCommitError(conflictTx.Commit()); !errors.Is(err, errDownstreamConflict) {
		t.Fatalf("mapped conflict error=%v, want errDownstreamConflict", err)
	}
	if got, err := opened.KV.Get([]byte("target")); err != nil || got != nil {
		t.Fatalf("target after rejected conflict=(%q,%v), want missing", got, err)
	}
}

func TestAdapterReadinessCommandWALReopenPreservesRevisionAndFailsClosedConditional(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	opened, err := Open(OpenConfig{
		ParentDir:         parent,
		Name:              "application",
		DefaultProfile:    treedb.ProfileCommandWALRelaxed,
		DefaultKeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("Open command WAL adapter: %v", err)
	}
	if got := opened.DB.Stats()["treedb.write_path.mode"]; got != "command_wal_cached" {
		_ = opened.DB.Close()
		t.Fatalf("write_path.mode=%q, want command_wal_cached", got)
	}
	if err := opened.KV.Set([]byte("k"), []byte("v1")); err != nil {
		_ = opened.DB.Close()
		t.Fatalf("adapter Set command WAL: %v", err)
	}
	value, revision, err := opened.DB.GetVersioned([]byte("k"))
	if err != nil {
		_ = opened.DB.Close()
		t.Fatalf("GetVersioned command WAL: %v", err)
	}
	if !bytes.Equal(value, []byte("v1")) || revision == treedb.LegacyEntryRevision {
		_ = opened.DB.Close()
		t.Fatalf("command WAL token=(%q,%d), want v1/non-legacy", value, revision)
	}
	if _, err := opened.DB.NewConditionalTxn(); !errors.Is(err, treedb.ErrConditionalTxnUnsupported) {
		_ = opened.DB.Close()
		t.Fatalf("command WAL NewConditionalTxn error=%v, want ErrConditionalTxnUnsupported", err)
	}
	if err := opened.DB.Close(); err != nil {
		t.Fatalf("Close command WAL adapter: %v", err)
	}

	reopened, err := Open(OpenConfig{
		ParentDir:         parent,
		Name:              "application",
		DefaultProfile:    treedb.ProfileCommandWALRelaxed,
		DefaultKeepRecent: 1,
	})
	if err != nil {
		t.Fatalf("reopen command WAL adapter: %v", err)
	}
	defer func() { _ = reopened.DB.Close() }()
	reopenedValue, reopenedRevision, err := reopened.DB.GetVersioned([]byte("k"))
	if err != nil {
		t.Fatalf("reopen GetVersioned: %v", err)
	}
	if !bytes.Equal(reopenedValue, value) || reopenedRevision != revision {
		t.Fatalf("reopen token=(%q,%d), want (%q,%d)", reopenedValue, reopenedRevision, value, revision)
	}
}
