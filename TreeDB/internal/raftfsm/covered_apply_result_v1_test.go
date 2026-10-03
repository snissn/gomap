package raftfsm

import (
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"path/filepath"
	"testing"
)

func TestLookupCoveredApplyResultV1ReadsWithoutInventingProgress(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db := openFSMTestDB(t, dir)
	fsm := openFSMForTest(t, db, dir)
	defer func() { _ = fsm.Close(); _ = db.Close() }()
	id := raftentry.ApplyEntryID{Term: 2, Index: 1}
	before := db.State().AppliedCommandLSN
	if _, ok, err := fsm.LookupCoveredApplyResultV1(id); err != nil || ok {
		t.Fatalf("absent lookup: ok=%v err=%v", ok, err)
	}
	if db.State().AppliedCommandLSN != before || fsm.results.Len() != 0 || fsm.progress.Len() != 0 {
		t.Fatal("read-only lookup invented coverage")
	}
	entry := committedCommand(id.Term, id.Index, deterministicCreateCollectionEntry(t, "users", "lookup-covered-create"))
	result, err := fsm.ApplyCommittedEntryV1(entry)
	if err != nil {
		t.Fatal(err)
	}
	record, ok, err := fsm.LookupCoveredApplyResultV1(id)
	if err != nil || !ok || record.EntryID != id || record.Result != result || record.CommandDigest != result.CommandDigest || record.AppliedCommandLSN == 0 {
		t.Fatalf("covered result=%+v ok=%v err=%v", record, ok, err)
	}
}
func TestLookupCoveredApplyResultV1RootVisibleResultMissingRefuses(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db := openFSMTestDB(t, dir)
	fsm := openFSMForTest(t, db, dir)
	defer func() { _ = fsm.Close(); _ = db.Close() }()
	entry := committedCommand(2, 1, deterministicCreateCollectionEntry(t, "users", "lookup-root-visible-missing"))
	if _, err := applyCommittedEntryWithFaultForTest(t, fsm, entry, raftapply.FaultAfterVisibleBeforeResultRecordV1); err == nil {
		t.Fatal("missed real executor cut")
	}
	lsn := db.State().AppliedCommandLSN
	if lsn == 0 {
		t.Fatal("fault did not publish the command root")
	}
	if _, ok, err := fsm.LookupCoveredApplyResultV1(raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}); err != nil || ok {
		t.Fatalf("missing result was invented: ok=%v err=%v", ok, err)
	}
	if db.State().AppliedCommandLSN != lsn || fsm.results.Len() != 0 || fsm.progress.Len() != 0 {
		t.Fatal("lookup repaired an unsupported recovery cut")
	}
}
