package caching

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func finiteLeafTestPage(t *testing.T) []byte {
	t.Helper()
	data := make([]byte, page.PageSize)
	b := node.NewBuilder(data, page.PageTypeLeaf)
	if err := b.AddLeafEntry([]byte("a"), []byte("owned"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	b.FinishNoNode()
	b.ReleaseScratch()
	return data
}
func finiteLeafTestWorkspace(t *testing.T, reserve func(uint64) error) (*PreparedFiniteLeafWorkspace, *DB) {
	t.Helper()
	db := &DB{closeCh: make(chan struct{}), indexOuterLeavesInValueLog: true}
	db.leafLog.id = leafLogLaneID
	owner, err := NewPreparedFiniteLeafWorkspace(&cachingLeafPageLog{db: db, lane: &db.leafLog}, 3, 2, reserve)
	if err != nil {
		t.Fatal(err)
	}
	return owner, db
}
func TestPreparedFiniteLeafWorkspaceOwnsCompactBatchAndGlobalCredit(t *testing.T) {
	var charged uint64
	owner, db := finiteLeafTestWorkspace(t, func(n uint64) error { charged += n; return nil })
	input := finiteLeafTestPage(t)
	records, err := owner.preparePages([][]byte{input, input})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].RID != 0 || records[1].RID != 0 {
		t.Fatal("identity assigned by scratch preparation")
	}
	first := bytes.Clone(records[0].Value)
	// A compact result lives in request-owned scratch, independent of source page.
	if len(records[0].Value) >= len(input) {
		t.Fatal("fixture did not compact")
	}
	clear(input)
	if !bytes.Equal(records[0].Value, first) {
		t.Fatal("compact result aliases caller")
	}
	if err = owner.Close(); err == nil {
		t.Fatal("closed borrowed compact values")
	}
	if err = owner.reservePreparedRIDs(7); err != nil {
		t.Fatal(err)
	}
	if records[0].RID != 7 || records[1].RID != 8 {
		t.Fatal("batch RID order")
	}
	if err = owner.reservePreparedRIDs(9); err == nil {
		t.Fatal("duplicate RID assignment")
	}
	owner.releasePreparedPages()
	for _, r := range owner.records[:cap(owner.records)] {
		if r.Value != nil || r.RID != 0 {
			t.Fatal("retained borrowed record alias")
		}
	}
	amount := charged
	if _, err = owner.preparePages([][]byte{finiteLeafTestPage(t)}); err != nil {
		t.Fatal(err)
	}
	if charged != amount {
		t.Fatal("same owned compact slot copied/allocated again")
	}
	if err = owner.reservePreparedRIDs(9); err != nil {
		t.Fatal(err)
	}
	owner.releasePreparedPages()
	if _, err = owner.preparePages([][]byte{finiteLeafTestPage(t)}); err == nil {
		t.Fatal("per-batch reset bypassed global output credit")
	}
	if err = owner.validate(db, &lane{id: leafLogLaneID}); err == nil {
		t.Fatal("foreign lane accepted")
	}
	if charged != owner.BackingBytes() {
		t.Fatal("backing ledger mismatch")
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.preparePages(nil); err == nil {
		t.Fatal("closed owner reused")
	}
}
func TestPreparedFiniteLeafWorkspaceCreditBeforeScratch(t *testing.T) {
	reject := false
	denied := errors.New("denied")
	owner, _ := finiteLeafTestWorkspace(t, func(uint64) error {
		if reject {
			return denied
		}
		return nil
	})
	reject = true
	if _, err := owner.preparePages([][]byte{finiteLeafTestPage(t)}); !errors.Is(err, denied) {
		t.Fatalf("refusal: %v", err)
	}
	if cap(owner.records) != 0 || cap(owner.compact) != 0 || owner.active || owner.used != 0 {
		t.Fatal("scratch allocation/credit spend preceded admission")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedFiniteLeafWorkspaceUsesExistingDefaultAppendPaths(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	var debit uint64
	owner, err := NewPreparedFiniteLeafWorkspace(captured.leafLog, 3, 2, func(n uint64) error { debit += n; return nil })
	if err != nil {
		t.Fatal(err)
	}
	input := finiteLeafTestPage(t)
	first, err := owner.AppendLeafPage(input)
	if err != nil {
		t.Fatal(err)
	}
	if owner.lane.finiteWorkspace != nil {
		t.Fatal("writer loan escaped vlogMu")
	}
	got, err := db.ReadValueLogRecord(first.ValuePtr())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, input) {
		t.Fatal("owned scalar append changed public leaf bytes")
	}
	batch, err := owner.AppendLeafPages([][]byte{input, input})
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 || batch[0] == batch[1] {
		t.Fatal("batch grouping lost physical pointers")
	}
	for _, p := range batch {
		got, err := db.ReadValueLogRecord(p.ValuePtr())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, input) {
			t.Fatal("owned batch append changed public leaf bytes")
		}
	}
	for _, rec := range owner.records[:cap(owner.records)] {
		if rec.Value != nil {
			t.Fatal("append retained borrowed record aliases")
		}
	}
	if owner.lane.finiteWorkspace != nil || debit != owner.BackingBytes() {
		t.Fatal("loan/credit lifetime")
	}
	if _, err = owner.AppendLeafPage(input); err == nil {
		t.Fatal("per-append reset bypassed global output credit")
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPreparedFiniteLeafWorkspaceRejectsReboundInstalledOwner(t *testing.T) {
	owner, db := finiteLeafTestWorkspace(t, func(uint64) error { return nil })
	installed := owner.installed.(*cachingLeafPageLog)
	bound := installed.lane
	installed.lane = &lane{}
	if err := owner.validate(db, bound); err == nil {
		t.Fatal("rebound installed lane admitted")
	}
	if err := owner.Flush(); err == nil {
		t.Fatal("flush through rebound installed lane")
	}
	installed.lane = bound
	installed.db = &DB{closeCh: make(chan struct{})}
	if err := owner.validate(db, bound); err == nil {
		t.Fatal("rebound DB admitted")
	}
	installed.db = db
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}
