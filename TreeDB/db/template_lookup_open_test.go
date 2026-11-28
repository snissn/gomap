package db

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestOpenReadWrite_TemplateLookupDecodesExistingPointerValues(t *testing.T) {
	dir := t.TempDir()
	mainDir := filepath.Join(dir, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(maindb): %v", err)
	}

	def := templ.TemplateDef{
		Kind: templ.TemplateAnchors,
		Anchors: [][]byte{
			[]byte(`{"type":"account","id":"`),
			[]byte(`","status":"bonded","chain":"celestia"}`),
		},
	}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	const templateID uint64 = 42

	writer, err := Open(Options{
		Dir: mainDir,
		ValueLog: ValueLogOptions{
			ForcePointers:    true,
			PointerThreshold: 1,
			Compression:      ValueLogCompressionOff,
		},
	})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	inlineDef := make([]byte, 1+len(defBytes))
	inlineDef[0] = SystemDictEncodingInline
	copy(inlineDef[1:], defBytes)
	if err := writer.ApplySystemOps([]SystemKVOp{{
		Key:   SystemTemplateDefKey(templateID),
		Value: inlineDef,
	}}, true); err != nil {
		_ = writer.Close()
		t.Fatalf("ApplySystemOps(template): %v", err)
	}

	ptrs := appendPointersInNewSegment(t, mainDir, 0, 1, 1, 1, func(int) []byte {
		payload, err := templ.EncodePayload(templateID, [][]byte{
			nil,
			[]byte("acct-000000"),
			nil,
		})
		if err != nil {
			t.Fatalf("EncodePayload: %v", err)
		}
		return payload
	})
	batch := writer.NewBatch()
	ptrBatch, ok := any(batch).(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		_ = batch.Close()
		_ = writer.Close()
		t.Fatalf("missing SetPointer")
	}
	if err := ptrBatch.SetPointer([]byte("acct/000000"), ptrs[0]); err != nil {
		_ = batch.Close()
		_ = writer.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		_ = writer.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	_ = batch.Close()
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	want := []byte(`{"type":"account","id":"acct-000000","status":"bonded","chain":"celestia"}`)

	// First verify default SystemRootPageID template lookup works with no external lookup wired.
	defaultDB, err := Open(Options{
		Dir:      mainDir,
		ReadOnly: false,
	})
	if err != nil {
		t.Fatalf("open rw default lookup: %v", err)
	}
	gotDefault, err := defaultDB.Get([]byte("acct/000000"))
	if err != nil {
		_ = defaultDB.Close()
		t.Fatalf("defaultDB.Get: %v", err)
	}
	if string(gotDefault) != string(want) {
		_ = defaultDB.Close()
		t.Fatalf("defaultDB.Get mismatch: got=%q want=%q", gotDefault, want)
	}
	if err := defaultDB.Close(); err != nil {
		t.Fatalf("close defaultDB: %v", err)
	}

	// Next verify an explicit TemplateLookup override still works on reopen.
	var lookupCalls atomic.Int64
	lookup := func(id uint64) ([]byte, error) {
		lookupCalls.Add(1)
		if id == templateID {
			return append([]byte(nil), defBytes...), nil
		}
		return nil, templ.ErrMissingTemplate
	}

	db, err := Open(Options{
		Dir:      mainDir,
		ReadOnly: false,
		ValueLog: ValueLogOptions{
			TemplateLookup: lookup,
		},
	})
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer func() { _ = db.Close() }()
	lookupCalls.Store(0)

	managerGot, err := db.valueLogManager.Read(ptrs[0])
	if err != nil {
		t.Fatalf("manager.Read: %v", err)
	}
	if string(managerGot) != string(want) {
		before := append([]byte(nil), managerGot...)
		db.valueLogManager.SetTemplateLookup(lookup, templ.DecodeOptions{})
		managerGot, err = db.valueLogManager.Read(ptrs[0])
		if err != nil {
			t.Fatalf("manager.Read after reapply: %v", err)
		}
		t.Fatalf("manager.Read mismatch before reapply: calls=%d got=%q after=%q want=%q", lookupCalls.Load(), before, managerGot, want)
	}

	got, err := db.Get([]byte("acct/000000"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Get mismatch: got=%q want=%q", got, want)
	}
}
