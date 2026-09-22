package db

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

type templateKVForTest struct {
	db *DB
}

func (kv templateKVForTest) Get(key []byte) ([]byte, error) {
	if kv.db == nil {
		return nil, nil
	}
	return kv.db.Get(key)
}

func (kv templateKVForTest) SetSync(key, value []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.SetSync(key, value)
}

func (kv templateKVForTest) DeleteSync(key []byte) error {
	if kv.db == nil {
		return nil
	}
	return kv.db.DeleteSync(key)
}

func (kv templateKVForTest) NewBatch() templatedb.Batch {
	if kv.db == nil {
		return nil
	}
	return kv.db.NewBatch()
}

func TestOpenReadWrite_TemplateLookupDecodesExistingPointerValues(t *testing.T) {
	dir := t.TempDir()
	templateDir := filepath.Join(dir, "templatedb")
	if err := os.MkdirAll(templateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(templatedb): %v", err)
	}
	templateBackend, err := Open(Options{Dir: templateDir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open templatedb: %v", err)
	}
	templateStore := templatedb.New(templateKVForTest{db: templateBackend}, templatedb.Config{})

	def := templ.TemplateDef{
		Kind: templ.TemplateAnchors,
		Anchors: [][]byte{
			[]byte(`{"type":"account","id":"`),
			[]byte(`","status":"bonded","chain":"celestia"}`),
		},
	}
	defBytes, err := templ.EncodeTemplateDef(def, templ.Config{})
	if err != nil {
		_ = templateBackend.Close()
		t.Fatalf("EncodeTemplateDef: %v", err)
	}
	templateID, err := templateStore.PutTemplateDef(context.Background(), defBytes, nil)
	if err != nil {
		_ = templateBackend.Close()
		t.Fatalf("PutTemplateDef: %v", err)
	}
	if err := templateBackend.Close(); err != nil {
		t.Fatalf("close templatedb: %v", err)
	}

	mainDir := filepath.Join(dir, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(maindb): %v", err)
	}
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

	var lookupCalls atomic.Int64
	lookup := func(id uint64) ([]byte, error) {
		lookupCalls.Add(1)
		rb, err := Open(Options{Dir: templateDir, ReadOnly: true, DisableBackgroundPrune: true})
		if err != nil {
			return nil, err
		}
		defer rb.Close()
		store := templatedb.New(templateKVForTest{db: rb}, templatedb.Config{})
		return store.GetTemplateDef(context.Background(), id)
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
	want := []byte(`{"type":"account","id":"acct-000000","status":"bonded","chain":"celestia"}`)
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
