package treedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

func TestValueLogRewriteOfflineRejectsTemplateActivationBeforeSideStoreOpen(t *testing.T) {
	root := t.TempDir()
	database, err := Open(OptionsFor(ProfileNoWALFast, root))
	if err != nil {
		t.Fatalf("open main database: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close main database: %v", err)
	}

	templateDir := filepath.Join(root, "templatedb")
	templateBackend, err := treedbdb.Open(treedbdb.Options{Dir: templateDir})
	if err != nil {
		t.Fatalf("hold templatedb open: %v", err)
	}
	defer templateBackend.Close()

	opts := OptionsFor(ProfileNoWALFast, root)
	opts.IgnoreFormatConfig = true
	opts.ValueLog.TemplateMode = templ.TemplateOnly
	_, err = ValueLogRewriteOffline(opts)
	if !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("offline template rewrite error=%v want unresolved authority before side-store open", err)
	}
	if err := templateBackend.SetSync([]byte("still-open"), []byte("yes")); err != nil {
		t.Fatalf("rejected rewrite disturbed held templatedb: %v", err)
	}
}

func TestVacuumIndexOffline_WithTemplateFrames_WiresTemplateLookup(t *testing.T) {
	dir := t.TempDir()

	templateDir := filepath.Join(dir, "templatedb")
	if err := os.MkdirAll(templateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(templatedb): %v", err)
	}
	templateBackend, err := treedbdb.Open(treedbdb.Options{
		Dir:                    templateDir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open templatedb backend: %v", err)
	}
	templateStore := templatedb.New(templateBackendKV{db: templateBackend}, templatedb.Config{})

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
		t.Fatalf("close templatedb backend: %v", err)
	}

	plainValue := func(id int) []byte {
		return []byte(fmt.Sprintf(`{"type":"account","id":"acct-%06d","status":"bonded","chain":"celestia"}`, id))
	}
	encodedValue := func(id int) []byte {
		payload, err := templ.EncodePayload(templateID, [][]byte{
			nil,
			[]byte(fmt.Sprintf("acct-%06d", id)),
			nil,
		})
		if err != nil {
			t.Fatalf("EncodePayload: %v", err)
		}
		return payload
	}

	mainDir := filepath.Join(dir, "maindb")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(maindb): %v", err)
	}
	writerOpts := OptionsFor(ProfileNoWALFast, mainDir)
	writerOpts.IndexOuterLeavesInValueLog = false
	writerOpts.ValueLog.ForcePointers = true
	writerOpts.ValueLog.PointerThreshold = 1
	writerOpts.ValueLog.Compression = treedbdb.ValueLogCompressionOff
	writer, err := treedbdb.Open(writerOpts)
	if err != nil {
		t.Fatalf("Open(writer): %v", err)
	}
	valueLogDir := filepath.Join(mainDir, "value_vlog")
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		_ = writer.Close()
		t.Fatalf("MkdirAll(value_vlog): %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("EncodeFileID: %v", err)
	}
	vlogWriter, err := valuelog.NewWriter(filepath.Join(valueLogDir, "value-l0-000001.log"), fileID)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("NewWriter: %v", err)
	}

	ptrs := make([]page.ValuePtr, 0, 32)
	for i := 0; i < 32; i++ {
		ptr, err := vlogWriter.Append(0, nil, uint64(i+1), encodedValue(i))
		if err != nil {
			_ = vlogWriter.Close()
			_ = writer.Close()
			t.Fatalf("Append(%d): %v", i, err)
		}
		ptrs = append(ptrs, ptr)
	}
	if err := vlogWriter.Close(); err != nil {
		_ = writer.Close()
		t.Fatalf("vlogWriter.Close: %v", err)
	}
	if err := writer.RegisterValueLogSegment(filepath.Join(valueLogDir, "value-l0-000001.log"), fileID); err != nil {
		_ = writer.Close()
		t.Fatalf("register value-log producer segment: %v", err)
	}

	batch := writer.NewBatch()
	ptrBatch, ok := any(batch).(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		_ = batch.Close()
		_ = writer.Close()
		t.Fatalf("writer.NewBatch missing SetPointer")
	}
	for i := range ptrs {
		if err := ptrBatch.SetPointer([]byte(fmt.Sprintf("acct/%06d", i)), ptrs[i]); err != nil {
			_ = batch.Close()
			_ = writer.Close()
			t.Fatalf("batch.SetPointer: %v", err)
		}
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		_ = writer.Close()
		t.Fatalf("batch.WriteSync: %v", err)
	}
	if err := batch.Close(); err != nil {
		_ = writer.Close()
		t.Fatalf("batch.Close: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	verifyDecoded := func(stage string) {
		reader, cleanup, err := OpenBackend(OptionsFor(ProfileNoWALFast, dir))
		if err != nil {
			t.Fatalf("OpenBackend(%s): %v", stage, err)
		}
		defer func() { _ = cleanup() }()

		for i := 0; i < 32; i++ {
			key := []byte(fmt.Sprintf("acct/%06d", i))
			got, err := reader.Get(key)
			if err != nil {
				t.Fatalf("%s Get(%q): %v", stage, key, err)
			}
			want := plainValue(i)
			if string(got) != string(want) {
				t.Fatalf("%s Get(%q) mismatch: got=%q want=%q", stage, key, got, want)
			}
		}
	}

	verifyDecoded("before-vacuum")

	if err := VacuumIndexOffline(OptionsFor(ProfileNoWALFast, dir)); err != nil {
		t.Fatalf("VacuumIndexOffline: %v", err)
	}

	verifyDecoded("after-vacuum")
}
