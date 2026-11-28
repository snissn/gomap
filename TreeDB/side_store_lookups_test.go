package treedb

import (
	"context"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/templatedb"
)

func TestOpenBackend_ReadOnlyResolvesInMainDBDictAndTemplateWithoutWrites(t *testing.T) {
	root := t.TempDir()
	opened, err := Open(Options{Dir: root})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	dictStore := dictdb.New(opened.backend)
	dictID, err := dictStore.PutDictBytes(context.Background(), []byte("maintenance dictionary"))
	if err != nil {
		_ = opened.Close()
		t.Fatal(err)
	}
	templateStore := templatedb.NewBackend(opened.backend, templatedb.Config{})
	templateID, err := templateStore.PutTemplateDef(context.Background(), []byte("maintenance template"), nil)
	if err != nil {
		_ = opened.Close()
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ro, cleanup, err := OpenBackend(Options{Dir: root, ReadOnly: true})
	if err != nil {
		t.Fatalf("OpenBackend(ReadOnly): %v", err)
	}
	defer func() { _ = cleanup() }()

	gotDict, err := ro.LookupSystemDictBytes(dictID)
	if err != nil || string(gotDict) != "maintenance dictionary" {
		t.Fatalf("read-only dictionary lookup: got=%q err=%v", gotDict, err)
	}
	gotTemplate, err := ro.LookupSystemTemplateDef(templateID)
	if err != nil || string(gotTemplate) != "maintenance template" {
		t.Fatalf("read-only template lookup: got=%q err=%v", gotTemplate, err)
	}
	roDictStore := dictdb.New(ro)
	if _, err := roDictStore.PutDictBytes(context.Background(), []byte("blocked write")); err == nil {
		t.Fatal("expected read-only PutDictBytes to fail")
	}
	roTemplateStore := templatedb.NewBackend(ro, templatedb.Config{})
	if _, err := roTemplateStore.PutTemplateDef(context.Background(), []byte("blocked template"), nil); err == nil {
		t.Fatal("expected read-only PutTemplateDef to fail")
	}
}

func TestWireSideStoreLookups_IsNoOp(t *testing.T) {
	root := t.TempDir()
	opts := &Options{CommandWAL: true, ReadOnly: true}
	cleanup, err := wireSideStoreLookups(root, opts)
	if err != nil {
		t.Fatalf("wireSideStoreLookups: %v", err)
	}
	defer func() { _ = cleanup() }()
	if !opts.CommandWAL {
		t.Fatal("wireSideStoreLookups cleared main DB CommandWAL option")
	}
	if opts.PhysicalSnapshotSideStoreCapture != nil {
		t.Fatal("expected PhysicalSnapshotSideStoreCapture to remain nil with in-maindb system storage")
	}
}
