package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestParseConfigSecondaryIndexesUpTo32AndExtraFields(t *testing.T) {
	cfg, err := parseConfig([]string{"-secondary-indexes", "32", "-doc-extra-index-fields", "32"})
	if err != nil {
		t.Fatalf("parseConfig(32): %v", err)
	}
	if cfg.SecondaryIndexes != 32 || cfg.DocExtraIndexFields != 32 {
		t.Fatalf("got SecondaryIndexes=%d DocExtraIndexFields=%d want 32,32", cfg.SecondaryIndexes, cfg.DocExtraIndexFields)
	}
	if effectiveDocExtraIndexFields(cfg) != 32 {
		t.Fatalf("effectiveDocExtraIndexFields=%d want 32", effectiveDocExtraIndexFields(cfg))
	}
	if _, err := parseConfig([]string{"-secondary-indexes", "33"}); err == nil {
		t.Fatalf("expected error for secondary-indexes=33")
	}
	if _, err := parseConfig([]string{"-doc-extra-index-fields", "33"}); err == nil {
		t.Fatalf("expected error for doc-extra-index-fields=33")
	}
	if _, err := parseConfig([]string{"-document-shape", "ycsb", "-doc-extra-index-fields", "8"}); err == nil {
		t.Fatalf("expected error for ycsb with doc-extra-index-fields")
	}

	// Default 0..3 behavior stays unchanged when DocExtraIndexFields == 0.
	baseDoc := benchmarkGatewayDocument(7)
	extra3Doc := benchmarkGatewayDocumentWithExtraFields(7, 3)
	if len(baseDoc) != len(extra3Doc) {
		t.Fatalf("len(baseDoc)=%d != len(extra3Doc)=%d", len(baseDoc), len(extra3Doc))
	}
	extra32Doc := benchmarkGatewayDocumentWithExtraFields(7, 32)
	if len(extra32Doc) != len(baseDoc)+29 {
		t.Fatalf("len(extra32Doc)=%d want %d", len(extra32Doc), len(baseDoc)+29)
	}

	// Index builders produce exact counts for 32 secondary indexes + range index.
	if got := len(treedbCreateIndexDocs(32, true)); got != 33 {
		t.Fatalf("treedbCreateIndexDocs(32,true) len=%d want 33", got)
	}
	if got := len(directTreeDBIndexDefinitions(config{SecondaryIndexes: 32, RangeIndex: true})); got != 33 {
		t.Fatalf("directTreeDBIndexDefinitions(32,true) len=%d want 33", got)
	}
	if got := len(nativeWireBenchmarkIndexes(config{SecondaryIndexes: 32, RangeIndex: true})); got != 33 {
		t.Fatalf("nativeWireBenchmarkIndexes(32,true) len=%d want 33", got)
	}
}

func TestBuildRawInsertCommandBatchParity(t *testing.T) {
	a, err := rawInsertCommandWithWriteConcern("docs", 0, 16, documentShapeGateway, nil, nil, "w1")
	if err != nil {
		t.Fatalf("rawInsertCommandWithWriteConcern: %v", err)
	}
	b, err := buildRawInsertCommandBatch("docs", 0, 16, documentShapeGateway, 0, "w1")
	if err != nil {
		t.Fatalf("buildRawInsertCommandBatch: %v", err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("prebuilt raw insert command bytes differ from dynamic command")
	}
}

func TestRunBenchmarkTreeDB32SecondaryIndexesPrebuiltRaw(t *testing.T) {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	cfg, err := parseConfig([]string{
		"-target", "treedb",
		"-treedb-dir", dir,
		"-treedb-profile", "no_wal_fast",
		"-treedb-document-format", "bson",
		"-client-mode", "driver-command-raw",
		"-prebuild-documents",
		"-documents", "64",
		"-batch-size", "32",
		"-insert-producers", "2",
		"-secondary-indexes", "32",
		"-doc-extra-index-fields", "32",
		"-reads", "8",
		"-range-reads", "0",
		"-updates", "0",
		"-deletes", "0",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	target, err := openTarget(ctx, cfg)
	if err != nil {
		t.Fatalf("openTarget: %v", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if err := closeBenchTarget(cleanupCtx, target); err != nil {
			t.Errorf("closeBenchTarget: %v", err)
		}
	}()
	res, err := runBenchmark(ctx, cfg, target, nil)
	if err != nil {
		t.Fatalf("runBenchmark(32 indexes): %v", err)
	}
	if res.SecondaryIndexes != 32 || res.DocExtraIndexFields != 32 {
		t.Fatalf("unexpected result metadata: %+v", res)
	}
	if len(res.Phases) == 0 || res.Phases[0].Operations != 64 {
		t.Fatalf("unexpected load phase: %+v", res.Phases)
	}
}
