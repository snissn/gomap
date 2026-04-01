package main

import (
	"bytes"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestCollectValueLogAttribution_DirectPointersOnly(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:        dir,
		Durability: treedb.DurabilityWALOffRelaxed,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 128; i++ {
		key := []byte(fmt.Sprintf("direct-%03d", i))
		val := bytes.Repeat([]byte{byte(i)}, 96)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report, err := collectValueLogAttribution(dir, 8)
	if err != nil {
		t.Fatalf("collectValueLogAttribution: %v", err)
	}
	if report.SegmentsOnDisk == 0 || report.BytesOnDisk == 0 {
		t.Fatalf("expected on-disk value-log segments, got segments=%d bytes=%d", report.SegmentsOnDisk, report.BytesOnDisk)
	}
	direct := collectClassReportByName(report.Classes, string(valueLogAttributionDirectPointer))
	if direct == nil || direct.StoredBytes == 0 || direct.Refs == 0 {
		t.Fatalf("expected direct pointer attribution, got %+v", direct)
	}
	if outer := collectClassReportByName(report.Classes, string(valueLogAttributionOuterLeaf)); outer != nil && outer.StoredBytes != 0 {
		t.Fatalf("unexpected outer-leaf attribution: %+v", outer)
	}
	if nested := collectClassReportByName(report.Classes, string(valueLogAttributionNestedOuterLeafPointer)); nested != nil && nested.StoredBytes != 0 {
		t.Fatalf("unexpected nested outer-leaf attribution: %+v", nested)
	}
}

func TestCollectValueLogAttribution_OuterLeafAndNestedPointers(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:                        dir,
		Durability:                 treedb.DurabilityWALOffRelaxed,
		IndexOuterLeavesInValueLog: true,
		ValueLog: treedb.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	for i := 0; i < 2048; i++ {
		key := []byte(fmt.Sprintf("leaf-%05d", i))
		val := bytes.Repeat([]byte(fmt.Sprintf("v%04d|", i%97)), 24)
		if err := db.Set(key, val); err != nil {
			_ = db.Close()
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report, err := collectValueLogAttribution(dir, 8)
	if err != nil {
		t.Fatalf("collectValueLogAttribution: %v", err)
	}
	if report.ReferencedStoredBytes == 0 || report.ReferencedFiles == 0 {
		t.Fatalf("expected referenced value-log bytes, got stored=%d files=%d", report.ReferencedStoredBytes, report.ReferencedFiles)
	}
	outer := collectClassReportByName(report.Classes, string(valueLogAttributionOuterLeaf))
	if outer == nil || outer.StoredBytes == 0 || outer.Refs == 0 {
		t.Fatalf("expected outer-leaf attribution, got %+v", outer)
	}
	nested := collectClassReportByName(report.Classes, string(valueLogAttributionNestedOuterLeafPointer))
	if nested == nil || nested.StoredBytes == 0 || nested.Refs == 0 {
		t.Fatalf("expected nested outer-leaf pointer attribution, got %+v", nested)
	}
	if file := collectFileReportByPath(report.Files, "value-l0-"); file == nil {
		t.Fatalf("expected top file breakdown to include lane-0 value-log path: %+v", report.Files)
	}
}
