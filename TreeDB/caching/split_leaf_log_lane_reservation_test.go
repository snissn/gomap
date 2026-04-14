package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestOpen_Lane255ValueSegmentsRemainRegularWhenLeafLogDisabled(t *testing.T) {
	dir := t.TempDir()
	valueDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(value_vlog): %v", err)
	}
	fileID, err := valuelog.EncodeFileID(leafLogLaneID, 7)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	w, err := valuelog.NewWriter(filepath.Join(valueDir, "value-l255-000007.log"), fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("v"), 64)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err := Open(dir, NewMockBackend(), Options{JournalLanes: leafLogLaneID + 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got := db.lanes[leafLogLaneID].vlogSeq; got <= 7 {
		t.Fatalf("lane 255 vlogSeq=%d want >7 after recovering existing regular lane-255 segments", got)
	}
	if db.leafLog.vlogSeq != 0 {
		t.Fatalf("leaf log unexpectedly reserved seq=%d when leaf-log mode disabled", db.leafLog.vlogSeq)
	}
}

func TestOpen_RejectsLeafLogLaneConflictWithJournalLanes(t *testing.T) {
	_, err := Open(t.TempDir(), NewMockBackend(), Options{JournalLanes: leafLogLaneID + 1, IndexOuterLeavesInValueLog: true})
	if err == nil {
		t.Fatal("expected JournalLanes/leaf_vlog lane conflict error")
	}
	if !strings.Contains(err.Error(), "IndexOuterLeavesInValueLog reserves lane 255") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValueLogDirForLane_UsesLeafDirOnlyForReservedLeafLog(t *testing.T) {
	db := &DB{
		valueLogDir: "value_vlog",
		leafLogDir:  "leaf_vlog",
	}
	regularLane255 := &lane{id: leafLogLaneID}
	if got := db.valueLogDirForLane(regularLane255); got != db.valueLogDir {
		t.Fatalf("valueLogDirForLane regular lane255=%q want %q when leaf-log mode disabled", got, db.valueLogDir)
	}

	db.indexOuterLeavesInValueLog = true
	if got := db.valueLogDirForLane(regularLane255); got != db.valueLogDir {
		t.Fatalf("valueLogDirForLane regular lane255=%q want %q", got, db.valueLogDir)
	}
	db.leafLog.id = leafLogLaneID
	if got := db.valueLogDirForLane(&db.leafLog); got != db.leafLogDir {
		t.Fatalf("valueLogDirForLane leaf log=%q want %q", got, db.leafLogDir)
	}
}

func TestOpen_RejectsValueLogLane255ConflictWhenLeafLogEnabled(t *testing.T) {
	dir := t.TempDir()
	valueDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(valueDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(value_vlog): %v", err)
	}
	fileID, err := valuelog.EncodeFileID(leafLogLaneID, 3)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	w, err := valuelog.NewWriter(filepath.Join(valueDir, "value-l255-000003.log"), fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 64)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = Open(dir, NewMockBackend(), Options{JournalLanes: 4, IndexOuterLeavesInValueLog: true})
	if err == nil {
		t.Fatal("expected reserved leaf lane conflict error")
	}
	if !strings.Contains(err.Error(), "value_vlog lane 255 conflicts with reserved leaf_vlog lane") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpen_RejectsRecoveredWALLane255ConflictWhenLeafLogEnabled(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l255-000001.log"), bytes.Repeat([]byte("w"), 64), 0o600); err != nil {
		t.Fatalf("WriteFile(wal): %v", err)
	}

	_, err := Open(dir, NewMockBackend(), Options{JournalLanes: 4, IndexOuterLeavesInValueLog: true})
	if err == nil {
		t.Fatal("expected recovered lane-255 WAL conflict error")
	}
	if !strings.Contains(err.Error(), "recovered WAL/value lanes require rebuild before reopen") {
		t.Fatalf("unexpected error: %v", err)
	}
}
