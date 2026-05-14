package collectionwal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestIsSegmentName(t *testing.T) {
	valid := []string{
		"collection-l0-000000.log",
		"collection-l0-1.log",
		"collection-l1-000123.log",
		"collection-l2-1000000.log",
		"collection-l4294967295-999999.log",
	}
	for _, name := range valid {
		if !IsSegmentName(name) {
			t.Fatalf("IsSegmentName(%q)=false, want true", name)
		}
	}

	invalid := []string{
		"collection-0-1.log",
		"collection-l-1.log",
		"collection-l0-.log",
		"collection-l0-1.tmp",
		"collection-l4294967296-1.log",
		"commit-l0-1.log",
	}
	for _, name := range invalid {
		if IsSegmentName(name) {
			t.Fatalf("IsSegmentName(%q)=true, want false", name)
		}
	}
}

func TestParseSegmentName(t *testing.T) {
	lane, seq, ok := ParseSegmentName("collection-l7-000042.log")
	if !ok || lane != 7 || seq != 42 {
		t.Fatalf("ParseSegmentName valid got lane=%d seq=%d ok=%v, want 7/42/true", lane, seq, ok)
	}
	if _, _, ok := ParseSegmentName("collection-l4294967296-1.log"); ok {
		t.Fatalf("ParseSegmentName accepted lane overflow")
	}
}

func TestRequireCleanRejectsCollectionWALSegment(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "commit-l0-000001.log"), []byte("commit"), 0o600); err != nil {
		t.Fatalf("write commit segment: %v", err)
	}
	if err := RequireCleanForReadOnlyOpen(dir); err != nil {
		t.Fatalf("commit WAL must not be classified as collection WAL: %v", err)
	}
	if err := os.WriteFile(filepath.Join(walDir, "collection-l0-000001.log"), []byte("collection"), 0o600); err != nil {
		t.Fatalf("write collection segment: %v", err)
	}

	err := RequireCleanForReadOnlyOpen(dir)
	if !errors.Is(err, ErrCollectionWALRecoveryRequired) {
		t.Fatalf("RequireCleanForReadOnlyOpen error=%v, want ErrCollectionWALRecoveryRequired", err)
	}
	err = RequireCleanForOfflineMaintenance(dir)
	if !errors.Is(err, ErrCollectionWALRecoveryRequired) {
		t.Fatalf("RequireCleanForOfflineMaintenance error=%v, want ErrCollectionWALRecoveryRequired", err)
	}
}

func TestDirtySegmentsRejectsMissingSegmentGapWithoutCleanup(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	segmentPath := filepath.Join(walDir, "collection-l0-000002.log")
	if err := os.WriteFile(segmentPath, []byte("collection"), 0o600); err != nil {
		t.Fatalf("write collection segment: %v", err)
	}

	dirty, err := DirtySegments(dir)
	if !errors.Is(err, ErrCollectionWALCorruptMiddle) {
		t.Fatalf("DirtySegments error=%v, want ErrCollectionWALCorruptMiddle", err)
	}
	if got := CategoryOf(err); got != ErrorCategorySegmentGapWithoutCleanup {
		t.Fatalf("DirtySegments category=%q want %q", got, ErrorCategorySegmentGapWithoutCleanup)
	}
	if len(dirty) != 1 || dirty[0] != segmentPath {
		t.Fatalf("DirtySegments dirty=%v want [%s]", dirty, segmentPath)
	}
}

func TestDirtySegmentsRejectsDuplicateSegmentSequence(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walDir, 0o700); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	for _, name := range []string{"collection-l0-1.log", "collection-l0-000001.log"} {
		if err := os.WriteFile(filepath.Join(walDir, name), []byte("collection"), 0o600); err != nil {
			t.Fatalf("write collection segment %s: %v", name, err)
		}
	}

	dirty, err := DirtySegments(dir)
	if !errors.Is(err, ErrCollectionWALCorruptMiddle) {
		t.Fatalf("DirtySegments error=%v, want ErrCollectionWALCorruptMiddle", err)
	}
	if len(dirty) != 2 {
		t.Fatalf("DirtySegments dirty len=%d want 2: %v", len(dirty), dirty)
	}
}
