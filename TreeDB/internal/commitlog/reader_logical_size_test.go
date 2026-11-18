package commitlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReaderLogicalSizeUsesValidatedFixedState(t *testing.T) {
	if !FixedWALCreationSupported() {
		t.Skip("fixed-container publication unsupported")
	}
	journal, err := openFixedTestCommandJournal(t.TempDir(), CommandJournalOptions{SegmentTargetBytes: 8192, CaptureStableResources: true})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if _, err = journal.AppendCommand(fixedWALTestBarrier()); err != nil {
		t.Fatal(err)
	}
	if err = journal.Flush(); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(journal.path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	want, err := LogicalFileSize(file)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderFromFileWithOptions(file, Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.LogicalSize()
	if err != nil || got != want || got == 0 {
		t.Fatalf("logical=%d want=%d err=%v", got, want, err)
	}
	if _, err = reader.ReadValidatedCommandFrameLSN(); err != nil {
		t.Fatal(err)
	}
	if size, err := reader.LogicalSize(); err != nil || size != want {
		t.Fatalf("logical changed with cursor: %d %v", size, err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Stat(); err != nil {
		t.Fatalf("reader closed caller handle: %v", err)
	}
}

func TestReaderLogicalSizeLegacyStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.log")
	if err := os.WriteFile(path, []byte("legacy stream"), 0600); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderWithOptions(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got, err := reader.LogicalSize(); err != nil || got != 13 {
		t.Fatalf("logical=%d err=%v", got, err)
	}
}
