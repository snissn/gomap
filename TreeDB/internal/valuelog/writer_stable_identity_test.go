package valuelog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestWriterStableIdentityPreservesUnresolvedResourceClassification(t *testing.T) {
	tests := []struct {
		name   string
		writer *Writer
	}{
		{name: "nil-receiver"},
		{name: "non-file-backed", writer: &Writer{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity, err := test.writer.StableIdentity()
			if identity != (rootpublication.StableIdentity{}) || !errors.Is(err, rootpublication.ErrUnresolvedResource) {
				t.Fatalf("StableIdentity identity=%+v error=%v want zero identity and unresolved resource", identity, err)
			}
		})
	}
}

func TestWriterOwnsOnlyItsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	id, err := EncodeFileID(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := SegmentPath(dir, id)
	w, err := NewWriter(path, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(); err != nil {
			t.Error(err)
		}
	})

	other := filepath.Join(dir, "other")
	if err := os.WriteFile(other, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !w.OwnsEmptyFile(path, id) || w.OwnsEmptyFile(other, id) || w.OwnsEmptyFile(path, id+1) {
		t.Fatal("empty-file proof did not bind the open file and logical ID")
	}
	if _, err := w.Append(0, nil, 1, []byte("still buffered")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 || w.PendingBytes() == 0 {
		t.Fatalf("append did not leave a buffered prefix at physical EOF zero: %v, %v", info, err)
	}
	if w.OwnsEmptyFile(path, id) {
		t.Fatal("writer with a buffered prefix reported an empty file")
	}
}
