package vlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func seedVlogData() ([]byte, uint32, error) {
	dir, err := os.MkdirTemp("", "vlog-fuzz")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(dir)

	fileID := page.ValueLogFileID(1)
	path := filepath.Join(dir, "vlog-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		return nil, 0, err
	}
	_, _ = w.Append(1, OpSet, []byte("k"), []byte("v"))
	_ = w.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	return data, fileID, nil
}

func FuzzVlogReader(f *testing.F) {
	f.Add([]byte{})
	if seed, fileID, err := seedVlogData(); err == nil {
		f.Add(seed)
		_ = fileID
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "vlog-000001.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			return
		}
		fileID := page.ValueLogFileID(1)
		r, err := NewReader(path, fileID)
		if err != nil {
			return
		}
		defer r.Close()
		for i := 0; i < 8; i++ {
			_, _, _, _, _, err := r.ReadNext()
			if err != nil {
				break
			}
		}
	})
}
