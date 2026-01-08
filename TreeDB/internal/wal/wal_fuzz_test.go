package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func seedWALData() ([]byte, error) {
	dir, err := os.MkdirTemp("", "wal-fuzz")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "wal-000001.log")
	w, err := NewWriter(path)
	if err != nil {
		return nil, err
	}
	_ = w.Append(1, OpSet, []byte("k"), []byte("v"))
	_ = w.Close()
	return os.ReadFile(path)
}

func FuzzWALReader(f *testing.F) {
	f.Add([]byte{})
	if seed, err := seedWALData(); err == nil {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal-000001.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			return
		}
		r, err := NewReader(path)
		if err != nil {
			return
		}
		defer r.Close()
		for i := 0; i < 8; i++ {
			_, _, _, _, err := r.ReadNext()
			if err != nil {
				break
			}
		}
	})
}
