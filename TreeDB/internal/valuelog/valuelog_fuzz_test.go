package valuelog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/slab"
)

const valuelogFuzzMaxRecord = 1 << 20

func seedValueLogData() ([]byte, uint32, error) {
	dir, err := os.MkdirTemp("", "valuelog-fuzz")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(dir)

	fileID, err := EncodeFileID(0, 1)
	if err != nil {
		return nil, 0, err
	}
	path := filepath.Join(dir, "value-l0-000001.log")
	w, err := NewWriter(path, fileID)
	if err != nil {
		return nil, 0, err
	}
	if _, err := w.Append(0, nil, 1, []byte("v")); err != nil {
		_ = w.Close()
		return nil, 0, err
	}
	if err := w.Close(); err != nil {
		return nil, 0, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	return data, fileID, nil
}

func FuzzValueLogReader(f *testing.F) {
	f.Add([]byte{})
	if seed, fileID, err := seedValueLogData(); err == nil {
		f.Add(seed)
		_ = fileID
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > valuelogFuzzMaxRecord {
			return
		}
		oldMax := slab.MaxRecordSize
		slab.MaxRecordSize = valuelogFuzzMaxRecord
		t.Cleanup(func() { slab.MaxRecordSize = oldMax })

		dir := t.TempDir()
		path := filepath.Join(dir, "value-l0-000001.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			return
		}
		fileID, err := EncodeFileID(0, 1)
		if err != nil {
			return
		}
		r, err := NewReader(path, fileID)
		if err != nil {
			return
		}
		r.SetDictLookup(func(uint64) ([]byte, error) {
			return nil, ErrMissingDict
		})
		defer r.Close()
		for i := 0; i < 8; i++ {
			if _, _, _, err := r.ReadNext(); err != nil {
				break
			}
		}
	})
}
