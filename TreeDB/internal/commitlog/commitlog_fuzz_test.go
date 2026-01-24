package commitlog

import (
	"os"
	"path/filepath"
	"testing"
)

const commitlogFuzzMaxSegment = 1 << 20

func seedCommitLogData() ([]byte, error) {
	dir, err := os.MkdirTemp("", "commitlog-fuzz")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "commit.log")
	w, err := NewWriter(path)
	if err != nil {
		return nil, err
	}
	records := []Record{
		{Op: OpSetInline, Key: []byte("k1"), Value: []byte("v1"), Seq: 1},
		{Op: OpDelete, Key: []byte("k2"), Seq: 1},
	}
	if err := w.AppendBatch(records); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return os.ReadFile(path)
}

func FuzzCommitLogReader(f *testing.F) {
	f.Add([]byte{})
	if seed, err := seedCommitLogData(); err == nil {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > commitlogFuzzMaxSegment {
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "commit.log")
		if err := os.WriteFile(path, data, 0600); err != nil {
			return
		}

		r, err := NewReaderWithOptions(path, Options{MaxSegmentSize: commitlogFuzzMaxSegment})
		if err != nil {
			return
		}
		defer r.Close()

		for i := 0; i < 8; i++ {
			if _, err := r.ReadBatch(); err != nil {
				break
			}
		}
	})
}
