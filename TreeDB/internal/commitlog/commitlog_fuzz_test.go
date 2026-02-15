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

func seedCommitLogFenceGroupedData(encoding FenceRIDGroupEncoding) ([]byte, error) {
	dir, err := os.MkdirTemp("", "commitlog-fuzz-fence")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "commit.log")
	w, err := NewWriter(path)
	if err != nil {
		return nil, err
	}
	payload, err := EncodeFenceRIDGroupPayload([]FenceRIDGroupEntry{
		{Key: []byte("k001"), RID: 1},
		{Key: []byte("k002"), RID: 1},
		{Key: []byte("k003"), RID: 2},
	}, encoding)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.AppendBatch([]Record{{Op: OpSetFenceRIDGroup, Value: payload, Seq: 1}}); err != nil {
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
	if seed, err := seedCommitLogFenceGroupedData(FenceRIDGroupEncodingSimple); err == nil {
		f.Add(seed)
	}
	if seed, err := seedCommitLogFenceGroupedData(FenceRIDGroupEncodingPrefix); err == nil {
		f.Add(seed)
	}
	// Malformed grouped payload seeds: broken key varint and truncated RID varint.
	f.Add([]byte{
		// segment header (len=32, crc=0) + tiny malformed payload
		0x20, 0x00, 0x00, 0x00, 0, 0, 0, 0,
		1, 1, 1, // version, prefix encoding, count=1
		0x80, // truncated key-len varint
	})
	f.Add([]byte{
		0x21, 0x00, 0x00, 0x00, 0, 0, 0, 0,
		1, 0, 1, // version, simple encoding, count=1
		1, 'k',
		0x80, // truncated RID varint
	})
	f.Add([]byte{
		0x21, 0x00, 0x00, 0x00, 0, 0, 0, 0,
		1, 0, 100, // version, simple encoding, impossible count for payload bytes
		1, 'k', 1,
	})
	f.Add([]byte{
		0x24, 0x00, 0x00, 0x00, 0, 0, 0, 0,
		1, 1, 2, // version, prefix encoding, count=2
		1, 'b', 1, // first key/rid
		0, 1, 'a', 2, // non-monotonic second key
	})

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
