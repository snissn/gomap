package valuelog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/limits"
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
		oldMax := limits.MaxRecordSize
		limits.MaxRecordSize = valuelogFuzzMaxRecord
		t.Cleanup(func() { limits.MaxRecordSize = oldMax })

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

func FuzzValueLogDecodeMixedFrame(f *testing.F) {
	f.Add(byte(BlockCodecSnappy), []byte("abc"), uint32(3))
	f.Add(byte(BlockCodecLZ4), []byte(""), uint32(0))
	f.Add(byte(BlockCodecZSTD), []byte(""), uint32(0))
	f.Add(byte(0xFF), []byte{0x01, 0x02, 0x03}, uint32(16))

	f.Fuzz(func(t *testing.T, codecID byte, payload []byte, rawLen uint32) {
		if len(payload) > valuelogFuzzMaxRecord {
			return
		}
		if rawLen > valuelogFuzzMaxRecord {
			rawLen = valuelogFuzzMaxRecord
		}
		header := FrameHeader{
			Version:  FrameVersion,
			Flags:    FrameFlagCompressed,
			Reserved: codecID,
		}
		_, _ = decodeFramePayloadTo(header, payload, nil, rawLen, nil)
	})
}
