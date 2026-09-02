package commitlog

import (
	"bytes"
	"encoding/binary"
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

func FuzzCommandWALDecodeFrame(f *testing.F) {
	if payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("k1"), Value: []byte("v1")},
		{Op: RawKVOpDelete, Key: []byte("k2")},
	}); err == nil {
		if frame, err := EncodeCommandFrame(CommandEnvelope{
			LSN:           1,
			Kind:          CommandKindRawKVBatch,
			Scope:         CommandScopeRawKV,
			PayloadFormat: PayloadFormatRawKVBatchV1,
			Payload:       payload,
		}); err == nil {
			f.Add(frame)
		}
	}
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > commitlogFuzzMaxSegment {
			return
		}
		env, err := DecodeCommandFrame(data)
		if err != nil {
			return
		}
		if env.Kind == CommandKindRawKVBatch {
			_, _ = DecodeRawKVBatchPayload(env.Payload)
		}
	})
}

func FuzzCommandWALRawKVBatchPayload(f *testing.F) {
	f.Add([]byte{})
	if payload, err := EncodeRawKVBatchPayload([]RawKVOperation{
		{Op: RawKVOpSet, Key: []byte("alpha"), Value: []byte("one")},
		{Op: RawKVOpDelete, Key: []byte("beta")},
		{Op: RawKVOpSet, Key: []byte{}, Value: []byte("empty-key-value")},
		{Op: RawKVOpSetRID, Key: []byte("rid-key"), RID: 42},
	}); err == nil {
		f.Add(payload)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > commitlogFuzzMaxSegment {
			return
		}
		var scanned []RawKVOperation
		scanErr := ScanRawKVBatchPayload(data, func(op RawKVOp, key, value []byte) error {
			entry := RawKVOperation{
				Op:    op,
				Key:   append([]byte(nil), key...),
				Value: append([]byte(nil), value...),
			}
			if op == RawKVOpSetRID {
				entry.RID = binary.LittleEndian.Uint64(value)
				entry.Value = nil
			}
			scanned = append(scanned, entry)
			return nil
		})
		decoded, decodeErr := DecodeRawKVBatchPayload(data)
		if (scanErr == nil) != (decodeErr == nil) {
			t.Fatalf("scanErr=%v decodeErr=%v", scanErr, decodeErr)
		}
		if scanErr != nil {
			return
		}
		if len(scanned) != len(decoded) {
			t.Fatalf("scanned len=%d decoded len=%d", len(scanned), len(decoded))
		}
		for i := range scanned {
			if scanned[i].Op != decoded[i].Op || scanned[i].RID != decoded[i].RID || !bytes.Equal(scanned[i].Key, decoded[i].Key) || !bytes.Equal(scanned[i].Value, decoded[i].Value) {
				t.Fatalf("op[%d] scan=%+v decode=%+v", i, scanned[i], decoded[i])
			}
		}
		encoded, err := EncodeRawKVBatchPayload(decoded)
		if err != nil {
			t.Fatalf("re-encode decoded valid payload: %v", err)
		}
		if !bytes.Equal(encoded, data) {
			t.Fatalf("re-encoded valid payload changed bytes")
		}
	})
}
