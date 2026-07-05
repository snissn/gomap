package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func BenchmarkCommandWALRawKVDirectWrite(b *testing.B) {
	for _, tc := range []struct {
		name       string
		commandWAL bool
	}{
		{name: "backend_no_command_wal", commandWAL: false},
		{name: "command_wal_raw_kv", commandWAL: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(Options{Dir: dir, CommandWAL: tc.commandWAL, DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer db.Close()
			key := []byte("bench-key")
			value := []byte("bench-value-0123456789-0123456789-0123456789")
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch()
				if err := batch.Set(key, value); err != nil {
					b.Fatalf("Set: %v", err)
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close batch: %v", err)
				}
			}
		})
	}
}

func BenchmarkCommandWALRawKVDirectWriteRevision(b *testing.B) {
	for _, tc := range []struct {
		name       string
		commandWAL bool
	}{
		{name: "backend_no_command_wal_revision", commandWAL: false},
		{name: "command_wal_raw_kv_revision", commandWAL: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			db, err := Open(Options{Dir: dir, CommandWAL: tc.commandWAL, DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer db.Close()
			key := []byte("bench-key")
			value := []byte("bench-value-0123456789-0123456789-0123456789")
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch().(*Batch)
				if err := batch.SetWithRevision(key, value, 1); err != nil {
					b.Fatalf("SetWithRevision: %v", err)
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close batch: %v", err)
				}
			}
		})
	}
}

func BenchmarkCommandWALRawKVPointerDirectWrite(b *testing.B) {
	value := []byte("bench-value-backed-by-value-log")
	for _, tc := range []struct {
		name       string
		commandWAL bool
	}{
		{name: "backend_no_command_wal_pointer", commandWAL: false},
		{name: "command_wal_raw_kv_pointer", commandWAL: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			ptr := writeValueLogRID(b, dir, 17, value)
			db, err := Open(Options{Dir: dir, CommandWAL: tc.commandWAL, DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer db.Close()
			key := []byte("bench-pointer-key")
			b.ReportAllocs()
			b.SetBytes(int64(len(key) + len(value)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch().(*Batch)
				if err := batch.SetPointer(key, ptr); err != nil {
					b.Fatalf("SetPointer: %v", err)
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close batch: %v", err)
				}
			}
		})
	}
}

func BenchmarkCommandWALRawKVPointerBatchDirectWrite(b *testing.B) {
	value := []byte("bench-value-backed-by-value-log")
	for _, tc := range []struct {
		name   string
		unique bool
	}{
		{name: "repeated_pointer_256", unique: false},
		{name: "unique_pointers_256", unique: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			ptrs := writeValueLogRIDBatch(b, dir, 256, value)
			if !tc.unique {
				ptr := ptrs[0]
				for i := range ptrs {
					ptrs[i] = ptr
				}
			}
			db, err := Open(Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true})
			if err != nil {
				b.Fatalf("Open: %v", err)
			}
			defer db.Close()
			keys := make([][]byte, len(ptrs))
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("bench-pointer-key-%06d", i))
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(ptrs) * (len(keys[0]) + len(value))))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch().(*Batch)
				for j := range ptrs {
					if err := batch.SetPointer(keys[j], ptrs[j]); err != nil {
						b.Fatalf("SetPointer: %v", err)
					}
				}
				if err := batch.Write(); err != nil {
					b.Fatalf("Write: %v", err)
				}
				if err := batch.Close(); err != nil {
					b.Fatalf("Close batch: %v", err)
				}
			}
		})
	}
}

func BenchmarkCommandWALReplayFrameClassification(b *testing.B) {
	payload := commandWALBenchRawKVPayload(b, 16, 64)
	frames := make([]commandWALReplayFrame, 256)
	for i := range frames {
		frames[i].env = commitlog.CommandEnvelope{
			LSN:           uint64(i + 1),
			Kind:          commitlog.CommandKindRawKVBatch,
			Scope:         commitlog.CommandScopeRawKV,
			PayloadFormat: commitlog.PayloadFormatRawKVBatchV1,
			Payload:       payload,
		}
	}
	b.SetBytes(int64(len(payload) * len(frames)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		needsValueLog, err := commandWALReplayFramesNeedLogSupport(nil, frames, 0)
		if err != nil {
			b.Fatalf("commandWALReplayFramesNeedLogSupport: %v", err)
		}
		if needsValueLog {
			b.Fatal("inline RawKVBatch frames unexpectedly need value-log support")
		}
	}
}

func BenchmarkCommandWALReadReplayFrames(b *testing.B) {
	dir := b.TempDir()
	for i := 1; i <= 64; i++ {
		writeCommandWALRawKVFrame(b, dir, uint64(i), uint64(i), []commitlog.RawKVOperation{{
			Op:    commitlog.RawKVOpSet,
			Key:   []byte("bench-key"),
			Value: []byte("bench-value"),
		}})
	}
	segments, err := listWALSegments(dir)
	if err != nil {
		b.Fatalf("listWALSegments: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frames, err := readCommandWALReplayFrames(segments, 0, 0)
		if err != nil {
			b.Fatalf("readCommandWALReplayFrames: %v", err)
		}
		if len(frames) != 64 {
			b.Fatalf("len(frames)=%d, want 64", len(frames))
		}
	}
}

func BenchmarkCommandWALCoveredSegmentCleanupProof(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, err := os.MkdirTemp("", "treedb-command-wal-cleanup-*")
		if err != nil {
			b.Fatalf("MkdirTemp: %v", err)
		}
		for seq := 1; seq <= 2; seq++ {
			writeCommandWALRawKVFrame(b, dir, uint64(seq), uint64(seq), []commitlog.RawKVOperation{{
				Op:    commitlog.RawKVOpSet,
				Key:   []byte("bench-key"),
				Value: []byte("bench-value"),
			}})
		}
		b.StartTimer()
		decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 2, 0)
		b.StopTimer()
		if err != nil {
			_ = os.RemoveAll(dir)
			b.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
		}
		if len(decisions) != 2 {
			_ = os.RemoveAll(dir)
			b.Fatalf("len(decisions)=%d, want 2", len(decisions))
		}
		covered, removed := 0, 0
		for _, decision := range decisions {
			if decision.Covered {
				covered++
			}
			if decision.Removed {
				removed++
			}
		}
		if covered != 2 || removed != 1 {
			_ = os.RemoveAll(dir)
			b.Fatalf("covered=%d removed=%d, want covered=2 removed=1 active segment retained", covered, removed)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatalf("RemoveAll: %v", err)
		}
		b.StartTimer()
	}
}

func commandWALBenchRawKVPayload(b testing.TB, ops int, valueSize int) []byte {
	b.Helper()
	rawOps := make([]commitlog.RawKVOperation, ops)
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'v'
	}
	for i := range rawOps {
		rawOps[i] = commitlog.RawKVOperation{
			Op:    commitlog.RawKVOpSet,
			Key:   []byte("bench-key"),
			Value: value,
		}
	}
	payload, err := commitlog.EncodeRawKVBatchPayload(rawOps)
	if err != nil {
		b.Fatalf("EncodeRawKVBatchPayload: %v", err)
	}
	return payload
}

func writeValueLogRIDBatch(t testing.TB, dir string, count int, value []byte) []page.ValuePtr {
	t.Helper()
	valueLogDir := resolveStorageLayout(dir).valueVLogDir
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatalf("MkdirAll value_vlog: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(valueLogDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("New value writer: %v", err)
	}
	ptrs := make([]page.ValuePtr, count)
	for i := range ptrs {
		ptr, err := w.Append(0, nil, uint64(i+1), value)
		if err != nil {
			_ = w.Close()
			t.Fatalf("Append value log: %v", err)
		}
		ptrs[i] = ptr
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close value writer: %v", err)
	}
	return ptrs
}
