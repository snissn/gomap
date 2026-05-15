package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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
		needsValueLog, err := commandWALReplayFramesNeedLogSupport(nil, frames)
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
	dir := b.TempDir()
	for i := 1; i <= 64; i++ {
		writeCommandWALRawKVFrame(b, dir, uint64(i), uint64(i), []commitlog.RawKVOperation{{
			Op:    commitlog.RawKVOpSet,
			Key:   []byte("bench-key"),
			Value: []byte("bench-value"),
		}})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decisions, err := cleanupCommandWALSegmentsCoveredByAppliedLSN(dir, 0, 0)
		if err != nil {
			b.Fatalf("cleanupCommandWALSegmentsCoveredByAppliedLSN: %v", err)
		}
		if len(decisions) != 64 {
			b.Fatalf("len(decisions)=%d, want 64", len(decisions))
		}
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
