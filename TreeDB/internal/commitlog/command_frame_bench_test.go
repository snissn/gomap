package commitlog

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

var commandWALBenchSink struct {
	frame []byte
	ops   []RawKVOperation
}

func BenchmarkCommandWALRawKVBatchPayloadEncode(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			ops := makeCommandWALBenchOps(tc.ops, tc.valueSize)
			payloadLen := commandWALRawKVPayloadLen(ops)
			b.SetBytes(int64(payloadLen))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				frame, err := EncodeRawKVBatchPayload(ops)
				if err != nil {
					b.Fatal(err)
				}
				commandWALBenchSink.frame = frame
			}
		})
	}
}

func BenchmarkCommandWALRawKVBatchPayloadDecode(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			payload := mustCommandWALBenchPayload(b, tc.ops, tc.valueSize)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ops, err := DecodeRawKVBatchPayload(payload)
				if err != nil {
					b.Fatal(err)
				}
				commandWALBenchSink.ops = ops
			}
		})
	}
}

func BenchmarkCommandWALRawKVBatchPayloadScan(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			payload := mustCommandWALBenchPayload(b, tc.ops, tc.valueSize)
			var visited int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				visited = 0
				if err := ScanRawKVBatchPayload(payload, func(op RawKVOp, key, value []byte) error {
					visited += int(op) + len(key) + len(value)
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
			if visited == 0 {
				b.Fatal("scan did not visit payload")
			}
		})
	}
}

func BenchmarkCommandWALFrameEncode(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			payload := mustCommandWALBenchPayload(b, tc.ops, tc.valueSize)
			env := CommandEnvelope{
				LSN:           1,
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
				Payload:       payload,
			}
			b.SetBytes(int64(commandFrameHeaderSize + len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				env.LSN = uint64(i + 1)
				frame, err := EncodeCommandFrame(env)
				if err != nil {
					b.Fatal(err)
				}
				commandWALBenchSink.frame = frame
			}
		})
	}
}

func BenchmarkCommandWALFrameDecode(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			frame := mustCommandWALBenchFrame(b, tc.ops, tc.valueSize)
			b.SetBytes(int64(len(frame)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				env, err := DecodeCommandFrame(frame)
				if err != nil {
					b.Fatal(err)
				}
				commandWALBenchSink.frame = env.Payload
			}
		})
	}
}

func BenchmarkWriterAppendCommandRawKVBatch(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			payload := mustCommandWALBenchPayload(b, tc.ops, tc.valueSize)
			env := CommandEnvelope{
				LSN:           1,
				Kind:          CommandKindRawKVBatch,
				Scope:         CommandScopeRawKV,
				PayloadFormat: PayloadFormatRawKVBatchV1,
				Payload:       payload,
			}
			path := filepath.Join(b.TempDir(), "commit.log")
			w, err := NewWriterWithOptions(path, Options{Compress: false})
			if err != nil {
				b.Fatalf("NewWriterWithOptions: %v", err)
			}
			b.Cleanup(func() { _ = w.Close() })

			b.SetBytes(int64(commandFrameHeaderSize + len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				env.LSN = uint64(i + 1)
				if err := w.AppendCommand(env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriterAppendRawKVBatchPayloadScanCommandDirectTrusted(b *testing.B) {
	for _, opsCount := range []int{1, 16, 32, 128} {
		b.Run(fmt.Sprintf("setrid_revisions_ops_%d", opsCount), func(b *testing.B) {
			ops := makeCommandWALBenchRIDOps(opsCount)
			scan := rawKVOperationSliceScanner(ops)
			plan, err := PlanRawKVBatchPayloadScan(scan)
			if err != nil {
				b.Fatalf("PlanRawKVBatchPayloadScan: %v", err)
			}
			path := filepath.Join(b.TempDir(), "commit.log")
			w, err := NewWriterWithOptions(path, Options{Compress: false, DeferredCommandBufferSize: 1 << 20})
			if err != nil {
				b.Fatalf("NewWriterWithOptions: %v", err)
			}
			b.Cleanup(func() { _ = w.Close() })

			b.SetBytes(int64(commandFrameHeaderSize + plan.PayloadLen))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := w.AppendRawKVBatchPayloadScanCommandDirectTrusted(uint64(i+1), 0, plan, scan); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriterAppendBatchLegacyRawKV(b *testing.B) {
	for _, tc := range commandWALBenchCases() {
		b.Run(tc.name, func(b *testing.B) {
			ops := makeCommandWALBenchOps(tc.ops, tc.valueSize)
			records := make([]Record, len(ops))
			for i := range ops {
				records[i] = Record{
					Op:    OpSetInline,
					Key:   ops[i].Key,
					Value: ops[i].Value,
					Seq:   1,
				}
			}
			path := filepath.Join(b.TempDir(), "commit.log")
			w, err := NewWriterWithOptions(path, Options{Compress: false})
			if err != nil {
				b.Fatalf("NewWriterWithOptions: %v", err)
			}
			b.Cleanup(func() { _ = w.Close() })

			b.SetBytes(int64(batchHeaderSize + len(records)*(recordHeaderSize+len(records[0].Key)+len(records[0].Value))))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				seq := uint64(i + 1)
				for j := range records {
					records[j].Seq = seq
				}
				if err := w.AppendBatch(records); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func commandWALBenchCases() []struct {
	name      string
	ops       int
	valueSize int
} {
	return []struct {
		name      string
		ops       int
		valueSize int
	}{
		{name: "ops_1_value_64", ops: 1, valueSize: 64},
		{name: "ops_16_value_64", ops: 16, valueSize: 64},
		{name: "ops_128_value_64", ops: 128, valueSize: 64},
		{name: "ops_16_value_1024", ops: 16, valueSize: 1024},
	}
}

func makeCommandWALBenchOps(n, valueSize int) []RawKVOperation {
	ops := make([]RawKVOperation, n)
	value := bytes.Repeat([]byte("v"), valueSize)
	for i := range ops {
		ops[i] = RawKVOperation{
			Op:    RawKVOpSet,
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: value,
		}
	}
	return ops
}

func makeCommandWALBenchRIDOps(n int) []RawKVOperation {
	ops := make([]RawKVOperation, n)
	for i := range ops {
		ops[i] = RawKVOperation{
			Op:       RawKVOpSetRID,
			Key:      []byte(fmt.Sprintf("key-%06d", i)),
			RID:      uint64(i + 1),
			Revision: uint64(i + 1),
		}
	}
	return ops
}

func commandWALRawKVPayloadLen(ops []RawKVOperation) int {
	total := rawKVBatchHeaderSize
	for i := range ops {
		total += rawKVOpHeaderSize + len(ops[i].Key) + len(ops[i].Value)
	}
	return total
}

func mustCommandWALBenchPayload(b *testing.B, ops, valueSize int) []byte {
	b.Helper()
	payload, err := EncodeRawKVBatchPayload(makeCommandWALBenchOps(ops, valueSize))
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

func mustCommandWALBenchFrame(b *testing.B, ops, valueSize int) []byte {
	b.Helper()
	payload := mustCommandWALBenchPayload(b, ops, valueSize)
	frame, err := EncodeCommandFrame(CommandEnvelope{
		LSN:           1,
		Kind:          CommandKindRawKVBatch,
		Scope:         CommandScopeRawKV,
		PayloadFormat: PayloadFormatRawKVBatchV1,
		Payload:       payload,
	})
	if err != nil {
		b.Fatal(err)
	}
	return frame
}
