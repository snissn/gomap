package commitlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
)

var commandFrameV2BenchSink struct {
	frame  []byte
	frames []CommandEnvelope
}

func BenchmarkCommandFrameV2Encode(b *testing.B) {
	payload := commandFrameV2BenchPayload(b, 16)
	env := CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             1,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         payload,
	}
	b.SetBytes(int64(commandFrameHeaderSize + len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.LSN = uint64(i + 1)
		frame, err := EncodeCommandFrameV2(env)
		if err != nil {
			b.Fatal(err)
		}
		commandFrameV2BenchSink.frame = frame
	}
}

func BenchmarkCommandFrameV2Decode(b *testing.B) {
	frame := commandFrameV2BenchFrame(b, 1, 16)
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env, err := DecodeCommandFrameV2(frame)
		if err != nil {
			b.Fatal(err)
		}
		commandFrameV2BenchSink.frame = env.Payload
	}
}

func BenchmarkCommandFrameV2StreamScan(b *testing.B) {
	const frameCount = 128
	path := filepath.Join(b.TempDir(), "commit-l0-000001.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		b.Fatal(err)
	}
	var totalBytes int64
	for i := 0; i < frameCount; i++ {
		frame := commandFrameV2BenchFrame(b, uint64(i+1), 16)
		var header [8]byte
		binary.LittleEndian.PutUint32(header[0:4], uint32(len(frame)))
		binary.LittleEndian.PutUint32(header[4:8], crc.Checksum(frame))
		if _, err := f.Write(header[:]); err == nil {
			_, err = f.Write(frame)
		}
		if err != nil {
			_ = f.Close()
			b.Fatal(err)
		}
		totalBytes += int64(len(header) + len(frame))
	}
	if err := f.Close(); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		frames, err := ScanCommandFramesV2(path, Options{})
		if err != nil {
			b.Fatal(err)
		}
		if len(frames) != frameCount {
			b.Fatalf("frames=%d, want %d", len(frames), frameCount)
		}
		commandFrameV2BenchSink.frames = frames
	}
}

func commandFrameV2BenchPayload(b *testing.B, count int) []byte {
	b.Helper()
	ops := make([]RawKVOperation, count)
	for i := range ops {
		ops[i] = RawKVOperation{Op: RawKVOpSetRID, Key: []byte(fmt.Sprintf("key-%04d", i)), RID: uint64(i + 1)}
	}
	payload, err := EncodeRawKVBatchPayload(ops)
	if err != nil {
		b.Fatal(err)
	}
	return payload
}

func commandFrameV2BenchFrame(b *testing.B, lsn uint64, count int) []byte {
	b.Helper()
	frame, err := EncodeCommandFrameV2(CommandEnvelope{
		DurabilityClass: CommandDurabilityRelaxed,
		LSN:             lsn,
		Kind:            CommandKindRawKVBatch,
		Scope:           CommandScopeRawKV,
		PayloadFormat:   PayloadFormatRawKVBatchV1,
		Payload:         commandFrameV2BenchPayload(b, count),
	})
	if err != nil {
		b.Fatal(err)
	}
	return frame
}
