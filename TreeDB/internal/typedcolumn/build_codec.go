package typedcolumn

import (
	"fmt"
	"time"

	"github.com/snissn/compress/zstd"
)

// BuildCodecCapacity bounds the pinned SpeedFastest, concurrency-one encoder.
// enc_base.ensureHist reserves twice the default 4 MiB window; enc_fast has
// 32768 eight-byte hash entries. A 64 KiB block has at most one sequence per
// three input bytes. Fourfold sequence backing covers append growth plus the
// old buffer. Block literals/output, six FSE coders, Huffman tables/four lanes,
// and encoder/channel headers are covered by the explicit fixed terms below.
// Output byte slices belong to the enclosing source-derived image allowance.
func BuildCodecCapacity() uint64 {
	const windowHistory = 2 * (4 << 20)
	const hashTable = 32768 * 8
	const sequenceGrowth = 4 * ((64<<10)/3 + 1) * 16
	const blockBuffers = 4 * (128 << 10)
	const huffmanBuffers = 8 * (256 << 10)
	const fseAndHeaders = 6*(16<<10) + (64 << 10)
	return windowHistory + hashTable + sequenceGrowth + blockBuffers + huffmanBuffers + fseAndHeaders
}

// BuildCodec borrows the caller's preadmitted construction credit. It owns no
// budget or publication authority. Native construction uses an operation-local
// encoder so a global pool cannot keep its workspace after the receipt closes.
// The options are exactly those of getZstdEncoder.
type BuildCodec struct{ encoder *zstd.Encoder }

func (c *BuildCodec) Close() error {
	if c == nil || c.encoder == nil {
		return nil
	}
	encoder := c.encoder
	c.encoder = nil
	return encoder.Close()
}

func admitCompressionIntoWithCodec(dst, raw []byte, encoding Encoding, compression Compression, codec *BuildCodec) (CompressionSelection, error) {
	if codec == nil || compression != CompressionZSTD {
		return admitCompressionInto(dst, raw, encoding, compression)
	}
	if codec.encoder == nil {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1), zstd.WithEncoderCRC(false))
		if err != nil {
			return CompressionSelection{}, fmt.Errorf("typedcolumn: build zstd encoder: %w", err)
		}
		codec.encoder = enc
	}
	start := time.Now()
	out := codec.encoder.EncodeAll(raw, dst[:0])
	report := CodecReport{Encoding: encoding, RequestedCompression: compression, ActualCompression: compression, RawBytes: len(raw), CompressionAttempted: true, CompressionNanos: time.Since(start).Nanoseconds()}
	if len(out) >= len(raw) {
		report.ActualCompression = CompressionNone
		report.CompressionFallbackReason = "not_smaller"
		report.StoredBytes = len(raw)
		return CompressionSelection{Payload: raw, Actual: CompressionNone, Scratch: out[:0], Report: report}, nil
	}
	report.CompressionKept, report.StoredBytes = true, len(out)
	return CompressionSelection{Payload: out, Actual: compression, Scratch: out, Report: report}, nil
}
