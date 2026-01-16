package slab

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const (
	defaultCompressionMinBytes   = 256
	defaultCompressionMinSavings = 16
	compressedHeaderSize         = 4
)

var errCompressedCorrupt = errors.New("slab: invalid compressed value")

type CompressionKind uint8

const (
	CompressionNone CompressionKind = iota
	CompressionZSTD
)

type CompressionOptions struct {
	Kind            CompressionKind
	MinBytes        int
	MinSavingsBytes int
	Level           int
}

type Options struct {
	Compression                            CompressionOptions
	OmitSlabKeys                           bool
	CompressionMetrics                     bool
	CompressionMetricsWindowBytes          int
	CompressionAdaptiveRatio               float64
	CompressionAdaptivePauseBytes          int
	CompressionAdaptiveMinRecords          int
	CompressionAdaptiveTrainBytes          int
	CompressionAdaptiveTrainDictBytes      int
	CompressionAdaptiveTrainMinRecords     int
	CompressionAdaptiveTrainMaxRecordBytes int
	CompressionAdaptiveTrainSampleStride   int
	CompressionAdaptiveTrainDedupWindow    int
}

type compressionConfig struct {
	kind       CompressionKind
	minBytes   int
	minSavings int
	level      zstd.EncoderLevel
	zstdEncs   *sync.Pool
	zstdDecs   *sync.Pool
	bufferPool *sync.Pool
}

func normalizeCompressionOptions(opts CompressionOptions) (compressionConfig, error) {
	cfg := compressionConfig{
		kind:       opts.Kind,
		minBytes:   opts.MinBytes,
		minSavings: opts.MinSavingsBytes,
	}
	if cfg.minBytes <= 0 {
		cfg.minBytes = defaultCompressionMinBytes
	}
	if cfg.minSavings <= 0 {
		cfg.minSavings = defaultCompressionMinSavings
	}
	if opts.Level == 0 {
		cfg.level = zstd.SpeedFastest
	} else {
		cfg.level = zstd.EncoderLevel(opts.Level)
	}

	cfg.zstdDecs = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}

	cfg.bufferPool = &sync.Pool{
		New: func() any {
			// Start with a reasonable default capacity (e.g., 64KB).
			// Buffers will grow as needed.
			b := make([]byte, 0, 64*1024)
			return &b
		},
	}

	if opts.Kind == CompressionZSTD {
		cfg.zstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(cfg.level), zstd.WithEncoderCRC(false))
				return enc
			},
		}
	}

	return cfg, nil
}

func (c *compressionConfig) compressValue(value []byte) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || c.zstdEncs == nil {
		return value, false, nil
	}
	if len(value) < c.minBytes {
		return value, false, nil
	}

	// Use a pooled buffer for the compression destination.
	pbuf := c.bufferPool.Get().(*[]byte)
	dst := (*pbuf)[:0]

	enc := c.zstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(value, dst)
	c.zstdEncs.Put(enc)

	if len(compressed)+compressedHeaderSize+c.minSavings >= len(value) {
		*pbuf = compressed
		c.bufferPool.Put(pbuf)
		return value, false, nil
	}

	// Final output still requires a copy because it's passed out of the manager,
	// but we've avoided the internal intermediate allocations.
	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(len(value)))
	copy(out[compressedHeaderSize:], compressed)

	*pbuf = compressed
	c.bufferPool.Put(pbuf)
	return out, true, nil
}

func (c *compressionConfig) compressRecord(key, value []byte) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || c.zstdEncs == nil {
		return nil, false, nil
	}
	// combined length: 2 (keyLen) + key + value
	combinedLen := 2 + len(key) + len(value)
	if combinedLen < c.minBytes {
		return nil, false, nil
	}

	// Use pooled buffer for combined record assembly
	pCombined := c.bufferPool.Get().(*[]byte)
	combined := (*pCombined)[:0]
	if cap(combined) < combinedLen {
		combined = make([]byte, 0, combinedLen)
	}
	combined = combined[:combinedLen]

	binary.LittleEndian.PutUint16(combined[:2], uint16(len(key)))
	copy(combined[2:2+len(key)], key)
	copy(combined[2+len(key):], value)

	// Use another pooled buffer for compression destination
	pDst := c.bufferPool.Get().(*[]byte)
	dst := (*pDst)[:0]

	enc := c.zstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(combined, dst)
	c.zstdEncs.Put(enc)

	// Return combined buffer to pool early
	*pCombined = combined
	c.bufferPool.Put(pCombined)

	if len(compressed)+compressedHeaderSize+c.minSavings >= combinedLen {
		*pDst = compressed
		c.bufferPool.Put(pDst)
		return nil, false, nil
	}

	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(combinedLen))
	copy(out[compressedHeaderSize:], compressed)

	*pDst = compressed
	c.bufferPool.Put(pDst)
	return out, true, nil
}

func (c *compressionConfig) decompressValue(encoded []byte) ([]byte, error) {
	if len(encoded) < compressedHeaderSize {
		return nil, errCompressedCorrupt
	}
	rawLen := binary.LittleEndian.Uint32(encoded[:compressedHeaderSize])
	payload := encoded[compressedHeaderSize:]

	if c.zstdDecs == nil {
		return nil, errCompressedCorrupt
	}
	dec := c.zstdDecs.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
	c.zstdDecs.Put(dec)
	if err != nil {
		return nil, err
	}
	if uint32(len(out)) != rawLen {
		return nil, errCompressedCorrupt
	}
	return out, nil
}

func (c *compressionConfig) decompressRecord(encoded []byte) ([]byte, []byte, error) {
	decompressed, err := c.decompressValue(encoded)
	if err != nil {
		return nil, nil, err
	}
	if len(decompressed) < 2 {
		return nil, nil, errCompressedCorrupt
	}
	keyLen := int(binary.LittleEndian.Uint16(decompressed[:2]))
	if len(decompressed) < 2+keyLen {
		return nil, nil, errCompressedCorrupt
	}
	return decompressed[2 : 2+keyLen], decompressed[2+keyLen:], nil
}
