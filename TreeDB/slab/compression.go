package slab

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/snissn/compress/zstd"
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
}

type Options struct {
	Compression CompressionOptions
}

type compressionConfig struct {
	kind       CompressionKind
	minBytes   int
	minSavings int
	zstdEnc    *zstd.Encoder
	zstdDecs   *sync.Pool
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

	cfg.zstdDecs = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}

	if opts.Kind == CompressionZSTD {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
		if err != nil {
			return compressionConfig{}, err
		}
		cfg.zstdEnc = enc
	}

	return cfg, nil
}

func (c *compressionConfig) compressValue(value []byte) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || c.zstdEnc == nil {
		return value, false, nil
	}
	if len(value) < c.minBytes {
		return value, false, nil
	}

	compressed := c.zstdEnc.EncodeAll(value, nil)
	if len(compressed)+compressedHeaderSize+c.minSavings >= len(value) {
		return value, false, nil
	}

	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(len(value)))
	copy(out[compressedHeaderSize:], compressed)
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
