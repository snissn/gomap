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
	Compression  CompressionOptions
	OmitSlabKeys bool
}

type compressionConfig struct {
	kind       CompressionKind
	minBytes   int
	minSavings int
	level      zstd.EncoderLevel
	zstdEncs   *sync.Pool
	zstdDecs   *sync.Pool
}

func (c *compressionConfig) newDictEncoderPool(dict []byte, dictID uint32, raw bool) *sync.Pool {
	if c == nil || c.kind == CompressionNone {
		return nil
	}
	return &sync.Pool{
		New: func() any {
			opts := []zstd.EOption{
				zstd.WithEncoderLevel(c.level),
				zstd.WithEncoderCRC(false),
			}
			if raw {
				opts = append(opts, zstd.WithEncoderDictRaw(dictID, dict))
			} else {
				opts = append(opts, zstd.WithEncoderDict(dict))
			}
			enc, _ := zstd.NewWriter(nil, opts...)
			return enc
		},
	}
}

func (c *compressionConfig) newDictDecoderPool(dict []byte, dictID uint32, raw bool) *sync.Pool {
	if c == nil || c.kind == CompressionNone {
		return nil
	}
	return &sync.Pool{
		New: func() any {
			var opts []zstd.DOption
			if raw {
				opts = append(opts, zstd.WithDecoderDictRaw(dictID, dict))
			} else {
				opts = append(opts, zstd.WithDecoderDicts(dict))
			}
			dec, _ := zstd.NewReader(nil, opts...)
			return dec
		},
	}
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

func (c *compressionConfig) compressValueWithPool(value []byte, encPool *sync.Pool) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || encPool == nil {
		return value, false, nil
	}
	if len(value) < c.minBytes {
		return value, false, nil
	}
	enc := encPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(value, nil)
	encPool.Put(enc)
	if len(compressed)+compressedHeaderSize+c.minSavings >= len(value) {
		return value, false, nil
	}
	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(len(value)))
	copy(out[compressedHeaderSize:], compressed)
	return out, true, nil
}

func (c *compressionConfig) compressRecordWithPool(key, value []byte, encPool *sync.Pool) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || encPool == nil {
		return nil, false, nil
	}
	combinedLen := 2 + len(key) + len(value)
	if combinedLen < c.minBytes {
		return nil, false, nil
	}
	combined := make([]byte, combinedLen)
	binary.LittleEndian.PutUint16(combined[:2], uint16(len(key)))
	copy(combined[2:2+len(key)], key)
	copy(combined[2+len(key):], value)
	enc := encPool.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(combined, nil)
	encPool.Put(enc)
	if len(compressed)+compressedHeaderSize+c.minSavings >= combinedLen {
		return nil, false, nil
	}
	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(combinedLen))
	copy(out[compressedHeaderSize:], compressed)
	return out, true, nil
}

func (c *compressionConfig) decompressValueWithPool(encoded []byte, decPool *sync.Pool) ([]byte, error) {
	if decPool == nil {
		return nil, errCompressedCorrupt
	}
	if len(encoded) < compressedHeaderSize {
		return nil, errCompressedCorrupt
	}
	rawLen := binary.LittleEndian.Uint32(encoded[:compressedHeaderSize])
	payload := encoded[compressedHeaderSize:]
	dec := decPool.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
	decPool.Put(dec)
	if err != nil {
		return nil, err
	}
	if uint32(len(out)) != rawLen {
		return nil, errCompressedCorrupt
	}
	return out, nil
}

func (c *compressionConfig) decompressRecordWithPool(encoded []byte, decPool *sync.Pool) ([]byte, []byte, error) {
	decompressed, err := c.decompressValueWithPool(encoded, decPool)
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

func (c *compressionConfig) compressValue(value []byte) ([]byte, bool, error) {
	if c == nil || c.kind == CompressionNone || c.zstdEncs == nil {
		return value, false, nil
	}
	if len(value) < c.minBytes {
		return value, false, nil
	}

	enc := c.zstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(value, nil)
	c.zstdEncs.Put(enc)

	if len(compressed)+compressedHeaderSize+c.minSavings >= len(value) {
		return value, false, nil
	}

	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(len(value)))
	copy(out[compressedHeaderSize:], compressed)
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

	// Prepare combined buffer
	combined := make([]byte, combinedLen)
	binary.LittleEndian.PutUint16(combined[:2], uint16(len(key)))
	copy(combined[2:2+len(key)], key)
	copy(combined[2+len(key):], value)

	enc := c.zstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(combined, nil)
	c.zstdEncs.Put(enc)

	if len(compressed)+compressedHeaderSize+c.minSavings >= combinedLen {
		return nil, false, nil
	}

	out := make([]byte, compressedHeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:compressedHeaderSize], uint32(combinedLen))
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
