package compression

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/snissn/compress/zstd"
)

const (
	DefaultMinBytes   = 256
	DefaultMinSavings = 16
	HeaderSize        = 4
)

var ErrCorrupt = errors.New("compression: invalid compressed value")

type Config struct {
	Kind       Kind
	MinBytes   int
	MinSavings int
	Level      zstd.EncoderLevel
	ZstdEncs   *sync.Pool
	ZstdDecs   *sync.Pool
	BufferPool *sync.Pool
}

func NormalizeOptions(opts Options) (Config, error) {
	cfg := Config{
		Kind:       opts.Kind,
		MinBytes:   opts.MinBytes,
		MinSavings: opts.MinSavingsBytes,
	}
	if cfg.MinBytes <= 0 {
		cfg.MinBytes = DefaultMinBytes
	}
	if cfg.MinSavings <= 0 {
		cfg.MinSavings = DefaultMinSavings
	}
	if opts.Level == 0 {
		cfg.Level = zstd.SpeedFastest
	} else {
		cfg.Level = zstd.EncoderLevel(opts.Level)
	}

	cfg.ZstdDecs = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil)
			return dec
		},
	}

	cfg.BufferPool = &sync.Pool{
		New: func() any {
			b := make([]byte, 0, 64*1024)
			return &b
		},
	}

	if opts.Kind == KindZSTD {
		cfg.ZstdEncs = &sync.Pool{
			New: func() any {
				enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(cfg.Level), zstd.WithEncoderCRC(false))
				return enc
			},
		}
	}

	return cfg, nil
}

func (c *Config) CompressValue(value []byte) ([]byte, bool, error) {
	if c == nil || c.Kind == KindNone || c.ZstdEncs == nil {
		return value, false, nil
	}
	if len(value) < c.MinBytes {
		return value, false, nil
	}

	pbuf := c.BufferPool.Get().(*[]byte)
	dst := (*pbuf)[:0]

	enc := c.ZstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(value, dst)
	c.ZstdEncs.Put(enc)

	if len(compressed)+HeaderSize+c.MinSavings >= len(value) {
		*pbuf = compressed
		c.BufferPool.Put(pbuf)
		return value, false, nil
	}

	out := make([]byte, HeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:HeaderSize], uint32(len(value)))
	copy(out[HeaderSize:], compressed)

	*pbuf = compressed
	c.BufferPool.Put(pbuf)
	return out, true, nil
}

func (c *Config) CompressValuePooled(value []byte) ([]byte, bool, func(), error) {
	if c == nil || c.Kind == KindNone || c.ZstdEncs == nil {
		return value, false, nil, nil
	}
	if len(value) < c.MinBytes {
		return value, false, nil, nil
	}

	pbuf := c.BufferPool.Get().(*[]byte)
	dst := (*pbuf)[:0]

	enc := c.ZstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(value, dst)
	c.ZstdEncs.Put(enc)

	if len(compressed)+HeaderSize+c.MinSavings >= len(value) {
		*pbuf = compressed
		c.BufferPool.Put(pbuf)
		return value, false, nil, nil
	}

	needed := HeaderSize + len(compressed)
	if cap(*pbuf) < needed {
		out := make([]byte, needed)
		binary.LittleEndian.PutUint32(out[:HeaderSize], uint32(len(value)))
		copy(out[HeaderSize:], compressed)
		*pbuf = compressed
		c.BufferPool.Put(pbuf)
		return out, true, nil, nil
	}

	out := (*pbuf)[:needed]
	binary.LittleEndian.PutUint32(out[:HeaderSize], uint32(len(value)))
	copy(out[HeaderSize:], compressed)

	release := func() {
		*pbuf = out
		c.BufferPool.Put(pbuf)
	}

	return out, true, release, nil
}

func (c *Config) CompressRecord(key, value []byte) ([]byte, bool, error) {
	if c == nil || c.Kind == KindNone || c.ZstdEncs == nil {
		return nil, false, nil
	}
	combinedLen := 2 + len(key) + len(value)
	if combinedLen < c.MinBytes {
		return nil, false, nil
	}

	pCombined := c.BufferPool.Get().(*[]byte)
	combined := (*pCombined)[:0]
	if cap(combined) < combinedLen {
		combined = make([]byte, 0, combinedLen)
	}
	combined = combined[:combinedLen]

	binary.LittleEndian.PutUint16(combined[:2], uint16(len(key)))
	copy(combined[2:2+len(key)], key)
	copy(combined[2+len(key):], value)

	pDst := c.BufferPool.Get().(*[]byte)
	dst := (*pDst)[:0]

	enc := c.ZstdEncs.Get().(*zstd.Encoder)
	compressed := enc.EncodeAll(combined, dst)
	c.ZstdEncs.Put(enc)

	*pCombined = combined
	c.BufferPool.Put(pCombined)

	if len(compressed)+HeaderSize+c.MinSavings >= combinedLen {
		*pDst = compressed
		c.BufferPool.Put(pDst)
		return nil, false, nil
	}

	out := make([]byte, HeaderSize+len(compressed))
	binary.LittleEndian.PutUint32(out[:HeaderSize], uint32(combinedLen))
	copy(out[HeaderSize:], compressed)

	*pDst = compressed
	c.BufferPool.Put(pDst)
	return out, true, nil
}

func (c *Config) DecompressValue(encoded []byte) ([]byte, error) {
	if len(encoded) < HeaderSize {
		return nil, ErrCorrupt
	}
	rawLen := binary.LittleEndian.Uint32(encoded[:HeaderSize])
	payload := encoded[HeaderSize:]

	if c.ZstdDecs == nil {
		return nil, ErrCorrupt
	}
	dec := c.ZstdDecs.Get().(*zstd.Decoder)
	out, err := dec.DecodeAll(payload, make([]byte, 0, rawLen))
	c.ZstdDecs.Put(dec)
	if err != nil {
		return nil, err
	}
	if uint32(len(out)) != rawLen {
		return nil, ErrCorrupt
	}
	return out, nil
}

func (c *Config) DecompressRecord(encoded []byte) ([]byte, []byte, error) {
	decompressed, err := c.DecompressValue(encoded)
	if err != nil {
		return nil, nil, err
	}
	if len(decompressed) < 2 {
		return nil, nil, ErrCorrupt
	}
	keyLen := int(binary.LittleEndian.Uint16(decompressed[:2]))
	if len(decompressed) < 2+keyLen {
		return nil, nil, ErrCorrupt
	}
	return decompressed[2 : 2+keyLen], decompressed[2+keyLen:], nil
}
