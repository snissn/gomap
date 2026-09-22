package valuelog

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/snissn/compress/zstd"
)

type dictCodecKey struct {
	dictID    uint64
	level     zstd.EncoderLevel
	noEntropy bool
}

type dictCodecEntry struct {
	key      dictCodecKey
	dictCopy []byte
	encPool  *sync.Pool
	decPool  *sync.Pool
}

type dictCodecCache struct {
	mu    sync.Mutex
	cap   int
	ll    *list.List
	items map[dictCodecKey]*list.Element
}

const defaultDictCodecCacheSize = 64

var dictCodecs = newDictCodecCache(defaultDictCodecCacheSize)

type dictCodecsFastEntry struct {
	key   dictCodecKey
	entry *dictCodecEntry
}

var dictCodecsFast atomic.Pointer[dictCodecsFastEntry]

func newDictCodecCache(capacity int) *dictCodecCache {
	if capacity < 0 {
		capacity = 0
	}
	return &dictCodecCache{
		cap:   capacity,
		ll:    list.New(),
		items: make(map[dictCodecKey]*list.Element),
	}
}

func (c *dictCodecCache) getOrAdd(dictID uint64, dict []byte, level zstd.EncoderLevel, noEntropy bool) *dictCodecEntry {
	if c == nil || c.cap == 0 || dictID == 0 || len(dict) == 0 {
		return nil
	}
	level = normalizeDictFrameEncodeLevel(level)
	key := dictCodecKey{dictID: dictID, level: level, noEntropy: noEntropy}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		c.ll.MoveToFront(elem)
		return elem.Value.(*dictCodecEntry)
	}
	// Copy dict bytes so pooled encoders/decoders never reference an ephemeral slice.
	dictCopy := append([]byte(nil), dict...)
	entry := &dictCodecEntry{
		key:      key,
		dictCopy: dictCopy,
	}
	enc0, err := zstd.NewWriter(nil,
		zstd.WithEncoderDict(dictCopy),
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderCRC(false),
		// Trade ratio for throughput: dict-compressed payload streams tend to be
		// match-heavy, so literal entropy coding can be an expensive marginal win.
		zstd.WithNoEntropyCompression(noEntropy),
	)
	if err != nil {
		return nil
	}
	dec0, err := zstd.NewReader(nil, zstd.WithDecoderDicts(dictCopy))
	if err != nil {
		enc0.Close()
		return nil
	}
	entry.encPool = &sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil,
				zstd.WithEncoderDict(dictCopy),
				zstd.WithEncoderLevel(level),
				zstd.WithEncoderConcurrency(1),
				zstd.WithEncoderCRC(false),
				zstd.WithNoEntropyCompression(noEntropy),
			)
			return enc
		},
	}
	entry.decPool = &sync.Pool{
		New: func() any {
			dec, _ := zstd.NewReader(nil, zstd.WithDecoderDicts(dictCopy))
			return dec
		},
	}
	entry.encPool.Put(enc0)
	entry.decPool.Put(dec0)
	elem := c.ll.PushFront(entry)
	c.items[key] = elem
	if c.ll.Len() > c.cap {
		tail := c.ll.Back()
		if tail != nil {
			c.ll.Remove(tail)
			old := tail.Value.(*dictCodecEntry)
			delete(c.items, old.key)
		}
	}
	return entry
}

func getDictCodecs(dictID uint64, dict []byte) *dictCodecEntry {
	return getDictCodecsWithOpts(dictID, dict, zstd.SpeedFastest, true)
}

func getDictCodecsWithOpts(dictID uint64, dict []byte, level zstd.EncoderLevel, noEntropy bool) *dictCodecEntry {
	if dictID == 0 {
		return nil
	}
	level = normalizeDictFrameEncodeLevel(level)
	key := dictCodecKey{dictID: dictID, level: level, noEntropy: noEntropy}
	if fast := dictCodecsFast.Load(); fast != nil && fast.entry != nil && fast.key == key {
		return fast.entry
	}
	entry := dictCodecs.getOrAdd(dictID, dict, level, noEntropy)
	if entry != nil {
		dictCodecsFast.Store(&dictCodecsFastEntry{key: key, entry: entry})
	}
	return entry
}
