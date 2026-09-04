package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)

const (
	pageSize                  = 4096
	nodeHeaderSize            = 16
	directoryEntrySize        = 2
	valuePtrSize              = 16
	leafPrefixRestartInterval = 16
	benchKeySize              = 32
	benchValueSize            = 128
	benchKeyCount             = 1 << 12
	smallSearchThreshold      = 16

	flagInline    = 0x00
	flagPointer   = 0x01
	flagTombstone = 0x02

	leafColumnarV2MetaSize       = 3
	leafColumnarPrefixV2MetaSize = 7
)

type options struct {
	prefix   bool
	columnar bool
	keyKind  keyKind
}

type keyKind uint8

const (
	keyKindBytes keyKind = iota
	keyKindFixedBE8
)

type columnarEntry struct {
	keyOff    int
	keyLen    int
	valueOff  int
	valueLen  int
	flags     byte
	prefixLen int
}

type builder struct {
	data       []byte
	opts       options
	count      int
	heapStart  int
	dirEnd     int
	leafIndex  int
	prevKeyBuf [64]byte
	prevKey    []byte
	arena      []byte
	valueArena []byte
	entries    []columnarEntry
	keyBytes   int
	valueBytes int
}

type page struct {
	data  []byte
	opts  options
	count int
}

type prefixLayout struct {
	prefixLen int
	suffixLen int
	keyOff    int
}

type splitmix64 struct {
	state uint64
}

var sink uint64

func main() {
	buildIters := envIntAny([]string{"MATCHED_GO_LEAF_BUILD_ITERS", "LEAF_BUILD_ITERS"}, 500_000)
	searchIters := envIntAny([]string{"MATCHED_GO_LEAF_SEARCH_ITERS", "LEAF_SEARCH_ITERS"}, 2_000_000)
	selectedCase := os.Getenv("LEAF_CASE")
	ran := false

	if caseEnabled(selectedCase, "builder/no_prefix") {
		values := makeBenchValues(benchKeyCount)
		keys := makeBenchKeys(benchKeyCount, 0)
		runCase("builder/no_prefix", buildIters, func() uint64 {
			return benchBuilderPrepared(buildIters, false, keys, values)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "builder/prefix_heavy") {
		values := makeBenchValues(benchKeyCount)
		keys := makeBenchKeys(benchKeyCount, 16)
		runCase("builder/prefix_heavy", buildIters, func() uint64 {
			return benchBuilderPrepared(buildIters, true, keys, values)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "builder/prefix_light") {
		values := makeBenchValues(benchKeyCount)
		keys := makeBenchKeys(benchKeyCount, 2)
		runCase("builder/prefix_light", buildIters, func() uint64 {
			return benchBuilderPrepared(buildIters, true, keys, values)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "search/columnar_fixed_be8") {
		page, queries := setupSearchColumnar(true)
		runCase("search/columnar_fixed_be8", searchIters, func() uint64 {
			return benchSearchPrepared(searchIters, page, queries)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "search/columnar_variable16") {
		page, queries := setupSearchColumnar(false)
		runCase("search/columnar_variable16", searchIters, func() uint64 {
			return benchSearchPrepared(searchIters, page, queries)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "search/columnar_variable_len") {
		page, queries := setupSearchColumnarVariableLen()
		runCase("search/columnar_variable_len", searchIters, func() uint64 {
			return benchSearchPrepared(searchIters, page, queries)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "search/prefix_v2") {
		page, queries := setupSearchPrefixVariant(options{prefix: true})
		runCase("search/prefix_v2", searchIters, func() uint64 {
			return benchSearchPrepared(searchIters, page, queries)
		})
		ran = true
	}
	if caseEnabled(selectedCase, "search/columnar_prefix_v2") {
		page, queries := setupSearchPrefixVariant(options{prefix: true, columnar: true})
		runCase("search/columnar_prefix_v2", searchIters, func() uint64 {
			return benchSearchPrepared(searchIters, page, queries)
		})
		ran = true
	}
	if !ran {
		fmt.Fprintf(os.Stderr, "unknown LEAF_CASE=%s\n", selectedCase)
		os.Exit(2)
	}
}

func caseEnabled(selected, name string) bool {
	return selected == "" || selected == name
}

func envIntAny(names []string, def int) int {
	for _, name := range names {
		raw := os.Getenv(name)
		if raw == "" {
			continue
		}
		n, err := strconv.Atoi(raw)
		if err == nil && n > 0 {
			return n
		}
	}
	return def
}

func runCase(name string, iters int, fn func() uint64) {
	start := time.Now()
	sum := fn()
	elapsed := time.Since(start)
	ns := float64(elapsed.Nanoseconds()) / float64(iters)
	sink ^= sum
	fmt.Printf("MATCHED_GO\t%s\t%.2f\t%d\t%d\n", name, ns, iters, sum)
}

func (r *splitmix64) next() uint64 {
	r.state += 0x9e3779b97f4a7c15
	z := r.state
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (r *splitmix64) fill(dst []byte) {
	for len(dst) > 0 {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], r.next())
		n := copy(dst, tmp[:])
		dst = dst[n:]
	}
}

func makeBenchKeys(count, prefixBytes int) [][]byte {
	keys := make([][]byte, count)
	rng := splitmix64{state: 1}
	for i := range keys {
		k := make([]byte, benchKeySize)
		p := prefixBytes
		if p > len(k) {
			p = len(k)
		}
		for j := 0; j < p; j++ {
			k[j] = 0x42
		}
		rng.fill(k[p:])
		keys[i] = k
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	return keys
}

func makeBenchValues(count int) [][]byte {
	values := make([][]byte, count)
	rng := splitmix64{state: 2}
	for i := range values {
		v := make([]byte, benchValueSize)
		rng.fill(v)
		values[i] = v
	}
	return values
}

func be8(v uint64) []byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], v)
	return append([]byte(nil), out[:]...)
}

func be16(a, b uint64) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[:8], a)
	binary.BigEndian.PutUint64(out[8:], b)
	return out
}

func fillVariableLenKey(dst []byte, i int) []byte {
	keyLen := 9 + (i % (benchKeySize - 8))
	binary.BigEndian.PutUint64(dst[:8], uint64(i))
	for j := 8; j < keyLen; j++ {
		dst[j] = byte(i*31 + (j - 8))
	}
	return dst[:keyLen]
}

func benchBuilderPrepared(iters int, prefix bool, keys, values [][]byte) uint64 {
	b := newBuilder(options{prefix: prefix})
	var checksum uint64
	for i := 0; i < iters; {
		idx := i & (len(keys) - 1)
		entrySize, prefixLen, suffixLen := b.leafEntrySizeWithPrefix(keys[idx], values[idx], flagInline)
		if b.addLeafEntryWithPrefix(keys[idx], values[idx], flagInline, entrySize, prefixLen, suffixLen) {
			checksum += uint64(b.count)
			i++
			continue
		}
		b.reset(options{prefix: prefix})
	}
	return checksum
}

func setupSearchColumnar(fixedBE8 bool) (*page, [][]byte) {
	kind := keyKindBytes
	if fixedBE8 {
		kind = keyKindFixedBE8
	}
	b := newBuilder(options{columnar: true, keyKind: kind})
	inserted := 0
	for i := 0; i < benchKeyCount; i++ {
		key := be8(uint64(i))
		if !fixedBE8 {
			key = be16(uint64(i), uint64(i*17+3))
		}
		if !b.addLeafEntry(key, nil, flagPointer) {
			break
		}
		inserted++
	}
	p := b.finish()
	queries := make([][]byte, inserted)
	for i := range queries {
		if fixedBE8 {
			queries[i] = be8(uint64(i))
		} else {
			queries[i] = be16(uint64(i), uint64(i*17+3))
		}
	}
	return p, queries
}

func setupSearchColumnarVariableLen() (*page, [][]byte) {
	b := newBuilder(options{columnar: true, keyKind: keyKindBytes})
	inserted := 0
	var key [benchKeySize]byte
	for i := 0; i < benchKeyCount; i++ {
		if !b.addLeafEntry(fillVariableLenKey(key[:], i), nil, flagPointer) {
			break
		}
		inserted++
	}

	p := b.finish()
	queries := make([][]byte, inserted)
	for i := range queries {
		q := make([]byte, len(fillVariableLenKey(key[:], i)))
		copy(q, fillVariableLenKey(key[:], i))
		queries[i] = q
	}
	return p, queries
}

func benchSearchPrepared(iters int, p *page, queries [][]byte) uint64 {
	var checksum uint64
	for i := 0; i < iters; i++ {
		idx, found := p.searchLeaf(queries[i%len(queries)])
		checksum += uint64(idx)
		if found {
			checksum++
		}
	}
	return checksum
}

func setupSearchPrefixVariant(opts options) (*page, [][]byte) {
	const keyCount = 128
	keys := makeBenchKeys(keyCount, 24)
	b := newBuilder(opts)
	for i, key := range keys {
		flags := byte(flagInline)
		var value []byte
		switch i % 3 {
		case 0:
			value = []byte{byte(i), byte(i + 1)}
		case 1:
			flags = flagPointer
		default:
			flags = flagTombstone
		}
		if !b.addLeafEntry(key, value, flags) {
			panic("prefix setup should fit")
		}
	}
	p := b.finish()
	queries := make([][]byte, 4096)
	for i := range queries {
		q := append([]byte(nil), keys[i%len(keys)]...)
		if i&1 == 1 && len(q) > 0 {
			q[len(q)-1] ^= 0x01
		}
		queries[i] = q
	}
	return p, queries
}

func newBuilder(opts options) *builder {
	b := &builder{
		data: make([]byte, pageSize),
	}
	b.reset(opts)
	return b
}

func (b *builder) reset(opts options) {
	b.opts = opts
	b.count = 0
	b.heapStart = pageSize
	b.dirEnd = nodeHeaderSize
	b.leafIndex = 0
	b.prevKey = nil
	b.arena = b.arena[:0]
	b.valueArena = b.valueArena[:0]
	b.entries = b.entries[:0]
	b.keyBytes = 0
	b.valueBytes = 0
}

func (b *builder) rememberPrevKey(key []byte) {
	if len(key) <= len(b.prevKeyBuf) {
		b.prevKey = b.prevKeyBuf[:len(key)]
	} else {
		if cap(b.prevKey) < len(key) {
			b.prevKey = make([]byte, len(key))
		}
		b.prevKey = b.prevKey[:len(key)]
	}
	copy(b.prevKey, key)
}

func (b *builder) addLeafEntry(key, value []byte, flags byte) bool {
	entrySize, prefixLen, suffixLen := b.leafEntrySizeWithPrefix(key, value, flags)
	return b.addLeafEntryWithPrefix(key, value, flags, entrySize, prefixLen, suffixLen)
}

func (b *builder) leafEntrySizeWithPrefix(key, value []byte, flags byte) (entrySize, prefixLen, suffixLen int) {
	suffixLen = len(key)
	if b.opts.prefix && b.leafIndex%leafPrefixRestartInterval != 0 && len(b.prevKey) > 0 {
		prefixLen = sharedPrefixLen(key, b.prevKey)
		if prefixLen > len(key) {
			prefixLen = len(key)
		}
		suffixLen = len(key) - prefixLen
	}
	valSize := valueSize(value, flags)
	if b.opts.columnar && b.opts.prefix {
		return suffixLen + valSize + leafColumnarPrefixV2MetaSize, prefixLen, suffixLen
	}
	if b.opts.columnar {
		return suffixLen + valSize + leafColumnarV2MetaSize, 0, suffixLen
	}
	headerSize := 7
	if b.opts.prefix {
		headerSize = leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, len(value))
	}
	return headerSize + suffixLen + valSize, prefixLen, suffixLen
}

func (b *builder) addLeafEntryWithPrefix(key, value []byte, flags byte, entrySize, prefixLen, suffixLen int) bool {
	if b.opts.columnar && b.opts.prefix {
		return b.addLeafEntryColumnarPrefixV2(key, value, flags, prefixLen, suffixLen)
	}
	if b.opts.columnar {
		return b.addLeafEntryColumnarV2(key, value, flags)
	}

	if b.opts.prefix && b.leafIndex%leafPrefixRestartInterval == 0 {
		prefixLen = 0
		suffixLen = len(key)
	}
	required := entrySize + directoryEntrySize
	if b.heapStart < b.dirEnd+required {
		return false
	}
	entryStart := b.heapStart - entrySize
	off := entryStart
	headerSize := 7
	if b.opts.prefix {
		headerSize = leafPrefixHeaderSizeV2(prefixLen, suffixLen, flags, len(value))
		extended := prefixLen > 254 || suffixLen > 254
		if extended {
			b.data[off] = 0xff
			b.data[off+1] = 0xff
		} else {
			b.data[off] = byte(prefixLen)
			b.data[off+1] = byte(suffixLen)
		}
		b.data[off+2] = flags
		off += 3
		if extended {
			putU16(b.data[off:off+2], uint16(prefixLen))
			putU16(b.data[off+2:off+4], uint16(suffixLen))
			off += 4
		}
		if flags&flagPointer == 0 && flags&flagTombstone == 0 {
			putUvarint(b.data[off:], uint64(len(value)))
		}
	} else {
		putU16(b.data[off:off+2], uint16(len(key)))
		putU32(b.data[off+2:off+6], uint32(len(value)))
		b.data[off+6] = flags
	}

	keyStart := entryStart + headerSize
	copy(b.data[keyStart:keyStart+suffixLen], key[prefixLen:])
	valueStart := keyStart + suffixLen
	if flags&flagPointer != 0 {
		clear(b.data[valueStart : valueStart+valuePtrSize])
	} else if flags&flagTombstone == 0 {
		copy(b.data[valueStart:valueStart+len(value)], value)
	}

	putU16(b.data[b.dirEnd:b.dirEnd+directoryEntrySize], uint16(entryStart))
	b.heapStart = entryStart
	b.dirEnd += directoryEntrySize
	b.count++
	b.leafIndex++
	if b.opts.prefix {
		b.rememberPrevKey(key)
	}
	return true
}

func (b *builder) addLeafEntryColumnarV2(key, value []byte, flags byte) bool {
	valSize := valueSize(value, flags)
	entrySize := leafColumnarV2MetaSize + len(key) + valSize
	if b.heapStart < b.dirEnd+entrySize+directoryEntrySize {
		return false
	}
	keyOff := len(b.arena)
	b.arena = append(b.arena, key...)
	valueOff := len(b.arena)
	if flags&flagPointer != 0 {
		b.arena = append(b.arena, make([]byte, valuePtrSize)...)
	} else if flags&flagTombstone == 0 {
		b.arena = append(b.arena, value...)
	}
	b.entries = append(b.entries, columnarEntry{
		keyOff:   keyOff,
		keyLen:   len(key),
		valueOff: valueOff,
		valueLen: len(value),
		flags:    flags,
	})
	b.keyBytes += len(key)
	b.valueBytes += valSize
	b.dirEnd += directoryEntrySize + leafColumnarV2MetaSize
	b.heapStart -= len(key) + valSize
	b.count++
	b.leafIndex++
	return true
}

func (b *builder) addLeafEntryColumnarPrefixV2(key, value []byte, flags byte, prefixLen, suffixLen int) bool {
	if b.leafIndex%leafPrefixRestartInterval == 0 {
		prefixLen = 0
		suffixLen = len(key)
	}
	valSize := valueSize(value, flags)
	nextCount := b.count + 1
	nextKeyBytes := b.keyBytes + suffixLen
	nextValueBytes := b.valueBytes + valSize
	dirEnd := nodeHeaderSize + nextCount*leafColumnarPrefixV2MetaSize
	heapStart := pageSize - (nextKeyBytes + nextValueBytes)
	if heapStart < dirEnd {
		return false
	}
	keyOff := len(b.arena)
	b.arena = append(b.arena, key[prefixLen:]...)
	valueOff := len(b.valueArena)
	if flags&flagPointer != 0 {
		b.valueArena = append(b.valueArena, make([]byte, valuePtrSize)...)
	} else if flags&flagTombstone == 0 {
		b.valueArena = append(b.valueArena, value...)
	}
	b.entries = append(b.entries, columnarEntry{
		keyOff:    keyOff,
		keyLen:    suffixLen,
		valueOff:  valueOff,
		valueLen:  len(value),
		flags:     flags,
		prefixLen: prefixLen,
	})
	b.keyBytes = nextKeyBytes
	b.valueBytes = nextValueBytes
	b.count = nextCount
	b.leafIndex++
	b.dirEnd = dirEnd
	b.heapStart = heapStart
	b.rememberPrevKey(key)
	return true
}

func (b *builder) finish() *page {
	if b.opts.columnar && b.opts.prefix {
		b.finishColumnarPrefixV2()
	} else if b.opts.columnar {
		b.finishColumnarV2()
	}
	return &page{data: b.data, opts: b.opts, count: b.count}
}

func (b *builder) finishColumnarV2() {
	count := b.count
	keyDirStart := nodeHeaderSize
	valDirStart := keyDirStart + count*directoryEntrySize
	flagsStart := valDirStart + count*directoryEntrySize
	keysStart := pageSize - b.keyBytes
	valuesStart := keysStart - b.valueBytes
	keyOff := keysStart
	valOff := valuesStart
	for i := range b.entries {
		e := &b.entries[i]
		putU16(b.data[keyDirStart+i*2:keyDirStart+i*2+2], uint16(keyOff))
		putU16(b.data[valDirStart+i*2:valDirStart+i*2+2], uint16(valOff))
		b.data[flagsStart+i] = e.flags
		valSize := e.valueLen
		if e.flags&flagPointer != 0 {
			valSize = valuePtrSize
		} else if e.flags&flagTombstone != 0 {
			valSize = 0
		}
		if valSize > 0 {
			copy(b.data[valOff:valOff+valSize], b.arena[e.valueOff:e.valueOff+valSize])
			valOff += valSize
		}
		copy(b.data[keyOff:keyOff+e.keyLen], b.arena[e.keyOff:e.keyOff+e.keyLen])
		keyOff += e.keyLen
	}
}

func (b *builder) finishColumnarPrefixV2() {
	count := b.count
	keyDirStart := nodeHeaderSize
	valDirStart := keyDirStart + count*directoryEntrySize
	flagsStart := valDirStart + count*directoryEntrySize
	prefixStart := flagsStart + count
	suffixStart := pageSize - b.keyBytes
	valuesStart := suffixStart - b.valueBytes
	copy(b.data[valuesStart:suffixStart], b.valueArena)
	copy(b.data[suffixStart:], b.arena)
	for i := range b.entries {
		e := &b.entries[i]
		putU16(b.data[keyDirStart+i*2:keyDirStart+i*2+2], uint16(suffixStart+e.keyOff))
		putU16(b.data[valDirStart+i*2:valDirStart+i*2+2], uint16(valuesStart+e.valueOff))
		b.data[flagsStart+i] = e.flags
		putU16(b.data[prefixStart+i*2:prefixStart+i*2+2], uint16(e.prefixLen))
	}
}

func (p *page) searchLeaf(key []byte) (int, bool) {
	if p.opts.columnar && p.opts.prefix {
		return p.searchColumnarPrefixV2(key)
	}
	if p.opts.columnar {
		if p.opts.keyKind == keyKindFixedBE8 {
			return p.searchColumnarV2FixedBE8(key)
		}
		return p.searchColumnarV2(key)
	}
	if p.opts.prefix {
		return p.searchPrefixV2(key)
	}
	return p.searchPlain(key)
}

func (p *page) searchPlain(key []byte) (int, bool) {
	lo, hi := 0, p.count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if compareLeafKey(p.plainKeyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < p.count {
		return lo, compareLeafKey(p.plainKeyAt(lo), key) == 0
	}
	return lo, false
}

func (p *page) plainKeyAt(index int) []byte {
	off := p.offsetAt(index)
	keyLen := int(getU16(p.data[off : off+2]))
	return p.data[off+7 : off+7+keyLen]
}

func (p *page) searchColumnarV2(key []byte) (int, bool) {
	if p.count <= smallSearchThreshold {
		for idx := 0; idx < p.count; idx++ {
			cmp := compareLeafKey(p.columnarKeyAt(idx), key)
			if cmp >= 0 {
				return idx, cmp == 0
			}
		}
		return p.count, false
	}
	lo, hi := 0, p.count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if compareLeafKey(p.columnarKeyAt(mid), key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < p.count {
		return lo, compareLeafKey(p.columnarKeyAt(lo), key) == 0
	}
	return lo, false
}

func (p *page) searchColumnarV2FixedBE8(key []byte) (int, bool) {
	target := binary.BigEndian.Uint64(key)
	if p.count <= smallSearchThreshold {
		for idx := 0; idx < p.count; idx++ {
			entry := p.columnarFixedBE8At(idx)
			if entry >= target {
				return idx, entry == target
			}
		}
		return p.count, false
	}
	lo, hi := 0, p.count
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if p.columnarFixedBE8At(mid) < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < p.count {
		return lo, p.columnarFixedBE8At(lo) == target
	}
	return lo, false
}

func (p *page) columnarFixedBE8At(index int) uint64 {
	keyStart := p.offsetAt(index)
	return binary.BigEndian.Uint64(p.data[keyStart : keyStart+8])
}

func (p *page) columnarKeyAt(index int) []byte {
	keyStart := p.offsetAt(index)
	keyEnd := pageSize
	if index+1 < p.count {
		keyEnd = p.offsetAt(index + 1)
	}
	return p.data[keyStart:keyEnd]
}

func (p *page) searchPrefixV2(key []byte) (int, bool) {
	if p.count == 0 {
		return 0, false
	}
	if p.count <= smallSearchThreshold {
		return p.searchPrefixBlock(0, p.count, key)
	}
	restarts := (p.count + leafPrefixRestartInterval - 1) / leafPrefixRestartInterval
	lo, hi := 0, restarts
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		idx := mid * leafPrefixRestartInterval
		if idx >= p.count {
			hi = mid
			continue
		}
		if compareLeafKey(p.prefixRestartKey(idx), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	blockStart := 0
	if lo > 0 {
		blockStart = (lo - 1) * leafPrefixRestartInterval
	}
	blockEnd := blockStart + leafPrefixRestartInterval
	if blockEnd > p.count {
		blockEnd = p.count
	}
	return p.searchPrefixBlock(blockStart, blockEnd, key)
}

func (p *page) searchPrefixBlock(blockStart, blockEnd int, target []byte) (int, bool) {
	if blockStart >= blockEnd {
		return blockEnd, false
	}
	restart := p.prefixRestartKey(blockStart)
	cmp := compareLeafKey(restart, target)
	if cmp >= 0 {
		return blockStart, cmp == 0
	}
	var prev [benchKeySize]byte
	prevLen := len(restart)
	copy(prev[:], restart)
	for idx := blockStart + 1; idx < blockEnd; idx++ {
		off := p.offsetAt(idx)
		layout := parsePrefixLayout(p.data, off)
		suffix := p.data[off+layout.keyOff : off+layout.keyOff+layout.suffixLen]
		keyLen := layout.prefixLen + layout.suffixLen
		copy(prev[layout.prefixLen:keyLen], suffix)
		prevLen = keyLen
		cmp = compareLeafKey(prev[:prevLen], target)
		if cmp >= 0 {
			return idx, cmp == 0
		}
	}
	return blockEnd, false
}

func (p *page) prefixRestartKey(index int) []byte {
	off := p.offsetAt(index)
	layout := parsePrefixLayout(p.data, off)
	return p.data[off+layout.keyOff : off+layout.keyOff+layout.suffixLen]
}

func (p *page) searchColumnarPrefixV2(key []byte) (int, bool) {
	if p.count == 0 {
		return 0, false
	}
	if p.count <= smallSearchThreshold {
		return p.searchColumnarPrefixBlock(0, p.count, key)
	}
	restarts := (p.count + leafPrefixRestartInterval - 1) / leafPrefixRestartInterval
	lo, hi := 0, restarts
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		idx := mid * leafPrefixRestartInterval
		if idx >= p.count {
			hi = mid
			continue
		}
		if compareLeafKey(p.columnarPrefixSuffixAt(idx), key) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	blockStart := 0
	if lo > 0 {
		blockStart = (lo - 1) * leafPrefixRestartInterval
	}
	blockEnd := blockStart + leafPrefixRestartInterval
	if blockEnd > p.count {
		blockEnd = p.count
	}
	return p.searchColumnarPrefixBlock(blockStart, blockEnd, key)
}

func (p *page) searchColumnarPrefixBlock(blockStart, blockEnd int, target []byte) (int, bool) {
	if blockStart >= blockEnd {
		return blockEnd, false
	}
	restart := p.columnarPrefixSuffixAt(blockStart)
	cmp := compareLeafKey(restart, target)
	if cmp >= 0 {
		return blockStart, cmp == 0
	}
	var prev [benchKeySize]byte
	prevLen := len(restart)
	copy(prev[:], restart)
	for idx := blockStart + 1; idx < blockEnd; idx++ {
		prefixLen := p.columnarPrefixLenAt(idx)
		suffix := p.columnarPrefixSuffixAt(idx)
		keyLen := prefixLen + len(suffix)
		copy(prev[prefixLen:keyLen], suffix)
		prevLen = keyLen
		cmp = compareLeafKey(prev[:prevLen], target)
		if cmp >= 0 {
			return idx, cmp == 0
		}
	}
	return blockEnd, false
}

func (p *page) columnarPrefixSuffixAt(index int) []byte {
	keyStart := p.offsetAt(index)
	keyEnd := pageSize
	if index+1 < p.count {
		keyEnd = p.offsetAt(index + 1)
	}
	return p.data[keyStart:keyEnd]
}

func (p *page) columnarPrefixLenAt(index int) int {
	flagsStart := nodeHeaderSize + p.count*4
	prefixStart := flagsStart + p.count
	return int(getU16(p.data[prefixStart+index*2 : prefixStart+index*2+2]))
}

func (p *page) offsetAt(index int) int {
	return int(getU16(p.data[nodeHeaderSize+index*2 : nodeHeaderSize+index*2+2]))
}

func valueSize(value []byte, flags byte) int {
	if flags&flagPointer != 0 {
		return valuePtrSize
	}
	if flags&flagTombstone != 0 {
		return 0
	}
	return len(value)
}

func sharedPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func leafPrefixHeaderSizeV2(prefixLen, suffixLen int, flags byte, valLen int) int {
	headerSize := 3
	if prefixLen > 254 || suffixLen > 254 {
		headerSize += 4
	}
	if flags&flagPointer == 0 && flags&flagTombstone == 0 {
		headerSize += uvarintLen(uint64(valLen))
	}
	return headerSize
}

func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

func putUvarint(dst []byte, x uint64) int {
	i := 0
	for x >= 0x80 {
		dst[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	dst[i] = byte(x)
	return i + 1
}

func readUvarint(src []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range src {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}

func parsePrefixLayout(data []byte, off int) prefixLayout {
	shared8 := data[off]
	suffix8 := data[off+1]
	flags := data[off+2]
	header := 3
	prefixLen := int(shared8)
	suffixLen := int(suffix8)
	if shared8 == 0xff && suffix8 == 0xff {
		header += 4
		prefixLen = int(getU16(data[off+3 : off+5]))
		suffixLen = int(getU16(data[off+5 : off+7]))
	}
	if flags&flagPointer == 0 && flags&flagTombstone == 0 {
		_, n := readUvarint(data[off+header:])
		header += n
	}
	return prefixLayout{prefixLen: prefixLen, suffixLen: suffixLen, keyOff: header}
}

func compareLeafKey(a, b []byte) int {
	return bytes.Compare(a, b)
}

func putU16(dst []byte, v uint16) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
}

func putU32(dst []byte, v uint32) {
	dst[0] = byte(v)
	dst[1] = byte(v >> 8)
	dst[2] = byte(v >> 16)
	dst[3] = byte(v >> 24)
}

func getU16(src []byte) uint16 {
	return uint16(src[0]) | uint16(src[1])<<8
}
