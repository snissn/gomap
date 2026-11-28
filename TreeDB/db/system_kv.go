package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	templ "github.com/snissn/gomap/TreeDB/template"
	"github.com/snissn/gomap/TreeDB/tree"
	"github.com/snissn/gomap/TreeDB/zipper"
)

const (
	// SystemDictEncodingInline stores raw dictionary bytes inline in
	// dict/bytes/<id> right after the 1-byte encoding tag.
	SystemDictEncodingInline byte = 0x00
	// SystemDictEncodingChunked stores a fixed metadata header in
	// dict/bytes/<id> and 2 KiB inline chunks in dict/chunk/<id>/<idx>.
	SystemDictEncodingChunked byte = 0x01
	// SystemDictChunkSize is the inline chunk size used when storing dictionaries
	// as inline entries in SystemRootPageID.
	SystemDictChunkSize = 2048
	// SystemDictChunkHeaderSize is: tag(1) + chunkCount(4) + totalLen(4) + sha256(32).
	SystemDictChunkHeaderSize = 1 + 4 + 4 + 32
	// MaxSystemDictBytes bounds inline/chunked dictionary allocation sizes to
	// guard against corrupt chunk headers triggering oversized allocations.
	MaxSystemDictBytes = 64 << 20

	// SystemValueLogLaneID is a dedicated ordinary value-log lane used when
	// system-tree entries opt into value-log pointer storage (dictID=0,
	// templateID=0 raw records) so system writers never contend with user lanes
	// or ReservedLeafLogLaneID.
	SystemValueLogLaneID uint32 = valuelog.ReservedLeafLogLaneID - 1
)

// SystemKVOp describes a single key mutation in SystemRootPageID.
type SystemKVOp struct {
	Key       []byte
	Value     []byte
	ValuePtr  page.ValuePtr
	IsPointer bool
	Delete    bool
}

// SystemDictBytesKey returns the canonical system-tree key for dictID's payload
// header (inline bytes, chunked-inline header, or raw value-log pointer).
func SystemDictBytesKey(dictID uint64) []byte {
	buf := make([]byte, len("dict/bytes/")+8)
	copy(buf, "dict/bytes/")
	binary.BigEndian.PutUint64(buf[len("dict/bytes/"):], dictID)
	return buf
}

// SystemDictChunkKey returns the canonical system-tree key for chunk idx of dictID.
func SystemDictChunkKey(dictID uint64, chunkIdx uint32) []byte {
	buf := make([]byte, len("dict/chunk/")+8+1+4)
	copy(buf, "dict/chunk/")
	binary.BigEndian.PutUint64(buf[len("dict/chunk/"):], dictID)
	buf[len("dict/chunk/")+8] = '/'
	binary.BigEndian.PutUint32(buf[len("dict/chunk/")+9:], chunkIdx)
	return buf
}

// SystemTemplateDefKey returns the canonical system-tree key for templateID's
// definition payload (inline bytes, chunked-inline header, or raw value-log pointer).
func SystemTemplateDefKey(templateID uint64) []byte {
	buf := make([]byte, len("template/def/")+8)
	copy(buf, "template/def/")
	binary.BigEndian.PutUint64(buf[len("template/def/"):], templateID)
	return buf
}

// SystemTemplateDefChunkKey returns the canonical system-tree key for chunk idx
// of templateID's definition payload.
func SystemTemplateDefChunkKey(templateID uint64, chunkIdx uint32) []byte {
	buf := make([]byte, len("template/chunk/")+8+1+4)
	copy(buf, "template/chunk/")
	binary.BigEndian.PutUint64(buf[len("template/chunk/"):], templateID)
	buf[len("template/chunk/")+8] = '/'
	binary.BigEndian.PutUint32(buf[len("template/chunk/")+9:], chunkIdx)
	return buf
}

// SystemTemplateFPKey returns the canonical system-tree key for a template
// routing fingerprint's encoded candidate list.
func SystemTemplateFPKey(fp uint64) []byte {
	buf := make([]byte, len("template/fp/")+8)
	copy(buf, "template/fp/")
	binary.BigEndian.PutUint64(buf[len("template/fp/"):], fp)
	return buf
}

// EncodeSystemDictChunkHeader encodes the inline chunked dictionary header.
func EncodeSystemDictChunkHeader(chunkCount, totalLen uint32, sum [32]byte) []byte {
	buf := make([]byte, SystemDictChunkHeaderSize)
	buf[0] = SystemDictEncodingChunked
	binary.BigEndian.PutUint32(buf[1:5], chunkCount)
	binary.BigEndian.PutUint32(buf[5:9], totalLen)
	copy(buf[9:41], sum[:])
	return buf
}

// DecodeSystemDictChunkHeader decodes and validates an inline chunked
// dictionary header (including exact chunk-count geometry and bounded size).
func DecodeSystemDictChunkHeader(raw []byte) (chunkCount, totalLen uint32, sum [32]byte, err error) {
	if len(raw) != SystemDictChunkHeaderSize || raw[0] != SystemDictEncodingChunked {
		return 0, 0, sum, fmt.Errorf("treedb: invalid chunked system dict header (len=%d)", len(raw))
	}
	chunkCount = binary.BigEndian.Uint32(raw[1:5])
	totalLen = binary.BigEndian.Uint32(raw[5:9])
	copy(sum[:], raw[9:41])
	if totalLen > MaxSystemDictBytes {
		return 0, 0, sum, fmt.Errorf("treedb: system dict length %d exceeds max %d", totalLen, MaxSystemDictBytes)
	}
	var wantChunks uint32
	if totalLen > 0 {
		wantChunks = uint32((uint64(totalLen) + SystemDictChunkSize - 1) / SystemDictChunkSize)
	}
	if chunkCount != wantChunks {
		return 0, 0, sum, fmt.Errorf("treedb: invalid chunked system dict header (chunks=%d want=%d len=%d)", chunkCount, wantChunks, totalLen)
	}
	return chunkCount, totalLen, sum, nil
}

// CountSystemPrefix counts live keys in SystemRootPageID matching prefix.
func (db *DB) CountSystemPrefix(prefix []byte) (int, error) {
	if db == nil || db.closing.Load() {
		return 0, ErrClosed
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		return 0, ErrClosed
	}
	defer func() { _ = snap.Close() }()
	if snap.state == nil || snap.state.SystemRootPageID == 0 {
		return 0, nil
	}
	var end []byte
	if len(prefix) > 0 {
		end = append([]byte(nil), prefix...)
		for i := len(end) - 1; i >= 0; i-- {
			if end[i] != 0xFF {
				end[i]++
				end = end[:i+1]
				break
			}
			if i == 0 {
				end = nil
			}
		}
	}
	it := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID).Iterator(prefix, end)
	if it == nil {
		return 0, nil
	}
	defer func() { _ = it.Close() }()
	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return count, nil
}

// GetSystem reads a value from SystemRootPageID (resolving both inline values
// and raw value-log pointers). Returns (nil, nil) when key is absent.
func (db *DB) GetSystem(key []byte) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if db.closing.Load() {
		return nil, ErrClosed
	}
	snap := db.AcquireSnapshot()
	if snap != nil {
		defer func() { _ = snap.Close() }()
		if snap.state == nil || snap.state.SystemRootPageID == 0 {
			return nil, nil
		}
		val, err := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID).Get(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, nil
			}
			return nil, err
		}
		return append([]byte(nil), val...), nil
	}
	// Early open/recovery fallback before initialState is published.
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if sysRoot == 0 {
		return nil, nil
	}
	var set *valuelog.Set
	if db.valueLogManager != nil {
		set = db.valueLogManager.CurrentSetNoRefresh()
		defer func() {
			if set != nil {
				_ = db.valueLogManager.Release(set)
			}
		}()
	}
	reader := newValueReader(set)
	val, err := tree.New(idx.pager, reader, sysRoot).Get(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return append([]byte(nil), val...), nil
}

// GetSystemEntry reads the raw LeafEntry (flags, inline value copy, or ValuePtr)
// for key from SystemRootPageID. Returns (zero, false, nil) when absent.
func (db *DB) GetSystemEntry(key []byte) (node.LeafEntry, bool, error) {
	if db == nil {
		return node.LeafEntry{}, false, ErrClosed
	}
	if db.closing.Load() {
		return node.LeafEntry{}, false, ErrClosed
	}
	snap := db.AcquireSnapshot()
	if snap != nil {
		defer func() { _ = snap.Close() }()
		if snap.state == nil || snap.state.SystemRootPageID == 0 {
			return node.LeafEntry{}, false, nil
		}
		entry, err := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID).GetEntry(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return node.LeafEntry{}, false, nil
			}
			return node.LeafEntry{}, false, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return node.LeafEntry{}, false, nil
		}
		if entry.Flags&node.FlagPointer == 0 && entry.Value != nil {
			entry.Value = append([]byte(nil), entry.Value...)
		}
		return entry, true, nil
	}
	// Early open/recovery fallback before initialState is published.
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return node.LeafEntry{}, false, ErrClosed
	}
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if sysRoot == 0 {
		return node.LeafEntry{}, false, nil
	}
	var set *valuelog.Set
	if db.valueLogManager != nil {
		set = db.valueLogManager.CurrentSetNoRefresh()
		defer func() {
			if set != nil {
				_ = db.valueLogManager.Release(set)
			}
		}()
	}
	reader := newValueReader(set)
	entry, err := tree.New(idx.pager, reader, sysRoot).GetEntry(key)
	if err != nil {
		if errors.Is(err, tree.ErrKeyNotFound) {
			return node.LeafEntry{}, false, nil
		}
		return node.LeafEntry{}, false, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return node.LeafEntry{}, false, nil
	}
	if entry.Flags&node.FlagPointer == 0 && entry.Value != nil {
		entry.Value = append([]byte(nil), entry.Value...)
	}
	return entry, true, nil
}

// GetSystemEntryExactWithFixedScratch reads a persisted SystemRootPageID entry
// using caller-admitted fixed-page scratch. When SystemRootPageID stores outer
// leaves in leaf_vlog (FreshSystemPagerLeaves=false), it resolves them through
// a bounded, strictly non-dictionary (dictID=0) reader so dictionary lookups
// remain acyclic in every leaf-placement mode.
func (s *Snapshot) GetSystemEntryExactWithFixedScratch(key, keyScratch, leafScratch []byte) (node.LeafEntry, error) {
	var readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)
	if s != nil && s.state != nil && s.state.ValueLogSet != nil {
		readLeaf = func(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
			vptr := ptr.ValuePtr()
			f := s.state.ValueLogSet.Files[vptr.FileID]
			if f == nil {
				return nil, valuelog.ErrFileNotFound
			}
			dictID, err := f.PreparedRecordDictionaryID(vptr)
			if err != nil {
				return nil, err
			}
			if dictID != 0 {
				return nil, fmt.Errorf("treedb: system outer leaf unexpectedly uses dict %d", dictID)
			}
			raw, usedDst, err := s.reader.ReadUnsafeTo(vptr, dst[:0])
			if err != nil {
				return nil, err
			}
			if usedDst {
				return raw, nil
			}
			if len(raw) > cap(dst) {
				return nil, fmt.Errorf("treedb: system outer leaf size %d exceeds scratch %d", len(raw), cap(dst))
			}
			out := dst[:len(raw)]
			copy(out, raw)
			return out, nil
		}
	}
	return s.GetSystemEntryExactWithFixedScratchAndLeafReader(key, keyScratch, leafScratch, readLeaf)
}

// GetSystemEntryExactWithFixedScratchAndLeafReader reads a persisted
// SystemRootPageID entry using caller-admitted fixed-page scratch and a
// caller-supplied bounded outer-leaf reader (such as dictdb's COW-admitted
// non-dictionary leaf reader).
func (s *Snapshot) GetSystemEntryExactWithFixedScratchAndLeafReader(key, keyScratch, leafScratch []byte, readLeaf func(page.LeafLogPtr, []byte) ([]byte, error)) (node.LeafEntry, error) {
	if s == nil {
		return node.LeafEntry{}, ErrClosed
	}
	if err := s.beginRead(); err != nil {
		return node.LeafEntry{}, err
	}
	defer s.endRead()
	if s.state == nil || s.state.SystemRootPageID == 0 {
		return node.LeafEntry{}, tree.ErrKeyNotFound
	}
	sysTree := tree.New(s.idx.pager, &s.reader, s.state.SystemRootPageID)
	return sysTree.GetEntryWithFixedScratch(key, keyScratch, leafScratch, readLeaf)
}

// LookupSystemDictBytes resolves dictionary bytes for dictID from
// SystemRootPageID (supporting direct inline, 2 KiB chunked inline, and raw
// value-log pointer entries) with an in-memory immutable cache.
func (db *DB) LookupSystemDictBytes(dictID uint64) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if dictID == 0 {
		return nil, valuelog.ErrMissingDict
	}
	if cached, ok := db.systemDictCache.Load(dictID); ok {
		return cached.([]byte), nil
	}
	raw, err := db.loadSystemDictBytesUncached(dictID)
	if err != nil {
		return nil, err
	}
	actual, _ := db.systemDictCache.LoadOrStore(dictID, raw)
	return actual.([]byte), nil
}

func (db *DB) cacheSystemDictBytes(dictID uint64, dictBytes []byte) {
	if db == nil || dictID == 0 {
		return
	}
	cp := append([]byte(nil), dictBytes...)
	db.systemDictCache.LoadOrStore(dictID, cp)
}

func (db *DB) loadSystemDictBytesUncached(dictID uint64) ([]byte, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}
	loadFromTree := func(tr *tree.Tree, readPtr func(page.ValuePtr) ([]byte, error)) ([]byte, error) {
		entry, err := tr.GetEntry(SystemDictBytesKey(dictID))
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, fmt.Errorf("treedb: dict %d not found: %w", dictID, valuelog.ErrMissingDict)
			}
			return nil, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, fmt.Errorf("treedb: dict %d not found: %w", dictID, valuelog.ErrMissingDict)
		}
		if entry.Flags&node.FlagPointer != 0 {
			if readPtr == nil {
				return nil, fmt.Errorf("treedb: dict %d requires value log reader", dictID)
			}
			val, err := readPtr(entry.ValuePtr)
			if err != nil {
				return nil, err
			}
			sum := sha256.Sum256(val)
			if binary.BigEndian.Uint64(sum[:8]) != dictID {
				return nil, fmt.Errorf("treedb: dict %d pointer sha256 mismatch", dictID)
			}
			return append([]byte(nil), val...), nil
		}
		if len(entry.Value) == 0 {
			return nil, fmt.Errorf("treedb: dict %d empty inline entry", dictID)
		}
		switch entry.Value[0] {
		case SystemDictEncodingInline:
			out := append([]byte(nil), entry.Value[1:]...)
			sum := sha256.Sum256(out)
			if binary.BigEndian.Uint64(sum[:8]) != dictID {
				return nil, fmt.Errorf("treedb: dict %d inline sha256 mismatch", dictID)
			}
			return out, nil
		case SystemDictEncodingChunked:
			chunkCount, totalLen, wantSum, err := DecodeSystemDictChunkHeader(entry.Value)
			if err != nil {
				return nil, err
			}
			if binary.BigEndian.Uint64(wantSum[:8]) != dictID {
				return nil, fmt.Errorf("treedb: dict %d chunk header sha256 prefix mismatch", dictID)
			}
			out := make([]byte, 0, totalLen)
			for i := uint32(0); i < chunkCount; i++ {
				chunkEntry, err := tr.GetEntry(SystemDictChunkKey(dictID, i))
				if err != nil {
					return nil, fmt.Errorf("treedb: dict %d chunk %d/%d: %w", dictID, i, chunkCount, err)
				}
				if chunkEntry.Flags&(node.FlagTombstone|node.FlagPointer) != 0 {
					return nil, fmt.Errorf("treedb: dict %d chunk %d invalid flags %#x", dictID, i, chunkEntry.Flags)
				}
				out = append(out, chunkEntry.Value...)
			}
			if uint32(len(out)) != totalLen {
				return nil, fmt.Errorf("treedb: dict %d chunk length mismatch: got=%d want=%d", dictID, len(out), totalLen)
			}
			gotSum := sha256.Sum256(out)
			if gotSum != wantSum {
				return nil, fmt.Errorf("treedb: dict %d chunk sha256 mismatch", dictID)
			}
			return out, nil
		default:
			return nil, fmt.Errorf("treedb: dict %d unsupported encoding tag %#x", dictID, entry.Value[0])
		}
	}

	snap := db.AcquireSnapshot()
	if snap != nil {
		defer func() { _ = snap.Close() }()
		if snap.state == nil || snap.state.SystemRootPageID == 0 {
			return nil, fmt.Errorf("treedb: dict %d not found: %w", dictID, valuelog.ErrMissingDict)
		}
		tr := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
		return loadFromTree(tr, snap.reader.Read)
	}
	// Early open/recovery fallback before initialState is published.
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if sysRoot == 0 {
		return nil, fmt.Errorf("treedb: dict %d not found: %w", dictID, valuelog.ErrMissingDict)
	}
	var set *valuelog.Set
	if db.valueLogManager != nil {
		set = db.valueLogManager.CurrentSetNoRefresh()
		defer func() {
			if set != nil {
				_ = db.valueLogManager.Release(set)
			}
		}()
	}
	reader := newValueReader(set)
	tr := tree.New(idx.pager, reader, sysRoot)
	return loadFromTree(tr, reader.Read)
}

var (
	systemTemplateDefPrefix    = []byte("template/def/")
	systemTemplateDefPrefixEnd = []byte("template/def0") // '/' + 1 == '0'
)

func normalizeTemplateDecodeOptions(opts ValueLogOptions) templ.DecodeOptions {
	decodeOpts := opts.TemplateDecodeOptions
	if decodeOpts == (templ.DecodeOptions{}) {
		tcfg := templ.NormalizeConfig(opts.TemplateConfig)
		decodeOpts = templ.DecodeOptions{
			MaxGaps:         tcfg.MaxGaps,
			MaxDecodedBytes: tcfg.MaxDecodedBytes,
			DefCacheSize:    tcfg.DefCacheSize,
		}
	}
	return decodeOpts
}

func (db *DB) hasSystemTemplateDefs(p *pager.Pager, vm *valuelog.Manager) bool {
	if db == nil || p == nil {
		return false
	}
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if sysRoot == 0 {
		return false
	}
	var set *valuelog.Set
	if vm != nil {
		set = vm.CurrentSetNoRefresh()
		defer func() {
			if set != nil {
				_ = vm.Release(set)
			}
		}()
	}
	it := tree.New(p, newValueReader(set), sysRoot).Iterator(systemTemplateDefPrefix, systemTemplateDefPrefixEnd)
	if it == nil {
		return false
	}
	defer func() { _ = it.Close() }()
	return it.Valid()
}

// EnsureSystemTemplateLookup wires SystemRootPageID template decoding onto the
// value-log manager when no explicit TemplateLookup is installed yet.
func (db *DB) EnsureSystemTemplateLookup() {
	if db == nil || db.valueLogManager == nil {
		return
	}
	if db.valueLogManager.PreparedReadProfile().HasTemplateLookup {
		return
	}
	db.valueLogManager.SetTemplateLookup(db.LookupSystemTemplateDef, db.defaultTemplateDecodeOpts)
}

// LookupSystemTemplateDef resolves template definition bytes for templateID
// from SystemRootPageID (supporting direct inline, 2 KiB chunked inline, and
// raw value-log pointer entries) with an in-memory immutable cache.
func (db *DB) LookupSystemTemplateDef(templateID uint64) ([]byte, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if templateID == 0 {
		return nil, templ.ErrMissingTemplate
	}
	if cached, ok := db.systemTemplateCache.Load(templateID); ok {
		return cached.([]byte), nil
	}
	raw, err := db.loadSystemTemplateDefUncached(templateID)
	if err != nil {
		return nil, err
	}
	actual, _ := db.systemTemplateCache.LoadOrStore(templateID, raw)
	return actual.([]byte), nil
}

func (db *DB) loadSystemTemplateDefUncached(templateID uint64) ([]byte, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}
	loadFromTree := func(tr *tree.Tree, readPtr func(page.ValuePtr) ([]byte, error)) ([]byte, error) {
		entry, err := tr.GetEntry(SystemTemplateDefKey(templateID))
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return nil, templ.ErrMissingTemplate
			}
			return nil, err
		}
		if entry.Flags&node.FlagTombstone != 0 {
			return nil, templ.ErrMissingTemplate
		}
		if entry.Flags&node.FlagPointer != 0 {
			if readPtr == nil {
				return nil, fmt.Errorf("treedb: template %d requires value log reader", templateID)
			}
			val, err := readPtr(entry.ValuePtr)
			if err != nil {
				return nil, err
			}
			return append([]byte(nil), val...), nil
		}
		if len(entry.Value) == 0 {
			return nil, fmt.Errorf("treedb: template %d empty inline entry", templateID)
		}
		switch entry.Value[0] {
		case SystemDictEncodingInline:
			return append([]byte(nil), entry.Value[1:]...), nil
		case SystemDictEncodingChunked:
			chunkCount, totalLen, wantSum, err := DecodeSystemDictChunkHeader(entry.Value)
			if err != nil {
				return nil, err
			}
			out := make([]byte, 0, totalLen)
			for i := uint32(0); i < chunkCount; i++ {
				chunkEntry, err := tr.GetEntry(SystemTemplateDefChunkKey(templateID, i))
				if err != nil {
					return nil, fmt.Errorf("treedb: template %d chunk %d/%d: %w", templateID, i, chunkCount, err)
				}
				if chunkEntry.Flags&(node.FlagTombstone|node.FlagPointer) != 0 {
					return nil, fmt.Errorf("treedb: template %d chunk %d invalid flags %#x", templateID, i, chunkEntry.Flags)
				}
				out = append(out, chunkEntry.Value...)
			}
			if uint32(len(out)) != totalLen {
				return nil, fmt.Errorf("treedb: template %d chunk length mismatch: got=%d want=%d", templateID, len(out), totalLen)
			}
			if sha256.Sum256(out) != wantSum {
				return nil, fmt.Errorf("treedb: template %d chunk sha256 mismatch", templateID)
			}
			return out, nil
		default:
			return nil, fmt.Errorf("treedb: template %d unsupported encoding tag %#x", templateID, entry.Value[0])
		}
	}

	snap := db.AcquireSnapshot()
	if snap != nil {
		defer func() { _ = snap.Close() }()
		if snap.state == nil || snap.state.SystemRootPageID == 0 {
			return nil, templ.ErrMissingTemplate
		}
		tr := tree.New(snap.idx.pager, &snap.reader, snap.state.SystemRootPageID)
		return loadFromTree(tr, snap.reader.Read)
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return nil, ErrClosed
	}
	db.mu.RLock()
	sysRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if sysRoot == 0 {
		return nil, templ.ErrMissingTemplate
	}
	var set *valuelog.Set
	if db.valueLogManager != nil {
		set = db.valueLogManager.CurrentSetNoRefresh()
		defer func() {
			if set != nil {
				_ = db.valueLogManager.Release(set)
			}
		}()
	}
	reader := newValueReader(set)
	tr := tree.New(idx.pager, reader, sysRoot)
	return loadFromTree(tr, reader.Read)
}

// SetDictCallbacks updates the backend's dictionary callbacks and refreshes
// valueLogManager's DictLookup hook. Passing nil for lookup falls back to
// db.LookupSystemDictBytes.
func (db *DB) SetDictCallbacks(
	lookup valuelog.DictLookup,
	currentForClass func(context.Context, string) (uint64, error),
	leafPayloadMode func(context.Context, uint64) (bool, bool, error),
	put func(context.Context, []byte) (uint64, error),
	setCurrentForClass func(context.Context, string, uint64) error,
	setLeafPayloadMode func(context.Context, uint64, bool) error,
	publishRewriteLeaf func(context.Context, []byte, string, bool) (uint64, error),
) {
	if db == nil {
		return
	}
	if lookup == nil {
		if db.valueLogDictLookup != nil {
			lookup = db.valueLogDictLookup
		} else {
			lookup = db.LookupSystemDictBytes
		}
	}
	db.valueLogDictLookup = lookup
	db.valueLogDictCurrentForClass = currentForClass
	db.valueLogDictLeafPayloadMode = leafPayloadMode
	db.valueLogDictPut = put
	db.valueLogDictSetCurrentForClass = setCurrentForClass
	db.valueLogDictSetLeafPayloadMode = setLeafPayloadMode
	db.valueLogDictPublishRewriteLeaf = publishRewriteLeaf
	if db.valueLogManager != nil {
		db.valueLogManager.SetDictLookup(lookup)
	}
}

// AppendRawSystemValueLog appends an uncompressed, non-dictionary-encoded
// (dictID=0, templateID=0) payload to maindb's dedicated system value-log lane,
// protects the returned ValuePtr from concurrent ValueLogGC until a subsequent
// ApplySystemOps (or ReleaseValueLogValues on abandonment), and returns it for
// atomic inclusion in SystemRootPageID.
func (db *DB) AppendRawSystemValueLog(rid uint64, val []byte) (page.ValuePtr, error) {
	if db == nil {
		return page.ValuePtr{}, ErrClosed
	}
	if db.closing.Load() {
		return page.ValuePtr{}, ErrClosed
	}
	if db.readOnly {
		return page.ValuePtr{}, ErrReadOnly
	}
	db.publishPrepareMu.RLock()
	defer db.publishPrepareMu.RUnlock()
	db.systemVLogMu.Lock()
	defer db.systemVLogMu.Unlock()
	if db.closing.Load() {
		return page.ValuePtr{}, ErrClosed
	}

	writer, err := db.ensureSystemValueLogWriterLocked()
	if err != nil {
		return page.ValuePtr{}, err
	}
	if rid == 0 {
		db.systemVLogNextRID++
		rid = db.systemVLogNextRID
	}
	ptr, err := writer.Append(0, nil, rid, val)
	if err != nil {
		return page.ValuePtr{}, err
	}
	if err := writer.Flush(); err != nil {
		return page.ValuePtr{}, err
	}
	db.protectPendingValueLogAppendPtrs([]page.ValuePtr{ptr})
	return ptr, nil
}

func (db *DB) ensureSystemValueLogWriterLocked() (*valuelog.Writer, error) {
	if db.systemVLogWriter != nil {
		stillRegistered := true
		if db.valueLogManager != nil && db.systemVLogFileID != 0 {
			set := db.valueLogManager.CurrentSetNoRefresh()
			if set != nil {
				_, stillRegistered = set.Files[db.systemVLogFileID]
				_ = db.valueLogManager.Release(set)
			} else {
				stillRegistered = false
			}
		}
		if stillRegistered {
			return db.systemVLogWriter, nil
		}
		_ = db.systemVLogWriter.Close()
		db.systemVLogWriter = nil
		db.systemVLogFileID = 0
	}
	if db.dir == "" {
		return nil, fmt.Errorf("treedb: missing backend dir for system value log")
	}
	valueLogDir := ValueLogDirPath(db.dir)
	if err := os.MkdirAll(valueLogDir, 0o700); err != nil {
		return nil, err
	}
	seq, err := nextSystemValueLogSeq(valueLogDir, SystemValueLogLaneID)
	if err != nil {
		return nil, err
	}
	if seq <= db.systemVLogSeq {
		seq = db.systemVLogSeq + 1
	}
	fileID, err := valuelog.EncodeFileID(SystemValueLogLaneID, seq)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(valueLogDir, fmt.Sprintf("value-l%d-%06d.log", SystemValueLogLaneID, seq))
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, db.StableResourceIdentityPinRegistry())
	if err != nil {
		return nil, err
	}
	if err := db.RegisterValueLogSegment(path, fileID); err != nil {
		return nil, errors.Join(err, writer.Close())
	}
	db.systemVLogWriter = writer
	db.systemVLogFileID = fileID
	db.systemVLogSeq = seq
	return writer, nil
}

func (db *DB) closeSystemValueLogWriter() error {
	if db == nil {
		return nil
	}
	db.systemVLogMu.Lock()
	writer := db.systemVLogWriter
	db.systemVLogWriter = nil
	db.systemVLogFileID = 0
	db.systemVLogMu.Unlock()
	if writer == nil {
		return nil
	}
	return writer.Close()
}

func (db *DB) closeSystemValueLogWriterIfFileID(fileID uint32) error {
	if db == nil || fileID == 0 {
		return nil
	}
	db.systemVLogMu.Lock()
	if db.systemVLogWriter == nil || db.systemVLogFileID != fileID {
		db.systemVLogMu.Unlock()
		return nil
	}
	writer := db.systemVLogWriter
	db.systemVLogWriter = nil
	db.systemVLogFileID = 0
	db.systemVLogMu.Unlock()
	return writer.Close()
}

func (db *DB) syncSystemValueLogWriterIfOpen() error {
	if db == nil {
		return nil
	}
	db.systemVLogMu.Lock()
	defer db.systemVLogMu.Unlock()
	if db.systemVLogWriter == nil {
		return nil
	}
	return db.systemVLogWriter.Sync()
}

func nextSystemValueLogSeq(dir string, lane uint32) (uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	prefix := fmt.Sprintf("value-l%d-", lane)
	var maxSeq uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".log") {
			continue
		}
		core := strings.TrimSuffix(strings.TrimPrefix(name, prefix), ".log")
		seqVal, err := strconv.ParseUint(core, 10, 32)
		if err != nil {
			continue
		}
		if uint32(seqVal) > maxSeq {
			maxSeq = uint32(seqVal)
		}
	}
	return maxSeq + 1, nil
}

// SetSystemRootMutationFence registers an optional begin/finish fence (used by
// caching.DB in COW mode) that fences cached writers and refreshes the COW
// backend basis around SystemRootPageID-only metadata commits such as
// dictionary publication.
func (db *DB) SetSystemRootMutationFence(begin func() (func() error, error)) {
	if db == nil {
		return
	}
	db.systemRootMutationFenceMu.Lock()
	db.systemRootMutationFence = begin
	db.systemRootMutationFenceMu.Unlock()
}

func (db *DB) systemRootMutationFenceFn() func() (func() error, error) {
	if db == nil {
		return nil
	}
	db.systemRootMutationFenceMu.RLock()
	begin := db.systemRootMutationFence
	db.systemRootMutationFenceMu.RUnlock()
	return begin
}

func collectPendingSystemValueLogPtrCounts(ops []SystemKVOp) map[page.ValuePtr]int64 {
	var counts map[page.ValuePtr]int64
	for _, op := range ops {
		if op.Delete || !op.IsPointer || op.ValuePtr.FileID == 0 || !page.IsValueLogFileID(op.ValuePtr.FileID) {
			continue
		}
		if counts == nil {
			counts = make(map[page.ValuePtr]int64, 1)
		}
		counts[op.ValuePtr]++
	}
	return counts
}

// ReleasePendingSystemKVOps releases GC protection pins for any raw system
// value-log pointers accumulated in ops when a caller aborts before invoking
// ApplySystemOps or StageSystemOps.
func (db *DB) ReleasePendingSystemKVOps(ops []SystemKVOp) {
	if db == nil || len(ops) == 0 {
		return
	}
	if pendingPtrs := collectPendingSystemValueLogPtrCounts(ops); len(pendingPtrs) > 0 {
		db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
	}
}

// ApplySystemOps atomically applies a batch of inline, pointer, or tombstone
// mutations to SystemRootPageID while preserving UserRootPageID and
// AppliedCommandLSN. System-tree leaf placement follows the DB's uniform
// systemRootOrderedPublishOptions policy so internal nodes never mix pager and
// leaf-log child references.
func (db *DB) ApplySystemOps(ops []SystemKVOp, sync bool) error {
	if db == nil {
		return ErrClosed
	}
	pendingPtrs := collectPendingSystemValueLogPtrCounts(ops)
	if len(pendingPtrs) > 0 {
		defer db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
	}
	if db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if len(ops) == 0 {
		return nil
	}
	var finish func() error
	if begin := db.systemRootMutationFenceFn(); begin != nil {
		var fenceErr error
		finish, fenceErr = begin()
		if fenceErr != nil {
			return fenceErr
		}
	}
	err := db.applySystemOpsDirect(ops, sync)
	if finish != nil {
		return errors.Join(err, finish())
	}
	return err
}

func (db *DB) applySystemOpsDirect(ops []SystemKVOp, sync bool) error {
	_, _, _, err := db.applySystemOpsDirectWithCommitSeq(ops, sync)
	return err
}

func (db *DB) applySystemOpsDirectWithCommitSeq(ops []SystemKVOp, sync bool) (commitSeq uint64, accepted bool, physicallyDurable bool, retErr error) {
	commitSeq, accepted, physicallyDurable, _, retErr = db.applySystemOpsDirectWithCommitSeqGuard(ops, sync, false, 0, 0, nil)
	return commitSeq, accepted, physicallyDurable, retErr
}

// applySystemOpsDirectIfCommitSeq applies ops to SystemRootPageID only if
// db.meta.CommitSeq still equals wantSeq and db.meta.SystemRootPageID still
// equals wantSystemRoot (and preCommitCheck, when non-nil, returns true) while
// db.writeMu is held. If the guard fails, it returns stale=true with zero
// mutations.
func (db *DB) applySystemOpsDirectIfCommitSeq(ops []SystemKVOp, wantSeq, wantSystemRoot uint64, sync bool, preCommitCheck func() bool) (stale bool, err error) {
	_, _, _, stale, err = db.applySystemOpsDirectWithCommitSeqGuard(ops, sync, true, wantSeq, wantSystemRoot, preCommitCheck)
	return stale, err
}

func (db *DB) applySystemOpsDirectWithCommitSeqGuard(ops []SystemKVOp, sync bool, checkGuard bool, wantSeq, wantSystemRoot uint64, preCommitCheck func() bool) (commitSeq uint64, accepted bool, physicallyDurable bool, stale bool, retErr error) {
	if db == nil {
		return 0, false, false, false, ErrClosed
	}
	if db.closing.Load() {
		return 0, false, false, false, ErrClosed
	}
	if db.readOnly {
		return 0, false, false, false, ErrReadOnly
	}
	if len(ops) == 0 {
		return 0, false, false, false, nil
	}
	sortedOps, hasPointer, hasDelete := normalizeSystemKVOps(ops)
	db.recordRecentSystemMetadataIDs(sortedOps)
	if len(sortedOps) == 0 {
		return 0, false, false, false, nil
	}
	if sync && hasPointer {
		if err := db.syncSystemValueLogWriterIfOpen(); err != nil {
			return 0, false, false, false, err
		}
	}

	db.writeMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			db.writeMu.Unlock()
		}
	}()
	if db.closing.Load() {
		return 0, false, false, false, ErrClosed
	}
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return 0, false, false, false, err
	}
	if db.readOnly {
		return 0, false, false, false, ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	if checkGuard && (baseSeq != wantSeq || baseSystemRoot != wantSystemRoot) {
		return 0, false, false, true, nil
	}
	if preCommitCheck != nil && !preCommitCheck() {
		return 0, false, false, true, nil
	}
	if err := db.validateDirectSystemOpReferencesLocked(baseSystemRoot, sortedOps); err != nil {
		return 0, false, false, false, err
	}

	systemOpts := systemRootOrderedPublishOptions(db)

	trackValueLogRefs := (hasPointer || hasDelete) && db.valueLogRefTracker != nil
	iter := newSystemKVOpIterator(sortedOps)
	newSystemRoot, retired, metrics, touchedValueLogSegments, vlogRefDelta, err := db.publishOrderedRootDeltaIteratorWithValueLogRefs(
		baseSystemRoot, iter, systemOpts, baseSeq, trackValueLogRefs, false,
	)
	if err != nil {
		return 0, false, false, false, err
	}
	defer func() {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
	}()

	db.mu.RLock()
	curUserRoot := db.meta.UserRootPageID
	curSystemRoot := db.meta.SystemRootPageID
	db.mu.RUnlock()
	if curUserRoot != userRoot || curSystemRoot != baseSystemRoot {
		return 0, false, false, false, errors.New("treedb: concurrent modification detected during system KV publish")
	}

	var finalizeOpts finalizeCommitOptions
	if db.privateCOWOwner != nil {
		finalizeOpts.privateCOWRewrite = newPrivateCOWRewriteFinalization(db.privateCOWOwner, nil)
	}
	forceRefresh := hasPointer || len(touchedValueLogSegments) > 0
	post, err := db.finalizeCommitReleasingRootSerialization(
		userRoot, newSystemRoot, retired, sync, metrics, touchedValueLogSegments,
		forceRefresh, vlogRefDelta, nil, nil, finalizeOpts,
		baseSeq,
		func() {
			db.writeMu.Unlock()
			writeLocked = false
		},
		nil,
	)
	if err != nil {
		return post.commitSeq, post.accepted, post.physicallyDurable, false, err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	if hasPointer {
		if err := db.RefreshValueLogSet(); err != nil {
			return post.commitSeq, post.accepted, post.physicallyDurable, false, err
		}
	}
	for _, op := range sortedOps {
		if !op.Delete && bytes.HasPrefix(op.Key, systemTemplateDefPrefix) {
			db.EnsureSystemTemplateLookup()
			break
		}
	}
	return post.commitSeq, post.accepted, post.physicallyDurable, false, nil
}

type stagedSystemOpBatch struct {
	ops         []SystemKVOp
	pendingPtrs map[page.ValuePtr]int64
	onDurable   func()
}

type stagedSystemSyncWaiter struct {
	commitSeq uint64
	onDurable func()
}

type piggybackedSystemOps struct {
	batches                 []stagedSystemOpBatch
	newSystemRoot           uint64
	retired                 []uint64
	metrics                 adaptive.Metrics
	touchedValueLogSegments []uint32
	vlogRefDelta            *valueLogRefDelta
	hasPointer              bool
	inFlightUnsynced        bool
}

func cloneSystemKVOps(ops []SystemKVOp) []SystemKVOp {
	out := make([]SystemKVOp, len(ops))
	for i, op := range ops {
		cp := op
		if op.Key != nil {
			cp.Key = append([]byte(nil), op.Key...)
		}
		if op.Value != nil {
			cp.Value = append([]byte(nil), op.Value...)
		}
		out[i] = cp
	}
	return out
}

func (db *DB) requiresSyncForSystemDictActivation() bool {
	if db == nil {
		return true
	}
	return db.durability != DurabilityWALOffRelaxed
}

func (db *DB) updateStagedSystemOpsCountLocked() {
	db.stagedSystemOpsCount.Store(int32(len(db.stagedSystemOps)))
}

// StageSystemOps queues SystemRootPageID mutations in memory so the next
// user-root flush, batch write, or checkpoint piggybacks them atomically into
// its single commit without an extra fsync or COW write fence. Any raw system
// value-log pointers remain GC-pinned until the piggybacked commit accepts or
// the DB closes. onDurable runs only after the committed SystemRootPageID is
// safe for foreground writers to reference (synced when WAL/CommandWAL is
// enabled; immediately upon commit in WALOffRelaxed mode).
func (db *DB) StageSystemOps(ops []SystemKVOp, onDurable func()) error {
	if db == nil {
		return ErrClosed
	}
	pendingPtrs := collectPendingSystemValueLogPtrCounts(ops)
	if db.closing.Load() {
		if len(pendingPtrs) > 0 {
			db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
		}
		return ErrClosed
	}
	if db.readOnly {
		if len(pendingPtrs) > 0 {
			db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
		}
		return ErrReadOnly
	}
	if len(ops) == 0 {
		if onDurable != nil {
			onDurable()
		}
		return nil
	}
	cloned := cloneSystemKVOps(ops)
	// Serialize staging against destructive SystemMetadataGC so a staged
	// activation marker (e.g. dict/current* or template/fp/*) cannot be queued
	// concurrently with GC deleting the referenced definition.
	db.systemMetaPublishMu.RLock()
	defer db.systemMetaPublishMu.RUnlock()
	if err := db.validateStagedSystemOpReferencesLocked(cloned); err != nil {
		if len(pendingPtrs) > 0 {
			db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
		}
		return err
	}
	db.stagedSystemOpsMu.Lock()
	if db.closing.Load() {
		db.stagedSystemOpsMu.Unlock()
		if len(pendingPtrs) > 0 {
			db.releasePendingValueLogAppendPtrCounts(pendingPtrs)
		}
		return ErrClosed
	}
	db.stagedSystemOps = append(db.stagedSystemOps, stagedSystemOpBatch{
		ops:         cloned,
		pendingPtrs: pendingPtrs,
		onDurable:   onDurable,
	})
	db.updateStagedSystemOpsCountLocked()
	db.stagedSystemOpsMu.Unlock()
	db.recordRecentSystemMetadataIDs(cloned)
	return nil
}

// validateDirectSystemOpReferencesLocked verifies inside db.writeMu against
// baseSystemRoot that any dict/current* or template/fp/* activation in ops
// references a definition either written in ops (and not deleted) or present
// in baseSystemRoot. This prevents direct publishes (PublishDict, SetCurrent,
// PutTemplateDef) from committing a dangling activation after a concurrent
// SystemMetadataGC deletes an unreferenced definition.
func (db *DB) validateDirectSystemOpReferencesLocked(baseSystemRoot uint64, ops []SystemKVOp) error {
	var definedDicts, deletedDicts map[uint64]struct{}
	var definedTpls, deletedTpls map[uint64]struct{}
	var refDicts, refTpls []uint64
	for _, op := range ops {
		switch {
		case bytes.HasPrefix(op.Key, systemDictBytesPrefix):
			if id, ok := parseSystemDictBytesKey(op.Key); ok {
				if op.Delete {
					if deletedDicts == nil {
						deletedDicts = make(map[uint64]struct{}, 2)
					}
					deletedDicts[id] = struct{}{}
				} else {
					if definedDicts == nil {
						definedDicts = make(map[uint64]struct{}, 2)
					}
					definedDicts[id] = struct{}{}
				}
			}
		case !op.Delete && (bytes.Equal(op.Key, systemDictCurrentKey) || bytes.HasPrefix(op.Key, systemDictCurrentPrefix)):
			if !op.IsPointer && len(op.Value) == 8 {
				if id := binary.BigEndian.Uint64(op.Value); id != 0 {
					refDicts = append(refDicts, id)
				}
			}
		case bytes.HasPrefix(op.Key, systemTemplateDefPrefix):
			if id, ok := parseSystemTemplateDefKey(op.Key); ok {
				if op.Delete {
					if deletedTpls == nil {
						deletedTpls = make(map[uint64]struct{}, 2)
					}
					deletedTpls[id] = struct{}{}
				} else {
					if definedTpls == nil {
						definedTpls = make(map[uint64]struct{}, 2)
					}
					definedTpls[id] = struct{}{}
				}
			}
		case !op.Delete && bytes.HasPrefix(op.Key, systemTemplateFPPrefix):
			if !op.IsPointer && len(op.Value) > 0 {
				cands, err := decodeSystemTemplateFPCandidates(op.Value)
				if err != nil {
					return err
				}
				for _, c := range cands {
					if c.id != 0 {
						refTpls = append(refTpls, c.id)
					}
				}
			}
		}
	}
	if len(refDicts) == 0 && len(refTpls) == 0 {
		return nil
	}
	var tr *tree.Tree
	if baseSystemRoot != 0 {
		idx := db.idx.Load()
		if idx == nil || idx.pager == nil {
			return ErrClosed
		}
		tr = tree.New(idx.pager, newValueReader(nil), baseSystemRoot)
	}
	entryExists := func(key []byte) (bool, error) {
		if tr == nil {
			return false, nil
		}
		entry, err := tr.GetEntry(key)
		if err != nil {
			if errors.Is(err, tree.ErrKeyNotFound) {
				return false, nil
			}
			return false, err
		}
		return entry.Flags&node.FlagTombstone == 0, nil
	}
	for _, id := range refDicts {
		if _, deleted := deletedDicts[id]; deleted {
			return fmt.Errorf("treedb: cannot activate deleted dict %d: %w", id, valuelog.ErrMissingDict)
		}
		if _, defined := definedDicts[id]; defined {
			continue
		}
		ok, err := entryExists(SystemDictBytesKey(id))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("treedb: cannot activate missing dict %d: %w", id, valuelog.ErrMissingDict)
		}
	}
	for _, id := range refTpls {
		if _, deleted := deletedTpls[id]; deleted {
			return fmt.Errorf("treedb: cannot index deleted template %d: %w", id, templ.ErrMissingTemplate)
		}
		if _, defined := definedTpls[id]; defined {
			continue
		}
		ok, err := entryExists(SystemTemplateDefKey(id))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("treedb: cannot index missing template %d: %w", id, templ.ErrMissingTemplate)
		}
	}
	return nil
}

// validateStagedSystemOpReferencesLocked verifies that any dict/current* or
// template/fp/* activation queued in ops refers to a definition either created
// in ops, already queued in stagedSystemOps, or currently present in
// SystemRootPageID. Callers must hold db.systemMetaPublishMu.RLock().
func (db *DB) validateStagedSystemOpReferencesLocked(ops []SystemKVOp) error {
	var definedDicts map[uint64]struct{}
	var definedTpls map[uint64]struct{}
	var refDicts []uint64
	var refTpls []uint64
	for _, op := range ops {
		if op.Delete {
			continue
		}
		switch {
		case bytes.HasPrefix(op.Key, systemDictBytesPrefix):
			if id, ok := parseSystemDictBytesKey(op.Key); ok {
				if definedDicts == nil {
					definedDicts = make(map[uint64]struct{}, 2)
				}
				definedDicts[id] = struct{}{}
			}
		case bytes.Equal(op.Key, systemDictCurrentKey) || bytes.HasPrefix(op.Key, systemDictCurrentPrefix):
			if !op.IsPointer && len(op.Value) == 8 {
				if id := binary.BigEndian.Uint64(op.Value); id != 0 {
					refDicts = append(refDicts, id)
				}
			}
		case bytes.HasPrefix(op.Key, systemTemplateDefPrefix):
			if id, ok := parseSystemTemplateDefKey(op.Key); ok {
				if definedTpls == nil {
					definedTpls = make(map[uint64]struct{}, 2)
				}
				definedTpls[id] = struct{}{}
			}
		case bytes.HasPrefix(op.Key, systemTemplateFPPrefix):
			if !op.IsPointer && len(op.Value) > 0 {
				cands, err := decodeSystemTemplateFPCandidates(op.Value)
				if err != nil {
					return err
				}
				for _, c := range cands {
					if c.id != 0 {
						refTpls = append(refTpls, c.id)
					}
				}
			}
		}
	}
	if len(refDicts) == 0 && len(refTpls) == 0 {
		return nil
	}
	stagedDicts, stagedTpls := db.snapshotStagedSystemMetadataIDs()
	for _, id := range refDicts {
		if _, ok := definedDicts[id]; ok {
			continue
		}
		if _, ok := stagedDicts[id]; ok {
			continue
		}
		raw, err := db.GetSystem(SystemDictBytesKey(id))
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("treedb: cannot stage current marker for missing dict %d: %w", id, valuelog.ErrMissingDict)
		}
	}
	for _, id := range refTpls {
		if _, ok := definedTpls[id]; ok {
			continue
		}
		if _, ok := stagedTpls[id]; ok {
			continue
		}
		raw, err := db.GetSystem(SystemTemplateDefKey(id))
		if err != nil {
			return err
		}
		if len(raw) == 0 {
			return fmt.Errorf("treedb: cannot stage routing entry for missing template %d: %w", id, templ.ErrMissingTemplate)
		}
	}
	return nil
}

// HasPendingStagedSystemOps reports whether uncommitted staged SystemRootPageID
// mutations are queued in memory waiting to be piggybacked onto a commit.
func (db *DB) HasPendingStagedSystemOps() bool {
	return db != nil && db.stagedSystemOpsCount.Load() > 0
}

// HasUnsyncedSystemOps reports whether any piggybacked SystemRootPageID
// mutations are in-flight or committed without a durability sync and are
// awaiting sync before activation.
func (db *DB) HasUnsyncedSystemOps() bool {
	return db != nil && (db.stagedSystemSyncWaitersCount.Load() > 0 || db.stagedSystemInFlightUnsynced.Load() > 0)
}

// HasStagedSystemOps reports whether any staged SystemRootPageID mutations are
// queued for piggybacking or awaiting durability sync before activation.
func (db *DB) HasStagedSystemOps() bool {
	if db == nil {
		return false
	}
	if db.stagedSystemOpsCount.Load() > 0 || db.stagedSystemSyncWaitersCount.Load() > 0 {
		return true
	}
	db.stagedSystemOpsMu.Lock()
	has := len(db.stagedSystemOps) > 0 || len(db.stagedSystemSyncWaiters) > 0
	db.stagedSystemOpsMu.Unlock()
	return has
}

func (db *DB) drainStagedSystemOps() []stagedSystemOpBatch {
	if db == nil || db.stagedSystemOpsCount.Load() == 0 {
		return nil
	}
	db.stagedSystemOpsMu.Lock()
	if len(db.stagedSystemOps) == 0 {
		db.updateStagedSystemOpsCountLocked()
		db.stagedSystemOpsMu.Unlock()
		return nil
	}
	drained := db.stagedSystemOps
	db.stagedSystemOps = nil
	db.updateStagedSystemOpsCountLocked()
	db.stagedSystemOpsMu.Unlock()
	return drained
}

func (db *DB) requeueStagedSystemOpBatches(batches []stagedSystemOpBatch) {
	if db == nil || len(batches) == 0 {
		return
	}
	db.stagedSystemOpsMu.Lock()
	if db.closing.Load() {
		db.stagedSystemOpsMu.Unlock()
		db.releaseStagedSystemOpBatchPins(batches)
		return
	}
	combined := make([]stagedSystemOpBatch, 0, len(batches)+len(db.stagedSystemOps))
	combined = append(combined, batches...)
	combined = append(combined, db.stagedSystemOps...)
	db.stagedSystemOps = combined
	db.updateStagedSystemOpsCountLocked()
	db.stagedSystemOpsMu.Unlock()
}

func (db *DB) releaseStagedSystemOpBatchPins(batches []stagedSystemOpBatch) {
	if db == nil || len(batches) == 0 {
		return
	}
	var merged map[page.ValuePtr]int64
	for _, b := range batches {
		for ptr, count := range b.pendingPtrs {
			if merged == nil {
				merged = make(map[page.ValuePtr]int64, len(b.pendingPtrs))
			}
			merged[ptr] += count
		}
	}
	if len(merged) > 0 {
		db.releasePendingValueLogAppendPtrCounts(merged)
	}
}

func flattenStagedSystemOpBatches(batches []stagedSystemOpBatch) []SystemKVOp {
	total := 0
	for _, b := range batches {
		total += len(b.ops)
	}
	all := make([]SystemKVOp, 0, total)
	for _, b := range batches {
		all = append(all, b.ops...)
	}
	return all
}

func runStagedSystemCallbacks(cbs []func()) {
	for _, fn := range cbs {
		if fn != nil {
			fn()
		}
	}
}

func (db *DB) advanceStagedSystemDurableSeqLocked(commitSeq uint64) uint64 {
	cur := db.stagedSystemDurableSeq.Load()
	if commitSeq > cur {
		db.stagedSystemDurableSeq.Store(commitSeq)
		return commitSeq
	}
	return cur
}

func (db *DB) drainStagedSyncWaitersLocked(throughSeq uint64) []func() {
	if len(db.stagedSystemSyncWaiters) == 0 {
		db.stagedSystemSyncWaitersCount.Store(0)
		return nil
	}
	var ready []func()
	kept := db.stagedSystemSyncWaiters[:0]
	for _, w := range db.stagedSystemSyncWaiters {
		if w.commitSeq <= throughSeq {
			if w.onDurable != nil {
				ready = append(ready, w.onDurable)
			}
		} else {
			kept = append(kept, w)
		}
	}
	if len(kept) == 0 {
		db.stagedSystemSyncWaiters = nil
	} else {
		db.stagedSystemSyncWaiters = kept
	}
	db.stagedSystemSyncWaitersCount.Store(int32(len(db.stagedSystemSyncWaiters)))
	return ready
}

func (db *DB) completeStagedSystemOpBatches(batches []stagedSystemOpBatch, commitSeq uint64, physicallyDurable bool) {
	if db == nil || len(batches) == 0 {
		return
	}
	db.releaseStagedSystemOpBatchPins(batches)
	if !db.requiresSyncForSystemDictActivation() {
		cbs := make([]func(), 0, len(batches))
		for _, b := range batches {
			if b.onDurable != nil {
				cbs = append(cbs, b.onDurable)
			}
		}
		runStagedSystemCallbacks(cbs)
		return
	}
	db.stagedSystemOpsMu.Lock()
	durableSeq := db.stagedSystemDurableSeq.Load()
	if physicallyDurable {
		durableSeq = db.advanceStagedSystemDurableSeqLocked(commitSeq)
	}
	if physicallyDurable || durableSeq >= commitSeq {
		// Dispatch older unsynced sync waiters before this commit's callbacks
		// so activation strictly preserves commit order.
		ready := db.drainStagedSyncWaitersLocked(durableSeq)
		for _, b := range batches {
			if b.onDurable != nil {
				ready = append(ready, b.onDurable)
			}
		}
		db.stagedSystemOpsMu.Unlock()
		runStagedSystemCallbacks(ready)
		return
	}
	for _, b := range batches {
		if b.onDurable != nil {
			db.stagedSystemSyncWaiters = append(db.stagedSystemSyncWaiters, stagedSystemSyncWaiter{
				commitSeq: commitSeq,
				onDurable: b.onDurable,
			})
		}
	}
	db.stagedSystemSyncWaitersCount.Store(int32(len(db.stagedSystemSyncWaiters)))
	db.stagedSystemOpsMu.Unlock()
}

func (db *DB) notifySystemOpsSyncedThrough(commitSeq uint64) {
	if db == nil || commitSeq == 0 {
		return
	}
	db.stagedSystemOpsMu.Lock()
	durableSeq := db.advanceStagedSystemDurableSeqLocked(commitSeq)
	ready := db.drainStagedSyncWaitersLocked(durableSeq)
	db.stagedSystemOpsMu.Unlock()
	runStagedSystemCallbacks(ready)
}

func (db *DB) clearStagedSystemOpsOnClose() {
	if db == nil {
		return
	}
	db.stagedSystemOpsMu.Lock()
	drained := db.stagedSystemOps
	db.stagedSystemOps = nil
	db.stagedSystemSyncWaiters = nil
	db.updateStagedSystemOpsCountLocked()
	db.stagedSystemSyncWaitersCount.Store(0)
	db.stagedSystemOpsMu.Unlock()
	db.releaseStagedSystemOpBatchPins(drained)
}

func (db *DB) preparePiggybackedSystemOpsLocked(baseSystemRoot, baseSeq uint64, sync bool, alloc zipper.PageAllocator) (*piggybackedSystemOps, error) {
	drained := db.drainStagedSystemOps()
	if len(drained) == 0 {
		return nil, nil
	}
	allOps := flattenStagedSystemOpBatches(drained)
	sortedOps, hasPointer, hasDelete := normalizeSystemKVOps(allOps)
	if len(sortedOps) == 0 {
		db.completeStagedSystemOpBatches(drained, baseSeq, sync)
		return nil, nil
	}
	if sync && hasPointer {
		if err := db.syncSystemValueLogWriterIfOpen(); err != nil {
			db.requeueStagedSystemOpBatches(drained)
			return nil, err
		}
	}
	systemOpts := systemRootOrderedPublishOptions(db)
	trackValueLogRefs := (hasPointer || hasDelete) && db.valueLogRefTracker != nil
	iter := newSystemKVOpIterator(sortedOps)
	newSystemRoot, retired, metrics, touchedValueLogSegments, vlogRefDelta, err := db.publishOrderedRootDeltaIteratorWithValueLogRefsAndAllocator(
		baseSystemRoot, iter, systemOpts, baseSeq, trackValueLogRefs, false, alloc,
	)
	if err != nil {
		if vlogRefDelta != nil {
			releaseValueLogRefDelta(vlogRefDelta)
		}
		db.requeueStagedSystemOpBatches(drained)
		return nil, err
	}
	inFlightUnsynced := db.requiresSyncForSystemDictActivation()
	if inFlightUnsynced {
		db.stagedSystemInFlightUnsynced.Add(1)
	}
	return &piggybackedSystemOps{
		batches:                 drained,
		newSystemRoot:           newSystemRoot,
		retired:                 retired,
		metrics:                 metrics,
		touchedValueLogSegments: touchedValueLogSegments,
		vlogRefDelta:            vlogRefDelta,
		hasPointer:              hasPointer,
		inFlightUnsynced:        inFlightUnsynced,
	}, nil
}

func (db *DB) commitPiggybackedSystemOps(p *piggybackedSystemOps, commitSeq uint64, synced bool) {
	if db == nil || p == nil {
		return
	}
	db.completeStagedSystemOpBatches(p.batches, commitSeq, synced)
	if p.inFlightUnsynced {
		p.inFlightUnsynced = false
		db.stagedSystemInFlightUnsynced.Add(-1)
	}
	p.batches = nil
}

func (db *DB) requeuePiggybackedSystemOps(p *piggybackedSystemOps) {
	if db == nil || p == nil || len(p.batches) == 0 {
		return
	}
	if p.inFlightUnsynced {
		p.inFlightUnsynced = false
		db.stagedSystemInFlightUnsynced.Add(-1)
	}
	if p.vlogRefDelta != nil {
		releaseValueLogRefDelta(p.vlogRefDelta)
		p.vlogRefDelta = nil
	}
	db.requeueStagedSystemOpBatches(p.batches)
	p.batches = nil
}

// flushStagedSystemOpsDirect commits pending staged ops without invoking
// systemRootMutationFence (used by direct/legacy Checkpoint callers that
// already own flush serialization and have no private COW basis to refresh).
func (db *DB) flushStagedSystemOpsDirect(sync bool) error {
	if db == nil || db.stagedSystemOpsCount.Load() == 0 {
		return nil
	}
	drained := db.drainStagedSystemOps()
	if len(drained) == 0 {
		return nil
	}
	inFlight := db.requiresSyncForSystemDictActivation()
	if inFlight {
		db.stagedSystemInFlightUnsynced.Add(1)
		defer db.stagedSystemInFlightUnsynced.Add(-1)
	}
	allOps := flattenStagedSystemOpBatches(drained)
	commitSeq, accepted, physicallyDurable, err := db.applySystemOpsDirectWithCommitSeq(allOps, sync)
	if accepted {
		db.completeStagedSystemOpBatches(drained, commitSeq, physicallyDurable)
	} else {
		db.requeueStagedSystemOpBatches(drained)
	}
	return err
}

// FlushStagedSystemOps commits any pending staged SystemRootPageID mutations
// (when no user flush or checkpoint has piggybacked them yet) and, when sync is
// true, ensures any earlier unsynced piggybacked mutations are synced before
// invoking their activation callbacks.
func (db *DB) FlushStagedSystemOps(sync bool) error {
	if db == nil {
		return ErrClosed
	}
	if db.closing.Load() {
		return ErrClosed
	}
	if db.readOnly {
		return ErrReadOnly
	}
	if db.stagedSystemOpsCount.Load() > 0 {
		var finish func() error
		if begin := db.systemRootMutationFenceFn(); begin != nil {
			var fenceErr error
			finish, fenceErr = begin()
			if fenceErr != nil {
				return fenceErr
			}
		}
		drained := db.drainStagedSystemOps()
		var err error
		if len(drained) > 0 {
			inFlight := db.requiresSyncForSystemDictActivation()
			if inFlight {
				db.stagedSystemInFlightUnsynced.Add(1)
			}
			allOps := flattenStagedSystemOpBatches(drained)
			commitSeq, accepted, physicallyDurable, applyErr := db.applySystemOpsDirectWithCommitSeq(allOps, sync)
			if accepted {
				db.completeStagedSystemOpBatches(drained, commitSeq, physicallyDurable)
			} else {
				db.requeueStagedSystemOpBatches(drained)
			}
			if inFlight {
				db.stagedSystemInFlightUnsynced.Add(-1)
			}
			err = applyErr
		}
		if finish != nil {
			err = errors.Join(err, finish())
		}
		if err != nil {
			return err
		}
	}
	if !sync {
		return nil
	}
	db.stagedSystemOpsMu.Lock()
	var maxWaiterSeq uint64
	for _, w := range db.stagedSystemSyncWaiters {
		if w.commitSeq > maxWaiterSeq {
			maxWaiterSeq = w.commitSeq
		}
	}
	db.stagedSystemOpsMu.Unlock()
	if maxWaiterSeq == 0 {
		return nil
	}
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return ErrClosed
	}
	if runtime := db.rootPublication; runtime != nil && runtime.coordinator != nil {
		if err := runtime.coordinator.WaitThrough(context.Background(), maxWaiterSeq); err != nil {
			return publicRootPublicationErrorV1(err)
		}
		db.notifySystemOpsSyncedThrough(maxWaiterSeq)
		return nil
	}
	idx := db.idx.Load()
	if idx == nil || idx.pager == nil {
		return ErrClosed
	}
	if err := idx.pager.Sync(); err != nil {
		return err
	}
	db.notifySystemOpsSyncedThrough(maxWaiterSeq)
	return nil
}

// EvictSystemDictCache removes the given dictionary IDs from the DB's
// in-memory dictionary byte cache and the global value-log zstd codec cache.
func (db *DB) EvictSystemDictCache(dictIDs []uint64) {
	if db == nil || len(dictIDs) == 0 {
		return
	}
	for _, id := range dictIDs {
		if id != 0 {
			db.systemDictCache.Delete(id)
		}
	}
	valuelog.EvictDictCodecs(dictIDs)
}

// EvictSystemTemplateCache removes the given template IDs from the DB's
// in-memory template byte cache and the value-log manager's decoded template
// definition cache.
func (db *DB) EvictSystemTemplateCache(templateIDs []uint64) {
	if db == nil || len(templateIDs) == 0 {
		return
	}
	for _, id := range templateIDs {
		if id != 0 {
			db.systemTemplateCache.Delete(id)
		}
	}
	if db.valueLogManager != nil {
		db.valueLogManager.EvictTemplateDefs(templateIDs)
	}
}

func normalizeSystemKVOps(ops []SystemKVOp) ([]SystemKVOp, bool, bool) {
	type indexedOp struct {
		op  SystemKVOp
		idx int
	}
	list := make([]indexedOp, len(ops))
	for i, op := range ops {
		list[i] = indexedOp{op: op, idx: i}
	}
	sort.Slice(list, func(i, j int) bool {
		if cmp := bytes.Compare(list[i].op.Key, list[j].op.Key); cmp != 0 {
			return cmp < 0
		}
		return list[i].idx < list[j].idx
	})
	out := make([]SystemKVOp, 0, len(list))
	hasPointer := false
	hasDelete := false
	for i := 0; i < len(list); i++ {
		if i+1 < len(list) && bytes.Equal(list[i].op.Key, list[i+1].op.Key) {
			continue
		}
		op := list[i].op
		if op.Delete {
			hasDelete = true
		} else if op.IsPointer {
			hasPointer = true
		}
		out = append(out, op)
	}
	return out, hasPointer, hasDelete
}

type systemKVOpIterator struct {
	ops []SystemKVOp
	idx int
}

func newSystemKVOpIterator(ops []SystemKVOp) *systemKVOpIterator {
	return &systemKVOpIterator{ops: ops, idx: 0}
}

func (it *systemKVOpIterator) Seek(key []byte) {
	it.idx = sort.Search(len(it.ops), func(i int) bool {
		return bytes.Compare(it.ops[i].Key, key) >= 0
	})
}

func (it *systemKVOpIterator) Valid() bool {
	return it != nil && it.idx >= 0 && it.idx < len(it.ops)
}

func (it *systemKVOpIterator) Next() {
	if it != nil && it.idx < len(it.ops) {
		it.idx++
	}
}

func (it *systemKVOpIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return append([]byte(nil), it.ops[it.idx].Key...)
}

func (it *systemKVOpIterator) Value() []byte {
	if !it.Valid() || it.ops[it.idx].Delete || it.ops[it.idx].IsPointer {
		return nil
	}
	return append([]byte(nil), it.ops[it.idx].Value...)
}

func (it *systemKVOpIterator) KeyCopy(dst []byte) []byte {
	if !it.Valid() {
		return dst
	}
	return append(dst[:0], it.ops[it.idx].Key...)
}

func (it *systemKVOpIterator) ValueCopy(dst []byte) []byte {
	if !it.Valid() || it.ops[it.idx].Delete || it.ops[it.idx].IsPointer {
		return dst[:0]
	}
	return append(dst[:0], it.ops[it.idx].Value...)
}

func (it *systemKVOpIterator) UnsafeKey() []byte {
	if !it.Valid() {
		return nil
	}
	return it.ops[it.idx].Key
}

func (it *systemKVOpIterator) UnsafeValue() []byte {
	if !it.Valid() || it.ops[it.idx].Delete || it.ops[it.idx].IsPointer {
		return nil
	}
	return it.ops[it.idx].Value
}

func (it *systemKVOpIterator) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	if !it.Valid() {
		return nil, page.ValuePtr{}, 0
	}
	op := it.ops[it.idx]
	if op.Delete {
		return nil, page.ValuePtr{}, node.FlagTombstone
	}
	if op.IsPointer {
		return nil, op.ValuePtr, node.FlagPointer
	}
	return op.Value, page.ValuePtr{}, node.FlagInline
}

func (it *systemKVOpIterator) IsDeleted() bool {
	return it.Valid() && it.ops[it.idx].Delete
}

func (it *systemKVOpIterator) Error() error { return nil }
func (it *systemKVOpIterator) Close() error { return nil }
func (it *systemKVOpIterator) Domain() (start, end []byte) {
	return nil, nil
}

var _ iterator.UnsafeIterator = (*systemKVOpIterator)(nil)
