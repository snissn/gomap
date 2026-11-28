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

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
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
// (dictID=0, templateID=0) payload to maindb's dedicated system value-log lane
// and returns its ValuePtr for atomic inclusion in a subsequent ApplySystemOps.
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

// ApplySystemOps atomically applies a batch of inline, pointer, or tombstone
// mutations to SystemRootPageID while preserving UserRootPageID and
// AppliedCommandLSN. System-tree leaf placement follows the DB's uniform
// systemRootOrderedPublishOptions policy so internal nodes never mix pager and
// leaf-log child references.
func (db *DB) ApplySystemOps(ops []SystemKVOp, sync bool) error {
	if db == nil {
		return ErrClosed
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

func (db *DB) applySystemOpsDirect(ops []SystemKVOp, sync bool) (retErr error) {
	if db == nil {
		return ErrClosed
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
	sortedOps, hasPointer := normalizeSystemKVOps(ops)
	if len(sortedOps) == 0 {
		return nil
	}
	if sync && hasPointer {
		if err := db.syncSystemValueLogWriterIfOpen(); err != nil {
			return err
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
		return ErrClosed
	}
	if err := db.checkWriteAdmissionLocked(); err != nil {
		return err
	}
	if db.readOnly {
		return ErrReadOnly
	}

	db.mu.RLock()
	userRoot := db.meta.UserRootPageID
	baseSystemRoot := db.meta.SystemRootPageID
	baseSeq := db.meta.CommitSeq
	db.mu.RUnlock()

	systemOpts := systemRootOrderedPublishOptions(db)

	trackValueLogRefs := hasPointer && db.valueLogRefTracker != nil
	iter := newSystemKVOpIterator(sortedOps)
	newSystemRoot, retired, metrics, touchedValueLogSegments, vlogRefDelta, err := db.publishOrderedRootDeltaIteratorWithValueLogRefs(
		baseSystemRoot, iter, systemOpts, baseSeq, trackValueLogRefs, false,
	)
	if err != nil {
		return err
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
		return errors.New("treedb: concurrent modification detected during system KV publish")
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
		return err
	}
	vlogRefDelta = nil
	db.finalizeCommitPostWork(post)
	if hasPointer {
		if err := db.RefreshValueLogSet(); err != nil {
			return err
		}
	}
	return nil
}

func normalizeSystemKVOps(ops []SystemKVOp) ([]SystemKVOp, bool) {
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
	for i := 0; i < len(list); i++ {
		if i+1 < len(list) && bytes.Equal(list[i].op.Key, list[i+1].op.Key) {
			continue
		}
		op := list[i].op
		if op.IsPointer && !op.Delete {
			hasPointer = true
		}
		out = append(out, op)
	}
	return out, hasPointer
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
