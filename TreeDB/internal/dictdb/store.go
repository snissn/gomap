package dictdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

var errStoreUnavailable = errors.New("dictdb: store unavailable")

// Store provides dictionary storage directly inside a TreeDB backend's
// SystemRootPageID (inline/chunked by default, or optionally backed by raw
// dictID=0 value-log pointers when PointerThreshold > 0).
type Store struct {
	backend          *db.DB
	ownsBackend      bool
	pointerThreshold atomic.Int64
}

const (
	dictKMin = 1
	dictKMax = valuelog.MaxFrameK
)

// Open opens a standalone backend at path and wraps it in a Store.
// Retained primarily for focused unit tests; production TreeDB wraps maindb
// directly via New(mainBackend).
func Open(path string, opts db.Options) (*Store, error) {
	opts.Dir = path
	backend, err := db.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{backend: backend, ownsBackend: true}, nil
}

// New wraps an existing TreeDB backend so dictionary metadata and bytes live
// inside its SystemRootPageID. Closing a Store created with New does not close
// the underlying main backend.
func New(backend *db.DB) *Store {
	return &Store{backend: backend, ownsBackend: false}
}

// SetPointerThreshold configures the byte size above which newly stored
// dictionaries are placed in maindb's dedicated system value-log lane
// (dictID=0, templateID=0 raw records referenced by ValuePtr in
// SystemRootPageID). A value <= 0 keeps all dictionaries inline in
// SystemRootPageID using 2 KiB chunks (the default).
func (s *Store) SetPointerThreshold(threshold int) {
	if s == nil {
		return
	}
	s.pointerThreshold.Store(int64(threshold))
}

// Close closes the underlying backend if and only if this Store opened it via
// Open. For Stores created with New(mainBackend), Close is a no-op.
func (s *Store) Close() error {
	if s == nil || s.backend == nil || !s.ownsBackend {
		return nil
	}
	return s.backend.Close()
}

// GetCurrent returns the current dictionary ID or 0 if unset.
func (s *Store) GetCurrent(_ context.Context) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	val, err := s.backend.GetSystem(currentKey())
	if err != nil {
		return 0, err
	}
	if val == nil {
		return 0, nil
	}
	if len(val) != 8 {
		return 0, fmt.Errorf("dictdb: invalid current size %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

// GetCurrentForClass returns the current dictionary ID for a payload class.
// Empty class and aliases "single"/"default" fall back to the legacy global
// current marker.
func (s *Store) GetCurrentForClass(ctx context.Context, class string) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	key := currentKeyForClass(class)
	val, err := s.backend.GetSystem(key)
	if err != nil {
		return 0, err
	}
	if val == nil {
		return s.GetCurrent(ctx)
	}
	if len(val) != 8 {
		return 0, fmt.Errorf("dictdb: invalid current size %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

// PutDictBytes inserts dict bytes (deduped by hash) into SystemRootPageID and
// returns its dictID.
func (s *Store) PutDictBytes(_ context.Context, dictBytes []byte) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if dictBytes == nil {
		dictBytes = []byte{}
	}
	checksum := sha256.Sum256(dictBytes)
	hKey := hashKey(checksum)

	val, err := s.backend.GetSystem(hKey)
	if err != nil {
		return 0, err
	}
	if val != nil {
		if len(val) != 8 {
			return 0, fmt.Errorf("dictdb: invalid hash entry size %d", len(val))
		}
		return binary.BigEndian.Uint64(val), nil
	}

	dictID := binary.BigEndian.Uint64(checksum[:8])
	existing, err := s.getDictBytesIfPresent(dictID)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		if bytes.Equal(existing, dictBytes) {
			valBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(valBuf, dictID)
			if err := s.backend.ApplySystemOps([]db.SystemKVOp{{Key: hKey, Value: valBuf}}, true); err != nil {
				return 0, err
			}
			return dictID, nil
		}
		return 0, fmt.Errorf("dictdb: dict id collision for %d", dictID)
	}

	ops, err := s.buildDictPayloadOpsLocked(dictID, checksum, dictBytes)
	if err != nil {
		return 0, err
	}
	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, dictID)
	ops = append(ops, db.SystemKVOp{Key: hKey, Value: valBuf})
	if err := s.backend.ApplySystemOps(ops, true); err != nil {
		return 0, err
	}
	return dictID, nil
}

// PublishDict atomically persists dictBytes (deduped by hash), optional frame
// group size k (> 0), and the class/global current markers in one durable
// maindb SystemRootPageID commit.
func (s *Store) PublishDict(_ context.Context, dictBytes []byte, k int, class string, alsoSetGlobal bool) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if k != 0 && (k < dictKMin || k > dictKMax) {
		return 0, fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	if dictBytes == nil {
		dictBytes = []byte{}
	}
	checksum := sha256.Sum256(dictBytes)
	hKey := hashKey(checksum)

	dictID, needWriteBytes, needWriteHash, err := s.resolvePublishIdentityLocked(hKey, checksum, dictBytes)
	if err != nil {
		return 0, err
	}

	var ops []db.SystemKVOp
	if needWriteBytes {
		payloadOps, err := s.buildDictPayloadOpsLocked(dictID, checksum, dictBytes)
		if err != nil {
			return 0, err
		}
		ops = append(ops, payloadOps...)
	}

	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, dictID)
	if needWriteHash {
		ops = append(ops, db.SystemKVOp{Key: hKey, Value: valBuf})
	}
	if k > 0 && dictID != 0 {
		ops = append(ops, db.SystemKVOp{Key: kKey(dictID), Value: []byte{byte(k)}})
	}
	cKey := currentKeyForClass(class)
	ops = append(ops, db.SystemKVOp{Key: cKey, Value: valBuf})
	if alsoSetGlobal && !bytes.Equal(cKey, currentKey()) {
		ops = append(ops, db.SystemKVOp{Key: currentKey(), Value: valBuf})
	}
	if err := s.backend.ApplySystemOps(ops, true); err != nil {
		return 0, err
	}
	return dictID, nil
}

// StagePublishDict queues dictBytes, optional k (> 0), and class/global current
// markers for atomic piggybacking into the next maindb user-root flush or
// checkpoint commit (zero standalone fsync or COW write fence). For standalone
// stores created via Open, it falls back to immediate PublishDict.
func (s *Store) StagePublishDict(ctx context.Context, dictBytes []byte, k int, class string, alsoSetGlobal bool, onDurable func(uint64)) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if s.ownsBackend {
		dictID, err := s.PublishDict(ctx, dictBytes, k, class, alsoSetGlobal)
		if err != nil {
			return 0, err
		}
		if onDurable != nil {
			onDurable(dictID)
		}
		return dictID, nil
	}
	if k != 0 && (k < dictKMin || k > dictKMax) {
		return 0, fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	if dictBytes == nil {
		dictBytes = []byte{}
	}
	checksum := sha256.Sum256(dictBytes)
	hKey := hashKey(checksum)

	dictID, needWriteBytes, needWriteHash, err := s.resolvePublishIdentityLocked(hKey, checksum, dictBytes)
	if err != nil {
		return 0, err
	}

	var ops []db.SystemKVOp
	if needWriteBytes {
		payloadOps, err := s.buildDictPayloadOpsLocked(dictID, checksum, dictBytes)
		if err != nil {
			return 0, err
		}
		ops = append(ops, payloadOps...)
	}

	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, dictID)
	if needWriteHash {
		ops = append(ops, db.SystemKVOp{Key: hKey, Value: valBuf})
	}
	if k > 0 && dictID != 0 {
		ops = append(ops, db.SystemKVOp{Key: kKey(dictID), Value: []byte{byte(k)}})
	}
	cKey := currentKeyForClass(class)
	ops = append(ops, db.SystemKVOp{Key: cKey, Value: valBuf})
	if alsoSetGlobal && !bytes.Equal(cKey, currentKey()) {
		ops = append(ops, db.SystemKVOp{Key: currentKey(), Value: valBuf})
	}
	var cb func()
	if onDurable != nil {
		id := dictID
		cb = func() { onDurable(id) }
	}
	if err := s.backend.StageSystemOps(ops, cb); err != nil {
		return 0, err
	}
	return dictID, nil
}

// StageSetK queues a K-parameter update for atomic piggybacking into the next
// maindb flush or checkpoint commit. For standalone stores, it applies
// immediately via SetK.
func (s *Store) StageSetK(ctx context.Context, dictID uint64, k int, onDurable func()) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	if s.ownsBackend {
		if err := s.SetK(ctx, dictID, k); err != nil {
			return err
		}
		if onDurable != nil {
			onDurable()
		}
		return nil
	}
	if dictID == 0 {
		return fmt.Errorf("dictdb: invalid dict id 0")
	}
	if k < dictKMin || k > dictKMax {
		return fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	return s.backend.StageSystemOps([]db.SystemKVOp{{
		Key:   kKey(dictID),
		Value: []byte{byte(k)},
	}}, onDurable)
}

// HasStagedOps reports whether this store has staged system operations awaiting
// piggybacked commit or durability sync.
func (s *Store) HasStagedOps() bool {
	if s == nil || s.backend == nil || s.ownsBackend {
		return false
	}
	return s.backend.HasStagedSystemOps()
}

// HasUnsyncedOps reports whether any piggybacked system operations have been
// committed without a durability sync and are awaiting sync before activation.
func (s *Store) HasUnsyncedOps() bool {
	if s == nil || s.backend == nil || s.ownsBackend {
		return false
	}
	return s.backend.HasUnsyncedSystemOps()
}

// FlushStagedOps commits and (when sync is true) syncs any staged dictionary
// operations that have not yet been piggybacked onto a user flush/checkpoint.
func (s *Store) FlushStagedOps(sync bool) error {
	if s == nil || s.backend == nil || s.ownsBackend {
		return nil
	}
	return s.backend.FlushStagedSystemOps(sync)
}

// PublishRewriteLeafDict atomically persists dictBytes (deduped by hash), sets
// the class current marker, and records the leaf payload mode in a single
// SystemRootPageID commit (replacing three sequential WriteSync calls).
func (s *Store) PublishRewriteLeafDict(_ context.Context, dictBytes []byte, class string, useRawPages bool) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if dictBytes == nil {
		dictBytes = []byte{}
	}
	checksum := sha256.Sum256(dictBytes)
	hKey := hashKey(checksum)

	dictID, needWriteBytes, needWriteHash, err := s.resolvePublishIdentityLocked(hKey, checksum, dictBytes)
	if err != nil {
		return 0, err
	}

	var ops []db.SystemKVOp
	if needWriteBytes {
		payloadOps, err := s.buildDictPayloadOpsLocked(dictID, checksum, dictBytes)
		if err != nil {
			return 0, err
		}
		ops = append(ops, payloadOps...)
	}
	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, dictID)
	if needWriteHash {
		ops = append(ops, db.SystemKVOp{Key: hKey, Value: valBuf})
	}
	if dictID != 0 {
		ops = append(ops, db.SystemKVOp{Key: currentKeyForClass(class), Value: valBuf})
		mode := []byte("compact")
		if useRawPages {
			mode = []byte("raw")
		}
		ops = append(ops, db.SystemKVOp{Key: leafPayloadModeKey(dictID), Value: mode})
	}
	if err := s.backend.ApplySystemOps(ops, true); err != nil {
		return 0, err
	}
	return dictID, nil
}

func (s *Store) resolvePublishIdentityLocked(hKey []byte, checksum [32]byte, dictBytes []byte) (dictID uint64, needWriteBytes, needWriteHash bool, err error) {
	val, err := s.backend.GetSystem(hKey)
	if err != nil {
		return 0, false, false, err
	}
	if val != nil {
		if len(val) != 8 {
			return 0, false, false, fmt.Errorf("dictdb: invalid hash entry size %d", len(val))
		}
		return binary.BigEndian.Uint64(val), false, false, nil
	}
	dictID = binary.BigEndian.Uint64(checksum[:8])
	existing, err := s.getDictBytesIfPresent(dictID)
	if err != nil {
		return 0, false, false, err
	}
	if existing != nil {
		if !bytes.Equal(existing, dictBytes) {
			return 0, false, false, fmt.Errorf("dictdb: dict id collision for %d", dictID)
		}
		return dictID, false, true, nil
	}
	return dictID, true, true, nil
}

func (s *Store) buildDictPayloadOpsLocked(dictID uint64, checksum [32]byte, dictBytes []byte) ([]db.SystemKVOp, error) {
	bKey := db.SystemDictBytesKey(dictID)
	threshold := int(s.pointerThreshold.Load())
	if threshold > 0 && len(dictBytes) > threshold {
		ptr, err := s.backend.AppendRawSystemValueLog(dictID, dictBytes)
		if err != nil {
			return nil, err
		}
		return []db.SystemKVOp{{Key: bKey, ValuePtr: ptr, IsPointer: true}}, nil
	}
	if len(dictBytes) <= db.SystemDictChunkSize {
		encoded := make([]byte, 1+len(dictBytes))
		encoded[0] = db.SystemDictEncodingInline
		copy(encoded[1:], dictBytes)
		return []db.SystemKVOp{{Key: bKey, Value: encoded}}, nil
	}
	if uint64(len(dictBytes)) > math.MaxUint32 || len(dictBytes) > db.MaxSystemDictBytes {
		return nil, fmt.Errorf("dictdb: dict length %d exceeds max inline chunked size %d", len(dictBytes), db.MaxSystemDictBytes)
	}
	chunkCount := uint32((len(dictBytes) + db.SystemDictChunkSize - 1) / db.SystemDictChunkSize)
	ops := make([]db.SystemKVOp, 0, 1+int(chunkCount))
	ops = append(ops, db.SystemKVOp{
		Key:   bKey,
		Value: db.EncodeSystemDictChunkHeader(chunkCount, uint32(len(dictBytes)), checksum),
	})
	for i := uint32(0); i < chunkCount; i++ {
		start := int(i) * db.SystemDictChunkSize
		end := start + db.SystemDictChunkSize
		if end > len(dictBytes) {
			end = len(dictBytes)
		}
		chunk := make([]byte, end-start)
		copy(chunk, dictBytes[start:end])
		ops = append(ops, db.SystemKVOp{
			Key:   db.SystemDictChunkKey(dictID, i),
			Value: chunk,
		})
	}
	return ops, nil
}

func (s *Store) getDictBytesIfPresent(dictID uint64) ([]byte, error) {
	if dictID == 0 {
		return nil, nil
	}
	_, ok, err := s.backend.GetSystemEntry(db.SystemDictBytesKey(dictID))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return s.backend.LookupSystemDictBytes(dictID)
}

func (s *Store) dictExists(dictID uint64) (bool, error) {
	if dictID == 0 {
		return false, nil
	}
	_, ok, err := s.backend.GetSystemEntry(db.SystemDictBytesKey(dictID))
	return ok, err
}

// SetCurrent marks dictID as the current dictionary.
func (s *Store) SetCurrent(_ context.Context, dictID uint64) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}

	if dictID == 0 {
		return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: currentKey(), Delete: true}}, true)
	}
	ok, err := s.dictExists(dictID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, dictID)
	return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: currentKey(), Value: buf}}, true)
}

// SetCurrentForClass marks dictID as the current dictionary for a payload class.
// Empty class and aliases "single"/"default" use the legacy global current key.
// A dictID of 0 clears the class marker.
func (s *Store) SetCurrentForClass(_ context.Context, class string, dictID uint64) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	key := currentKeyForClass(class)
	if dictID == 0 {
		return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: key, Delete: true}}, true)
	}
	ok, err := s.dictExists(dictID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, dictID)
	return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: key, Value: buf}}, true)
}

// SetK stores the preferred frame group size (K) for dictID.
func (s *Store) SetK(_ context.Context, dictID uint64, k int) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	if dictID == 0 {
		return nil
	}
	if k < dictKMin || k > dictKMax {
		return fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	ok, err := s.dictExists(dictID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: kKey(dictID), Value: []byte{byte(k)}}}, true)
}

// GetK loads the preferred frame group size (K) for dictID.
// Returns 0 when unset.
func (s *Store) GetK(_ context.Context, dictID uint64) (int, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if dictID == 0 {
		return 0, nil
	}
	val, err := s.backend.GetSystem(kKey(dictID))
	if err != nil {
		return 0, err
	}
	if len(val) == 0 {
		return 0, nil
	}
	if len(val) != 1 {
		return 0, fmt.Errorf("dictdb: invalid dict K size %d", len(val))
	}
	k := int(val[0])
	if k < dictKMin || k > dictKMax {
		return 0, fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	return k, nil
}

// GetLeafPayloadMode reports whether dictID expects raw 4KiB leaf pages or
// compact split-leaf payloads. ok=false means no explicit mode is recorded.
func (s *Store) GetLeafPayloadMode(_ context.Context, dictID uint64) (useRawPages bool, ok bool, err error) {
	if s == nil || s.backend == nil {
		return false, false, errStoreUnavailable
	}
	if dictID == 0 {
		return false, false, nil
	}
	val, err := s.backend.GetSystem(leafPayloadModeKey(dictID))
	if err != nil {
		return false, false, err
	}
	if len(val) == 0 {
		return false, false, nil
	}
	switch string(val) {
	case "raw":
		return true, true, nil
	case "compact":
		return false, true, nil
	default:
		return false, false, fmt.Errorf("dictdb: invalid leaf payload mode %q", string(val))
	}
}

// SetLeafPayloadMode records whether dictID expects raw 4KiB leaf pages or
// compact split-leaf payloads during rewrite.
func (s *Store) SetLeafPayloadMode(_ context.Context, dictID uint64, useRawPages bool) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	if dictID == 0 {
		return nil
	}
	ok, err := s.dictExists(dictID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	mode := []byte("compact")
	if useRawPages {
		mode = []byte("raw")
	}
	return s.backend.ApplySystemOps([]db.SystemKVOp{{Key: leafPayloadModeKey(dictID), Value: mode}}, true)
}

// GetDictBytes returns the dictionary bytes for dictID.
func (s *Store) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s == nil || s.backend == nil {
		return nil, errStoreUnavailable
	}
	val, err := s.backend.LookupSystemDictBytes(dictID)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), val...), nil
}

func hashKey(sum [32]byte) []byte {
	buf := make([]byte, len("dict/hash/")+len(sum))
	copy(buf, "dict/hash/")
	copy(buf[len("dict/hash/"):], sum[:])
	return buf
}

func kKey(dictID uint64) []byte {
	buf := make([]byte, len("dict/k/")+8)
	copy(buf, "dict/k/")
	binary.BigEndian.PutUint64(buf[len("dict/k/"):], dictID)
	return buf
}

func leafPayloadModeKey(dictID uint64) []byte {
	buf := make([]byte, len("dict/leafmode/")+8)
	copy(buf, "dict/leafmode/")
	binary.BigEndian.PutUint64(buf[len("dict/leafmode/"):], dictID)
	return buf
}

func currentKey() []byte {
	return []byte("dict/current")
}

func currentKeyForClass(class string) []byte {
	class = strings.TrimSpace(strings.ToLower(class))
	if class == "" || class == "single" || class == "default" {
		return currentKey()
	}
	return []byte("dict/current/" + class)
}
