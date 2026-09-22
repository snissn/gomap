package dictdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

var errStoreUnavailable = errors.New("dictdb: store unavailable")

// Store provides access to dictionary storage backed by a TreeDB backend.
type Store struct {
	backend *db.DB
	mu      sync.Mutex
	dir     string
	vlog    *valuelog.Writer
	vlogSeq uint32
	nextRID uint64
}

const (
	dictKMin = 1
	dictKMax = valuelog.MaxFrameK
)

// Open opens a dictdb backend at path and returns a Store.
func Open(path string, opts db.Options) (*Store, error) {
	opts.Dir = path
	backend, err := db.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{backend: backend, dir: path}, nil
}

// New wraps an existing backend.
func New(backend *db.DB) *Store {
	var dir string
	if backend != nil {
		dir = backend.Dir()
	}
	return &Store{backend: backend, dir: dir}
}

// Close closes the underlying backend.
func (s *Store) Close() error {
	if s == nil || s.backend == nil {
		return nil
	}
	s.mu.Lock()
	if s.vlog != nil {
		_ = s.vlog.Close()
		s.vlog = nil
	}
	s.mu.Unlock()
	return s.backend.Close()
}

// GetCurrent returns the current dictionary ID or 0 if unset.
func (s *Store) GetCurrent(_ context.Context) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	val, err := s.backend.Get(currentKey())
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
	val, err := s.backend.Get(key)
	if err != nil {
		return 0, err
	}
	if val == nil {
		// Backward compatibility: fall back to legacy global current marker.
		return s.GetCurrent(ctx)
	}
	if len(val) != 8 {
		return 0, fmt.Errorf("dictdb: invalid current size %d", len(val))
	}
	return binary.BigEndian.Uint64(val), nil
}

// PutDictBytes inserts dict bytes (deduped by hash) and returns its dictID.
func (s *Store) PutDictBytes(ctx context.Context, dictBytes []byte) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if dictBytes == nil {
		dictBytes = []byte{}
	}
	checksum := sha256.Sum256(dictBytes)
	hashKey := hashKey(checksum)
	s.mu.Lock()
	defer s.mu.Unlock()

	val, err := s.backend.Get(hashKey)
	if err != nil {
		return 0, err
	}
	if val != nil {
		if len(val) != 8 {
			return 0, fmt.Errorf("dictdb: invalid hash entry size %d", len(val))
		}
		return binary.BigEndian.Uint64(val), nil
	}

	// dictID uses the first 8 bytes of SHA256; verify content on collision.
	dictID := binary.BigEndian.Uint64(checksum[:8])
	bytesKey := bytesKey(dictID)
	existing, err := s.backend.Get(bytesKey)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		if bytes.Equal(existing, dictBytes) {
			valBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(valBuf, dictID)
			if err := s.backend.SetSync(hashKey, valBuf); err != nil {
				return 0, err
			}
			return dictID, nil
		}
		return 0, fmt.Errorf("dictdb: dict id collision for %d", dictID)
	}

	inlineThreshold := s.backend.InlineThreshold()
	usePointer := len(dictBytes) > inlineThreshold

	batch := s.backend.NewBatch().(*db.Batch)
	if usePointer {
		writer, err := s.ensureValueLogWriterLocked()
		if err != nil {
			_ = batch.Close()
			return 0, err
		}
		rid := dictID
		if rid == 0 {
			s.nextRID++
			rid = s.nextRID
		}
		ptr, err := writer.Append(0, nil, rid, dictBytes)
		if err != nil {
			_ = batch.Close()
			return 0, err
		}
		if err := writer.Sync(); err != nil {
			_ = batch.Close()
			return 0, err
		}
		if err := batch.SetPointer(bytesKey, ptr); err != nil {
			_ = batch.Close()
			return 0, err
		}
	} else {
		if err := batch.Set(bytesKey, dictBytes); err != nil {
			_ = batch.Close()
			return 0, err
		}
	}
	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, dictID)
	if err := batch.Set(hashKey, valBuf); err != nil {
		_ = batch.Close()
		return 0, err
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		return 0, err
	}
	if err := batch.Close(); err != nil {
		return 0, err
	}
	if usePointer {
		if err := s.backend.RefreshValueLogSet(); err != nil {
			return 0, err
		}
	}
	return dictID, nil
}

// SetCurrent marks dictID as the current dictionary.
func (s *Store) SetCurrent(ctx context.Context, dictID uint64) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if dictID == 0 {
		// Clear current dict marker.
		return s.backend.DeleteSync(currentKey())
	}

	val, err := s.backend.Get(bytesKey(dictID))
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, dictID)
	return s.backend.SetSync(currentKey(), buf)
}

// SetCurrentForClass marks dictID as the current dictionary for a payload class.
// Empty class and aliases "single"/"default" use the legacy global current key.
// A dictID of 0 clears the class marker via DeleteSync.
func (s *Store) SetCurrentForClass(ctx context.Context, class string, dictID uint64) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	key := currentKeyForClass(class)
	s.mu.Lock()
	defer s.mu.Unlock()
	if dictID == 0 {
		return s.backend.DeleteSync(key)
	}
	val, err := s.backend.Get(bytesKey(dictID))
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, dictID)
	return s.backend.SetSync(key, buf)
}

// SetK stores the preferred frame group size (K) for dictID.
func (s *Store) SetK(ctx context.Context, dictID uint64, k int) error {
	if s == nil || s.backend == nil {
		return errStoreUnavailable
	}
	if dictID == 0 {
		return nil
	}
	if k < dictKMin || k > dictKMax {
		return fmt.Errorf("dictdb: invalid dict K=%d", k)
	}
	// Ensure dict exists (best-effort).
	val, err := s.backend.Get(bytesKey(dictID))
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	return s.backend.SetSync(kKey(dictID), []byte{byte(k)})
}

// GetK loads the preferred frame group size (K) for dictID.
// Returns 0 when unset.
func (s *Store) GetK(ctx context.Context, dictID uint64) (int, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	if dictID == 0 {
		return 0, nil
	}
	val, err := s.backend.Get(kKey(dictID))
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
	val, err := s.backend.Get(leafPayloadModeKey(dictID))
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
	val, err := s.backend.Get(bytesKey(dictID))
	if err != nil {
		return err
	}
	if val == nil {
		return fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	mode := []byte("compact")
	if useRawPages {
		mode = []byte("raw")
	}
	return s.backend.SetSync(leafPayloadModeKey(dictID), mode)
}

// GetDictBytes returns the dictionary bytes for dictID.
func (s *Store) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s == nil || s.backend == nil {
		return nil, errStoreUnavailable
	}
	val, err := s.backend.Get(bytesKey(dictID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("dictdb: dict %d not found", dictID)
	}
	return val, nil
}

func bytesKey(dictID uint64) []byte {
	buf := make([]byte, len("bytes/")+8)
	copy(buf, "bytes/")
	binary.BigEndian.PutUint64(buf[len("bytes/"):], dictID)
	return buf
}

func hashKey(sum [32]byte) []byte {
	buf := make([]byte, len("hash/")+len(sum))
	copy(buf, "hash/")
	copy(buf[len("hash/"):], sum[:])
	return buf
}

func kKey(dictID uint64) []byte {
	buf := make([]byte, len("k/")+8)
	copy(buf, "k/")
	binary.BigEndian.PutUint64(buf[len("k/"):], dictID)
	return buf
}

func leafPayloadModeKey(dictID uint64) []byte {
	buf := make([]byte, len("leafmode/")+8)
	copy(buf, "leafmode/")
	binary.BigEndian.PutUint64(buf[len("leafmode/"):], dictID)
	return buf
}

func currentKey() []byte {
	return []byte("current")
}

func currentKeyForClass(class string) []byte {
	class = strings.TrimSpace(strings.ToLower(class))
	if class == "" || class == "single" || class == "default" {
		return currentKey()
	}
	return []byte("current/" + class)
}

func (s *Store) ensureValueLogWriterLocked() (*valuelog.Writer, error) {
	if s == nil || s.backend == nil {
		return nil, errStoreUnavailable
	}
	if s.vlog != nil {
		return s.vlog, nil
	}
	if s.dir == "" {
		return nil, fmt.Errorf("dictdb: missing backend dir")
	}
	valueLogDir := db.ValueLogDirPath(s.dir)
	if err := os.MkdirAll(valueLogDir, 0700); err != nil {
		return nil, err
	}
	seq, err := nextValueLogSeq(valueLogDir, 0)
	if err != nil {
		return nil, err
	}
	fileID, err := valuelog.EncodeFileID(0, seq)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(valueLogDir, fmt.Sprintf("value-l%d-%06d.log", 0, seq))
	writer, err := valuelog.NewWriterWithStableResourcePinRegistry(path, fileID, s.backend.StableResourceIdentityPinRegistry())
	if err != nil {
		return nil, err
	}
	// Register the producer's newly-created segment before any pointer to it can
	// enter a durable root. Durable publication deliberately refuses to discover
	// unreported dependencies by scanning or statting a derived path.
	if err := s.backend.RegisterValueLogSegment(path, fileID); err != nil {
		return nil, errors.Join(err, writer.Close())
	}
	s.vlog = writer
	s.vlogSeq = seq
	return writer, nil
}

func nextValueLogSeq(dir string, lane uint32) (uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	var maxSeq uint32
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "value-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		core := strings.TrimSuffix(strings.TrimPrefix(name, "value-"), ".log")
		if strings.HasPrefix(core, "l") {
			parts := strings.SplitN(strings.TrimPrefix(core, "l"), "-", 2)
			if len(parts) != 2 {
				continue
			}
			laneVal, err := strconv.ParseUint(parts[0], 10, 32)
			if err != nil || uint32(laneVal) != lane {
				continue
			}
			seqVal, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				continue
			}
			if uint32(seqVal) > maxSeq {
				maxSeq = uint32(seqVal)
			}
			continue
		}
		if lane != 0 {
			continue
		}
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
