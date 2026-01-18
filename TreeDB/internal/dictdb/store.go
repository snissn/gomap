package dictdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/db"
)

var errMissingCurrent = errors.New("dictdb: current dictionary missing")

// Store provides access to dictionary storage backed by a TreeDB backend.
type Store struct {
	backend *db.DB
	mu      sync.Mutex
}

// Open opens a dictdb backend at path and returns a Store.
func Open(path string, opts db.Options) (*Store, error) {
	opts.Dir = path
	backend, err := db.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Store{backend: backend}, nil
}

// New wraps an existing backend.
func New(backend *db.DB) *Store {
	return &Store{backend: backend}
}

// Close closes the underlying backend.
func (s *Store) Close() error {
	if s == nil || s.backend == nil {
		return nil
	}
	return s.backend.Close()
}

// GetCurrent returns the current dictionary ID or 0 if unset.
func (s *Store) GetCurrent(_ context.Context) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errMissingCurrent
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

// PutDictBytes inserts dict bytes (deduped by hash) and returns its dictID.
func (s *Store) PutDictBytes(ctx context.Context, dictBytes []byte) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errMissingCurrent
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

	dictID := binary.BigEndian.Uint64(checksum[:8])
	bytesKey := bytesKey(dictID)
	existing, err := s.backend.Get(bytesKey)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		return 0, fmt.Errorf("dictdb: dict bytes for id %d already exist", dictID)
	}

	batch := s.backend.NewBatch()
	if err := batch.Set(bytesKey, dictBytes); err != nil {
		_ = batch.Close()
		return 0, err
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
	return dictID, nil
}

// SetCurrent marks dictID as the current dictionary.
func (s *Store) SetCurrent(ctx context.Context, dictID uint64) error {
	if s == nil || s.backend == nil {
		return errMissingCurrent
	}
	s.mu.Lock()
	defer s.mu.Unlock()

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

// GetDictBytes returns the dictionary bytes for dictID.
func (s *Store) GetDictBytes(_ context.Context, dictID uint64) ([]byte, error) {
	if s == nil || s.backend == nil {
		return nil, errMissingCurrent
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

func currentKey() []byte {
	return []byte("current")
}
