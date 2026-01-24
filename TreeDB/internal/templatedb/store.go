package templatedb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/db"
)

var errStoreUnavailable = errors.New("templatedb: store unavailable")

// Store provides access to template storage backed by a TreeDB backend.
type Store struct {
	backend *db.DB
	mu      sync.Mutex
}

// Open opens a templatedb backend at path and returns a Store.
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

// PutTemplate stores a template (prefix+suffix) and returns its template ID.
// IDs are derived from a SHA256 hash, so repeated inserts are idempotent.
func (s *Store) PutTemplate(ctx context.Context, prefix, suffix []byte) (uint64, error) {
	if s == nil || s.backend == nil {
		return 0, errStoreUnavailable
	}
	h := sha256.New()
	_, _ = h.Write(prefix)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(suffix)
	sum := h.Sum(nil)
	id := binary.BigEndian.Uint64(sum[:8])
	if id == 0 {
		return 0, errors.New("templatedb: invalid template id")
	}

	bytesKey := bytesKey(id)
	hashKey := hashKey(sum)

	s.mu.Lock()
	defer s.mu.Unlock()

	val, err := s.backend.Get(hashKey)
	if err != nil {
		return 0, err
	}
	if val != nil {
		if len(val) != 8 {
			return 0, fmt.Errorf("templatedb: invalid hash entry size %d", len(val))
		}
		return binary.BigEndian.Uint64(val), nil
	}

	existing, err := s.backend.Get(bytesKey)
	if err != nil {
		return 0, err
	}
	if existing != nil {
		p, sfx, err := decodeTemplate(existing)
		if err != nil {
			return 0, err
		}
		if equalBytes(p, prefix) && equalBytes(sfx, suffix) {
			valBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(valBuf, id)
			if err := s.backend.SetSync(hashKey, valBuf); err != nil {
				return 0, err
			}
			return id, nil
		}
		return 0, fmt.Errorf("templatedb: template id collision for %d", id)
	}

	encoded, err := encodeTemplate(prefix, suffix)
	if err != nil {
		return 0, err
	}

	batch := s.backend.NewBatch()
	if err := batch.Set(bytesKey, encoded); err != nil {
		_ = batch.Close()
		return 0, err
	}
	valBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(valBuf, id)
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
	return id, nil
}

// GetTemplate loads prefix/suffix for a template ID.
func (s *Store) GetTemplate(_ context.Context, id uint64) ([]byte, []byte, error) {
	if s == nil || s.backend == nil {
		return nil, nil, errStoreUnavailable
	}
	val, err := s.backend.Get(bytesKey(id))
	if err != nil {
		return nil, nil, err
	}
	if val == nil {
		return nil, nil, fmt.Errorf("templatedb: template %d not found", id)
	}
	return decodeTemplate(val)
}

func encodeTemplate(prefix, suffix []byte) ([]byte, error) {
	if len(prefix) > int(^uint32(0)) || len(suffix) > int(^uint32(0)) {
		return nil, fmt.Errorf("templatedb: template too large")
	}
	buf := make([]byte, 8+len(prefix)+len(suffix))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(prefix)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(suffix)))
	copy(buf[8:8+len(prefix)], prefix)
	copy(buf[8+len(prefix):], suffix)
	return buf, nil
}

func decodeTemplate(buf []byte) ([]byte, []byte, error) {
	if len(buf) < 8 {
		return nil, nil, fmt.Errorf("templatedb: corrupt template")
	}
	pLen := binary.BigEndian.Uint32(buf[0:4])
	sLen := binary.BigEndian.Uint32(buf[4:8])
	if int64(pLen)+int64(sLen) > int64(len(buf)-8) {
		return nil, nil, fmt.Errorf("templatedb: corrupt template")
	}
	prefix := buf[8 : 8+int(pLen)]
	suffix := buf[8+int(pLen) : 8+int(pLen)+int(sLen)]
	return prefix, suffix, nil
}

func bytesKey(id uint64) []byte {
	buf := make([]byte, 9)
	buf[0] = 'b'
	binary.BigEndian.PutUint64(buf[1:], id)
	return buf
}

func hashKey(sum []byte) []byte {
	out := make([]byte, 1+len(sum))
	out[0] = 'h'
	copy(out[1:], sum)
	return out
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
