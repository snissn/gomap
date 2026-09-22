package templatedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/snissn/gomap/TreeDB/template"
)

var errStoreUnavailable = errors.New("templatedb: store unavailable")

const (
	schemaVer byte = 1
)

// Config controls templatedb storage behavior.
type Config struct {
	MaxCandidatesPerFP    int
	MaxCandidateListBytes int
	MaxIDAttempts         int
}

// NormalizeConfig applies defaults.
func NormalizeConfig(cfg Config) Config {
	if cfg.MaxCandidatesPerFP <= 0 {
		cfg.MaxCandidatesPerFP = 32
	}
	if cfg.MaxCandidateListBytes <= 0 {
		cfg.MaxCandidateListBytes = 4 << 10
	}
	if cfg.MaxIDAttempts <= 0 {
		cfg.MaxIDAttempts = 4
	}
	return cfg
}

// KV is the minimal public DB interface used by templatedb.
type KV interface {
	Get(key []byte) ([]byte, error)
	SetSync(key, value []byte) error
	DeleteSync(key []byte) error
	NewBatch() Batch
}

// Batch is the minimal batch interface used by templatedb.
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	WriteSync() error
	Close() error
}

// Store provides access to template storage backed by a TreeDB public DB.
type Store struct {
	kv  KV
	cfg Config
	mu  sync.Mutex
}

// New wraps an existing public DB handle.
func New(kv KV, cfg Config) *Store {
	if kv == nil {
		return &Store{kv: nil, cfg: NormalizeConfig(cfg)}
	}
	return &Store{kv: kv, cfg: NormalizeConfig(cfg)}
}

// Close closes the underlying store if it implements io.Closer.
func (s *Store) Close() error {
	if s == nil || s.kv == nil {
		return nil
	}
	if c, ok := s.kv.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// GetTemplateDef loads TemplateDefBytes for a template ID.
func (s *Store) GetTemplateDef(_ context.Context, id uint64) ([]byte, error) {
	if s == nil || s.kv == nil {
		return nil, errStoreUnavailable
	}
	val, err := s.kv.Get(templateKey(id))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, template.ErrMissingTemplate
	}
	return val, nil
}

// GetCandidates loads the candidate list for a fingerprint.
func (s *Store) GetCandidates(_ context.Context, fp uint64, max int) ([]template.Candidate, error) {
	if s == nil || s.kv == nil {
		return nil, errStoreUnavailable
	}
	val, err := s.kv.Get(fpKey(fp))
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, nil
	}
	cands, err := decodeCandidates(val)
	if err != nil {
		return nil, err
	}
	if max > 0 && len(cands) > max {
		cands = cands[:max]
	}
	return cands, nil
}

// PutTemplateDef stores a template definition and updates routing indices.
func (s *Store) PutTemplateDef(_ context.Context, defBytes []byte, routeFPs []uint64) (uint64, error) {
	if s == nil || s.kv == nil {
		return 0, errStoreUnavailable
	}
	cfg := s.cfg
	cfg = NormalizeConfig(cfg)
	s.mu.Lock()
	defer s.mu.Unlock()

	// Dedup and sort route fingerprints deterministically.
	if len(routeFPs) > 0 {
		sort.Slice(routeFPs, func(i, j int) bool { return routeFPs[i] < routeFPs[j] })
		j := 0
		for i := 0; i < len(routeFPs); i++ {
			if i == 0 || routeFPs[i] != routeFPs[i-1] {
				routeFPs[j] = routeFPs[i]
				j++
			}
		}
		routeFPs = routeFPs[:j]
	}

	id := uint64(0)
	for attempt := 0; attempt < cfg.MaxIDAttempts; attempt++ {
		id = template.TemplateID(defBytes, byte(attempt))
		if id == 0 {
			continue
		}
		key := templateKey(id)
		existing, err := s.kv.Get(key)
		if err != nil {
			return 0, err
		}
		if existing == nil {
			break
		}
		if bytes.Equal(existing, defBytes) {
			// Idempotent publish; still refresh routing lists.
			break
		}
		if attempt == cfg.MaxIDAttempts-1 {
			return 0, fmt.Errorf("templatedb: template id collision for %d", id)
		}
		id = 0
	}
	if id == 0 {
		return 0, fmt.Errorf("templatedb: unable to assign template id")
	}

	candidateSize := len(defBytes)
	updates := make(map[uint64][]byte, len(routeFPs))
	for _, fp := range routeFPs {
		listBytes, err := s.kv.Get(fpKey(fp))
		if err != nil {
			return 0, err
		}
		cands, err := decodeCandidates(listBytes)
		if err != nil {
			return 0, err
		}
		cands = upsertCandidate(cands, template.Candidate{ID: id, Size: candidateSize})
		if cfg.MaxCandidatesPerFP > 0 && len(cands) > cfg.MaxCandidatesPerFP {
			cands = cands[:cfg.MaxCandidatesPerFP]
		}
		encoded := encodeCandidates(cands)
		for cfg.MaxCandidateListBytes > 0 && len(encoded) > cfg.MaxCandidateListBytes && len(cands) > 1 {
			cands = cands[:len(cands)-1]
			encoded = encodeCandidates(cands)
		}
		updates[fp] = encoded
	}

	batch := s.kv.NewBatch()
	if err := batch.Set(templateKey(id), defBytes); err != nil {
		_ = batch.Close()
		return 0, err
	}
	for fp, enc := range updates {
		if err := batch.Set(fpKey(fp), enc); err != nil {
			_ = batch.Close()
			return 0, err
		}
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

// PutTemplateDefs stores multiple template definitions and updates routing
// indices in a single durable batch.
//
// This is an optional acceleration used by the template engine when publishing
// bursts of templates; it is equivalent to calling PutTemplateDef for each
// template, but coalesces WriteSync.
func (s *Store) PutTemplateDefs(_ context.Context, defs []template.PublishSpec) ([]uint64, error) {
	if s == nil || s.kv == nil {
		return nil, errStoreUnavailable
	}
	if len(defs) == 0 {
		return nil, nil
	}
	cfg := NormalizeConfig(s.cfg)

	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]uint64, len(defs))
	reserved := make(map[uint64][]byte, len(defs))

	type candAdd struct {
		fp   uint64
		cand template.Candidate
	}
	additions := make([]candAdd, 0, len(defs)*8)

	// Assign IDs and collect candidate list additions.
	for i := range defs {
		defBytes := defs[i].DefBytes
		routeFPs := defs[i].RouteFPs

		// Dedup and sort route fingerprints deterministically.
		if len(routeFPs) > 0 {
			sort.Slice(routeFPs, func(i, j int) bool { return routeFPs[i] < routeFPs[j] })
			j := 0
			for k := 0; k < len(routeFPs); k++ {
				if k == 0 || routeFPs[k] != routeFPs[k-1] {
					routeFPs[j] = routeFPs[k]
					j++
				}
			}
			routeFPs = routeFPs[:j]
		}
		defs[i].RouteFPs = routeFPs

		id := uint64(0)
		for attempt := 0; attempt < cfg.MaxIDAttempts; attempt++ {
			id = template.TemplateID(defBytes, byte(attempt))
			if id == 0 {
				continue
			}
			if existingDef, ok := reserved[id]; ok {
				if bytes.Equal(existingDef, defBytes) {
					// Duplicate within the batch; idempotent publish.
					break
				}
				if attempt == cfg.MaxIDAttempts-1 {
					return nil, fmt.Errorf("templatedb: template id collision for %d", id)
				}
				id = 0
				continue
			}
			key := templateKey(id)
			existing, err := s.kv.Get(key)
			if err != nil {
				return nil, err
			}
			if existing == nil || bytes.Equal(existing, defBytes) {
				// Free ID, or idempotent publish.
				break
			}
			if attempt == cfg.MaxIDAttempts-1 {
				return nil, fmt.Errorf("templatedb: template id collision for %d", id)
			}
			id = 0
		}
		if id == 0 {
			return nil, fmt.Errorf("templatedb: unable to assign template id")
		}
		ids[i] = id
		reserved[id] = defBytes

		if len(routeFPs) == 0 {
			continue
		}
		candidateSize := len(defBytes)
		for _, fp := range routeFPs {
			additions = append(additions, candAdd{
				fp:   fp,
				cand: template.Candidate{ID: id, Size: candidateSize},
			})
		}
	}

	// Coalesce per-fingerprint updates.
	type fpAdds struct {
		fp    uint64
		cands []template.Candidate
	}
	fpMap := make(map[uint64][]template.Candidate, len(additions))
	for _, add := range additions {
		fpMap[add.fp] = append(fpMap[add.fp], add.cand)
	}

	updates := make(map[uint64][]byte, len(fpMap))
	for fp, adds := range fpMap {
		listBytes, err := s.kv.Get(fpKey(fp))
		if err != nil {
			return nil, err
		}
		cands, err := decodeCandidates(listBytes)
		if err != nil {
			return nil, err
		}
		for _, cand := range adds {
			cands = upsertCandidate(cands, cand)
		}
		// Deterministic trimming.
		sort.Slice(cands, func(i, j int) bool {
			if cands[i].Size != cands[j].Size {
				return cands[i].Size < cands[j].Size
			}
			return cands[i].ID < cands[j].ID
		})
		if cfg.MaxCandidatesPerFP > 0 && len(cands) > cfg.MaxCandidatesPerFP {
			cands = cands[:cfg.MaxCandidatesPerFP]
		}
		encoded := encodeCandidates(cands)
		for cfg.MaxCandidateListBytes > 0 && len(encoded) > cfg.MaxCandidateListBytes && len(cands) > 1 {
			cands = cands[:len(cands)-1]
			encoded = encodeCandidates(cands)
		}
		updates[fp] = encoded
	}

	batch := s.kv.NewBatch()
	for i := range defs {
		if err := batch.Set(templateKey(ids[i]), defs[i].DefBytes); err != nil {
			_ = batch.Close()
			return nil, err
		}
	}
	for fp, enc := range updates {
		if err := batch.Set(fpKey(fp), enc); err != nil {
			_ = batch.Close()
			return nil, err
		}
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		return nil, err
	}
	if err := batch.Close(); err != nil {
		return nil, err
	}
	return ids, nil
}

func templateKey(id uint64) []byte {
	buf := make([]byte, 2+8)
	buf[0] = schemaVer
	buf[1] = 't'
	binary.BigEndian.PutUint64(buf[2:], id)
	return buf
}

func fpKey(fp uint64) []byte {
	buf := make([]byte, 2+8)
	buf[0] = schemaVer
	buf[1] = 'f'
	binary.BigEndian.PutUint64(buf[2:], fp)
	return buf
}

func decodeCandidates(buf []byte) ([]template.Candidate, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	out := make([]template.Candidate, 0, 8)
	off := 0
	for off < len(buf) {
		id, n := binary.Uvarint(buf[off:])
		if n <= 0 {
			return nil, fmt.Errorf("templatedb: corrupt candidate list")
		}
		off += n
		size, n := binary.Uvarint(buf[off:])
		if n <= 0 {
			return nil, fmt.Errorf("templatedb: corrupt candidate list")
		}
		off += n
		out = append(out, template.Candidate{ID: id, Size: int(size)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Size != out[j].Size {
			return out[i].Size < out[j].Size
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

func encodeCandidates(cands []template.Candidate) []byte {
	if len(cands) == 0 {
		return nil
	}
	// Ensure deterministic order.
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Size != cands[j].Size {
			return cands[i].Size < cands[j].Size
		}
		return cands[i].ID < cands[j].ID
	})
	size := 0
	for _, c := range cands {
		size += uvarintLen(c.ID) + uvarintLen(uint64(c.Size))
	}
	buf := make([]byte, size)
	off := 0
	for _, c := range cands {
		off += binary.PutUvarint(buf[off:], c.ID)
		off += binary.PutUvarint(buf[off:], uint64(c.Size))
	}
	return buf
}

func upsertCandidate(cands []template.Candidate, cand template.Candidate) []template.Candidate {
	for i := range cands {
		if cands[i].ID == cand.ID {
			if cand.Size > 0 && (cands[i].Size == 0 || cand.Size < cands[i].Size) {
				cands[i].Size = cand.Size
			}
			return cands
		}
	}
	cands = append(cands, cand)
	return cands
}

func uvarintLen(v uint64) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	case v < 1<<28:
		return 4
	case v < 1<<35:
		return 5
	case v < 1<<42:
		return 6
	case v < 1<<49:
		return 7
	case v < 1<<56:
		return 8
	case v < 1<<63:
		return 9
	default:
		return 10
	}
}
