// Package sourcepartition contains canonical source identities and pure ownership
// validation. It has no catalog, Raft, collection or ANN-routing authority.
package sourcepartition

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	SourceShardMapFormatV2     = "canonical_source_shard_map_v2"
	DocumentIDTokenAlgorithmV2 = "sha256_prefix64_document_id_v2"
	MaxSourceShardsV2          = 1 << 16
	MaxSourceDocumentIDBytesV2 = 4096
	MaxSourceImportBytesV2     = 16 << 20
	MaxSourceShardMapBytesV2   = 32 << 20
)

var ErrInvalidSourceShardMapV2 = errors.New("sourcepartition: invalid canonical source shard map")

type CollectionRefV2 struct{ Database, Catalog, Collection string }
type SourceShardV2 struct {
	ShardID, GroupID string
	Start, End       uint64
}
type SourceShardMapV2 struct {
	Format         string
	Collection     CollectionRefV2
	Epoch          uint64
	TokenAlgorithm string
	Shards         []SourceShardV2
	Digest         string
}

// ResolvedSourceShardMapV2 is immutable structural validation, not authority.
// Its private state prevents later caller mutation from rerouting an import.
type ResolvedSourceShardMapV2 struct{ value SourceShardMapV2 }

func validScopeV2(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\x00/:") && strings.TrimSpace(value) == value && utf8.ValidString(value)
}
func validIDV2(value string) bool {
	if value == "" || len(value) > 128 || value == "." || value == ".." {
		return false
	}
	for _, ch := range value {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}

// CanonicalSourceShardMapV2 checks complete, nonoverlapping uint64 coverage and
// exact stable shard IDs. The caller separately admits every group via catalog.
func CanonicalSourceShardMapV2(input SourceShardMapV2) (SourceShardMapV2, error) {
	if input.Format != SourceShardMapFormatV2 || input.TokenAlgorithm != DocumentIDTokenAlgorithmV2 || input.Epoch == 0 || len(input.Shards) == 0 || len(input.Shards) > MaxSourceShardsV2 || !validScopeV2(input.Collection.Database) || !validScopeV2(input.Collection.Catalog) || !validScopeV2(input.Collection.Collection) {
		return SourceShardMapV2{}, fmt.Errorf("%w: map header or bound", ErrInvalidSourceShardMapV2)
	}
	out := input
	out.Shards = slices.Clone(input.Shards)
	seen := make(map[string]struct{}, len(out.Shards))
	for _, shard := range out.Shards {
		if !validIDV2(shard.ShardID) || !validIDV2(shard.GroupID) || shard.Start > shard.End {
			return SourceShardMapV2{}, fmt.Errorf("%w: shard identity or range", ErrInvalidSourceShardMapV2)
		}
		if _, ok := seen[shard.ShardID]; ok {
			return SourceShardMapV2{}, fmt.Errorf("%w: duplicate shard", ErrInvalidSourceShardMapV2)
		}
		seen[shard.ShardID] = struct{}{}
	}
	slices.SortFunc(out.Shards, func(a, b SourceShardV2) int {
		if a.Start < b.Start {
			return -1
		}
		if a.Start > b.Start {
			return 1
		}
		return strings.Compare(a.ShardID, b.ShardID)
	})
	if out.Shards[0].Start != 0 || out.Shards[len(out.Shards)-1].End != ^uint64(0) {
		return SourceShardMapV2{}, fmt.Errorf("%w: incomplete coverage", ErrInvalidSourceShardMapV2)
	}
	for i := 1; i < len(out.Shards); i++ {
		previous := out.Shards[i-1]
		if previous.End == ^uint64(0) || out.Shards[i].Start != previous.End+1 {
			return SourceShardMapV2{}, fmt.Errorf("%w: gap or overlap", ErrInvalidSourceShardMapV2)
		}
	}
	out.Digest = sourceShardMapDigestV2(out)
	return out, nil
}

func ValidateSourceShardMapV2(input SourceShardMapV2) (ResolvedSourceShardMapV2, error) {
	out, err := CanonicalSourceShardMapV2(input)
	if err != nil {
		return ResolvedSourceShardMapV2{}, err
	}
	if input.Digest != out.Digest || !slices.Equal(input.Shards, out.Shards) {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: noncanonical order or digest", ErrInvalidSourceShardMapV2)
	}
	return ResolvedSourceShardMapV2{value: out}, nil
}

func (m ResolvedSourceShardMapV2) Collection() CollectionRefV2 { return m.value.Collection }
func (m ResolvedSourceShardMapV2) Epoch() uint64               { return m.value.Epoch }
func (m ResolvedSourceShardMapV2) Digest() string              { return m.value.Digest }
func (m ResolvedSourceShardMapV2) Copy() SourceShardMapV2 {
	out := m.value
	out.Shards = slices.Clone(out.Shards)
	return out
}
func (m ResolvedSourceShardMapV2) ResolveDocumentID(id []byte) (SourceShardV2, error) {
	token, err := DocumentIDTokenV2(id)
	if err != nil {
		return SourceShardV2{}, err
	}
	i := sort.Search(len(m.value.Shards), func(i int) bool { return m.value.Shards[i].End >= token })
	if i == len(m.value.Shards) || token < m.value.Shards[i].Start {
		return SourceShardV2{}, fmt.Errorf("%w: unassigned ID token", ErrInvalidSourceShardMapV2)
	}
	return m.value.Shards[i], nil
}

// EncodeSourceShardMapV2 encodes canonical content only, never an admission.
func EncodeSourceShardMapV2(input SourceShardMapV2) ([]byte, error) {
	if _, err := ValidateSourceShardMapV2(input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxSourceShardMapBytesV2 {
		return nil, fmt.Errorf("%w: bytes cap", ErrInvalidSourceShardMapV2)
	}
	return raw, nil
}
func DecodeSourceShardMapV2(raw []byte) (ResolvedSourceShardMapV2, error) {
	if len(raw) == 0 || len(raw) > MaxSourceShardMapBytesV2 {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: bytes cap", ErrInvalidSourceShardMapV2)
	}
	// Decode the directory as raw bytes first, then bound each entry before
	// growing the slice. JSON's ordinary slice decoder would allocate by an
	// attacker's entry count before the structural shard-count check.
	var wire struct {
		Format         string
		Collection     CollectionRefV2
		Epoch          uint64
		TokenAlgorithm string
		Shards         json.RawMessage
		Digest         string
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return ResolvedSourceShardMapV2{}, errors.Join(ErrInvalidSourceShardMapV2, err)
	}
	input := SourceShardMapV2{Format: wire.Format, Collection: wire.Collection, Epoch: wire.Epoch, TokenAlgorithm: wire.TokenAlgorithm, Digest: wire.Digest}
	shards := json.NewDecoder(bytes.NewReader(wire.Shards))
	shards.DisallowUnknownFields()
	opening, err := shards.Token()
	if err != nil || opening != json.Delim('[') {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: shard directory array", ErrInvalidSourceShardMapV2)
	}
	for shards.More() {
		if len(input.Shards) >= MaxSourceShardsV2 {
			return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: shard count cap", ErrInvalidSourceShardMapV2)
		}
		var shard SourceShardV2
		if err := shards.Decode(&shard); err != nil {
			return ResolvedSourceShardMapV2{}, errors.Join(ErrInvalidSourceShardMapV2, err)
		}
		if !validIDV2(shard.ShardID) || !validIDV2(shard.GroupID) {
			return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: shard identity", ErrInvalidSourceShardMapV2)
		}
		input.Shards = append(input.Shards, shard)
	}
	closing, err := shards.Token()
	if err != nil || closing != json.Delim(']') {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: shard directory end", ErrInvalidSourceShardMapV2)
	}
	out, err := ValidateSourceShardMapV2(input)
	if err != nil {
		return ResolvedSourceShardMapV2{}, err
	}
	canonical, err := json.Marshal(out.value)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: noncanonical encoded map", ErrInvalidSourceShardMapV2)
	}
	return out, nil
}

func DocumentIDTokenV2(id []byte) (uint64, error) {
	if len(id) == 0 || len(id) > MaxSourceDocumentIDBytesV2 {
		return 0, fmt.Errorf("%w: document ID length", ErrInvalidSourceShardMapV2)
	}
	h := sha256.New()
	_, _ = h.Write([]byte("gomap/canonical-document-id-token/v2\x00"))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(id)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(id)
	var sum [sha256.Size]byte
	digest := h.Sum(sum[:0])
	return binary.BigEndian.Uint64(digest[:8]), nil
}

// ValidateImportIDs checks one bounded range before row publication. It uses
// exact IDs for duplicate detection, never their routing tokens. Durable
// collection uniqueness must additionally reject duplicates in earlier ranges.
func (m ResolvedSourceShardMapV2) ValidateImportIDs(shardID string, ids [][]byte) error {
	if m.value.Epoch == 0 || m.value.Digest == "" || shardID == "" || len(ids) == 0 || len(ids) > 1<<16 {
		return fmt.Errorf("%w: import identity or row cap", ErrInvalidSourceShardMapV2)
	}
	// Sorting temporary indexes preserves caller buffers and costs only one
	// bounded allocation. No global ID map is built.
	order := make([]int, len(ids))
	totalBytes := 0
	for i, id := range ids {
		if len(id) > MaxSourceImportBytesV2-totalBytes {
			return fmt.Errorf("%w: import ID byte cap", ErrInvalidSourceShardMapV2)
		}
		totalBytes += len(id)
		owner, err := m.ResolveDocumentID(id)
		if err != nil {
			return err
		}
		if owner.ShardID != shardID {
			return fmt.Errorf("%w: input row %d belongs to shard %q", ErrInvalidSourceShardMapV2, i, owner.ShardID)
		}
		order[i] = i
	}
	slices.SortFunc(order, func(i, j int) int { return bytes.Compare(ids[i], ids[j]) })
	for i := 1; i < len(order); i++ {
		if bytes.Equal(ids[order[i-1]], ids[order[i]]) {
			return fmt.Errorf("%w: duplicate exact document ID", ErrInvalidSourceShardMapV2)
		}
	}
	return nil
}

func sourceShardMapDigestV2(m SourceShardMapV2) string {
	h := sha256.New()
	writeSourceShardStringV2(h, m.Format)
	writeSourceShardStringV2(h, m.Collection.Database)
	writeSourceShardStringV2(h, m.Collection.Catalog)
	writeSourceShardStringV2(h, m.Collection.Collection)
	writeSourceShardUintV2(h, m.Epoch)
	writeSourceShardStringV2(h, m.TokenAlgorithm)
	writeSourceShardUintV2(h, uint64(len(m.Shards)))
	for _, shard := range m.Shards {
		writeSourceShardStringV2(h, shard.ShardID)
		writeSourceShardStringV2(h, string(shard.GroupID))
		writeSourceShardUintV2(h, shard.Start)
		writeSourceShardUintV2(h, shard.End)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeSourceShardStringV2(h hash.Hash, s string) {
	writeSourceShardUintV2(h, uint64(len(s)))
	_, _ = h.Write([]byte(s))
}

func writeSourceShardUintV2(h hash.Hash, n uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], n)
	_, _ = h.Write(encoded[:])
}
