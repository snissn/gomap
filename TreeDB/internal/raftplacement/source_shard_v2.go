package raftplacement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"slices"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	SourceShardMapFormatV2 = "canonical_source_shard_map_v2"
	DocumentIDTokenAlgorithmV2 = "sha256_prefix64_document_id_v2"
	MaxSourceShardsV2 = 1 << 16
	MaxSourceDocumentIDBytesV2 = 4096
	MaxSourceImportBytesV2 = 16 << 20
)

var ErrInvalidSourceShardMapV2 = errors.New("raftplacement: invalid canonical source shard map")

// SourceShardV2 owns an inclusive range in the canonical document-ID token
// space. Its stable ID is independent of ANN domain/pack and local row ordinal.
type SourceShardV2 struct {
	ShardID string
	GroupID raftcluster.GroupID
	Start uint64
	End uint64
}

// SourceShardMapV2 is an immutable, digest-bound source ownership description.
// Validation alone does not admit it to catalog authority or enable the V1
// simulation-only token mutation route. Its epoch and digest must be bound by
// the generation's admitted V2 root before production use.
type SourceShardMapV2 struct {
	Format string
	Collection CollectionRefV1
	Epoch uint64
	TokenAlgorithm string
	Shards []SourceShardV2
	Digest string
}

type ResolvedSourceShardMapV2 struct {
	collection CollectionRefV1
	epoch uint64
	digest string
	ring ResolvedTokenRingPlanV1
}

// DocumentIDTokenV2 hashes the exact ID bytes, without normalization. Token
// collisions only colocate IDs; callers must retain the full ID as the key.
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

// CanonicalSourceShardMapV2 is the builder entrypoint. It validates complete
// nonoverlapping coverage against known groups, owns its returned slice, and
// fills the canonical digest. It does not publish or grant routing authority.
func (c ResolvedCatalogV1) CanonicalSourceShardMapV2(input SourceShardMapV2) (SourceShardMapV2, error) {
	if input.Format != SourceShardMapFormatV2 || input.TokenAlgorithm != DocumentIDTokenAlgorithmV2 || input.Epoch == 0 || len(input.Shards) == 0 || len(input.Shards) > MaxSourceShardsV2 {
		return SourceShardMapV2{}, fmt.Errorf("%w: format, epoch, algorithm or count", ErrInvalidSourceShardMapV2)
	}
	if err := validateCollectionRef(input.Collection); err != nil {
		return SourceShardMapV2{}, errors.Join(ErrInvalidSourceShardMapV2, err)
	}
	partitions := make([]TokenPartitionV1, len(input.Shards))
	for i, shard := range input.Shards {
		if len(shard.ShardID) > 128 || len(shard.GroupID) > 128 {
			return SourceShardMapV2{}, fmt.Errorf("%w: shard or group ID length", ErrInvalidSourceShardMapV2)
		}
		partitions[i] = TokenPartitionV1{ID: TokenPartitionID(shard.ShardID), GroupID: shard.GroupID, Start: shard.Start, End: shard.End}
	}
	ring, err := ValidateTokenRingPlan(c, TokenRingPlanV1{Partitions: partitions})
	if err != nil {
		return SourceShardMapV2{}, errors.Join(ErrInvalidSourceShardMapV2, err)
	}
	canonical := input
	canonical.Shards = make([]SourceShardV2, len(ring.partitions))
	for i, shard := range ring.partitions {
		canonical.Shards[i] = SourceShardV2{ShardID: string(shard.ID), GroupID: shard.GroupID, Start: shard.Start, End: shard.End}
	}
	canonical.Digest = sourceShardMapDigestV2(canonical)
	return canonical, nil
}

// ValidateSourceShardMapV2 refuses unbound, noncanonical or changed maps and
// returns private immutable lookup state. Caller mutation cannot reroute it.
func (c ResolvedCatalogV1) ValidateSourceShardMapV2(input SourceShardMapV2) (ResolvedSourceShardMapV2, error) {
	canonical, err := c.CanonicalSourceShardMapV2(input)
	if err != nil {
		return ResolvedSourceShardMapV2{}, err
	}
	if input.Digest != canonical.Digest {
		return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: digest", ErrInvalidSourceShardMapV2)
	}
	for i := range input.Shards {
		if input.Shards[i] != canonical.Shards[i] {
			return ResolvedSourceShardMapV2{}, fmt.Errorf("%w: noncanonical order", ErrInvalidSourceShardMapV2)
		}
	}
	partitions := make([]ResolvedTokenPartitionV1, len(canonical.Shards))
	for i, shard := range canonical.Shards {
		partitions[i] = ResolvedTokenPartitionV1{ID: TokenPartitionID(shard.ShardID), GroupID: shard.GroupID, Start: shard.Start, End: shard.End}
	}
	return ResolvedSourceShardMapV2{collection: canonical.Collection, epoch: canonical.Epoch, digest: canonical.Digest, ring: ResolvedTokenRingPlanV1{partitions: partitions}}, nil
}

func (m ResolvedSourceShardMapV2) Collection() CollectionRefV1 { return m.collection }
func (m ResolvedSourceShardMapV2) Epoch() uint64 { return m.epoch }
func (m ResolvedSourceShardMapV2) Digest() string { return m.digest }

func (m ResolvedSourceShardMapV2) ResolveDocumentID(id []byte) (SourceShardV2, error) {
	token, err := DocumentIDTokenV2(id)
	if err != nil {
		return SourceShardV2{}, err
	}
	resolved, err := m.ring.ResolveToken(token)
	if err != nil {
		return SourceShardV2{}, errors.Join(ErrInvalidSourceShardMapV2, err)
	}
	return SourceShardV2{ShardID: string(resolved.ID), GroupID: resolved.GroupID, Start: resolved.Start, End: resolved.End}, nil
}

// ValidateImportIDs checks one bounded range before row publication. It uses
// exact IDs for duplicate detection, never their routing tokens. Durable
// collection uniqueness must additionally reject duplicates in earlier ranges.
func (m ResolvedSourceShardMapV2) ValidateImportIDs(shardID string, ids [][]byte) error {
	if m.epoch == 0 || m.digest == "" || shardID == "" || len(ids) == 0 || len(ids) > 1<<16 {
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
