package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sync"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// VectorPartitionPreparedInputV2 is the bounded semantic BUILD binding. The
// production adapter obtains it from the applied catalog record; local APIs do
// not infer catalog authority from a caller-supplied checksum.
type VectorPartitionPreparedInputV2 struct {
	Generation                                                              uint64
	Collection, IndexName, IndexDefinitionDigest                            string
	SourceMapEpoch                                                          uint64
	SourceMapDigest, SnapshotSetDigest, GraphProfileDigest, PlacementDigest string
	Owners                                                                  []source.SourceOwnerCommitmentV2
	ANNOwners                                                               []source.ANNOwnerCommitmentV2
	LocalSourceOwners, LocalANNOwners                                       []string
}

// VectorPartitionPagedSourceSessionV2 verifies complete local owner streams
// once against BUILD, then keeps only bounded commitments and one pinned DB
// root. Domain opens reuse this immutable verification; no mutable descriptor
// callback or corpus-sized snapshot/ordinal map is retained.
type VectorPartitionPagedSourceSessionV2 struct {
	mu                sync.RWMutex
	collection        *Collection
	manifest          VectorPartitionManifestV1
	ownership         sourcepartition.ResolvedSourceShardMapV2
	snapshot          *backenddb.Snapshot
	generationPin     *VectorPartitionReaderPinV1
	verifiedOwners    []string
	verifiedANNOwners []string
}

func (c *Collection) OpenVectorPartitionPagedSourceSessionV2(ctx context.Context, input VectorPartitionPreparedInputV2, ownership sourcepartition.ResolvedSourceShardMapV2) (*VectorPartitionPagedSourceSessionV2, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil || c.db == nil {
		return nil, errCollectionDBNil
	}
	pin, err := c.AcquireVectorPartitionReaderPinWithContextV1(ctx, input.IndexName, input.Generation)
	if err != nil {
		return nil, err
	}
	store, err := OpenExistingVectorPartitionStoreV1(c.db.Dir())
	if err != nil {
		pin.Release()
		return nil, err
	}
	m, err := store.OpenWithContext(ctx, c.name, input.IndexName, input.Generation)
	if err != nil {
		pin.Release()
		return nil, err
	}
	snap := c.db.AcquireSnapshot()
	if snap == nil {
		pin.Release()
		return nil, backenddb.ErrClosed
	}
	session := &VectorPartitionPagedSourceSessionV2{collection: c, manifest: m, ownership: ownership, snapshot: snap, generationPin: pin}
	if err := session.verifyPreparedSourcesV2(ctx, input); err != nil {
		return nil, errors.Join(err, session.Close())
	}
	if err := session.verifyANNIntentV2(ctx, input, m.PagedRootV2.MetadataDirectory); err != nil {
		return nil, errors.Join(err, session.Close())
	}
	return session, nil
}

func (s *VectorPartitionPagedSourceSessionV2) verifyPreparedSourcesV2(ctx context.Context, input VectorPartitionPreparedInputV2) error {
	m := s.manifest
	if !m.isPagedRootV2() || m.PagedRootV2 == nil {
		return ErrVectorPartitionPagedRuntimeUnsupportedV2
	}
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		return err
	}
	r := m.PagedRootV2
	if !slices.Equal(r.SourceOwners, input.LocalSourceOwners) || !slices.Equal(r.ANNOwners, input.LocalANNOwners) {
		return fmt.Errorf("%w: complete local owner scope", ErrVectorPartitionManifestInvalid)
	}
	placement, err := source.ANNOwnerSetDigestV2(input.ANNOwners)
	if err != nil || placement != input.PlacementDigest {
		return fmt.Errorf("%w: prepared ANN placement binding", ErrVectorPartitionManifestInvalid)
	}
	root, err := source.SourceOwnerSetDigestV2(input.Owners)
	if err != nil {
		return err
	}
	if root != input.SnapshotSetDigest || m.Generation != input.Generation || m.Collection != input.Collection || m.IndexName != input.IndexName || m.IndexDefinitionDigest != input.IndexDefinitionDigest || r.SourceMapEpoch != input.SourceMapEpoch || r.SourceMapDigest != input.SourceMapDigest || r.SourceSnapshotSetDigest != input.SnapshotSetDigest || r.GraphProfileDigest != input.GraphProfileDigest || r.PlacementDigest != input.PlacementDigest || s.ownership.Epoch() != input.SourceMapEpoch || s.ownership.Digest() != input.SourceMapDigest {
		return fmt.Errorf("%w: prepared BUILD binding", ErrVectorPartitionManifestInvalid)
	}
	type ownerState struct {
		expected        source.SourceOwnerCommitmentV2
		count, mapCount uint64
		digest          hash.Hash
	}
	states := make(map[string]*ownerState, len(r.SourceOwners))
	for _, owner := range r.SourceOwners {
		i, ok := slices.BinarySearchFunc(input.Owners, owner, func(a source.SourceOwnerCommitmentV2, b string) int {
			if a.GroupID < b {
				return -1
			}
			if a.GroupID > b {
				return 1
			}
			return 0
		})
		if !ok {
			return fmt.Errorf("%w: local owner absent from BUILD", ErrVectorPartitionManifestInvalid)
		}
		states[owner] = &ownerState{expected: input.Owners[i], digest: source.NewOwnerSnapshotSetHashV2(owner)}
	}
	// The already admitted map is scanned once per cold session. This CPU cost
	// is separate from owner-page I/O; repeated domain opens reuse these counts.
	if err := s.ownership.WalkShards(func(shard sourcepartition.SourceShardV2) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if state := states[shard.GroupID]; state != nil {
			state.mapCount++
		}
		return nil
	}); err != nil {
		return err
	}
	var rows uint64
	cfg := s.collection.meta.Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		return fmt.Errorf("%w: source asset namespace", ErrVectorPartitionManifestInvalid)
	}
	namespace := cfg.AssetManager.Namespace
	read := func(a VectorPartitionAssetV1) ([]byte, error) {
		if a.Ref.Namespace != namespace {
			return nil, fmt.Errorf("%w: foreign source page", ErrVectorPartitionManifestInvalid)
		}
		return readVectorPartitionDirectoryAssetV2(s.collection.db.ColumnAssetRootDir(), a)
	}
	if r.SourceShardDirectory.Ref.Kind != "" {
		if err := walkVectorPartitionDirectoryV2(ctx, r.SourceShardDirectory, "source", "", read, nil, func(rec VectorPartitionDirectoryRecordV2) error {
			state := states[rec.Owner]
			if state == nil || state.count >= state.expected.ShardCount {
				return fmt.Errorf("%w: extra source owner/shard", ErrVectorPartitionManifestInvalid)
			}
			if hex.EncodeToString(rec.Snapshot.IndexDefinitionDigest[:]) != input.IndexDefinitionDigest {
				return fmt.Errorf("%w: source index definition", ErrVectorPartitionManifestInvalid)
			}
			reader, err := s.collection.openVectorPartitionSourceSnapshotAtSnapshotV2(*rec.Snapshot, m.IndexName, s.snapshot)
			if err != nil {
				return err
			}
			if err := reader.ValidateOwnerV2(s.ownership, rec.Owner); err != nil {
				return errors.Join(err, reader.Close())
			}
			if err := reader.Close(); err != nil {
				return err
			}
			source.WriteOwnerSnapshotIdentityV2(state.digest, *rec.Snapshot)
			state.count++
			if rec.Snapshot.RowCount > ^uint64(0)-rows {
				return fmt.Errorf("%w: source row overflow", ErrVectorPartitionManifestInvalid)
			}
			rows += rec.Snapshot.RowCount
			return nil
		}); err != nil {
			return err
		}
	}
	var shards uint64
	for _, owner := range r.SourceOwners {
		state := states[owner]
		if state.count != state.expected.ShardCount || state.count != state.mapCount || hex.EncodeToString(state.digest.Sum(nil)) != state.expected.SnapshotSetDigest {
			return fmt.Errorf("%w: incomplete or changed owner stream", ErrVectorPartitionManifestInvalid)
		}
		shards += state.count
	}
	if shards != r.LocalSourceShardCount || rows != r.LocalSourceRowCount {
		return fmt.Errorf("%w: local source counts", ErrVectorPartitionManifestInvalid)
	}
	s.verifiedOwners = slices.Clone(r.SourceOwners)
	return ctx.Err()
}

func (s *VectorPartitionPagedSourceSessionV2) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		return nil
	}
	err := s.snapshot.Close()
	s.snapshot = nil
	if s.generationPin != nil {
		s.generationPin.Release()
		s.generationPin = nil
	}
	return err
}

// ReadSourceRowV2 performs bounded point-path and chunk-proof work after the
// cold owner verification. It never loads a remote owner or uses current mutable
// collection rows; the retained generation snapshot supplies the exact bytes.
func (s *VectorPartitionPagedSourceSessionV2) ReadSourceRowV2(ctx context.Context, identity VectorPartitionSourceRowIdentityV2) (source.SourceRowV2, error) {
	if s == nil {
		return source.SourceRowV2{}, backenddb.ErrClosed
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readSourceRowV2(ctx, identity)
}

// readSourceRowV2 requires the session read lock or an unpublished builder lease.
func (s *VectorPartitionPagedSourceSessionV2) readSourceRowV2(ctx context.Context, identity VectorPartitionSourceRowIdentityV2) (source.SourceRowV2, error) {
	if s.snapshot == nil {
		return source.SourceRowV2{}, backenddb.ErrClosed
	}
	if _, ok := slices.BinarySearch(s.verifiedOwners, identity.SourceOwner); !ok {
		return source.SourceRowV2{}, fmt.Errorf("%w: source owner is not local", ErrVectorPartitionManifestInvalid)
	}
	key := vectorPartitionDirectoryOwnerPrefixV2(identity.SourceOwner) + identity.ShardID + "\x00"
	var expected *VectorPartitionSourceSnapshotV2
	read := func(a VectorPartitionAssetV1) ([]byte, error) {
		return readVectorPartitionDirectoryAssetV2(s.collection.db.ColumnAssetRootDir(), a)
	}
	err := walkVectorPartitionDirectoryV2(ctx, s.manifest.PagedRootV2.SourceShardDirectory, "source", key, read, nil, func(r VectorPartitionDirectoryRecordV2) error {
		if r.Owner != identity.SourceOwner || r.Snapshot.ShardID != identity.ShardID {
			return nil
		}
		if expected != nil {
			return fmt.Errorf("%w: duplicate source snapshot", ErrVectorPartitionManifestInvalid)
		}
		copy := *r.Snapshot
		expected = &copy
		return nil
	})
	if err != nil {
		return source.SourceRowV2{}, err
	}
	if expected == nil || expected.SnapshotRevision != identity.SnapshotRevision || expected.Digest != identity.SnapshotDigest || identity.Ordinal >= expected.RowCount {
		return source.SourceRowV2{}, fmt.Errorf("%w: selected source identity", ErrVectorPartitionManifestInvalid)
	}
	reader, err := s.collection.openVectorPartitionSourceSnapshotAtSnapshotV2(*expected, s.manifest.IndexName, s.snapshot)
	if err != nil {
		return source.SourceRowV2{}, err
	}
	defer reader.Close()
	chunk, err := reader.ReadChunk(identity.Ordinal / uint64(expected.RowsPerChunk))
	if err != nil {
		return source.SourceRowV2{}, err
	}
	offset := identity.Ordinal % uint64(expected.RowsPerChunk)
	if offset >= uint64(len(chunk.Chunk.Rows)) {
		return source.SourceRowV2{}, fmt.Errorf("%w: selected source ordinal", ErrVectorPartitionManifestInvalid)
	}
	row := chunk.Chunk.Rows[offset]
	if row.LocalOrdinal != identity.Ordinal || row.DocumentRevision != identity.DocumentRevision || expected.Digest == ([sha256.Size]byte{}) {
		return source.SourceRowV2{}, fmt.Errorf("%w: selected document revision", ErrVectorPartitionManifestInvalid)
	}
	return row, nil
}
