package raftcluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"math"

	hraft "github.com/hashicorp/raft"
)

// InspectReplacementSnapshotSeedV1 recovers the sole native artifact in an
// operation-owned store. It derives the commitment from the actual persisted
// archive; no request-provided digest or new snapshot can substitute on retry.
// Native Open's CRC pass is synchronous and remains owned until it returns.
func InspectReplacementSnapshotSeedV1(ctx context.Context, store hraft.SnapshotStore, old, target NodeID, address string, maxBytes int64) (ReplacementSnapshotSeedV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if store == nil || maxBytes <= 0 {
		return ReplacementSnapshotSeedV1{}, ErrInvalidConfig
	}
	metas, err := store.List()
	if err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	if len(metas) != 1 || metas[0] == nil || !replacementSnapshotIDValidV1(metas[0].ID) || metas[0].Size <= 0 || metas[0].Size > maxBytes || metas[0].Size == math.MaxInt64 {
		return ReplacementSnapshotSeedV1{}, ErrInvalidSnapshotManifest
	}
	if err := ctx.Err(); err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	meta, reader, err := store.Open(metas[0].ID)
	if err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	defer reader.Close()
	if meta == nil || meta.ID != metas[0].ID || meta.Size != metas[0].Size {
		return ReplacementSnapshotSeedV1{}, ErrInvalidSnapshotManifest
	}
	if err := validateReplacementSeedConfigurationV1(meta.Configuration, old, target, address); err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	hash := sha256.New()
	bounded := &io.LimitedReader{R: replacementSnapshotContextReaderV1{ctx: ctx, reader: reader}, N: meta.Size + 1}
	tee := io.TeeReader(bounded, hash)
	manifest, err := DecodeSnapshotManifestV1FromArchiveReader(tee)
	if err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	_, err = io.CopyBuffer(io.Discard, tee, make([]byte, hashicorpRaftSnapshotCopyBuffer))
	if err := errors.Join(err, ctx.Err()); err != nil {
		return ReplacementSnapshotSeedV1{}, err
	}
	if bounded.N != 1 {
		return ReplacementSnapshotSeedV1{}, ErrInvalidSnapshotManifest
	}
	return replacementSnapshotSeedFromMetaV1(meta, manifest, hex.EncodeToString(hash.Sum(nil)))
}
