package raftfsm

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// PreparedVectorPartitionManifestFromCurrentDBV1 captures full-local source
// evidence from this FSM's current database. The root barrier precedes f.mu,
// matching snapshot export/install; holding both prevents a restored snapshot
// from replacing the DB while the collection is opened and verified.
func (f *FSM) PreparedVectorPartitionManifestFromCurrentDBV1(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier, collection, index string, generation uint64) (raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
	var progress raftcluster.AppliedProgress
	var zero collections.VectorPartitionManifestV1
	if f == nil {
		return progress, zero, fmt.Errorf("raftfsm: source holder FSM is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	root := raftcluster.MainDBDir(f.cluster.Dir)
	if root == "" {
		return progress, zero, fmt.Errorf("raftfsm: source holder has no main DB root")
	}
	var manifest collections.VectorPartitionManifestV1
	err := collections.WithVectorPartitionStorageBarrierWithContextV1(ctx, root, func() error {
		f.mu.RLock()
		defer f.mu.RUnlock()
		if f.closed || f.db == nil || filepath.Clean(f.db.Dir()) != filepath.Clean(root) {
			return fmt.Errorf("raftfsm: source holder current DB is unavailable")
		}
		var err error
		progress, err = f.appliedProgressLocked()
		if err != nil {
			return err
		}
		if err := barrier.Check(progress); err != nil {
			return fmt.Errorf("raftfsm: %w", err)
		}
		local, err := collections.OpenCollectionForRaftSourceV1(f.db, collection)
		if err != nil {
			return err
		}
		manifest, err = local.PreparedVectorPartitionManifestUnderStorageBarrierWithContextV1(ctx, index, generation)
		return err
	})
	if err != nil {
		return progress, zero, err
	}
	return progress, manifest, nil
}
