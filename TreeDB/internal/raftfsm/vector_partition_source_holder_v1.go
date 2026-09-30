package raftfsm

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// HasCurrentDBV1 rejects a collection handle bound to a database that a Raft
// snapshot restore has replaced. The comparison is made under the same FSM
// lock used when the restore closes and swaps f.db.
func (f *FSM) HasCurrentDBV1(db *backenddb.DB) bool {
	if f == nil || db == nil {
		return false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.closed && f.db == db
}

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

// PreparedVectorPartitionScopedManifestFromCurrentDBV1 inspects installed hosted
// bytes on the current FSM DB without constructing or warming a serving runtime.
// Root barrier precedes f.mu, matching native snapshot replacement.
func (f *FSM) PreparedVectorPartitionScopedManifestFromCurrentDBV1(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier, collection, index string, generation uint64) (collections.CollectionMeta, collections.VectorPartitionManifestV1, collections.VectorPartitionLocalScopeV1, error) {
	var meta collections.CollectionMeta
	var manifest collections.VectorPartitionManifestV1
	var scope collections.VectorPartitionLocalScopeV1
	if f == nil {
		return meta, manifest, scope, raftcluster.ErrReadBarrierNotSatisfied
	}
	if ctx == nil {
		ctx = context.Background()
	}
	root := raftcluster.MainDBDir(f.cluster.Dir)
	if root == "" {
		return meta, manifest, scope, raftcluster.ErrReadBarrierNotSatisfied
	}
	err := collections.WithVectorPartitionStorageBarrierWithContextV1(ctx, root, func() error {
		f.mu.RLock()
		defer f.mu.RUnlock()
		if f.closed || f.db == nil || filepath.Clean(f.db.Dir()) != filepath.Clean(root) {
			return raftcluster.ErrReadBarrierNotSatisfied
		}
		progress, err := f.appliedProgressLocked()
		if err != nil {
			return err
		}
		if err := barrier.Check(progress); err != nil {
			return err
		}
		local, err := collections.OpenCollectionForRaftSourceV1(f.db, collection)
		if err != nil {
			return err
		}
		meta = local.MetaView()
		manifest, scope, err = local.PreparedVectorPartitionScopedManifestUnderStorageBarrierWithContextV1(ctx, index, generation)
		return err
	})
	return meta, manifest, scope, err
}

// OpenCollectionForRaftSourceFromCurrentDBV1 captures a cold source handle and
// its exact DB identity at an applied boundary. The returned handle does not
// retain the FSM DB across restore; callers must recheck HasCurrentDBV1 around
// source reads and retire stale handles outside the storage/FSM locks.
func (f *FSM) OpenCollectionForRaftSourceFromCurrentDBV1(ctx context.Context, barrier raftcluster.AppliedIndexReadBarrier, name string) (*collections.Collection, *backenddb.DB, error) {
	if f == nil {
		return nil, nil, raftcluster.ErrReadBarrierNotSatisfied
	}
	if ctx == nil {
		ctx = context.Background()
	}
	root := raftcluster.MainDBDir(f.cluster.Dir)
	if root == "" {
		return nil, nil, raftcluster.ErrReadBarrierNotSatisfied
	}
	var local *collections.Collection
	var database *backenddb.DB
	err := collections.WithVectorPartitionStorageBarrierWithContextV1(ctx, root, func() error {
		f.mu.RLock()
		defer f.mu.RUnlock()
		if f.closed || f.db == nil || filepath.Clean(f.db.Dir()) != filepath.Clean(root) {
			return raftcluster.ErrReadBarrierNotSatisfied
		}
		progress, err := f.appliedProgressLocked()
		if err != nil {
			return err
		}
		if err := barrier.Check(progress); err != nil {
			return err
		}
		local, err = collections.OpenCollectionForRaftSourceV1(f.db, name)
		if err != nil {
			return err
		}
		database = f.db
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return local, database, nil
}
