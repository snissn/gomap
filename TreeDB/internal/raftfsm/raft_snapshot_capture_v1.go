package raftfsm

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// retainSnapshotOperationV1 serializes admission with namespace shutdown.
// Cleanup retries run outside snapshotMu because releaseOwner clears this slot.
func (f *FSM) retainSnapshotOperationV1() (*lockfile.Lock, error) {
	if f == nil {
		return nil, fmt.Errorf("raftfsm: FSM is not open")
	}
	f.snapshotMu.Lock()
	owner := f.snapshotOwner
	f.snapshotMu.Unlock()
	if err := owner.RetryCleanupV1(); err != nil {
		return nil, err
	}
	f.snapshotMu.Lock()
	defer f.snapshotMu.Unlock()
	if f.snapshotNamespace == nil || !f.snapshotOperationActive.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("raftfsm: snapshot operation outstanding or closed")
	}
	namespace, err := f.snapshotNamespace.Retain()
	if err != nil {
		f.snapshotOperationActive.Store(false)
	}
	return namespace, err
}

func (f *FSM) rememberSnapshotOwnerV1(snapshot raftcluster.RaftSnapshotV1) bool {
	f.snapshotMu.Lock()
	defer f.snapshotMu.Unlock()
	f.snapshotOwner = snapshot
	return f.snapshotNamespace != nil
}

// CaptureRaftSnapshotV1 executes on HashiCorp's runFSM goroutine. It captures
// admitted immutable file ownership and the applied boundary. Checkpoint and
// lifecycle metadata selection remain capture work; bulk index/archive copying
// and the complete logical digest run later in Persist.
func (f *FSM) CaptureRaftSnapshotV1() (result raftcluster.RaftSnapshotV1, captureErr error) {
	if f == nil {
		return raftcluster.RaftSnapshotV1{}, codedError(raftentry.ErrorUnsafeDurabilityModeV1, "FSM is not open")
	}
	if !rootpublication.StableRelativeNamespaceSupported() {
		return raftcluster.RaftSnapshotV1{}, codedError(
			raftentry.ErrorUnsafeDurabilityModeV1,
			"%w: Raft snapshot capture requires durable rename and removal namespaces",
			rootpublication.ErrNamespacePersistenceUnsupported,
		)
	}
	namespace, err := f.retainSnapshotOperationV1()
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	keep := false
	files := &snapshotCapturedFilesV1{}
	cancel := func() {}
	releaseOwner := func() error {
		cancel()
		err := namespace.Close()
		f.releaseSnapshotOperationV1()
		return err
	}
	defer func() {
		if !keep {
			// Even a failure before a result exists keeps cleanup reachable from FSM.
			failed, err := raftcluster.NewDeferredRaftSnapshotV1(func(context.Context) (raftcluster.RaftSnapshotV1, error) {
				return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: capture failed")
			}, files.close, releaseOwner)
			if err != nil {
				captureErr = errors.Join(captureErr, err)
				return
			}
			f.rememberSnapshotOwnerV1(failed)
			captureErr = errors.Join(captureErr, failed.Release())
		}
	}()
	limits, err := f.snapshotCaptureLimits.normalized()
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	ctx, stopCapture := context.WithTimeout(context.Background(), limits.Lifetime)
	cancel = stopCapture
	files.limits = limits
	var manifest raftcluster.SnapshotManifestV1
	var progressDigest raftapply.LogicalDigestV1
	var options backenddb.Options
	err = collections.WithVectorPartitionStorageBarrierWithContextV1(ctx, raftcluster.MainDBDir(f.cluster.Dir), func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if err := f.requireRaftSnapshotOpenV1(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := f.db.Checkpoint(); err != nil {
			return err
		}
		record, ok, err := f.lastAppliedProgressRecord()
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("raftfsm: no applied snapshot boundary")
		}
		cut, err := f.db.CapturePhysicalSnapshotCutV1(ctx)
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				_ = cut.Close()
			}
		}()
		if cut.StateToken().AppliedCommandLSN != record.AppliedCommandLSN {
			_ = cut.Close()
			return fmt.Errorf("raftfsm: snapshot cut and progress LSN differ")
		}
		err = files.captureStorage(ctx, raftSnapshotDBPrefixV1, raftcluster.MainDBDir(f.cluster.Dir), cut)
		if err != nil {
			return err
		}
		if !f.cluster.DisableSideStores {
			if err := files.add(snapshotCapturedFileV1{name: raftSnapshotSidePrefixV1, mode: os.ModeDir | 0700}); err != nil {
				return err
			}
			sideRoot := snapshotSideStoreRootV1(raftcluster.MainDBDir(f.cluster.Dir))
			for _, name := range raftSnapshotSideStoreEntriesV1 {
				root := filepath.Join(sideRoot, name)
				info, err := os.Lstat(root)
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
					return fmt.Errorf("raftfsm: invalid snapshot side directory %q: %v", root, err)
				}
				if _, err := os.Lstat(filepath.Join(root, "index.db")); errors.Is(err, os.ErrNotExist) {
					// No owner exists for a non-database directory. Preserve an
					// empty namespace, but never silently drop unowned contents.
					dir, err := rootpublication.OpenStableParent(root)
					if err != nil {
						return err
					}
					got, statErr := dir.Stat()
					entries, readErr := dir.ReadDir(1)
					closeErr := dir.Close()
					if statErr != nil || !os.SameFile(info, got) || len(entries) != 0 || !errors.Is(readErr, io.EOF) || closeErr != nil {
						return fmt.Errorf("raftfsm: nonempty or changed unowned side directory %q: %v", root, errors.Join(statErr, readErr, closeErr))
					}
					if err := files.add(snapshotCapturedFileV1{name: raftSnapshotSidePrefixV1 + "/" + name, mode: os.ModeDir | 0700}); err != nil {
						return err
					}
					continue
				} else if err != nil {
					return err
				}
				side, err := f.db.CapturePhysicalSnapshotSideStoreV1(ctx, name)
				if err != nil {
					return err
				}
				if err := files.captureStorage(ctx, raftSnapshotSidePrefixV1+"/"+name, root, side); err != nil {
					_ = side.Close()
					return err
				}
			}
		}
		if err := files.add(snapshotCapturedFileV1{name: raftSnapshotApplyPrefixV1, mode: os.ModeDir | 0700}); err != nil {
			return err
		}
		progress, err := f.progress.CaptureSnapshotPrefixV1()
		if err != nil {
			return err
		}
		if err := files.add(snapshotCapturedFileV1{name: raftSnapshotApplyPrefixV1 + "/" + filepath.Base(raftapply.DurableApplyProgressStorePath("")), mode: 0600, size: progress.SizeBytes(), prefix: progress}); err != nil {
			_ = progress.Close()
			return err
		}
		results, err := f.results.CaptureSnapshotPrefixV1()
		if err != nil {
			return err
		}
		if err := files.add(snapshotCapturedFileV1{name: raftSnapshotApplyPrefixV1 + "/" + filepath.Base(raftapply.DurableApplyResultStorePath("")), mode: 0600, size: results.SizeBytes(), prefix: results}); err != nil {
			_ = results.Close()
			return err
		}
		manifest = raftcluster.SnapshotManifestV1{Format: raftcluster.SnapshotManifestFormatV1, Version: raftcluster.SnapshotManifestVersion1, GroupID: f.cluster.GroupID, NodeID: f.cluster.NodeID, LastIncludedTerm: record.EntryID.Term, LastIncludedIndex: record.EntryID.Index, AppliedCommandLSN: record.AppliedCommandLSN, Scope: f.snapshotScopeIdentityV1(), CreatedAt: time.Now().UTC()}
		progressDigest = record.LogicalDigestV1
		options = f.snapshotRestoreDBOptionsV1("")
		return ctx.Err()
	})
	if err != nil {
		cancel()
		return raftcluster.RaftSnapshotV1{}, errors.Join(err, files.close())
	}
	staging := raftSnapshotStagingDirV1(f.cluster.Layout.SnapshotDir)
	if err := os.MkdirAll(staging, 0700); err != nil {
		cancel()
		return raftcluster.RaftSnapshotV1{}, errors.Join(err, files.close())
	}
	// Two storage copies plus conservative per-entry PAX/tar padding and the
	// manifest. Overflow is avoided by subtracting before multiplication.
	overhead := int64(len(files.entries))*8192 + (1 << 20) + 8192
	if overhead > limits.MaxStagingBytes || files.bytes > (limits.MaxStagingBytes-overhead)/2 {
		cancel()
		return raftcluster.RaftSnapshotV1{}, errors.Join(fmt.Errorf("raftfsm: snapshot staging allowance exceeded"), files.close())
	}
	required := 2*files.bytes + overhead
	available, err := snapshotDiskAvailableV1(staging)
	if err != nil || uint64(required) > available {
		cancel()
		return raftcluster.RaftSnapshotV1{}, errors.Join(err, fmt.Errorf("raftfsm: snapshot disk admission failed: need=%d available=%d", required, available), files.close())
	}
	snapshot, err := raftcluster.NewDeferredRaftSnapshotWithBoundaryV1(func(persistCtx context.Context, nativeTerm, nativeIndex uint64) (raftcluster.RaftSnapshotV1, error) {
		combined, stop := context.WithCancel(persistCtx)
		defer stop()
		stopExpiry := context.AfterFunc(ctx, stop)
		defer stopExpiry()
		return materializeCapturedRaftSnapshotV1(combined, files, manifest, nativeTerm, nativeIndex, progressDigest, options, f.cluster.Layout.SnapshotDir)
	}, files.close, releaseOwner)
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	keep = true
	if !f.rememberSnapshotOwnerV1(snapshot) {
		return raftcluster.RaftSnapshotV1{}, errors.Join(fmt.Errorf("raftfsm: closed during capture"), snapshot.Release())
	}
	// Expiry also closes a finalized carrier. An outstanding reader retains
	// archive/admission until it closes; capture handles are already released.
	context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			_ = snapshot.Release()
		}
	})
	return snapshot, nil
}

func materializeCapturedRaftSnapshotV1(ctx context.Context, files *snapshotCapturedFilesV1, manifest raftcluster.SnapshotManifestV1, nativeTerm, nativeIndex uint64, progressDigest raftapply.LogicalDigestV1, options backenddb.Options, snapshotDir string) (raftcluster.RaftSnapshotV1, error) {
	stage, err := os.MkdirTemp(raftSnapshotStagingDirV1(snapshotDir), "treedb-cut-*")
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	files.stage = stage
	for _, entry := range files.entries {
		if err := ctx.Err(); err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
		path := filepath.Join(stage, filepath.FromSlash(entry.name))
		if entry.mode.IsDir() {
			if err := os.MkdirAll(path, 0700); err != nil {
				return raftcluster.RaftSnapshotV1{}, err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
		output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, entry.mode.Perm())
		if err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
		err = errors.Join(entry.writeTo(ctx, output), output.Close())
		if err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
	}
	mainDir, sideDir := filepath.Join(stage, raftSnapshotDBPrefixV1), filepath.Join(stage, raftSnapshotSidePrefixV1)
	if err := rebindExtractedRaftSnapshotDurableRootsWithContextV1(ctx, mainDir, sideDir, options.DisableSideStores); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	options.Dir = mainDir
	options.ReadOnly = true
	options.PhysicalSnapshotSideStoreCapture = nil
	closeSides, err := wireRaftSnapshotSideStoreLookupsV1(sideDir, &options)
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	database, err := backenddb.Open(options)
	if err != nil {
		_ = closeSides()
		return raftcluster.RaftSnapshotV1{}, err
	}
	digest, digestErr := raftapply.LogicalDigestV1ForSnapshotDBContext(ctx, database, raftapply.LogicalDigestOptionsV1{ScopeRule: raftentry.ScopeRuleV1(manifest.Scope.ScopeRule), DatabaseScope: manifest.Scope.DatabaseScope, CatalogScope: manifest.Scope.CatalogScope})
	closeErr := errors.Join(database.Close(), closeSides())
	if err := errors.Join(digestErr, closeErr, ctx.Err()); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if progressDigest != (raftapply.LogicalDigestV1{}) && progressDigest != digest {
		return raftcluster.RaftSnapshotV1{}, fmt.Errorf("raftfsm: captured progress digest differs from staged cut")
	}
	manifest.LogicalDigestV1 = digest.Hex()
	if nativeIndex != 0 {
		manifest, err = manifest.WithNativeBoundaryV1(nativeTerm, nativeIndex)
		if err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
	}
	header, err := raftcluster.EncodeRaftSnapshotArchiveHeaderV1(raftcluster.NewRaftSnapshotArchiveHeaderV1(manifest))
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	archive, path, err := createRaftSnapshotArchiveFileV1(snapshotDir)
	if err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	files.partialArchive = path
	defer archive.Close()
	tw := tar.NewWriter(archive)
	if err := writeRaftSnapshotFileV1(tw, raftcluster.RaftSnapshotArchiveManifestPathV1, header, 0600); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	for _, entry := range files.entries {
		if err := ctx.Err(); err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
		if entry.mode.IsDir() {
			if err := writeRaftSnapshotDirHeaderV1(tw, entry.name); err != nil {
				return raftcluster.RaftSnapshotV1{}, err
			}
			continue
		}
		input, err := os.Open(filepath.Join(stage, filepath.FromSlash(entry.name)))
		if err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
		info, err := input.Stat()
		if err != nil || info.Size() != entry.size {
			_ = input.Close()
			return raftcluster.RaftSnapshotV1{}, errors.Join(err, fmt.Errorf("raftfsm: staged snapshot length changed: %s", entry.name))
		}
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: int64(entry.mode.Perm()), Size: entry.size}); err != nil {
			_ = input.Close()
			return raftcluster.RaftSnapshotV1{}, err
		}
		staged := snapshotCapturedFileV1{file: input, size: entry.size}
		err = errors.Join(staged.writeTo(ctx, tw), input.Close())
		if err != nil {
			return raftcluster.RaftSnapshotV1{}, err
		}
	}
	if err := tw.Close(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := archive.Sync(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	if err := archive.Close(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	result := raftcluster.RaftSnapshotV1{Manifest: manifest, ArchivePath: path}
	if err := result.Validate(); err != nil {
		return raftcluster.RaftSnapshotV1{}, err
	}
	files.partialArchive = "" // ownership transfers to the returned carrier result
	return result, nil
}
