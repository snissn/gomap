package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// The segment phase can publish new manifests. Its old recovery capture is
// deliberately not reused: pin both durable slots and all publication resources
// again before admitting any immutable revision deletion.
func (db *DB) gcLeafManifestRevisions(ctx context.Context, opts LeafGenerationGCOptions, stats *LeafGenerationGCStats) error {
	db.teardownMu.RLock()
	defer db.teardownMu.RUnlock()
	if db.closing.Load() {
		return ErrClosed
	}
	if err := db.admitLeafGenerationMaintenance(ctx, opts.MaintenanceLimits); err != nil {
		return err
	}
	// Compatibility stores cannot mint immutable revision authority. Preserve
	// the lawful segment phase without requiring a stable revision capture.
	if db.leafGenerationManifestStore == nil {
		return rootpublication.ErrUnresolvedResource
	}
	if db.leafGenerationManifestStore.mode == leafGenerationManifestCompatibility {
		stats.ManifestRevisionGCUnsupported = true
		return nil
	}
	var roots *RecoverableRootSet
	var err error
	if opts.DryRun {
		roots, err = db.captureRecoverableRootSetForInspectionWithMaintenanceLockHeld(ctx)
	} else {
		roots, err = db.captureRecoverableRootSetWithMaintenanceLockHeld(ctx)
	}
	if err != nil {
		return err
	}
	defer roots.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	if err := roots.Revalidate(); err != nil {
		return err
	}
	// Freeze snapshot admission while consulting existing stale-view generation
	// pins. Ordinary snapshots retain these pins, not a manifest token.
	db.rootReuseMu.Lock()
	defer db.rootReuseMu.Unlock()
	return db.leafGenerationManifestStore.gcRevisionsWithHeldViews(ctx, opts, stats, func(m *leafGenerationManifest) bool {
		for _, gen := range m.Generations {
			if db.leafGenerationPins.count(gen.GenerationID) > 0 {
				return true
			}
		}
		return false
	})
}

// Each selected handle remains live through unlink: Unix numeric identities
// alone cannot authorize reopening an earlier capture after inode reuse.
const leafManifestRevisionGCBatchSize = 16

type leafManifestRevisionGCFile struct {
	name     string
	file     *os.File
	identity rootpublication.StableIdentity
	size     int64
	manifest *leafGenerationManifest
	lease    *rootpublication.IdentityDeleteLease
}

func (s *leafGenerationManifestStore) gcRevisions(ctx context.Context, opts LeafGenerationGCOptions, stats *LeafGenerationGCStats) error {
	return s.gcRevisionsWithHeldViews(ctx, opts, stats, nil)
}

func (s *leafGenerationManifestStore) gcRevisionsWithHeldViews(ctx context.Context, opts LeafGenerationGCOptions, stats *LeafGenerationGCStats, held func(*leafGenerationManifest) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.poisoned {
		return ErrRecoveryRequired
	}
	if s.mode != leafGenerationManifestStable || s.parent == nil || s.registry == nil || s.stableCapability == nil || !s.stableCapability() {
		return rootpublication.ErrNamespacePersistenceUnsupported
	}
	parentID, err := rootpublication.StableIdentityFromFile(s.parent)
	if err != nil {
		return err
	}
	view, err := s.readStableCompatibilityViewLocked()
	if err != nil {
		return err
	}
	current, err := decodeLeafGenerationManifest(view, leafGenerationManifestFileName)
	if err != nil {
		return err
	}
	currentName := leafGenerationDurableManifestFileName(current.ManifestRevision)
	// Saved names/digests control scheduling and detect inventory changes, never
	// deletion authority. Every batch captures fresh exact handles and validates
	// the entire directory before unlinking. This bounds descriptors independently
	// of revision churn, at O(N²/batch-size) worst-case maintenance scan cost.
	inventory := make(map[string][32]byte)
	pending := make(map[string]bool)
	first := true
	for {
		entries, scannedBytes := 0, int64(0)
		files, census, err := s.scanRevisionGCBatch(ctx, opts, held, parentID, currentName, view, first, inventory, pending, &entries, &scannedBytes)
		if err != nil {
			return err
		}
		if first {
			stats.ManifestRevisionsTotal += census.ManifestRevisionsTotal
			stats.ManifestRevisionsProtected += census.ManifestRevisionsProtected
			stats.ManifestRevisionsEligible += census.ManifestRevisionsEligible
			stats.ManifestRevisionBytesEligible += census.ManifestRevisionBytesEligible
			first = false
		}
		// All selected handles and leases survive validation and deletion. Close and
		// abort every remaining capture on partial failure, including cancellation.
		err = func() error {
			defer closeRevisionGCBatch(files)
			if opts.DryRun {
				return nil
			}
			for _, f := range files {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := s.deleteRevisionLocked(f, f.lease); err != nil {
					return err
				}
				stats.ManifestRevisionsDeleted++
				stats.ManifestRevisionBytesDeleted += f.size
				delete(pending, f.name)
				delete(inventory, f.name)
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if opts.DryRun || len(pending) == 0 {
			return nil
		}
	}
}

func closeRevisionGCBatch(files []leafManifestRevisionGCFile) {
	for _, f := range files {
		f.lease.Abort()
		_ = f.file.Close()
	}
}

// Footprint limits are checked independently for each complete inventory, as
// required by MaintenanceLimits; they are not cumulative I/O quotas. Each scan
// rechecks current/held/registry protection from fresh captures.
func (s *leafGenerationManifestStore) scanRevisionGCBatch(ctx context.Context, opts LeafGenerationGCOptions, held func(*leafGenerationManifest) bool, parentID rootpublication.StableIdentity, currentName string, view []byte, first bool, inventory map[string][32]byte, pending map[string]bool, entries *int, scannedBytes *int64) (files []leafManifestRevisionGCFile, census LeafGenerationGCStats, resultErr error) {
	defer func() {
		if resultErr != nil {
			closeRevisionGCBatch(files)
			files = nil
		}
	}()
	if _, err := s.parent.Seek(0, io.SeekStart); err != nil {
		return nil, census, err
	}
	currentFound := false
	seenPending := make(map[string]bool)
	for {
		if err := ctx.Err(); err != nil {
			return files, census, err
		}
		batch, readErr := s.parent.ReadDir(64)
		for _, entry := range batch {
			*entries++
			if opts.MaintenanceLimits.enabled() && *entries > opts.MaintenanceLimits.NativeEntries {
				return files, census, ErrLeafGenerationMaintenanceLimit
			}
			name := entry.Name()
			if strings.HasPrefix(name, "manifest.gc.") {
				return files, census, ErrRecoveryRequired
			}
			if !strings.HasPrefix(name, "manifest.durable.") {
				continue
			}
			// An unselected capture is closed immediately. Selected exact handles are
			// moved into files and kept open until the whole scan and deletion finish.
			err := func() error {
				file, err := rootpublication.OpenStableChildFile(s.parent, name, os.O_RDONLY, 0)
				if err != nil {
					return err
				}
				retained := false
				defer func() {
					if !retained {
						_ = file.Close()
					}
				}()
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() || info.Size() < 0 {
					return rootpublication.ErrResourceConflict
				}
				*scannedBytes += info.Size()
				if opts.MaintenanceLimits.enabled() && *scannedBytes > opts.MaintenanceLimits.NativeBytes {
					return ErrLeafGenerationMaintenanceLimit
				}
				identity, err := rootpublication.StableIdentityFromFile(file)
				if err != nil {
					return err
				}
				data, err := io.ReadAll(file)
				if err != nil {
					return err
				}
				if name == currentName {
					if !bytes.Equal(data, view) {
						return rootpublication.ErrResourceConflict
					}
					currentFound = true
				}
				manifest, err := decodeLeafGenerationManifest(data, name)
				if err != nil {
					return err
				}
				if name != leafGenerationDurableManifestFileName(manifest.ManifestRevision) {
					return rootpublication.ErrResourceConflict
				}
				if err := rootpublication.ValidateStableChildLink(s.parent, file, name); err != nil {
					return err
				}
				digest := sha256.Sum256(data)
				if first {
					inventory[name] = digest
				} else if expected, ok := inventory[name]; !ok || expected != digest {
					return rootpublication.ErrResourceConflict
				}
				if pending[name] {
					seenPending[name] = true
				}
				if first {
					census.ManifestRevisionsTotal++
				}
				if name == currentName || (held != nil && held(manifest)) {
					if first {
						census.ManifestRevisionsProtected++
					}
					delete(pending, name)
					return nil
				}
				if !first && !pending[name] {
					return nil
				}
				namespace := fmt.Sprintf("%s:%d:%x/%s", parentID.Platform, parentID.VolumeID, parentID.ObjectID, name)
				lease, err := s.registry.BeginDeleteAt(identity, namespace)
				if errors.Is(err, rootpublication.ErrResourcePinned) {
					if first {
						census.ManifestRevisionsProtected++
					}
					delete(pending, name)
					return nil
				}
				if err != nil {
					return err
				}
				if first {
					census.ManifestRevisionsEligible++
					census.ManifestRevisionBytesEligible += info.Size()
					pending[name] = true
				}
				if opts.DryRun || len(files) == leafManifestRevisionGCBatchSize {
					lease.Abort()
					return nil
				}
				files = append(files, leafManifestRevisionGCFile{name: name, file: file, identity: identity, size: info.Size(), manifest: manifest, lease: lease})
				retained = true
				return nil
			}()
			if err != nil {
				return files, census, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return files, census, readErr
		}
	}
	if !currentFound {
		return files, census, rootpublication.ErrUnresolvedResource
	}
	if !first {
		for name := range pending {
			if !seenPending[name] {
				return files, census, rootpublication.ErrResourceConflict
			}
		}
	}
	return files, census, nil
}

func (s *leafGenerationManifestStore) deleteRevisionLocked(f leafManifestRevisionGCFile, lease *rootpublication.IdentityDeleteLease) error {
	defer lease.Abort()
	if err := rootpublication.ValidateStableChildLink(s.parent, f.file, f.name); err != nil {
		return err
	}
	// A private same-parent name separates identity validation from deletion of
	// the canonical name. Rebinding the canonical name after this rename cannot
	// cause its replacement to be unlinked.
	s.tempSeq++
	quarantine := fmt.Sprintf("manifest.gc.%016x.tmp", s.tempSeq)
	placeholder, err := rootpublication.OpenStableChildFile(s.parent, quarantine, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	_ = placeholder.Close()
	if s.hooks.BeforeRename != nil {
		if err := s.hooks.BeforeRename(); err != nil {
			_ = rootpublication.RemoveStableChildFile(s.parent, quarantine)
			return err
		}
	}
	if err := rootpublication.RenameStableChildFile(s.parent, f.name, quarantine); err != nil {
		_ = rootpublication.RemoveStableChildFile(s.parent, quarantine)
		return err
	}
	if err := rootpublication.ValidateStableChildLink(s.parent, f.file, quarantine); err != nil {
		// Restore without overwriting any new canonical child. An unresolved
		// collision preserves both pieces of evidence and poisons maintenance.
		restoreErr := rootpublication.LinkStableChildFileNoReplace(s.parent, quarantine, f.name)
		if restoreErr == nil {
			restoreErr = rootpublication.RemoveStableChildFile(s.parent, quarantine)
		}
		return s.ambiguous(errors.Join(err, restoreErr))
	}
	if err := observeStableNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceOuterLeaf, s.leafDir, filepath.Join(s.leafDir, f.name), filepath.Join(s.leafDir, quarantine), s.parent, f.file, f.name, quarantine); err != nil {
		return s.ambiguous(err)
	}
	if err := rootpublication.RemoveStableChildFile(s.parent, quarantine); err != nil {
		return s.ambiguous(err)
	}
	if err := observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceOuterLeaf, s.leafDir, filepath.Join(s.leafDir, quarantine), "", s.parent, f.file, quarantine, ""); err != nil {
		return s.ambiguous(err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir); err != nil {
		return s.ambiguous(err)
	}
	if err := s.parent.Sync(); err != nil {
		return s.ambiguous(err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir); err != nil {
		return s.ambiguous(err)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)
	lease.CommitDeleted()
	return nil
}
