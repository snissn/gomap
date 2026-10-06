package db

import (
	"bytes"
	"context"
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

type leafManifestRevisionGCFile struct {
	name     string
	file     *os.File
	identity rootpublication.StableIdentity
	size     int64
	manifest *leafGenerationManifest
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
	// Enumerate the retained directory, never its possibly rebound diagnostic
	// pathname. Seek resets the directory cursor while the store lock excludes
	// replacement and other revision scans.
	if _, err := s.parent.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var files []leafManifestRevisionGCFile
	defer func() {
		for _, f := range files {
			_ = f.file.Close()
		}
	}()
	entries, scannedBytes := 0, int64(0)
	currentFound := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, readErr := s.parent.ReadDir(64)
		for _, entry := range batch {
			entries++
			if opts.MaintenanceLimits.enabled() && entries > opts.MaintenanceLimits.NativeEntries {
				return ErrLeafGenerationMaintenanceLimit
			}
			name := entry.Name()
			if strings.HasPrefix(name, "manifest.gc.") {
				return ErrRecoveryRequired
			}
			if !strings.HasPrefix(name, "manifest.durable.") {
				continue
			}
			file, err := rootpublication.OpenStableChildFile(s.parent, name, os.O_RDONLY, 0)
			if err != nil {
				return err
			}
			info, err := file.Stat()
			if err != nil {
				_ = file.Close()
				return err
			}
			if !info.Mode().IsRegular() || info.Size() < 0 {
				_ = file.Close()
				return rootpublication.ErrResourceConflict
			}
			scannedBytes += info.Size()
			if opts.MaintenanceLimits.enabled() && scannedBytes > opts.MaintenanceLimits.NativeBytes {
				_ = file.Close()
				return ErrLeafGenerationMaintenanceLimit
			}
			identity, err := rootpublication.StableIdentityFromFile(file)
			if err != nil {
				_ = file.Close()
				return err
			}
			f := leafManifestRevisionGCFile{name: name, file: file, identity: identity, size: info.Size()}
			files = append(files, f)
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
			files[len(files)-1].manifest = manifest
			if name != leafGenerationDurableManifestFileName(manifest.ManifestRevision) {
				return rootpublication.ErrResourceConflict
			}
			if err := rootpublication.ValidateStableChildLink(s.parent, file, name); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if !currentFound {
		return rootpublication.ErrUnresolvedResource
	}
	// Validate the complete candidate inventory before the first unlink.
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats.ManifestRevisionsTotal++
		if f.name == currentName || (held != nil && held(f.manifest)) {
			stats.ManifestRevisionsProtected++
			continue
		}
		namespace := fmt.Sprintf("%s:%d:%x/%s", parentID.Platform, parentID.VolumeID, parentID.ObjectID, f.name)
		lease, err := s.registry.BeginDeleteAt(f.identity, namespace)
		if errors.Is(err, rootpublication.ErrResourcePinned) {
			stats.ManifestRevisionsProtected++
			continue
		}
		if err != nil {
			return err
		}
		stats.ManifestRevisionsEligible++
		stats.ManifestRevisionBytesEligible += f.size
		if opts.DryRun {
			lease.Abort()
			continue
		}
		if err := s.deleteRevisionLocked(f, lease); err != nil {
			return err
		}
		stats.ManifestRevisionsDeleted++
		stats.ManifestRevisionBytesDeleted += f.size
	}
	return nil
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
