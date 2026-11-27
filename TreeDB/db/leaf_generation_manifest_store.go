package db

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/atomicfile"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type leafGenerationManifestReplacementMode uint8

const (
	leafGenerationManifestCompatibility leafGenerationManifestReplacementMode = iota + 1
	leafGenerationManifestStable
)

type leafGenerationManifestDurabilityCounters struct {
	ContentSyncs   atomic.Uint64
	NamespaceSyncs atomic.Uint64
}

type leafGenerationManifestStoreHooks struct {
	BeforeTempCreate            func() error
	BeforeRename                func() error
	BeforeDestinationValidation func() error
	BeforeParentSync            func() error
}

type leafGenerationManifestEvidence struct {
	old *os.File
	new *os.File
}

func (e *leafGenerationManifestEvidence) close() {
	if e == nil {
		return
	}
	if e.new != nil {
		_ = e.new.Close()
	}
	if e.old != nil {
		_ = e.old.Close()
	}
}

type leafGenerationManifestStore struct {
	leafDir            string
	registry           *rootpublication.IdentityPinRegistry
	mode               leafGenerationManifestReplacementMode
	poisonOwner        func()
	stableCapability   func() bool
	durabilityCounters *leafGenerationManifestDurabilityCounters
	hooks              leafGenerationManifestStoreHooks
	parent             *os.File
	parentErr          error
	tempSeq            uint64

	mu                  sync.Mutex
	poisoned            bool
	closed              bool
	lastRevision        uint64
	currentRevision     uint64
	legacyFloorRevision uint64
	lastRevisionSeeded  bool
	evidence            []*leafGenerationManifestEvidence
}

func newLeafGenerationManifestStore(leafDir string, registry *rootpublication.IdentityPinRegistry, mode leafGenerationManifestReplacementMode, poison func()) *leafGenerationManifestStore {
	store := &leafGenerationManifestStore{
		leafDir: leafDir, registry: registry, mode: mode, poisonOwner: poison,
		stableCapability:   rootpublication.StableRelativeNamespaceSupported,
		durabilityCounters: &leafGenerationManifestDurabilityCounters{},
	}
	if mode == leafGenerationManifestStable && rootpublication.StableRelativeNamespaceSupported() {
		store.parent, store.parentErr = os.Open(leafDir)
	}
	return store
}

func leafGenerationManifestReplacementModeForPlatform() leafGenerationManifestReplacementMode {
	if rootpublication.StableRelativeNamespaceSupported() {
		return leafGenerationManifestStable
	}
	return leafGenerationManifestCompatibility
}

func (db *DB) initializeLeafGenerationManifestStore(leafDir string, registry *rootpublication.IdentityPinRegistry) {
	if db == nil || db.leafGenerationManifestStore != nil {
		return
	}
	store := newLeafGenerationManifestStore(leafDir, registry, leafGenerationManifestReplacementModeForPlatform(), func() {
		db.publicationPoisoned.Store(true)
	})
	db.leafGenerationManifestStore = store
}

func (s *leafGenerationManifestStore) Replace(manifest *leafGenerationManifest) (*rootpublication.StableResourceToken, error) {
	return s.replace(manifest, nil)
}

// replaceForPreparedClosure persists one immutable manifest revision under the
// prepared closure's file/namespace custody so durable-root producers can
// transfer its exact token or abandon the unpublished revision before meta
// visibility.
func (s *leafGenerationManifestStore) replaceForPreparedClosure(manifest *leafGenerationManifest, closure *LeafGenerationManifestStablePreparedClosure) (*rootpublication.StableResourceToken, error) {
	return s.replace(manifest, closure)
}

func (s *leafGenerationManifestStore) replace(manifest *leafGenerationManifest, custody *LeafGenerationManifestStablePreparedClosure) (*rootpublication.StableResourceToken, error) {
	if s == nil || s.leafDir == "" {
		return nil, errors.New("missing leaf_vlog manifest store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, rootpublication.ErrResourceOwnership
	}
	if s.poisoned {
		return nil, fmt.Errorf("%w: leaf generation manifest replacement outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	if s.mode == leafGenerationManifestCompatibility {
		return s.replaceCompatibility(manifest)
	}
	if s.mode != leafGenerationManifestStable {
		return nil, fmt.Errorf("%w: unknown leaf generation manifest replacement mode %d", rootpublication.ErrUnresolvedResource, s.mode)
	}
	return s.replaceStable(manifest, custody)
}

// seedSelectedRevision seeds monotonic revision allocation and the store's
// current active revision from a durable root's verified DependencyManifestV1
// selection. It also advances lastRevision past any higher durable revision
// filenames present in the retained parent directory without decoding
// unreferenced orphans.
func (s *leafGenerationManifestStore) seedSelectedRevision(selectedRev uint64) {
	if s == nil || selectedRev == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.mode != leafGenerationManifestStable {
		return
	}
	if selectedRev > s.lastRevision {
		s.lastRevision = selectedRev
	}
	if selectedRev > s.currentRevision {
		s.currentRevision = selectedRev
	}
	if s.parent != nil && s.parentErr == nil {
		if maxRev, _, err := s.highestDurableManifestRevisionLocked(); err == nil && maxRev > s.lastRevision {
			s.lastRevision = maxRev
		}
	}
	s.lastRevisionSeeded = true
}

func (s *leafGenerationManifestStore) highestDurableManifestRevisionLocked() (uint64, string, error) {
	if s == nil || s.parent == nil {
		return 0, "", rootpublication.ErrResourceOwnership
	}
	if _, err := s.parent.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	entries, err := s.parent.ReadDir(-1)
	if err != nil {
		return 0, "", err
	}
	_, _ = s.parent.Seek(0, io.SeekStart)
	var maxRev uint64
	var maxName string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		rev, ok := parseLeafGenerationDurableManifestFileName(entry.Name())
		if !ok {
			continue
		}
		if rev > maxRev {
			maxRev = rev
			maxName = entry.Name()
		}
	}
	return maxRev, maxName, nil
}

func (s *leafGenerationManifestStore) highestDurableManifestRevisionBoundedLocked(ctx context.Context, limits LeafGenerationMaintenanceLimits) (uint64, string, error) {
	if s == nil || s.parent == nil {
		return 0, "", rootpublication.ErrResourceOwnership
	}
	if _, err := s.parent.Seek(0, io.SeekStart); err != nil {
		return 0, "", err
	}
	defer func() { _, _ = s.parent.Seek(0, io.SeekStart) }()
	var maxRev uint64
	var maxName string
	entries := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return 0, "", err
			}
		}
		batch, readErr := s.parent.ReadDir(64)
		for _, entry := range batch {
			entries++
			if limits.enabled() && entries > limits.NativeEntries {
				return 0, "", ErrLeafGenerationMaintenanceLimit
			}
			if entry.IsDir() {
				continue
			}
			rev, ok := parseLeafGenerationDurableManifestFileName(entry.Name())
			if !ok {
				continue
			}
			if rev > maxRev {
				maxRev = rev
				maxName = entry.Name()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", readErr
		}
	}
	return maxRev, maxName, nil
}

func (s *leafGenerationManifestStore) readStableManifestChildLocked(name string) (*leafGenerationManifest, error) {
	if s == nil || s.parent == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	file, err := rootpublication.OpenStableChildFile(s.parent, name, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return decodeLeafGenerationManifest(data, name)
}

func (s *leafGenerationManifestStore) readOptionalStableManifestChildLocked(name string) (*leafGenerationManifest, bool, error) {
	if s == nil || s.parent == nil {
		return nil, false, rootpublication.ErrResourceOwnership
	}
	file, err := rootpublication.OpenStableChildFile(s.parent, name, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, false, err
	}
	manifest, err := decodeLeafGenerationManifest(data, name)
	if err != nil {
		return nil, false, err
	}
	return manifest, true, nil
}

func (s *leafGenerationManifestStore) seedStableRevisionLocked() error {
	if s.lastRevisionSeeded {
		return nil
	}
	maxRev, maxName, err := s.highestDurableManifestRevisionLocked()
	if err != nil {
		return err
	}
	if maxRev > 0 {
		manifest, err := s.readStableManifestChildLocked(maxName)
		if err != nil {
			return err
		}
		if manifest.ManifestRevision != maxRev {
			return fmt.Errorf("%w: %s manifest_revision=%d does not match filename revision=%d", ErrLeafGenerationManifestIncompatible, maxName, manifest.ManifestRevision, maxRev)
		}
		if maxRev > s.lastRevision {
			s.lastRevision = maxRev
		}
		if maxRev > s.currentRevision {
			s.currentRevision = maxRev
		}
		s.lastRevisionSeeded = true
		return nil
	}
	// Fall back to a legacy manifest.json only when no immutable
	// manifest.durable.<revision>.json exists yet, preserving monotonic revision
	// allocation and fail-closed validation of corrupt legacy manifests.
	legacy, ok, err := s.readOptionalStableManifestChildLocked(leafGenerationManifestFileName)
	if err != nil {
		return err
	}
	if ok {
		if legacy.ManifestRevision > s.legacyFloorRevision {
			s.legacyFloorRevision = legacy.ManifestRevision
		}
		if legacy.ManifestRevision > s.lastRevision {
			s.lastRevision = legacy.ManifestRevision
		}
	}
	s.lastRevisionSeeded = true
	return nil
}

func (s *leafGenerationManifestStore) Load() (*leafGenerationManifest, bool, error) {
	if s == nil || s.leafDir == "" {
		return nil, false, errors.New("missing leaf_vlog manifest store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, false, rootpublication.ErrResourceOwnership
	}
	if s.poisoned {
		return nil, false, fmt.Errorf("%w: leaf generation manifest replacement outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	if s.mode == leafGenerationManifestCompatibility {
		return loadCompatibilityLeafGenerationManifest(s.leafDir)
	}
	if s.stableCapability == nil || !s.stableCapability() {
		return nil, false, fmt.Errorf("%w: retained-parent manifest load unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if s.parentErr != nil {
		return nil, false, s.parentErr
	}
	maxRev, maxName, err := s.highestDurableManifestRevisionLocked()
	if err != nil {
		return nil, false, err
	}
	if maxRev > 0 {
		manifest, err := s.readStableManifestChildLocked(maxName)
		if err != nil {
			return nil, false, err
		}
		if manifest.ManifestRevision != maxRev {
			return nil, false, fmt.Errorf("%w: %s manifest_revision=%d does not match filename revision=%d", ErrLeafGenerationManifestIncompatible, maxName, manifest.ManifestRevision, maxRev)
		}
		if maxRev > s.lastRevision {
			s.lastRevision = maxRev
		}
		if maxRev > s.currentRevision {
			s.currentRevision = maxRev
		}
		s.lastRevisionSeeded = true
		return manifest, true, nil
	}
	manifest, ok, err := s.readOptionalStableManifestChildLocked(leafGenerationManifestFileName)
	if err != nil || !ok {
		return nil, false, err
	}
	if manifest.ManifestRevision > s.legacyFloorRevision {
		s.legacyFloorRevision = manifest.ManifestRevision
	}
	if manifest.ManifestRevision > s.lastRevision {
		s.lastRevision = manifest.ManifestRevision
	}
	s.lastRevisionSeeded = true
	return manifest, true, nil
}

func (s *leafGenerationManifestStore) listBootstrapFiles() ([]leafGenerationBootstrapFile, error) {
	if s == nil || s.leafDir == "" {
		return nil, errors.New("missing leaf_vlog manifest store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, rootpublication.ErrResourceOwnership
	}
	if s.poisoned {
		return nil, fmt.Errorf("%w: leaf generation manifest replacement outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	if s.mode == leafGenerationManifestCompatibility {
		return listLeafGenerationBootstrapFiles(s.leafDir)
	}
	if s.stableCapability == nil || !s.stableCapability() {
		return nil, fmt.Errorf("%w: retained-parent manifest scan unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if s.parentErr != nil {
		return nil, s.parentErr
	}
	if _, err := s.parent.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	entries, err := s.parent.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	_, _ = s.parent.Seek(0, io.SeekStart)
	return leafGenerationBootstrapFilesFromEntries(entries)
}

func (s *leafGenerationManifestStore) replaceCompatibility(manifest *leafGenerationManifest) (*rootpublication.StableResourceToken, error) {
	candidate, data, err := s.prepareReplacement(manifest, nil, true)
	if err != nil {
		return nil, err
	}
	if err := atomicfile.Write(leafGenerationManifestPath(s.leafDir), data, 0o600); err != nil {
		if atomicfile.ReplacementMayHaveCommitted(err) {
			manifest.ManifestRevision = candidate.ManifestRevision
			return nil, s.ambiguous(err)
		}
		return nil, err
	}
	manifest.ManifestRevision = candidate.ManifestRevision
	return nil, nil
}

func (s *leafGenerationManifestStore) replaceStable(manifest *leafGenerationManifest, custody *LeafGenerationManifestStablePreparedClosure) (_ *rootpublication.StableResourceToken, retErr error) {
	if s.stableCapability == nil || !s.stableCapability() {
		return nil, fmt.Errorf("%w: retained-parent manifest replacement unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if s.registry == nil {
		return nil, fmt.Errorf("%w: stable manifest replacement requires a shared identity registry", rootpublication.ErrUnresolvedResource)
	}
	if s.parentErr != nil {
		return nil, s.parentErr
	}
	if s.parent == nil {
		return nil, fmt.Errorf("%w: stable manifest store has no retained parent", rootpublication.ErrResourceOwnership)
	}
	parent := s.parent
	var revisionFile *os.File
	installed := false
	defer func() {
		if custody == nil && installed && errors.Is(retErr, ErrRecoveryRequired) {
			s.evidence = append(s.evidence, &leafGenerationManifestEvidence{new: revisionFile})
			revisionFile = nil
		}
		if custody == nil && revisionFile != nil {
			retErr = errors.Join(retErr, revisionFile.Close())
		}
	}()

	if !s.lastRevisionSeeded {
		if err := s.seedStableRevisionLocked(); err != nil {
			return nil, err
		}
	}
	candidate, data, err := s.prepareStableReplacement(manifest)
	if err != nil {
		return nil, err
	}

	if hook := s.hooks.BeforeTempCreate; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	// A recovery-selectable slot must retain its exact manifest identity until
	// normal alternating publication overwrites that slot. Stage into a hidden
	// temp child, write and fsync it first, then atomically publish the final
	// immutable manifest.durable.<revision>.json name via no-replace hard link
	// + temp unlink so an unfinished/empty revision can never appear under the
	// authoritative durable revision filename.
	const maxTempCreateAttempts = 64
	var tempName string
	for attempt := 0; attempt < maxTempCreateAttempts; attempt++ {
		s.tempSeq++
		tempName = fmt.Sprintf("manifest.stage.tmp.%016x", s.tempSeq)
		if custody != nil {
			revisionFile, err = custody.openReplacementFile(0, s, tempName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		} else {
			revisionFile, err = rootpublication.OpenStableChildFile(parent, tempName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		}
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return nil, err
		}
	}
	if revisionFile == nil {
		return nil, fmt.Errorf("create stable manifest temp: exhausted %d exact-parent names", maxTempCreateAttempts)
	}
	defer func() {
		if installed {
			return
		}
		cleanupErr := s.unlinkStableRevisionLocked(revisionFile, tempName)
		if cleanupErr == nil {
			cleanupErr = s.syncStableRevisionDeletionLocked()
		}
		if cleanupErr != nil {
			installed = true // Keep evidence in the store or the producer's cleanup receipt.
			if custody != nil {
				custody.earlyRollbackFailed = true
			}
			retErr = s.ambiguous(errors.Join(retErr, fmt.Errorf("clean stable manifest temp %q: %w", tempName, cleanupErr)))
		}
	}()
	if err := observeStableNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceOuterLeaf, s.leafDir, "", revisionFile.Name(), parent, revisionFile, "", tempName); err != nil {
		return nil, s.ambiguous(err)
	}
	if err := revisionFile.Chmod(0o600); err != nil {
		return nil, err
	}
	newIdentity, err := rootpublication.StableIdentityFromFile(revisionFile)
	if err != nil {
		return nil, err
	}
	const maxRevisionCreateAttempts = 64
	var revisionName string
	admittedCustody := false
	for attempt := 0; attempt < maxRevisionCreateAttempts; attempt++ {
		if attempt > 0 {
			if _, err := revisionFile.Seek(0, io.SeekStart); err != nil {
				return nil, err
			}
			if err := revisionFile.Truncate(0); err != nil {
				return nil, err
			}
		}
		if _, err := revisionFile.Write(data); err != nil {
			return nil, err
		}
		if err := syncLeafGenerationManifestFile(revisionFile, s.leafDir); err != nil {
			return nil, err
		}
		s.durabilityCounters.ContentSyncs.Add(1)
		if attempt == 0 {
			if hook := s.hooks.BeforeRename; hook != nil {
				if err := hook(); err != nil {
					return nil, err
				}
			}
		}
		revisionName = leafGenerationDurableManifestFileName(candidate.ManifestRevision)
		if custody != nil {
			// Admit the exact rollback name and identity before exposing the
			// durable revision filename so capacity refusal rolls back cleanly.
			rollbackIdentity := newIdentity
			rollbackIdentity.Generation = candidate.ManifestRevision
			if !admittedCustody {
				if err := custody.admitReplacementIdentity(revisionName, rollbackIdentity); err != nil {
					return nil, err
				}
				admittedCustody = true
			} else {
				custody.updateAdmittedReplacementIdentity(revisionName, rollbackIdentity)
			}
		}
		if err := rootpublication.ValidateStableChildLink(parent, revisionFile, tempName); err != nil {
			return nil, err
		}
		linkErr := rootpublication.LinkStableChildFileNoReplace(parent, tempName, revisionName)
		if linkErr == nil {
			installed = true
			if err := rootpublication.RemoveStableChildFile(parent, tempName); err != nil {
				return nil, s.ambiguous(err)
			}
			if err := observeStableNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceOuterLeaf, s.leafDir, revisionFile.Name(), filepath.Join(s.leafDir, revisionName), parent, revisionFile, tempName, revisionName); err != nil {
				return nil, s.ambiguous(err)
			}
			break
		}
		if !os.IsExist(linkErr) {
			return nil, linkErr
		}
		if candidate.ManifestRevision == ^uint64(0) {
			return nil, fmt.Errorf("%w: manifest revision exhausted", ErrLeafGenerationManifestIncompatible)
		}
		candidate.ManifestRevision++
		data, err = json.MarshalIndent(candidate, "", "  ")
		if err != nil {
			return nil, err
		}
	}
	if !installed {
		return nil, fmt.Errorf("create stable manifest revision: exhausted %d exact-parent names", maxRevisionCreateAttempts)
	}
	if hook := s.hooks.BeforeDestinationValidation; hook != nil {
		if err := hook(); err != nil {
			return nil, s.ambiguous(err)
		}
	}
	if err := rootpublication.ValidateStableChildLink(parent, revisionFile, revisionName); err != nil {
		return nil, s.ambiguous(err)
	}
	proof, err := rootpublication.NewOwnedStableNamespaceCreationProof(parent, revisionFile, revisionName, s.registry.MetadataOwner())
	if custody != nil {
		custody.creationProof = proof
	}
	if err != nil {
		return nil, s.ambiguous(err)
	}
	namespace, bindErr := proof.BindWithMetadata(parent, candidate.ManifestRevision, revisionName, filepath.Join("leaf_vlog", revisionName), s.registry.MetadataOwner())
	if custody != nil {
		custody.namespaces[0] = namespace
	} else {
		defer func() { retErr = errors.Join(retErr, proof.ReleaseWithError()) }()
		if namespace != nil {
			defer func() { retErr = errors.Join(retErr, namespace.ReleaseWithError()) }()
		}
	}
	if bindErr != nil {
		return nil, s.ambiguous(bindErr)
	}
	if err := namespace.BindCleanupRegistry(s.registry); err != nil {
		return nil, s.ambiguous(err)
	}
	if hook := s.hooks.BeforeParentSync; hook != nil {
		if err := hook(); err != nil {
			return nil, s.ambiguous(err)
		}
	}
	if err := stabilizeLeafGenerationManifestNamespace(namespace, s.leafDir, parent); err != nil {
		return nil, s.ambiguous(err)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)

	if err := s.registry.Observe(newIdentity); err != nil {
		return nil, s.ambiguous(err)
	}
	newObserved := true
	defer func() {
		if newObserved {
			_ = s.registry.Unobserve(newIdentity)
		}
	}()
	digest := sha256.Sum256(data)
	token, err := NewStableOuterLeafResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceOuterLeafManifest, LogicalLane: "manifest",
		ResourceID: revisionName, Generation: candidate.ManifestRevision,
		DiagnosticPath: filepath.Join("leaf_vlog", revisionName), File: revisionFile,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(len(data))}, Digest: digest,
		Reachability: rootpublication.ReachabilityOuterLeafGeneration, Namespace: namespace,
		ContentSynced: true, PinRegistry: s.registry,
		OriginalObservation: s.registry, MetadataOwner: s.registry.MetadataOwner(),
	})
	if err != nil {
		return nil, s.ambiguous(err)
	}
	newObserved = false
	manifest.ManifestRevision = candidate.ManifestRevision
	s.lastRevision = candidate.ManifestRevision
	s.currentRevision = candidate.ManifestRevision
	if custody != nil {
		// From here on a private producer refusal must abandon this exact
		// unpublished revision.
		custody.revision, custody.digest = candidate.ManifestRevision, digest
	}
	return token, nil
}

func leafGenerationDurableManifestFileName(revision uint64) string {
	return fmt.Sprintf("manifest.durable.%016x.json", revision)
}

func parseLeafGenerationDurableManifestFileName(name string) (uint64, bool) {
	const prefix = "manifest.durable."
	const suffix = ".json"
	if len(name) != len(prefix)+16+len(suffix) || !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	hexPart := name[len(prefix) : len(name)-len(suffix)]
	rev, err := strconv.ParseUint(hexPart, 16, 64)
	if err != nil || rev == 0 || leafGenerationDurableManifestFileName(rev) != name {
		return 0, false
	}
	return rev, true
}

// abandonPreparedStableRevision removes an unpublished producer revision that
// never entered a durable-root candidate, syncs the retained parent directory,
// and reconciles tracked current/last revision state when no later revision
// superseded it.
func (s *leafGenerationManifestStore) abandonPreparedStableRevision(resourceID string, identity rootpublication.StableIdentity, revision uint64, custody *LeafGenerationManifestStablePreparedClosure) error {
	if s == nil {
		return rootpublication.ErrResourceOwnership
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.parent == nil {
		return rootpublication.ErrResourceOwnership
	}
	if s.poisoned {
		return fmt.Errorf("%w: leaf generation manifest replacement outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	if s.mode != leafGenerationManifestStable || resourceID != leafGenerationDurableManifestFileName(revision) || identity == (rootpublication.StableIdentity{}) {
		return fmt.Errorf("%w: invalid unpublished manifest cleanup authority", rootpublication.ErrResourceOwnership)
	}

	cleanupErr := custody.removeUnpublishedRevisionLocked(s)
	if cleanupErr != nil {
		return s.ambiguous(cleanupErr)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)
	if s.currentRevision == revision || s.lastRevision == revision {
		maxRev, _, err := s.highestDurableManifestRevisionLocked()
		if err != nil {
			return s.ambiguous(err)
		}
		if s.currentRevision == revision {
			s.currentRevision = maxRev
		}
		if s.lastRevision == revision {
			if s.legacyFloorRevision > maxRev {
				s.lastRevision = s.legacyFloorRevision
			} else {
				s.lastRevision = maxRev
			}
		}
	}
	return nil
}

// removeUnpublishedRevisionLocked uses only the original closure's captured
// authority. Its actual child remains in that preborn closure until the store
// lock releases and checked final physical completion runs.
func (closure *LeafGenerationManifestStablePreparedClosure) removeUnpublishedRevisionLocked(store *leafGenerationManifestStore) error {
	parentID, err := rootpublication.StableIdentityFromFile(store.parent)
	if err != nil {
		return err
	}
	child, err := closure.openReplacementFile(1, store, closure.resourceID, os.O_RDONLY, 0)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if child != nil {
		identity, err := rootpublication.StableIdentityFromFile(child)
		if err != nil {
			return err
		}
		if !rootpublication.SamePhysicalIdentity(identity, closure.identity) {
			return rootpublication.ErrResourceConflict
		}
		if err := store.unlinkStableRevisionLocked(child, closure.resourceID); err != nil {
			return err
		}
	}
	if err := store.syncStableRevisionDeletionLocked(); err != nil {
		return err
	}
	return store.registry.ForgetStableNamespaceLinkIdentity(parentID, closure.identity, closure.resourceID)
}

func (s *leafGenerationManifestStore) unlinkStableRevisionLocked(child *os.File, name string) error {
	if err := rootpublication.ValidateStableChildLink(s.parent, child, name); err != nil {
		return err
	}
	if err := rootpublication.RemoveStableChildFile(s.parent, name); err != nil {
		return err
	}
	return observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceOuterLeaf, s.leafDir, child.Name(), "", s.parent, child, name, "")
}

func (s *leafGenerationManifestStore) syncStableRevisionDeletionLocked() error {
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir, s.parent); err != nil {
		return err
	}
	if err := s.parent.Sync(); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterDeletionDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir, s.parent)
}

func (s *leafGenerationManifestStore) syncParentDir() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.parentErr != nil {
		return s.parentErr
	}
	if s.parent == nil {
		if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir); err != nil {
			return err
		}
		if err := syncDirFn(s.leafDir); err != nil {
			return err
		}
		return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir)
	}
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir, s.parent); err != nil {
		return err
	}
	if err := s.parent.Sync(); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, s.leafDir, s.leafDir, s.parent)
}

func syncLeafGenerationManifestFile(file *os.File, root string) error {
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceOuterLeaf, root, file.Name(), file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceOuterLeaf, root, file.Name(), file)
}

func stabilizeLeafGenerationManifestNamespace(namespace *rootpublication.StableNamespaceToken, root string, parent *os.File) error {
	if err := durabilitycut.EmitStablePath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, root, root, parent); err != nil {
		return err
	}
	if err := namespace.Stabilize(); err != nil {
		return err
	}
	return durabilitycut.EmitStablePath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, root, root, parent)
}

func (s *leafGenerationManifestStore) prepareStableReplacement(manifest *leafGenerationManifest) (*leafGenerationManifest, []byte, error) {
	if manifest == nil {
		return nil, nil, errors.New("treedb: leaf generation manifest is nil")
	}
	if manifest.Version != leafGenerationManifestVersion {
		return nil, nil, fmt.Errorf("%w: candidate version=%d want=%d", ErrLeafGenerationManifestIncompatible, manifest.Version, leafGenerationManifestVersion)
	}
	candidate := manifest.clone()
	if s.lastRevision == ^uint64(0) {
		return nil, nil, fmt.Errorf("%w: manifest revision exhausted", ErrLeafGenerationManifestIncompatible)
	}
	candidate.ManifestRevision = s.lastRevision + 1
	if err := validateLeafGenerationManifest(candidate); err != nil {
		return nil, nil, err
	}
	data, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	return candidate, data, nil
}

func (s *leafGenerationManifestStore) prepareReplacement(manifest *leafGenerationManifest, oldFile *os.File, allowPathRead bool) (*leafGenerationManifest, []byte, error) {
	if manifest == nil {
		return nil, nil, errors.New("treedb: leaf generation manifest is nil")
	}
	if manifest.Version != leafGenerationManifestVersion {
		return nil, nil, fmt.Errorf("%w: candidate version=%d want=%d", ErrLeafGenerationManifestIncompatible, manifest.Version, leafGenerationManifestVersion)
	}
	candidate := manifest.clone()
	oldRevision, err := persistedLeafGenerationManifestRevision(s.leafDir, oldFile, allowPathRead)
	if err != nil {
		return nil, nil, err
	}
	if oldRevision == ^uint64(0) {
		return nil, nil, fmt.Errorf("%w: manifest revision exhausted", ErrLeafGenerationManifestIncompatible)
	}
	candidate.ManifestRevision = oldRevision + 1
	if err := validateLeafGenerationManifest(candidate); err != nil {
		return nil, nil, err
	}
	data, err := json.MarshalIndent(candidate, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	return candidate, data, nil
}

func persistedLeafGenerationManifestRevision(leafDir string, oldFile *os.File, allowPathRead bool) (uint64, error) {
	var data []byte
	var err error
	if oldFile != nil {
		if _, err = oldFile.Seek(0, io.SeekStart); err == nil {
			data, err = io.ReadAll(oldFile)
		}
	} else if allowPathRead {
		data, err = os.ReadFile(leafGenerationManifestPath(leafDir))
		if os.IsNotExist(err) {
			return 0, nil
		}
	} else {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(data) == 0 {
		return 0, fmt.Errorf("%w: empty persisted leaf generation manifest", ErrLeafGenerationManifestIncompatible)
	}
	var persisted leafGenerationManifest
	if err := json.Unmarshal(data, &persisted); err != nil {
		return 0, fmt.Errorf("%w: decode persisted leaf generation manifest: %w", ErrLeafGenerationManifestIncompatible, err)
	}
	if err := validatePersistedLeafGenerationManifest(&persisted); err != nil {
		return 0, err
	}
	return persisted.ManifestRevision, nil
}

func (s *leafGenerationManifestStore) ambiguous(err error) error {
	s.poisoned = true
	if s.poisonOwner != nil {
		s.poisonOwner()
	}
	return errors.Join(err, ErrRecoveryRequired)
}

func (s *leafGenerationManifestStore) AmbiguousEvidenceCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.evidence)
}

func (s *leafGenerationManifestStore) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	evidence := s.evidence
	s.evidence = nil
	parent := s.parent
	s.parent = nil
	s.mu.Unlock()
	for _, item := range evidence {
		item.close()
	}
	if parent != nil {
		_ = parent.Close()
	}
}
