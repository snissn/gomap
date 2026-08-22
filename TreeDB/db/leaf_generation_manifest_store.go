package db

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	mu       sync.Mutex
	poisoned bool
	closed   bool
	evidence []*leafGenerationManifestEvidence
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
	token, _, err := s.replace(manifest, false)
	return token, err
}

// replaceForPreparedClosure additionally returns the exact compatibility view
// that preceded the new immutable revision. Durable-root producers retain this
// rollback material until their candidate owns the revision or abandons it
// before meta visibility.
func (s *leafGenerationManifestStore) replaceForPreparedClosure(manifest *leafGenerationManifest) (*rootpublication.StableResourceToken, []byte, error) {
	return s.replace(manifest, true)
}

func (s *leafGenerationManifestStore) replace(manifest *leafGenerationManifest, capturePrevious bool) (*rootpublication.StableResourceToken, []byte, error) {
	if s == nil || s.leafDir == "" {
		return nil, nil, errors.New("missing leaf_vlog manifest store")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil, rootpublication.ErrResourceOwnership
	}
	if s.poisoned {
		return nil, nil, fmt.Errorf("%w: leaf generation manifest replacement outcome is ambiguous; reopen required", ErrRecoveryRequired)
	}
	if s.mode == leafGenerationManifestCompatibility {
		token, err := s.replaceCompatibility(manifest)
		return token, nil, err
	}
	if s.mode != leafGenerationManifestStable {
		return nil, nil, fmt.Errorf("%w: unknown leaf generation manifest replacement mode %d", rootpublication.ErrUnresolvedResource, s.mode)
	}
	var previous []byte
	if capturePrevious {
		var err error
		previous, err = s.readStableCompatibilityViewLocked()
		if err != nil {
			return nil, nil, err
		}
	}
	token, err := s.replaceStable(manifest)
	return token, previous, err
}

func (s *leafGenerationManifestStore) readStableCompatibilityViewLocked() ([]byte, error) {
	if s == nil || s.parent == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	file, err := rootpublication.OpenStableChildFile(s.parent, leafGenerationManifestFileName, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	if _, err := decodeLeafGenerationManifest(data, leafGenerationManifestFileName); err != nil {
		return nil, err
	}
	return data, nil
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
		return loadLeafGenerationManifest(s.leafDir)
	}
	if s.stableCapability == nil || !s.stableCapability() {
		return nil, false, fmt.Errorf("%w: retained-parent manifest load unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
	}
	if s.parentErr != nil {
		return nil, false, s.parentErr
	}
	file, err := rootpublication.OpenStableChildFile(s.parent, leafGenerationManifestFileName, os.O_RDONLY, 0)
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
	manifest, err := decodeLeafGenerationManifest(data, leafGenerationManifestFileName)
	if err != nil {
		return nil, false, err
	}
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

func (s *leafGenerationManifestStore) replaceStable(manifest *leafGenerationManifest) (_ *rootpublication.StableResourceToken, retErr error) {
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
	var oldFile, revisionFile *os.File
	installed := false
	defer func() {
		if installed && errors.Is(retErr, ErrRecoveryRequired) {
			s.evidence = append(s.evidence, &leafGenerationManifestEvidence{new: revisionFile})
			revisionFile = nil
		}
		if revisionFile != nil {
			_ = revisionFile.Close()
		}
		if oldFile != nil {
			_ = oldFile.Close()
		}
	}()

	oldFile, err := rootpublication.OpenStableChildFile(parent, leafGenerationManifestFileName, os.O_RDONLY, 0)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		oldFile = nil
	}
	candidate, data, err := s.prepareReplacement(manifest, oldFile, false)
	if err != nil {
		return nil, err
	}

	if hook := s.hooks.BeforeTempCreate; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	// A recovery-selectable slot must retain its exact manifest identity until
	// normal alternating publication overwrites that slot. Therefore the stable
	// producer writes one immutable revision child and updates manifest.json only
	// as a compatibility view. Replacing the pinned compatibility path would
	// otherwise make the older meta unrecoverable or permanently block the next
	// manifest publication.
	const maxRevisionCreateAttempts = 64
	var revisionName string
	for attempt := 0; attempt < maxRevisionCreateAttempts; attempt++ {
		revisionName = leafGenerationDurableManifestFileName(candidate.ManifestRevision)
		revisionFile, err = rootpublication.OpenStableChildFile(parent, revisionName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return nil, err
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
	if revisionFile == nil {
		return nil, fmt.Errorf("create stable manifest revision: exhausted %d exact-parent names", maxRevisionCreateAttempts)
	}
	revisionLinked := true
	defer func() {
		if !installed && revisionLinked {
			cleanupErr := rootpublication.RemoveStableChildFile(parent, revisionName)
			if cleanupErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("clean stable manifest revision: %w", cleanupErr))
			}
		}
	}()
	if err := observeNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceOuterLeaf, s.leafDir, "", revisionFile.Name()); err != nil {
		return nil, s.ambiguous(err)
	}
	if err := revisionFile.Chmod(0o600); err != nil {
		return nil, err
	}
	if _, err := revisionFile.Write(data); err != nil {
		return nil, err
	}
	if err := syncLeafGenerationManifestFile(revisionFile, s.leafDir); err != nil {
		return nil, err
	}
	s.durabilityCounters.ContentSyncs.Add(1)
	if hook := s.hooks.BeforeRename; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	installed = true
	if hook := s.hooks.BeforeDestinationValidation; hook != nil {
		if err := hook(); err != nil {
			return nil, s.ambiguous(err)
		}
	}
	if err := rootpublication.ValidateStableChildLink(parent, revisionFile, revisionName); err != nil {
		return nil, s.ambiguous(err)
	}
	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: revisionFile, ParentGeneration: candidate.ManifestRevision,
		Operation: rootpublication.NamespaceCreate, NewName: revisionName,
		DiagnosticPath: filepath.Join("leaf_vlog", revisionName),
	})
	if err != nil {
		return nil, s.ambiguous(err)
	}
	defer namespace.Release()
	if hook := s.hooks.BeforeParentSync; hook != nil {
		if err := hook(); err != nil {
			return nil, s.ambiguous(err)
		}
	}
	if err := stabilizeLeafGenerationManifestNamespace(namespace, s.leafDir); err != nil {
		return nil, s.ambiguous(err)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)
	// Keep the legacy path as an operational/diagnostic view. New-format recovery
	// never trusts this path: it reads the exact immutable revision retained by
	// the selected durable-root resource set.
	if err := s.replaceStableCompatibilityView(parent, data); err != nil {
		manifest.ManifestRevision = candidate.ManifestRevision
		return nil, s.ambiguous(err)
	}

	newIdentity, err := rootpublication.StableIdentityFromFile(revisionFile)
	if err != nil {
		return nil, s.ambiguous(err)
	}
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
		OnRelease: func() { _ = s.registry.Unobserve(newIdentity) },
	})
	if err != nil {
		return nil, s.ambiguous(err)
	}
	newObserved = false
	manifest.ManifestRevision = candidate.ManifestRevision
	return token, nil
}

func leafGenerationDurableManifestFileName(revision uint64) string {
	return fmt.Sprintf("manifest.durable.%016x.json", revision)
}

// abandonPreparedStableRevision rolls back a producer revision that never
// entered a durable-root candidate. The fixed compatibility view is restored
// only if it still names this exact revision; a later replacement wins.
func (s *leafGenerationManifestStore) abandonPreparedStableRevision(resourceID string, identity rootpublication.StableIdentity, revision uint64, previousView []byte) error {
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

	currentView, err := s.readStableCompatibilityViewLocked()
	if err != nil {
		return s.ambiguous(err)
	}
	var currentRevision uint64
	if len(currentView) != 0 {
		current, decodeErr := decodeLeafGenerationManifest(currentView, leafGenerationManifestFileName)
		if decodeErr != nil {
			return s.ambiguous(decodeErr)
		}
		currentRevision = current.ManifestRevision
	}
	if currentRevision != 0 && currentRevision < revision {
		return s.ambiguous(fmt.Errorf("%w: compatibility manifest revision=%d precedes abandoned revision=%d", rootpublication.ErrResourceConflict, currentRevision, revision))
	}
	if currentRevision == 0 || currentRevision == revision {
		if len(previousView) != 0 {
			previous, decodeErr := decodeLeafGenerationManifest(previousView, leafGenerationManifestFileName)
			if decodeErr != nil || previous.ManifestRevision == 0 || previous.ManifestRevision >= revision {
				return s.ambiguous(errors.Join(decodeErr, fmt.Errorf("%w: invalid previous manifest view for abandoned revision=%d", rootpublication.ErrResourceConflict, revision)))
			}
			if err := s.replaceStableCompatibilityView(s.parent, previousView); err != nil {
				return s.ambiguous(err)
			}
			s.durabilityCounters.ContentSyncs.Add(1)
			s.durabilityCounters.NamespaceSyncs.Add(1)
		} else if currentRevision == revision {
			if err := rootpublication.RemoveStableChildFile(s.parent, leafGenerationManifestFileName); err != nil && !os.IsNotExist(err) {
				return s.ambiguous(err)
			}
			if err := s.parent.Sync(); err != nil {
				return s.ambiguous(err)
			}
			s.durabilityCounters.NamespaceSyncs.Add(1)
		}
	}

	cleanupErr := cleanupLeafGenerationPackStablePreparedSegmentsRetainingParent(
		s.parent,
		[]rewriteCreatedSegment{{path: filepath.Join(s.leafDir, resourceID), identity: identity}},
		s.registry,
	)
	if cleanupErr != nil {
		return s.ambiguous(cleanupErr)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)
	return nil
}

func (s *leafGenerationManifestStore) replaceStableCompatibilityView(parent *os.File, data []byte) (retErr error) {
	if s == nil || parent == nil {
		return rootpublication.ErrResourceOwnership
	}
	const maxTempCreateAttempts = 64
	var temp *os.File
	var tempName string
	for attempt := 0; attempt < maxTempCreateAttempts; attempt++ {
		s.tempSeq++
		tempName = fmt.Sprintf("%s.view.%016x", leafGenerationManifestFileName, s.tempSeq)
		var err error
		temp, err = rootpublication.OpenStableChildFile(parent, tempName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return err
		}
	}
	if temp == nil {
		return fmt.Errorf("create stable manifest compatibility view: exhausted %d names", maxTempCreateAttempts)
	}
	renamed := false
	defer func() {
		_ = temp.Close()
		if !renamed {
			retErr = errors.Join(retErr, rootpublication.RemoveStableChildFile(parent, tempName))
		}
	}()
	if err := observeNamespaceMutation(durabilitycut.NamespaceCreate, durabilitycut.ResourceOuterLeaf, s.leafDir, "", temp.Name()); err != nil {
		return err
	}
	if _, err := temp.Write(data); err != nil {
		return err
	}
	if err := syncLeafGenerationManifestFile(temp, s.leafDir); err != nil {
		return err
	}
	if err := rootpublication.RenameStableChildFile(parent, tempName, leafGenerationManifestFileName); err != nil {
		return err
	}
	renamed = true
	if err := observeNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceOuterLeaf, s.leafDir, temp.Name(), leafGenerationManifestPath(s.leafDir)); err != nil {
		return err
	}
	if err := rootpublication.ValidateStableChildLink(parent, temp, leafGenerationManifestFileName); err != nil {
		return err
	}
	parentGeneration, err := rootpublication.StableNamespaceParentGeneration(parent)
	if err != nil {
		return err
	}
	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: temp, ParentGeneration: parentGeneration, Operation: rootpublication.NamespaceRename,
		OldName: tempName, NewName: leafGenerationManifestFileName,
		DiagnosticPath: filepath.Join("leaf_vlog", leafGenerationManifestFileName),
	})
	if err != nil {
		return err
	}
	defer namespace.Release()
	return stabilizeLeafGenerationManifestNamespace(namespace, s.leafDir)
}

func syncLeafGenerationManifestFile(file *os.File, root string) error {
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceOuterLeaf, root, file.Name()); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceOuterLeaf, root, file.Name())
}

func stabilizeLeafGenerationManifestNamespace(namespace *rootpublication.StableNamespaceToken, root string) error {
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, root, root); err != nil {
		return err
	}
	if err := namespace.Stabilize(); err != nil {
		return err
	}
	return durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, root, root)
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
