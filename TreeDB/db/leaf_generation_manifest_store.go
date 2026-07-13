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

func (s *leafGenerationManifestStore) Replace(manifest *leafGenerationManifest) (*rootpublication.StableResourceToken, error) {
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
	return s.replaceStable(manifest)
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
	if s == nil || s.mode == leafGenerationManifestCompatibility {
		return listLeafGenerationBootstrapFiles(s.leafDir)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, rootpublication.ErrResourceOwnership
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
	var err error
	var oldFile, tempFile *os.File
	var oldIdentity rootpublication.StableIdentity
	var oldObserved bool
	var lease *rootpublication.IdentityDeleteLease
	renamed := false
	defer func() {
		if renamed && retErr != nil {
			s.evidence = append(s.evidence, &leafGenerationManifestEvidence{old: oldFile, new: tempFile})
			oldFile, tempFile = nil, nil
		}
		if tempFile != nil {
			_ = tempFile.Close()
		}
		if oldFile != nil {
			_ = oldFile.Close()
		}
	}()

	oldFile, err = rootpublication.OpenStableChildFile(parent, leafGenerationManifestFileName, os.O_RDONLY, 0)
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
	if oldFile != nil {
		oldIdentity, err = rootpublication.StableIdentityFromFile(oldFile)
		if err != nil {
			return nil, err
		}
		if err := s.registry.Observe(oldIdentity); err != nil {
			return nil, err
		}
		oldObserved = true
		defer func() {
			if oldObserved {
				_ = s.registry.Unobserve(oldIdentity)
			}
		}()
		parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
		if err != nil {
			return nil, err
		}
		namespace := fmt.Sprintf("%s:%d:%x/%s", parentIdentity.Platform, parentIdentity.VolumeID, parentIdentity.ObjectID, leafGenerationManifestFileName)
		lease, err = s.registry.BeginDeleteAt(oldIdentity, namespace)
		if err != nil {
			return nil, err
		}
		defer func() {
			if lease != nil {
				lease.Abort()
			}
		}()
	}

	if hook := s.hooks.BeforeTempCreate; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	const maxTempCreateAttempts = 64
	var tempName string
	for attempt := 0; attempt < maxTempCreateAttempts; attempt++ {
		s.tempSeq++
		tempName = fmt.Sprintf("%s.tmp.%016x", leafGenerationManifestFileName, s.tempSeq)
		tempFile, err = rootpublication.OpenStableChildFile(parent, tempName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return nil, err
		}
	}
	if tempFile == nil {
		return nil, fmt.Errorf("create stable manifest temp: exhausted %d exact-parent names", maxTempCreateAttempts)
	}
	tempLinked := true
	defer func() {
		if !renamed && tempLinked {
			cleanupErr := rootpublication.RemoveStableChildFile(parent, tempName)
			if cleanupErr != nil {
				retErr = errors.Join(retErr, fmt.Errorf("clean stable manifest temp: %w", cleanupErr))
			}
		}
	}()
	if err := tempFile.Chmod(0o600); err != nil {
		return nil, err
	}
	if _, err := tempFile.Write(data); err != nil {
		return nil, err
	}
	if err := tempFile.Sync(); err != nil {
		return nil, err
	}
	s.durabilityCounters.ContentSyncs.Add(1)
	if hook := s.hooks.BeforeRename; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	if err := rootpublication.RenameStableChildFile(parent, tempName, leafGenerationManifestFileName); err != nil {
		return nil, err
	}
	renamed = true
	tempLinked = false
	if lease != nil {
		lease.CommitDeleted()
		lease = nil
	}
	if oldObserved {
		if err := s.registry.Unobserve(oldIdentity); err != nil {
			return nil, s.ambiguous(err)
		}
		oldObserved = false
	}
	if hook := s.hooks.BeforeDestinationValidation; hook != nil {
		if err := hook(); err != nil {
			return nil, s.ambiguous(err)
		}
	}
	if err := rootpublication.ValidateStableChildLink(parent, tempFile, leafGenerationManifestFileName); err != nil {
		return nil, s.ambiguous(err)
	}
	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: tempFile, ParentGeneration: candidate.ManifestRevision,
		Operation: rootpublication.NamespaceRename, OldName: tempName, NewName: leafGenerationManifestFileName,
		DiagnosticPath: filepath.Join("leaf_vlog", leafGenerationManifestFileName),
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
	if err := namespace.Stabilize(); err != nil {
		return nil, s.ambiguous(err)
	}
	s.durabilityCounters.NamespaceSyncs.Add(1)

	newIdentity, err := rootpublication.StableIdentityFromFile(tempFile)
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
		ResourceID: leafGenerationManifestFileName, Generation: candidate.ManifestRevision,
		DiagnosticPath: filepath.Join("leaf_vlog", leafGenerationManifestFileName), File: tempFile,
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

func (s *leafGenerationManifestStore) prepareReplacement(manifest *leafGenerationManifest, oldFile *os.File, allowPathRead bool) (*leafGenerationManifest, []byte, error) {
	if manifest == nil {
		return nil, nil, errors.New("treedb: leaf generation manifest is nil")
	}
	candidate := manifest.clone()
	candidate.Version = leafGenerationManifestVersion
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
		return 0, fmt.Errorf("%w: decode persisted leaf generation manifest: %v", ErrLeafGenerationManifestIncompatible, err)
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
