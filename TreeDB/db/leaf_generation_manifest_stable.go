package db

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

const leafGenerationManifestResourceID = "leaf-generation-manifest"

func nextLeafGenerationManifestRevision(current *os.File, requested uint64) (uint64, error) {
	currentRevision := uint64(0)
	if current != nil {
		if _, err := current.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
		data, err := io.ReadAll(current)
		if err != nil {
			return 0, err
		}
		var disk leafGenerationManifest
		if err := json.Unmarshal(data, &disk); err != nil {
			return 0, fmt.Errorf("treedb: decode current %s revision: %w", leafGenerationManifestFileName, err)
		}
		currentRevision = disk.Revision
	}
	if requested > currentRevision {
		currentRevision = requested
	}
	if currentRevision == ^uint64(0) {
		return 0, fmt.Errorf("treedb: %s revision exhausted", leafGenerationManifestFileName)
	}
	return currentRevision + 1, nil
}

func openStableManifestTemp(parent *os.File) (*os.File, string, error) {
	for attempt := 0; attempt < 32; attempt++ {
		var suffix [8]byte
		if _, err := rand.Read(suffix[:]); err != nil {
			return nil, "", err
		}
		name := leafGenerationManifestFileName + ".tmp." + hex.EncodeToString(suffix[:])
		file, err := rootpublication.OpenStableChildFile(parent, name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err == nil {
			return file, name, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("treedb: allocate unique %s temporary file", leafGenerationManifestFileName)
}

func (db *DB) manifestPostRenameError(err error) error {
	if db != nil {
		db.publicationPoisoned.Store(true)
	}
	return errors.Join(err, ErrRecoveryRequired)
}

// saveLeafGenerationManifestWithStableResource replaces the generation
// manifest through one retained parent handle and returns a token for the exact
// installed inode. The returned token is already content- and namespace-stable
// but remains pinned for a future publication candidate to consume.
func (db *DB) saveLeafGenerationManifestWithStableResource(
	manifest *leafGenerationManifest,
	reachability rootpublication.ReachabilityField,
) (*rootpublication.StableResourceToken, error) {
	if db == nil || db.closing.Load() {
		return nil, ErrClosed
	}
	if err := db.publicationPoisonedError(); err != nil {
		return nil, err
	}
	if db.valueLogIdentityPins == nil {
		return nil, fmt.Errorf("%w: DB has no stable identity registry", rootpublication.ErrUnresolvedResource)
	}
	if reachability != rootpublication.ReachabilityOuterLeafGeneration {
		return nil, fmt.Errorf("%w: manifest reachability %q", rootpublication.ErrUnresolvedResource, reachability)
	}
	if err := validateLeafGenerationManifest(manifest); err != nil {
		return nil, err
	}

	db.leafGenerationManifestStableMu.Lock()
	defer db.leafGenerationManifestStableMu.Unlock()

	leafDir := LeafLogDirPath(db.dir)
	parent, err := os.Open(leafDir)
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	targetName := leafGenerationManifestFileName
	namespaceKey, err := rootpublication.StableChildNamespace(parent, targetName)
	if err != nil {
		return nil, err
	}

	var (
		current         *os.File
		currentIdentity rootpublication.StableIdentity
		currentObserved bool
		deleteLease     *rootpublication.IdentityDeleteLease
	)
	current, err = rootpublication.OpenStableChildFile(parent, targetName, os.O_RDONLY, 0)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if current != nil {
		defer current.Close()
		currentIdentity, err = rootpublication.StableIdentityFromFile(current)
		if err != nil {
			return nil, err
		}
		if err := db.valueLogIdentityPins.Observe(currentIdentity); err != nil {
			return nil, err
		}
		currentObserved = true
		defer func() {
			if currentObserved {
				_ = db.valueLogIdentityPins.Unobserve(currentIdentity)
			}
		}()
		deleteLease, err = db.valueLogIdentityPins.BeginDeleteAt(currentIdentity, namespaceKey)
		if err != nil {
			return nil, err
		}
		defer func() {
			if deleteLease != nil {
				deleteLease.Abort()
			}
		}()
	}

	revision, err := nextLeafGenerationManifestRevision(current, manifest.Revision)
	if err != nil {
		return nil, err
	}
	persisted := manifest.clone()
	persisted.Revision = revision
	data, err := json.MarshalIndent(persisted, "", "  ")
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)

	temp, tempName, err := openStableManifestTemp(parent)
	if err != nil {
		return nil, err
	}
	tempLinked := true
	defer func() {
		_ = temp.Close()
		if tempLinked {
			_ = rootpublication.RemoveStableChildFile(parent, tempName)
		}
	}()
	if _, err := temp.Write(data); err != nil {
		return nil, err
	}
	diagnosticPath := filepath.ToSlash(filepath.Join(leafVLogDirName, targetName))
	if err := durabilitycut.EmitPath(durabilitycut.BeforeDependencyFileSync, durabilitycut.ResourceOuterLeaf, db.dir, diagnosticPath); err != nil {
		return nil, err
	}
	if err := temp.Sync(); err != nil {
		return nil, err
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterDependencyFileSync, durabilitycut.ResourceOuterLeaf, db.dir, diagnosticPath); err != nil {
		return nil, err
	}

	newIdentity, err := rootpublication.StableIdentityFromFile(temp)
	if err != nil {
		return nil, err
	}
	if err := db.valueLogIdentityPins.Observe(newIdentity); err != nil {
		return nil, err
	}
	newObserved := true
	defer func() {
		if newObserved {
			_ = db.valueLogIdentityPins.Unobserve(newIdentity)
		}
	}()

	if err := rootpublication.RenameStableChildFile(parent, tempName, targetName); err != nil {
		return nil, err
	}
	tempLinked = false
	if deleteLease != nil {
		deleteLease.Commit()
		deleteLease = nil
		_ = db.valueLogIdentityPins.Unobserve(currentIdentity)
		currentObserved = false
	}
	targetPath := filepath.Join(leafDir, targetName)
	if err := observeNamespaceMutation(durabilitycut.NamespaceRename, durabilitycut.ResourceOuterLeaf, leafDir, filepath.Join(leafDir, tempName), targetPath); err != nil {
		return nil, db.manifestPostRenameError(err)
	}

	namespace, err := rootpublication.NewStableNamespaceToken(rootpublication.StableNamespaceSpec{
		Parent: parent, LinkedResource: temp, ParentGeneration: revision,
		Operation: rootpublication.NamespaceRename, OldName: tempName, NewName: targetName,
		DiagnosticPath: leafVLogDirName,
	})
	if err != nil {
		return nil, db.manifestPostRenameError(err)
	}
	defer namespace.Release()
	token, err := NewStableOuterLeafResourceToken(rootpublication.StableResourceSpec{
		Kind: rootpublication.ResourceOuterLeafManifest, LogicalLane: leafVLogDirName,
		ResourceID: leafGenerationManifestResourceID, Generation: revision,
		DiagnosticPath: diagnosticPath, File: temp,
		Frontier: rootpublication.DurableFrontier{Bytes: uint64(len(data))}, Digest: digest,
		Reachability: reachability, Namespace: namespace, ContentSynced: true,
		PinRegistry: db.valueLogIdentityPins,
		OnRelease:   func() { _ = db.valueLogIdentityPins.Unobserve(newIdentity) },
	})
	if err != nil {
		return nil, db.manifestPostRenameError(err)
	}
	newObserved = false
	if err := durabilitycut.EmitPath(durabilitycut.BeforeNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, db.dir, leafDir); err != nil {
		token.Release()
		return nil, db.manifestPostRenameError(err)
	}
	if err := token.Namespace().Stabilize(); err != nil {
		token.Release()
		return nil, db.manifestPostRenameError(err)
	}
	if err := durabilitycut.EmitPath(durabilitycut.AfterNewFileDirectorySync, durabilitycut.ResourceOuterLeaf, db.dir, leafDir); err != nil {
		token.Release()
		return nil, db.manifestPostRenameError(err)
	}
	manifest.Revision = revision
	return token, nil
}

func (db *DB) saveLeafGenerationManifestDurable(manifest *leafGenerationManifest) error {
	token, err := db.saveLeafGenerationManifestWithStableResource(manifest, rootpublication.ReachabilityOuterLeafGeneration)
	if err != nil {
		return err
	}
	token.Release()
	return nil
}
