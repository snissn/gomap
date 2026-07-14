package db

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// StableDictionaryResourceProvider captures the exact durable transitive
// closure needed to decode one dictionary generation.
type StableDictionaryResourceProvider interface {
	CaptureDictionaryResources(context.Context, uint64) (*rootpublication.StableResourceSet, error)
}

// ValidateStableDictionaryResourceClosure binds the bytes selected by an
// encoder to every physical resource returned by a dictionary provider.
func ValidateStableDictionaryResourceClosure(resources *rootpublication.StableResourceSet, dictID uint64, dictionary []byte) error {
	if resources == nil || dictID == 0 || len(dictionary) == 0 {
		return fmt.Errorf("%w: incomplete dictionary resource closure", rootpublication.ErrUnresolvedResource)
	}
	digest := sha256.Sum256(dictionary)
	expectedLength := int64(len(dictionary))
	foundDictionaryResource := false
	for _, descriptor := range resources.Descriptors() {
		isDictionaryResource := false
		for _, field := range descriptor.ReachabilityFields() {
			if field == rootpublication.ReachabilityDictionaryGeneration {
				isDictionaryResource = true
				foundDictionaryResource = true
				break
			}
		}
		if !isDictionaryResource {
			continue
		}
		matched := false
		for _, obligation := range descriptor.LogicalObligations() {
			if obligation.Generation == dictID && obligation.FileID == dictID &&
				obligation.Offset == 0 && obligation.Length == expectedLength &&
				obligation.Reachability == rootpublication.ReachabilityDictionaryGeneration &&
				obligation.Digest == digest {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: dictionary %d bytes do not match captured resource closure", rootpublication.ErrResourceConflict, dictID)
		}
	}
	if !foundDictionaryResource {
		return fmt.Errorf("%w: dictionary %d closure has no dictionary resource", rootpublication.ErrUnresolvedResource, dictID)
	}
	return nil
}

// SetStableDictionaryResourceProvider installs the authority provider used by
// rewrite and packed-generation producers. It is separate from DictLookup:
// bytes alone do not prove durable reachability or deletion safety.
func (db *DB) SetStableDictionaryResourceProvider(provider StableDictionaryResourceProvider) {
	if db == nil {
		return
	}
	db.stableDictionaryResourcesMu.Lock()
	db.stableDictionaryResources = provider
	db.stableDictionaryResourcesMu.Unlock()
}

func (db *DB) stableDictionaryResourceProvider() StableDictionaryResourceProvider {
	if db == nil {
		return nil
	}
	db.stableDictionaryResourcesMu.RLock()
	provider := db.stableDictionaryResources
	db.stableDictionaryResourcesMu.RUnlock()
	return provider
}

func captureStableDictionaryResources(provider StableDictionaryResourceProvider, dictID uint64, dictionary []byte) (*rootpublication.StableResourceSet, error) {
	if dictID == 0 || len(dictionary) == 0 {
		return nil, nil
	}
	if provider == nil {
		return nil, fmt.Errorf("%w: dictionary %d lacks stable resource provider", rootpublication.ErrUnresolvedResource, dictID)
	}
	resources, err := provider.CaptureDictionaryResources(context.Background(), dictID)
	if err != nil {
		return nil, err
	}
	if err := ValidateStableDictionaryResourceClosure(resources, dictID, dictionary); err != nil {
		resources.Release()
		return nil, err
	}
	return resources, nil
}

func (generation *indexGen) stableIndexNamespaceToken(dir string) (*rootpublication.StableNamespaceToken, error) {
	if generation == nil || generation.pager == nil || dir == "" {
		return nil, fmt.Errorf("%w: stable index namespace unavailable", rootpublication.ErrUnresolvedResource)
	}
	var namespace *rootpublication.StableNamespaceToken
	err := generation.pager.WithStableResourceFile(func(indexFile *os.File) error {
		generation.stableNamespaceMu.Lock()
		defer generation.stableNamespaceMu.Unlock()
		if generation.stableNamespaceProof == nil {
			parent, err := os.Open(dir)
			if err != nil {
				return err
			}
			proof, err := rootpublication.NewStableNamespaceCreationProof(parent, indexFile, indexFileName)
			if err != nil {
				_ = parent.Close()
				return err
			}
			generation.stableNamespaceParent = parent
			generation.stableNamespaceProof = proof
		}
		parentGeneration, err := rootpublication.StableNamespaceParentGeneration(generation.stableNamespaceParent)
		if err != nil {
			return err
		}
		namespace, err = generation.stableNamespaceProof.Bind(
			generation.stableNamespaceParent,
			parentGeneration,
			indexFileName,
			indexFileName,
		)
		return err
	})
	return namespace, err
}

// NewStableValueLogPhysicalResourceToken binds a producer-specific token to
// the exact value-log segment retained by this snapshot's manager generation.
func (snapshot *Snapshot) NewStableValueLogPhysicalResourceToken(
	fileID uint32,
	spec rootpublication.StableResourceSpec,
	constructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error),
) (*rootpublication.StableResourceToken, error) {
	if snapshot == nil || constructor == nil {
		return nil, fmt.Errorf("%w: stable value-log snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer snapshot.endRead()
	if !snapshot.stableIndexCapture || snapshot.vlogManager == nil {
		return nil, fmt.Errorf("%w: stable value-log manager unavailable", rootpublication.ErrUnresolvedResource)
	}
	return snapshot.vlogManager.StableExistingPhysicalResourceToken(fileID, spec, constructor)
}

// NewStableIndexResourceToken binds a producer-specific token to the exact
// index handle and namespace owned by this stable snapshot. The token takes
// ownership of the snapshot maintenance pin on success.
func (snapshot *Snapshot) NewStableIndexResourceToken(spec rootpublication.StableResourceSpec, constructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)) (*rootpublication.StableResourceToken, error) {
	if snapshot == nil || constructor == nil {
		return nil, fmt.Errorf("%w: stable index snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer snapshot.endRead()
	if !snapshot.stableIndexCapture || snapshot.idx == nil || snapshot.idx.pager == nil || snapshot.db == nil {
		return nil, fmt.Errorf("%w: stable index generation unavailable", rootpublication.ErrUnresolvedResource)
	}
	database := snapshot.db
	namespace, err := snapshot.idx.stableIndexNamespaceToken(snapshot.db.dir)
	if err != nil {
		return nil, err
	}
	defer namespace.Release()

	var (
		token            *rootpublication.StableResourceToken
		leaseTransferred atomic.Bool
	)
	err = snapshot.idx.pager.WithStableResourceFile(func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		spec.File = file
		spec.Generation = snapshot.idx.id
		spec.DiagnosticPath = indexFileName
		spec.Frontier.Bytes = uint64(info.Size())
		spec.Namespace = namespace
		callerRelease := spec.OnRelease
		spec.OnRelease = func() {
			if callerRelease != nil {
				callerRelease()
			}
			if leaseTransferred.CompareAndSwap(true, false) {
				database.stableIndexCaptures.Add(-1)
			}
			_ = snapshot.Close()
		}
		token, err = constructor(spec)
		return err
	})
	if err != nil {
		return nil, err
	}

	snapshot.iteratorMu.Lock()
	switch {
	case snapshot.closed.Load():
		err = ErrClosed
	case snapshot.stableIndexCaptureTransferred:
		err = fmt.Errorf("%w: stable index maintenance lease already transferred", rootpublication.ErrResourceOwnership)
	default:
		leaseTransferred.Store(true)
		snapshot.stableIndexCaptureTransferred = true
	}
	snapshot.iteratorMu.Unlock()
	if err != nil {
		token.Release()
		return nil, err
	}
	return token, nil
}

// ValueLogIdentityPinRegistry exposes the DB-scoped physical deletion gate to
// wrappers that manage the same value-log namespace.
func (db *DB) ValueLogIdentityPinRegistry() *rootpublication.IdentityPinRegistry {
	if db == nil {
		return nil
	}
	return db.valueLogIdentityPins
}

// StableResourceIdentityPinRegistry exposes the DB-scoped physical deletion
// gate to non-value-log producers and deleters that share durable files.
func (db *DB) StableResourceIdentityPinRegistry() *rootpublication.IdentityPinRegistry {
	return db.ValueLogIdentityPinRegistry()
}

// NewStableDBResourceToken registers the exact already-open index handle.
// Meta/root publication stays adjacent (#3679), while freelist/COW publication
// stays adjacent (#3678); neither has an independent external identity here.
func NewStableDBResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityIndexFile:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerDB, spec, "authoritative")
	case rootpublication.ReachabilityMetaPage, rootpublication.ReachabilityUserRoot,
		rootpublication.ReachabilitySystemRoot:
		return nil, fmt.Errorf("%w: %s is owned by adjacent root publication issue #3679", rootpublication.ErrResourceExcluded, spec.Reachability)
	case rootpublication.ReachabilityFreelist:
		return nil, fmt.Errorf("%w: %s is owned by adjacent freelist/COW publication issue #3678", rootpublication.ErrResourceExcluded, spec.Reachability)
	default:
		return nil, fmt.Errorf("%w: db producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}

// NewStableOuterLeafResourceToken registers an exact packed segment or
// generation-manifest handle captured before its rename result is exposed.
func NewStableOuterLeafResourceToken(spec rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error) {
	switch spec.Reachability {
	case rootpublication.ReachabilityOuterLeafPackedPointer, rootpublication.ReachabilityOuterLeafGeneration:
		return rootpublication.NewStableProducerResourceTokenForDomain(rootpublication.StableProducerOuterLeaf, spec, "authoritative")
	default:
		return nil, fmt.Errorf("%w: outer-leaf pack producer does not own reachability field %q", rootpublication.ErrUnresolvedResource, spec.Reachability)
	}
}
