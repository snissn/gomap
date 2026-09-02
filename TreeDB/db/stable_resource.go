package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	templ "github.com/snissn/gomap/TreeDB/template"
)

// StableDictionaryResourceProvider captures the exact durable transitive
// closure needed to decode one dictionary generation.
type StableDictionaryResourceProvider interface {
	CaptureDictionaryResources(context.Context, uint64) (*rootpublication.StableResourceSet, error)
}

// StableTemplateResourceProvider captures the exact durable transitive closure
// needed to decode one template generation.
type StableTemplateResourceProvider interface {
	CaptureTemplateResources(context.Context, uint64) (*rootpublication.StableResourceSet, error)
}

// StableResourceCaptureLease admits a producer that must retain DB-scoped
// physical identity and namespace authority while it constructs a stable
// resource closure. Close waits for admitted captures before tearing down
// resources and clearing the DB lifetime's namespace-sync proofs.
type StableResourceCaptureLease struct {
	db *DB

	mu       sync.Mutex
	released bool
}

// Release ends an admitted stable-resource capture. It is safe to call more
// than once.
func (lease *StableResourceCaptureLease) Release() {
	if lease == nil || lease.db == nil {
		return
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return
	}
	lease.released = true
	lease.db.teardownMu.RUnlock()
}

// RetainStableResourceCaptureRecovery transfers exact producer rollback
// authority into DB teardown while this admission lease is still live. The
// ambiguous producer mutation poisons later publication immediately; teardown
// retries cleanup after every admitted capture has released its lease.
func (lease *StableResourceCaptureLease) RetainStableResourceCaptureRecovery(cleanup func() error) error {
	if lease == nil || lease.db == nil || cleanup == nil {
		return ErrClosed
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.released {
		return ErrClosed
	}
	lease.db.publicationPoisoned.Store(true)
	_, registered := lease.db.tryRegisterCaptureTeardownHook(func() error {
		if err := cleanup(); err != nil {
			return errors.Join(err, ErrRecoveryRequired)
		}
		return nil
	})
	if !registered {
		return errors.Join(ErrClosed, ErrRecoveryRequired)
	}
	return nil
}

// AcquireStableResourceCaptureLease admits DB-external producers that use the
// DB-scoped stable identity registry. A successful lease excludes final DB
// teardown until Release; an acquisition that loses the race with Close fails
// with ErrClosed before the producer can add fresh namespace-sync proofs.
func (db *DB) AcquireStableResourceCaptureLease() (*StableResourceCaptureLease, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.teardownMu.RLock()
	if db.closing.Load() {
		db.teardownMu.RUnlock()
		return nil, ErrClosed
	}
	if err := db.commandWALPoisonedError(); err != nil {
		db.teardownMu.RUnlock()
		return nil, err
	}
	return &StableResourceCaptureLease{db: db}, nil
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

// ValidateStableTemplateResourceClosure binds the immutable definition selected
// by an encoder to every physical resource returned by a template provider.
func ValidateStableTemplateResourceClosure(resources *rootpublication.StableResourceSet, templateID uint64, definition []byte) error {
	if resources == nil || templateID == 0 || len(definition) == 0 {
		return fmt.Errorf("%w: incomplete template resource closure", rootpublication.ErrUnresolvedResource)
	}
	validID := false
	for salt := 0; salt <= 255; salt++ {
		if templ.TemplateID(definition, byte(salt)) == templateID {
			validID = true
			break
		}
	}
	if !validID {
		return fmt.Errorf("%w: template %d does not identify the selected definition", rootpublication.ErrResourceConflict, templateID)
	}
	digest := sha256.Sum256(definition)
	expectedLength := int64(len(definition))
	foundTemplateResource := false
	for _, descriptor := range resources.Descriptors() {
		isTemplateResource := false
		for _, field := range descriptor.ReachabilityFields() {
			if field == rootpublication.ReachabilityTemplateGeneration {
				isTemplateResource = true
				foundTemplateResource = true
				break
			}
		}
		if !isTemplateResource {
			continue
		}
		matched := false
		for _, obligation := range descriptor.LogicalObligations() {
			if obligation.Generation == templateID && obligation.FileID == templateID &&
				obligation.Offset == 0 && obligation.Length == expectedLength &&
				obligation.Reachability == rootpublication.ReachabilityTemplateGeneration &&
				obligation.Digest == digest {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%w: template %d definition does not match captured resource closure", rootpublication.ErrResourceConflict, templateID)
		}
	}
	if !foundTemplateResource {
		return fmt.Errorf("%w: template %d closure has no template resource", rootpublication.ErrUnresolvedResource, templateID)
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

func captureStableDictionaryResources(ctx context.Context, provider StableDictionaryResourceProvider, dictID uint64, dictionary []byte) (*rootpublication.StableResourceSet, error) {
	if dictID == 0 || len(dictionary) == 0 {
		return nil, nil
	}
	if provider == nil {
		return nil, fmt.Errorf("%w: dictionary %d lacks stable resource provider", rootpublication.ErrUnresolvedResource, dictID)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	resources, err := provider.CaptureDictionaryResources(ctx, dictID)
	if err != nil {
		return nil, err
	}
	if err := ValidateStableDictionaryResourceClosure(resources, dictID, dictionary); err != nil {
		resources.Release()
		return nil, err
	}
	return resources, nil
}

func captureStableTemplateResources(provider StableTemplateResourceProvider, store templ.Store, templateID uint64) (*rootpublication.StableResourceSet, error) {
	if templateID == 0 {
		return nil, nil
	}
	if provider == nil || store == nil {
		return nil, fmt.Errorf("%w: template %d lacks stable resource provider", rootpublication.ErrUnresolvedResource, templateID)
	}
	definition, err := store.GetTemplateDef(context.Background(), templateID)
	if err != nil {
		return nil, err
	}
	resources, err := provider.CaptureTemplateResources(context.Background(), templateID)
	if err != nil {
		return nil, err
	}
	if err := ValidateStableTemplateResourceClosure(resources, templateID, definition); err != nil {
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

// StableValueLogRecordLength returns the exact record length for a pointer in
// this snapshot's pinned value-log generation. Grouped pointers may omit their
// best-effort length hint; those lengths are read from the already-pinned
// segment header rather than rediscovered through the current manager lane.
func (snapshot *Snapshot) StableValueLogRecordLength(ptr page.ValuePtr) (uint32, error) {
	if snapshot == nil {
		return 0, fmt.Errorf("%w: stable value-log snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return 0, err
	}
	defer snapshot.endRead()
	if !snapshot.stableIndexCapture || snapshot.state == nil || snapshot.state.ValueLogSet == nil {
		return 0, fmt.Errorf("%w: stable value-log generation unavailable", rootpublication.ErrUnresolvedResource)
	}
	if hint := page.ValuePtrRecordLength(ptr); hint != 0 {
		return hint, nil
	}
	if ptr.Offset < 4 {
		return 0, fmt.Errorf("%w: invalid value-log pointer offset %d", rootpublication.ErrResourceConflict, ptr.Offset)
	}
	segment := snapshot.state.ValueLogSet.Files[ptr.FileID]
	if segment == nil || segment.File == nil {
		return 0, fmt.Errorf("%w: stable value-log file %d unavailable", rootpublication.ErrUnresolvedResource, ptr.FileID)
	}
	recordLength, err := readValueLogRecordLengthFromHeader(segment.File, int64(ptr.Offset-4))
	if err != nil {
		return 0, fmt.Errorf("stable value-log file %d record header: %w", ptr.FileID, err)
	}
	return recordLength, nil
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
	if spec.Reachability == rootpublication.ReachabilityIndexFile && spec.SyncThrough == nil {
		pager := snapshot.idx.pager
		spec.SyncThrough = func(file *os.File, _ rootpublication.DurableFrontier) error {
			return pager.SyncIndexDataWithStableFile(file)
		}
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
	captureCounter := snapshot.stableIndexCaptureCounter
	err = snapshot.idx.pager.WithStableResourceFile(func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		registry := database.StableResourceIdentityPinRegistry()
		identity, err := rootpublication.StableIdentityFromFile(file)
		if err != nil {
			return err
		}
		if err := registry.Observe(identity); err != nil {
			return err
		}
		spec.File = file
		spec.Generation = snapshot.idx.id
		spec.DiagnosticPath = indexFileName
		spec.Frontier.Bytes = uint64(info.Size())
		spec.Namespace = namespace
		spec.PinRegistry = registry
		callerRelease := spec.OnRelease
		spec.OnRelease = func() {
			_ = snapshot.Close()
			if leaseTransferred.CompareAndSwap(true, false) && captureCounter != nil {
				captureCounter.Add(-1)
			}
			if callerRelease != nil {
				callerRelease()
			}
		}
		token, err = constructor(spec)
		unobserveErr := registry.Unobserve(identity)
		if unobserveErr != nil {
			if token != nil {
				token.Release()
				token = nil
			}
			return errors.Join(err, unobserveErr)
		}
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

// CaptureStableIndexFileResource captures publication authority for the exact
// index generation already pinned by this stable snapshot. Unlike
// NewStableIndexResourceToken, this producer-owned entry point does not let a
// caller select the resource kind, reachability field, handle, frontier, or
// constructor.
func (snapshot *Snapshot) CaptureStableIndexFileResource() (*rootpublication.StableResourceToken, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: stable index generation unavailable", rootpublication.ErrUnresolvedResource)
	}
	return snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:          rootpublication.ResourceIndex,
		LogicalLane:   "db/index",
		ResourceID:    indexFileName,
		Digest:        sha256.Sum256([]byte("treedb/index-file/v1")),
		Reachability:  rootpublication.ReachabilityIndexFile,
		ContentSynced: false,
	}, NewStableDBResourceToken)
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
