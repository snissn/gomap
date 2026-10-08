package db

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/allocclass"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"

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

	mu             sync.Mutex
	released       bool
	borrowedGuard  *CommandWALStagingGuardV1
	borrowedIntent *CommandWALIntent
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
	if lease.borrowedGuard == nil {
		lease.db.teardownMu.RUnlock()
	}
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
	if err := lease.validateDBLocked(lease.db); err != nil {
		return err
	}
	if lease.borrowedGuard != nil {
		// Keep actual ownership live through rollback-authority transfer.
		lease.borrowedGuard.ownership.mu.Lock()
		defer lease.borrowedGuard.ownership.mu.Unlock()
		if err := lease.borrowedGuard.validateCaptureBorrowLocked(lease.db, lease.borrowedIntent); err != nil {
			return err
		}
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
	if err := resources.RequireMetadataExport(); err != nil {
		return err
	}
	if resources == nil || dictID == 0 || len(dictionary) == 0 {
		return fmt.Errorf("%w: incomplete dictionary resource closure", rootpublication.ErrUnresolvedResource)
	}
	return validateStableEncodedResourceClosure(resources, rootpublication.ReachabilityDictionaryGeneration, dictID, int64(len(dictionary)), sha256.Sum256(dictionary))
}

// ValidateStableTemplateResourceClosure binds the immutable definition selected
// by an encoder to every physical resource returned by a template provider.
func ValidateStableTemplateResourceClosure(resources *rootpublication.StableResourceSet, templateID uint64, definition []byte) error {
	if err := resources.RequireMetadataExport(); err != nil {
		return err
	}
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
	return validateStableEncodedResourceClosure(resources, rootpublication.ReachabilityTemplateGeneration, templateID, int64(len(definition)), sha256.Sum256(definition))
}

// Every relevant physical descriptor must independently contain the selected
// immutable bytes. Streaming avoids retaining any inherited logical corpus.
func validateStableEncodedResourceClosure(resources *rootpublication.StableResourceSet, field rootpublication.ReachabilityField, id uint64, length int64, digest [32]byte) error {
	type key struct {
		kind           rootpublication.ResourceKind
		lane, resource string
		generation     uint64
	}
	keyOf := func(descriptor rootpublication.StableResourcePhysicalDescriptor) key {
		return key{descriptor.Kind, descriptor.LogicalLane(), descriptor.ResourceID(), descriptor.Generation}
	}
	matched := make(map[key]bool)
	for _, descriptor := range resources.PhysicalDescriptors() {
		for _, reachable := range descriptor.ReachabilityFields() {
			if reachable == field {
				matched[keyOf(descriptor)] = false
				break
			}
		}
	}
	if len(matched) == 0 {
		return fmt.Errorf("%w: %s %d closure has no resource", rootpublication.ErrUnresolvedResource, field, id)
	}
	if err := resources.WalkLogicalObligations(func(descriptor rootpublication.StableResourcePhysicalDescriptor, obligation rootpublication.StableLogicalObligation) error {
		physical := keyOf(descriptor)
		if _, relevant := matched[physical]; relevant && obligation.Generation == id && obligation.FileID == id && obligation.Offset == 0 && obligation.Length == length && obligation.Reachability == field && obligation.Digest == digest {
			matched[physical] = true
		}
		return nil
	}); err != nil {
		return err
	}
	for _, found := range matched {
		if !found {
			return fmt.Errorf("%w: %s %d bytes do not match captured resource closure", rootpublication.ErrResourceConflict, field, id)
		}
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
) (result *rootpublication.StableResourceToken, retErr error) {
	if snapshot == nil || constructor == nil {
		return nil, fmt.Errorf("%w: stable value-log snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, snapshot.endReadChecked()) }()
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

// Generic original-token responsibility is independent of cloned provider edges.
// The capsule's role cursor, not a reusable handle or finalized CAS, proves cleanup.
type stableIndexReleaseEnvironment struct {
	snapshot          *Snapshot
	completion        *OriginalSnapshotCleanupV1
	captureCounter    *atomic.Int64
	transferred       atomic.Bool
	caller            func()
	callerEnvironment rootpublication.StableResourceReleaseEnvironment
}

func (e *stableIndexReleaseEnvironment) ReleaseStableResource() {
	_, _ = e.AdvanceStableResourceCleanupV1()
}
func (e *stableIndexReleaseEnvironment) AdvanceStableResourceCleanupV1() (out rootpublication.StableCleanupOutcomeV1, err error) {
	if e.completion != nil {
		snapshot := e.snapshot
		if snapshot != nil {
			err = snapshot.Close()
		}
		snapshot = nil
		out = e.completion.ObserveOriginalCleanupV1()
		if !out.Complete() {
			if err == nil {
				err = rootpublication.ErrStableResourceOperationBusy
			}
			return out, err
		}
		e.snapshot = nil
	}
	counter := e.captureCounter
	if e.transferred.CompareAndSwap(true, false) && counter != nil {
		counter.Add(-1)
	}
	e.captureCounter = nil
	counter = nil
	caller := e.caller
	if caller != nil {
		caller()
		e.caller = nil
	}
	caller = nil
	environment := e.callerEnvironment
	if checked, ok := environment.(rootpublication.StableResourceCleanupEnvironmentV1); ok {
		var outcome rootpublication.StableCleanupOutcomeV1
		var cleanupErr error
		outcome, cleanupErr = checked.AdvanceStableResourceCleanupV1()
		err = errors.Join(err, cleanupErr)
		if !outcome.Complete() {
			environment = nil
			return outcome, err
		}
	} else if environment != nil {
		environment.ReleaseStableResource()
	}
	e.callerEnvironment = nil
	environment = nil
	completion := e.completion
	e.completion = nil
	out.Phase = rootpublication.StableCleanupCompleteV1
	out.PendingRoles = 0
	// Complete means these original metadata roles were consumed, including
	// consumed-with-error. Namespace debt remains owned by the actual Manager.
	if completion != nil {
		completion.ReleaseOriginalCleanupV1()
		completion = nil
	}
	return out, err
}
func reserveStableIndexCallbackClass(creator *residentcredit.Scope, n uint64) error {
	class, err := allocclass.ClassBytes(n, true)
	if err != nil {
		return err
	}
	return creator.ReserveOriginalLifetime(class)
}

// NewStableIndexResourceToken binds the exact snapshot constructor lifetime.
// Hidden caller functions remain ordinary-only and permanently uncertified.
func (snapshot *Snapshot) NewStableIndexResourceToken(spec rootpublication.StableResourceSpec, constructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error)) (result *rootpublication.StableResourceToken, retErr error) {
	if snapshot == nil || constructor == nil {
		return nil, fmt.Errorf("%w: stable index snapshot unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, snapshot.endReadChecked()) }()
	if !snapshot.stableIndexCapture || snapshot.idx == nil || snapshot.idx.pager == nil || snapshot.db == nil || snapshot.pagerCreator == nil {
		return nil, fmt.Errorf("%w: stable index generation unavailable", rootpublication.ErrUnresolvedResource)
	}
	if spec.MetadataAccount != nil || spec.CallbackCreator != nil && spec.CallbackCreator != snapshot.pagerCreator || spec.CallbackProvider != nil {
		return nil, rootpublication.ErrStableMetadataShapeUnsupported
	}
	creator := snapshot.pagerCreator
	if err := creator.RetainOriginalLifetime(); err != nil {
		return nil, err
	}
	var provider *rootpublication.StableIndexOperationProvider
	var environment *stableIndexReleaseEnvironment
	environmentTransferred := false
	// Scrub all constructor aliases before the independent local Scope edge ends.
	defer func() {
		spec = rootpublication.StableResourceSpec{}
		constructor = nil
		if environment != nil && !environmentTransferred && environment.completion != nil {
			environment.completion.ReleaseOriginalCleanupV1()
			environment.completion = nil
		}
		environment = nil
		if provider != nil {
			provider.ReleaseStableResourceProvider()
			provider = nil
		}
		creator.ReleaseStableMetadata()
		creator = nil
	}()
	if spec.Reachability == rootpublication.ReachabilityIndexFile && spec.SyncThrough == nil {
		var err error
		provider, err = rootpublication.NewStableIndexOperationProvider(snapshot.idx.pager, creator)
		if err != nil {
			return nil, err
		}
		spec.CallbackProvider = provider
	}
	if err := reserveStableIndexCallbackClass(creator, uint64(unsafe.Sizeof(stableIndexReleaseEnvironment{}))); err != nil {
		return nil, err
	}
	completion := snapshot.originalCleanup
	if completion == nil {
		return nil, ErrClosed
	}
	if err := completion.RetainOriginalCleanupV1(); err != nil {
		return nil, err
	}
	environment = &stableIndexReleaseEnvironment{completion: completion, snapshot: snapshot, captureCounter: snapshot.stableIndexCaptureCounter, caller: spec.OnRelease, callerEnvironment: spec.ReleaseEnvironment}
	spec.OnRelease = nil
	spec.CallbackCreator = creator
	spec.ReleaseEnvironment = environment
	database := snapshot.db
	namespace, err := snapshot.idx.stableIndexNamespaceToken(database.dir)
	if err != nil {
		return nil, err
	}
	defer namespace.Release()
	var token *rootpublication.StableResourceToken
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
		token, err = constructor(spec)
		if token != nil {
			environmentTransferred = true
		}
		unobserveErr := registry.Unobserve(identity)
		if unobserveErr != nil {
			if token != nil {
				releaseErr := token.Release()
				if releaseErr == nil && token.CleanupCompleteV1() {
					token = nil
				}
				err = errors.Join(err, releaseErr)
			}
			return errors.Join(err, unobserveErr)
		}
		return err
	})
	if err != nil {
		return token, err
	}
	snapshot.iteratorMu.Lock()
	switch {
	case snapshot.closed.Load():
		err = ErrClosed
	case snapshot.stableIndexCaptureTransferred:
		err = fmt.Errorf("%w: stable index maintenance lease already transferred", rootpublication.ErrResourceOwnership)
	default:
		environment.transferred.Store(true)
		snapshot.stableIndexCaptureTransferred = true
	}
	snapshot.iteratorMu.Unlock()
	if err != nil {
		releaseErr := token.Release()
		if releaseErr == nil && token.CleanupCompleteV1() {
			token = nil
		}
		return token, errors.Join(err, releaseErr)
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

// ValidateDBV1 checks actual capture ownership without acquiring teardown.
// A borrower is invalid after its own Release or the staging guard's release.
func (lease *StableResourceCaptureLease) ValidateDBV1(db *DB) error {
	if lease == nil {
		return ErrClosed
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if err := lease.validateDBLocked(db); err != nil {
		return err
	}
	if lease.borrowedGuard != nil {
		lease.borrowedGuard.ownership.mu.Lock()
		defer lease.borrowedGuard.ownership.mu.Unlock()
		return lease.borrowedGuard.validateCaptureBorrowLocked(db, lease.borrowedIntent)
	}
	return nil
}
func (lease *StableResourceCaptureLease) validateDBLocked(db *DB) error {
	if lease == nil || lease.db == nil || db == nil || lease.db != db || lease.released {
		return ErrClosed
	}
	return nil
}

// ValidateCommandWALStagingCaptureV1 requires the exact actual staged intent,
// rather than accepting an ordinary independently admitted capture lease.
func (lease *StableResourceCaptureLease) ValidateCommandWALStagingCaptureV1(db *DB, intent *CommandWALIntent) error {
	if lease == nil {
		return ErrClosed
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if err := lease.validateDBLocked(db); err != nil {
		return err
	}
	if lease.borrowedGuard == nil || intent == nil || lease.borrowedIntent != intent {
		return ErrCommandWALRejected
	}
	lease.borrowedGuard.ownership.mu.Lock()
	defer lease.borrowedGuard.ownership.mu.Unlock()
	return lease.borrowedGuard.validateCaptureBorrowLocked(db, intent)
}

// ValidateCommandWALStagingCaptureDBV1 rejects an ordinary capture lease at
// owner setup. Exact intent equality is checked again when applying the frame.
func (lease *StableResourceCaptureLease) ValidateCommandWALStagingCaptureDBV1(db *DB) error {
	if lease == nil {
		return ErrClosed
	}
	return lease.ValidateCommandWALStagingCaptureV1(db, lease.borrowedIntent)
}

// requireStableResourceMetadataExportV1 certifies every operand before a
// consumer stages clones or treats unavailable metadata as an empty closure.
// The shared set provenance invariant makes ordinary checks constant time.
func requireStableResourceMetadataExportV1(resources ...*rootpublication.StableResourceSet) error {
	for _, resource := range resources {
		if err := resource.RequireMetadataExport(); err != nil {
			return err
		}
	}
	return nil
}
