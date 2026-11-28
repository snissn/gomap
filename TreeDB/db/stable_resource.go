package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

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

func (generation *indexGen) stableIndexNamespaceToken(dir string) (*rootpublication.StableNamespaceToken, error) {
	return generation.stablePagerNamespaceToken(dir, indexFileName, generation.pager)
}
func (generation *indexGen) stablePagerNamespaceToken(dir, name string, p *pager.Pager) (*rootpublication.StableNamespaceToken, error) {
	return generation.stablePagerNamespaceTokenCore(dir, name, p, false)
}

// rebindStablePagerNamespaceToken retires only the old creation proof. The
// exact admitted parent and its charge remain in the SAME original generation
// or physical owner through the canonical-name transition and marker removal.
func (generation *indexGen) rebindStablePagerNamespaceToken(dir, name string, p *pager.Pager) (*rootpublication.StableNamespaceToken, error) {
	return generation.stablePagerNamespaceTokenCore(dir, name, p, true)
}

func (generation *indexGen) stablePagerNamespaceTokenCore(dir, name string, p *pager.Pager, rebind bool) (*rootpublication.StableNamespaceToken, error) {
	if generation == nil || p == nil || dir == "" {
		return nil, fmt.Errorf("%w: stable index namespace unavailable", rootpublication.ErrUnresolvedResource)
	}
	if rebind {
		generation.closeFailure.mu.Lock()
		failed := generation.closeFailure.causes[2] != nil
		generation.closeFailure.mu.Unlock()
		if failed {
			return nil, &generation.closeFailure
		}
	}
	var namespace *rootpublication.StableNamespaceToken
	err := p.WithStableResourceFile(func(file *os.File) error {
		mutex := &generation.stableNamespaceMu
		parent, proof := &generation.stableNamespaceParent, &generation.stableNamespaceProof
		metadata := generation.stableNamespaceMetadata
		parentCharge := &generation.stableNamespaceParentCharge
		if generation.primaryOwner != nil {
			metadata = generation.primaryOwner.arena.MetadataOwner()
			parentCharge = &generation.stableNamespaceParentCharge
		}
		if name == primaryIndexFileName || name == primaryNewFileName {
			if generation.primaryOwner == nil {
				return rootpublication.ErrResourceOwnership
			}
			mutex = &generation.primaryOwner.namespaceMu
			parent, proof = &generation.primaryOwner.namespaceParent, &generation.primaryOwner.namespaceProof
			metadata = generation.primaryOwner.arena.MetadataOwner()
			parentCharge = &generation.primaryOwner.namespaceParentCharge
		}
		mutex.Lock()
		defer mutex.Unlock()
		if mutex == &generation.stableNamespaceMu && metadata != nil {
			generation.stableNamespaceMetadata = metadata
		}
		if rebind {
			if metadata == nil || *parent == nil {
				return rootpublication.ErrResourceOwnership
			}
			// Verify the actual retained parent before consuming the old proof.
			// An unusable handle is a physical refusal, not a new child-link conflict.
			if _, e := rootpublication.StableNamespaceParentGeneration(*parent); e != nil {
				return e
			}
			if *proof != nil {
				if e := (*proof).ReleaseWithError(); e != nil {
					return e
				}
				*proof = nil
			}
		}
		if *proof == nil {
			if *parent != nil && !rebind {
				return rootpublication.ErrResourceOwnership
			}
			var charge uint64
			f := *parent
			newParent := f == nil
			parentName := dir
			if metadata != nil && newParent {
				var e error
				charge, e = rootpublication.StableFileMetadataCharge(uint64(len(dir)))
				if e != nil {
					return e
				}
				if e = metadata.AddPending(charge); e != nil {
					return e
				}
				parentName = strings.Clone(dir)
			}
			var e error
			if newParent {
				f, e = os.Open(parentName)
				if e != nil {
					if metadata != nil {
						metadata.RemovePending(charge)
					}
					return e
				}
			}
			var v *rootpublication.StableNamespaceCreationProof
			if metadata != nil {
				v, e = rootpublication.NewOwnedStableNamespaceCreationProof(f, file, name, metadata)
			} else {
				v, e = rootpublication.NewStableNamespaceCreationProof(f, file, name)
			}
			if e != nil && v == nil {
				if !newParent {
					return e
				}
				closeErr := f.Close()
				if closeErr == nil || errors.Is(closeErr, os.ErrClosed) {
					if metadata != nil {
						metadata.RemovePending(charge)
					}
				} else {
					if metadata != nil {
						metadata.CleanupFailed()
						*parent = f
						*parentCharge = charge
					}
					return errors.Join(e, closeErr)
				}
				return e
			}
			*parent, *proof = f, v
			if metadata != nil && newParent {
				*parentCharge = charge
			}
			if e != nil {
				return e
			}
		}
		pg, e := rootpublication.StableNamespaceParentGeneration(*parent)
		if e != nil {
			return e
		}
		namespace, e = (*proof).Bind(*parent, pg, name, name)
		return e
	})
	if rebind && err != nil {
		// Physical callbacks have returned and both original owner locks are free.
		// This preborn generation receipt preserves the first hard refusal and the
		// exact unresolved proof/parent graph without a replacement cleanup owner.
		generation.closeFailure.retainCause(2, err)
		generation.metadata.CleanupFailed()
		return namespace, &generation.closeFailure
	}
	return namespace, err
}

// borrowStablePagerNamespaceToken retains independent selected descriptor
// storage while reusing an eligible original creation proof under its owner
// mutex. It neither installs supplied metadata on the original proof nor
// changes the original namespace lifetime.
func (generation *indexGen) borrowStablePagerNamespaceToken(dir, name string, p *pager.Pager, metadata *retainedalloc.Owner, registry *rootpublication.IdentityPinRegistry) (token *rootpublication.StableNamespaceToken, err error) {
	if generation == nil || p == nil || metadata == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	err = p.WithStableResourceFile(func(file *os.File) error {
		mutex := &generation.stableNamespaceMu
		parent, proof := &generation.stableNamespaceParent, &generation.stableNamespaceProof
		if name == primaryIndexFileName || name == primaryNewFileName {
			if generation.primaryOwner == nil {
				return rootpublication.ErrResourceOwnership
			}
			mutex = &generation.primaryOwner.namespaceMu
			parent, proof = &generation.primaryOwner.namespaceParent, &generation.primaryOwner.namespaceProof
		}
		mutex.Lock()
		defer mutex.Unlock()
		if *proof == nil {
			// An independent fresh capture is necessary when the original producer
			// has not supplied a complete creation proof.
			var e error
			token, e = rootpublication.CaptureStableNamespaceWithMetadata(metadata, registry, dir, file, name, name)
			return e
		}
		if *parent == nil {
			return rootpublication.ErrResourceOwnership
		}
		pg, e := rootpublication.StableNamespaceParentGeneration(*parent)
		if e != nil {
			return e
		}
		token, e = (*proof).BindWithMetadata(*parent, pg, name, name, metadata)
		if e == nil {
			e = token.BindCleanupRegistry(registry)
		}
		if e != nil && token != nil {
			token.Release()
			token = nil
		}
		return e
	})
	return token, err
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
	return snapshot.newStablePagerResourceToken(spec, constructor, indexFileName)
}
func (snapshot *Snapshot) newStablePagerResourceToken(spec rootpublication.StableResourceSpec, constructor func(rootpublication.StableResourceSpec) (*rootpublication.StableResourceToken, error), name string) (_ *rootpublication.StableResourceToken, retErr error) {
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
	p := snapshot.idx.pager
	if name == primaryIndexFileName || name == primaryNewFileName {
		if snapshot.idx.primary == nil {
			return nil, rootpublication.ErrUnresolvedResource
		}
		p = snapshot.idx.primary.Pager()
	}
	var ownedOperations *stablePagerOwnedOperations
	if spec.MetadataOwner != nil {
		if spec.OnRelease != nil || spec.FlushThrough != nil || spec.SyncThrough != nil || spec.OwnedOperations != nil {
			return nil, rootpublication.ErrResourceOwnership
		}
		defer func() {
			if ownedOperations == nil {
				return
			}
			consumed, err := ownedOperations.Release()
			if !consumed && err == nil {
				err = rootpublication.ErrResourceOwnership
			}
			retErr = errors.Join(retErr, err)
		}()
	}
	database := snapshot.db
	// A read-only reopened owner cannot mint Windows create-only persistence
	// evidence by upgrading its retained read handle to a writable sync handle.
	// Ordinary reads/physical inspection remain available without that authority.
	if database.readOnly && !rootpublication.StableRelativeNamespaceSupported() {
		return nil, rootpublication.ErrNamespacePersistenceUnsupported
	}
	var namespace *rootpublication.StableNamespaceToken
	var err error
	if spec.MetadataOwner == nil || (snapshot.idx.primary != nil && spec.MetadataOwner == snapshot.idx.primary.MetadataOwner()) {
		namespace, err = snapshot.idx.stablePagerNamespaceToken(snapshot.db.dir, name, p)
	} else {
		namespace, err = snapshot.idx.borrowStablePagerNamespaceToken(database.dir, name, p, spec.MetadataOwner, database.StableResourceIdentityPinRegistry())
	}
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, namespace.ReleaseWithError()) }()

	var (
		token            *rootpublication.StableResourceToken
		leaseTransferred atomic.Bool
	)
	captureCounter := snapshot.stableIndexCaptureCounter
	err = p.WithStableResourceFile(func(file *os.File) error {
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if spec.MetadataOwner != nil {
			ownedOperations, err = newStablePagerOwnedOperations(snapshot, p, spec.MetadataOwner, file.Name())
			if err != nil {
				return err
			}
			spec.OwnedOperations = ownedOperations
		} else if spec.Reachability == rootpublication.ReachabilityIndexFile && spec.SyncThrough == nil {
			// Capture the original spelling while the original handle is live.
			// The callback retains its duplicate handle, never a pathname authority.
			syncPath := file.Name()
			spec.SyncThrough = func(file *os.File, _ rootpublication.DurableFrontier) error {
				return p.SyncIndexDataWithStableFileAtPath(file, syncPath)
			}
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
		spec.DiagnosticPath = name
		spec.Frontier.Bytes = uint64(info.Size())
		spec.Namespace = namespace
		spec.PinRegistry = registry
		if ownedOperations == nil {
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
		}
		token, err = constructor(spec)
		unobserveErr := registry.Unobserve(identity)
		if unobserveErr != nil {
			if token != nil {
				err = errors.Join(err, token.ReleaseWithError())
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
		if ownedOperations != nil {
			ownedOperations.transferred.Store(true)
			snapshot.stablePagerCompletion = ownedOperations
		} else {
			leaseTransferred.Store(true)
		}
		snapshot.stableIndexCaptureTransferred = true
	}
	snapshot.iteratorMu.Unlock()
	if err != nil {
		return nil, errors.Join(err, token.ReleaseWithError())
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
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer snapshot.endRead()
	return snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		Kind:          rootpublication.ResourceIndex,
		LogicalLane:   "db/index",
		ResourceID:    indexFileName,
		Digest:        sha256.Sum256([]byte("treedb/index-file/v1")),
		Reachability:  rootpublication.ReachabilityIndexFile,
		ContentSynced: false,
	}, NewStableDBResourceToken)
}

func (snapshot *Snapshot) captureOwnedStableIndexFileResource() (*rootpublication.StableResourceToken, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("%w: stable index generation unavailable", rootpublication.ErrUnresolvedResource)
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer snapshot.endRead()
	metadata := snapshot.db.valueLogIdentityPins.MetadataOwner()
	if snapshot.idx != nil && snapshot.idx.primary != nil {
		metadata = snapshot.idx.primary.MetadataOwner()
	}
	return snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{
		MetadataOwner: metadata,
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

// StableResourceMetadataOwner supplies allocation admission for new selected
// ordinary resource captures. It does not transfer physical deletion authority:
// each capture keeps its original producer registry and exact handle owner.
// The DB-scoped registry survives index replacement; its closed Owner refuses
// any capture racing terminal shutdown before added storage is allocated.
func (db *DB) StableResourceMetadataOwner() *retainedalloc.Owner {
	if db == nil {
		return nil
	}
	db.idxMu.Lock()
	defer db.idxMu.Unlock()
	idx := db.idx.Load()
	if idx == nil {
		return nil
	}
	return db.valueLogIdentityPins.MetadataOwner()
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

func (snapshot *Snapshot) captureStablePrimaryIndexResource() (*rootpublication.StableResourceToken, error) {
	if snapshot == nil {
		return nil, ErrClosed
	}
	if err := snapshot.beginRead(); err != nil {
		return nil, err
	}
	defer snapshot.endRead()
	if snapshot.idx == nil || snapshot.idx.primary == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	return snapshot.newStablePagerResourceToken(rootpublication.StableResourceSpec{MetadataOwner: snapshot.idx.primary.MetadataOwner(), Kind: rootpublication.ResourceIndex, LogicalLane: "db/primary", ResourceID: primaryIndexFileName, Digest: sha256.Sum256([]byte("treedb/primary-bank/v1")), Reachability: rootpublication.ReachabilityIndexFile}, NewStableDBResourceToken, primaryIndexFileName)
}
