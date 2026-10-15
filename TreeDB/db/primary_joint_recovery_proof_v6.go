package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/lockfile"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"os"
	"path/filepath"
	"unsafe"
)

// OpenRecoveryCleanupError owns the original recovery payload and namespace
// LOCK after an actual cleanup refusal. Callers must preserve this fatal receipt
// until process shutdown; wrapping it preserves ownership. Close observes the
// immutable first failure and never retries cleanup or refunds retained debt.
// There is no finalizer, background retry, or separate physical authority.
type OpenRecoveryCleanupError struct {
	cause      error
	proof      *primaryJointRecoveryProofV6
	generation *indexGen
}

func (e *OpenRecoveryCleanupError) Error() string {
	return "treedb: fatal Open physical cleanup: " + e.cause.Error()
}
func (e *OpenRecoveryCleanupError) Unwrap() error { return e.cause }
func (e *OpenRecoveryCleanupError) Close() error  { return e.cause }
func recoveryCleanupOwnsLockV6(err error, lock *lockfile.Lock) bool {
	var receipt *OpenRecoveryCleanupError
	return errors.As(err, &receipt) &&
		((receipt.proof != nil && receipt.proof.lock == lock) ||
			(receipt.generation != nil && receipt.generation.startupLock == lock))
}

const primaryJointMarkerReadLimitV6 = 4141

var testHookPrimaryJointRecoveryProofV6 func(string, *primaryJointRecoveryProofV6)

type primaryJointRecoveryProofV6 struct {
	registry                                                    *rootpublication.IdentityPinRegistry
	metadata                                                    *retainedalloc.Owner
	custody                                                     *rootpublication.OriginalCleanupCustody
	lock                                                        *lockfile.Lock
	parent, marker, data, primary, dependencyParent, dependency *os.File
	primaryPager                                                *pager.Pager
	charge, pagerCharge                                         uint64
	decisionDeleted                                             bool
	image                                                       []byte
	failure                                                     error
	receipt                                                     OpenRecoveryCleanupError
}

func newPrimaryJointRecoveryProofV6(registry *rootpublication.IdentityPinRegistry, lock *lockfile.Lock) (*primaryJointRecoveryProofV6, error) {
	owner := registry.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryJointRecoveryProofV6{}))) + 6*retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(os.File{}))) + retainedalloc.AllocationCharge(primaryJointMarkerReadLimitV6) + 2*retainedalloc.AllocationCharge(rootpublication.PrimaryCapsuleSizeV6)
	if err := owner.AddPending(charge); err != nil {
		return nil, err
	}
	custody, err := rootpublication.NewOriginalCleanupCustody(registry, owner)
	if err != nil {
		owner.RemovePending(charge)
		return nil, err
	}
	return &primaryJointRecoveryProofV6{registry: registry, metadata: owner, custody: custody, lock: lock, charge: charge}, nil
}

func (p *primaryJointRecoveryProofV6) closeFile(slot **os.File) error {
	if *slot == nil {
		return nil
	}
	if err := (*slot).Close(); err != nil {
		return p.refuse(err)
	}
	*slot = nil
	return nil
}
func (p *primaryJointRecoveryProofV6) closeValidation() error {
	if testHookPrimaryJointRecoveryProofV6 != nil {
		testHookPrimaryJointRecoveryProofV6("before-proof-close", p)
	}
	if p.primaryPager != nil {
		if err := p.primaryPager.Close(); err != nil {
			return p.refuse(err)
		}
		p.primaryPager = nil
		p.metadata.RemovePending(p.pagerCharge)
		p.pagerCharge = 0
	}
	for _, slot := range []**os.File{&p.dependency, &p.dependencyParent, &p.data, &p.primary} {
		if err := p.closeFile(slot); err != nil {
			return err
		}
	}
	return nil
}
func (p *primaryJointRecoveryProofV6) refuse(err error) error {
	if p.failure == nil {
		p.failure = err
		p.receipt.cause, p.receipt.proof = errors.Join(err, ErrRecoveryRequired), p
		p.custody.Complete(p, err)
	}
	return &p.receipt
}
func (p *primaryJointRecoveryProofV6) finish(err error) error {
	if p.failure != nil {
		return errors.Join(err, &p.receipt)
	}
	cleanup := p.closeValidation()
	if cleanup == nil {
		cleanup = p.closeFile(&p.marker)
	}
	if cleanup == nil {
		if testHookPrimaryJointRecoveryProofV6 != nil {
			testHookPrimaryJointRecoveryProofV6("before-parent-close", p)
		}
		cleanup = p.closeFile(&p.parent)
	}
	if cleanup != nil {
		return errors.Join(err, p.refuse(cleanup))
	}
	owner, charge, custody := p.metadata, p.charge, p.custody
	p.registry, p.metadata, p.custody, p.lock = nil, nil, nil, nil
	p.charge = 0
	p.image = nil
	owner.RemovePending(charge)
	custody.Complete(nil, nil)
	return err
}

func (p *primaryJointRecoveryProofV6) validateEntry(dir string, entry rootpublication.DependencyManifestEntryV1, physicalOnly bool) (err error) {
	if p.failure != nil {
		return &p.receipt
	}
	path, err := durableDependencyPathForKindV1(dir, "", entry.Kind, entry.DiagnosticPath)
	if err != nil {
		return err
	}
	p.dependencyParent, err = rootpublication.OpenStableParent(filepath.Dir(path))
	if err != nil {
		return err
	}
	p.dependency, err = rootpublication.OpenStableChildFile(p.dependencyParent, filepath.Base(path), os.O_RDONLY, 0)
	if err == nil {
		err = validateDurableDependencyDescriptorV1(dir, p.dependencyParent, p.dependency, entry, physicalOnly)
	}
	if testHookPrimaryJointRecoveryProofV6 != nil {
		testHookPrimaryJointRecoveryProofV6("before-dependency-close", p)
	}
	if closeErr := p.closeFile(&p.dependency); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return errors.Join(err, p.closeFile(&p.dependencyParent))
}

// Only a complete canonical physical pair is eligible for this conditional
// branch. Original interrupted staged rollforward still requires its exact
// decision generation. The original ordinary selector validates every eligible
// current image, embedded parent, and dependency through the common checks.
func (p *primaryJointRecoveryProofV6) completedCanonical(dir string, d primaryJointDecisionV6) (bool, error) {
	parentID, err := rootpublication.StableIdentityFromFile(p.parent)
	if err != nil || !rootpublication.SamePhysicalIdentity(parentID, d.Parent) {
		return false, errors.Join(err, ErrRecoveryRequired)
	}
	p.data, err = rootpublication.OpenStableChildFile(p.parent, d.DataName, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	p.primary, err = rootpublication.OpenStableChildFile(p.parent, d.PrimaryName, os.O_RDONLY, 0)
	if os.IsNotExist(err) {
		return false, p.closeValidation()
	}
	if err != nil {
		return false, err
	}
	dataID, err := rootpublication.StableIdentityFromFile(p.data)
	if err != nil {
		return false, err
	}
	primaryID, err := rootpublication.StableIdentityFromFile(p.primary)
	if err != nil {
		return false, err
	}
	if !rootpublication.SamePhysicalIdentity(dataID, d.Data) || !rootpublication.SamePhysicalIdentity(primaryID, d.Primary) {
		return false, p.closeValidation()
	}
	ds, err := newSnapshotIndexPageStoreV1(p.data)
	if err != nil {
		return true, err
	}
	var consumed bool
	p.primaryPager, p.pagerCharge, consumed, err = pager.OpenReadOnlyOwnedFileWithMetadata(p.primary, filepath.Join(dir, d.PrimaryName), defaultChunkSize, pager.OpenOptions{}, p.metadata)
	if consumed {
		p.primary = nil
	}
	if err != nil {
		if p.primaryPager != nil {
			return true, p.refuse(err)
		}
		return true, err
	}
	h, err := p.primaryPager.ReadPage(0)
	if err != nil || !page.VerifyChecksumNonMutating(h) || string(h[16:24]) != "TDPRCAPB" || binary.LittleEndian.Uint16(h[24:26]) != 6 || !bytes.Equal(h[32:48], d.UUID[:]) {
		return true, errors.Join(err, ErrRecoveryRequired)
	}
	selected, _, err := selectPrimaryCapsuleAuthorityV6(ds, p.primaryPager, ds.pageCount, p.primaryPager.PageCount(), d.UUID,
		func(m *rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error) {
			err := m.WithEntriesV1(func(entries []rootpublication.DependencyManifestEntryV1) error {
				for _, entry := range entries {
					if err := p.validateEntry(dir, entry, false); err != nil {
						return err
					}
				}
				return nil
			})
			return nil, err
		}, func(r rootpublication.DurablePrimaryRootRecordV5) (*rootpublication.StableResourceSet, error) {
			directory, err := rootpublication.NewOwnedLocalPrimaryDependencyDirectoryV5(p.primaryPager, r.Record.Directory, r.Primary.ArenaHighWater, p.metadata, func() (bool, error) { return true, nil })
			if err != nil {
				return nil, err
			}
			defer directory.Release()
			err = rootpublication.ValidateDependencyDirectoryDescriptorsV2(directory, p.metadata, func(entry rootpublication.DependencyManifestEntryV1, physicalOnly bool) error {
				return p.validateEntry(dir, entry, physicalOnly)
			})
			return nil, err
		}, p.metadata)
	if p.failure != nil {
		return true, &p.receipt
	}
	if err != nil {
		return true, err
	}
	required, err := requiredFormatFeatureEnabled(dir, RequiredFeatureDependencyDirectoryV2)
	if err != nil {
		return true, err
	}
	for _, r := range selected.SlotRecords {
		if r.CommitSeq != 0 && (r.Directory.RootPageID != 0) != required {
			return true, ErrLegacyFormatRebuildRequired
		}
	}
	err = rootpublication.ValidateStableChildLink(p.parent, p.data, d.DataName)
	if err == nil {
		err = p.primaryPager.WithStableResourceFile(func(f *os.File) error {
			id, e := rootpublication.StableIdentityFromFile(f)
			if e != nil || !rootpublication.SamePhysicalIdentity(id, d.Primary) {
				return errors.Join(e, ErrRecoveryRequired)
			}
			return rootpublication.ValidateStableChildLink(p.parent, f, d.PrimaryName)
		})
	}
	if err == nil {
		id, e := rootpublication.StableIdentityFromFile(p.parent)
		if e != nil || !rootpublication.SamePhysicalIdentity(id, d.Parent) {
			err = errors.Join(e, ErrRecoveryRequired)
		}
	}
	if err != nil {
		return true, err
	}
	return true, p.closeValidation()
}

func (p *primaryJointRecoveryProofV6) deleteDecision(dir string) error {
	var err error
	p.marker, err = rootpublication.OpenStableChildFile(p.parent, indexReadyFileName, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err = jointSwapHookV6("before-decision-delete"); err != nil {
		return err
	}
	if err = rootpublication.ValidateStableChildLink(p.parent, p.marker, indexReadyFileName); err != nil {
		return err
	}
	if err = rootpublication.RemoveStableChildFile(p.parent, indexReadyFileName); err != nil {
		return err
	}
	p.decisionDeleted = true
	if err = observeStableNamespaceMutation(durabilitycut.NamespaceUnlink, durabilitycut.ResourceIndex, dir, filepath.Join(dir, indexReadyFileName), "", p.parent, p.marker, indexReadyFileName, ""); err != nil {
		return p.refuse(err)
	}
	if err = p.closeFile(&p.marker); err != nil {
		return err
	}
	if err = jointSwapHookV6("after-decision-delete"); err != nil {
		return p.refuse(err)
	}
	if err = syncJointParentV6(dir, p.parent, "deletion-barrier"); err != nil {
		return p.refuse(err)
	}
	return nil
}
