package db

import (
	"errors"
	"fmt"
	"math"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type packedRootPublication struct {
	afterActivation          func()
	afterManifestPreparation func() error
}

type ordinaryLeafManifestPublication struct {
	staged   stagedLeafGenerationManifestResult
	prepared *LeafGenerationManifestStablePreparedClosure
}

// The exact resource set belongs to the candidate after preparation. This
// separate owner keeps the compatibility rollback bytes until visible admission
// or terminal teardown, including while the same candidate retries a pre-meta cut.
type inlineLeafManifestPublication struct {
	candidate *durableRootPublishCandidateV1
	update    ordinaryLeafManifestPublication
}

// finishFailedInlineLeafManifest runs after admitted publishers have drained
// and before the stable manifest store closes its retained parent. Only the
// exact parked candidate with an untouched meta can be safely abandoned.
func (db *DB) finishFailedInlineLeafManifest() error {
	db.durablePublishMu.Lock()
	defer db.durablePublishMu.Unlock()
	inline := db.inlineLeafManifest
	db.inlineLeafManifest = nil
	if inline == nil {
		return nil
	}
	if db.durableRoot.pending == inline.candidate && !inline.candidate.metaMutated {
		return inline.update.prepared.abandonUnpublished()
	}
	// Preserve both immutable revisions when the root may already be durable.
	return inline.update.prepared.ReleaseWithError()
}

type ordinaryLeafManifestAuthority struct {
	revision uint64
	identity rootpublication.StableIdentity
	digest   [32]byte
}

type ordinaryLeafManifestCache struct {
	authority ordinaryLeafManifestAuthority
	view      *leafGenerationView
}

func (p ordinaryLeafManifestPublication) finish(keep bool) error {
	if p.prepared == nil {
		return nil
	}
	if keep {
		// The exact token has transferred. ReleaseWithError records any
		// remaining original cleanup failure for checked teardown.
		_ = p.prepared.ReleaseWithError()
		return nil
	}
	return p.prepared.abandonUnpublished()
}

func (p ordinaryLeafManifestPublication) bind(post *finalizeCommitPost, opts *finalizeCommitOptions) {
	opts.leafManifestAlreadyPersistent = true
	opts.ordinaryLeafManifestBound = true
	post.clearLeafGenerationPendingFileIDs = p.staged.pendingFileIDs
}

// bindPublicationLeafManifest checks the exact immutable generation authority in
// the captured candidate closure. An ordinary successor with no newly uncovered
// leaf files keeps its existing token. A pack stages promoted files and any
// late descriptor appends against its original copy basis. The
// exact staged coverage precedes build lock release, and persistence runs under
// the next-root gate. The superseded durable slot keeps its original authority.
// This function consumes resources on error or replacement.
func (db *DB) bindPublicationLeafManifest(resources *rootpublication.StableResourceSet, commitSeq uint64, pack *leafGenerationManifest, packRawFileIDs []uint32, opts *finalizeCommitOptions) (result *rootpublication.StableResourceSet, update ordinaryLeafManifestPublication, retErr error) {
	result = resources
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, db.releaseCapturedRootResources(result), update.finish(db.publicationPoisoned.Load()))
			result = nil
			update = ordinaryLeafManifestPublication{}
		}
	}()
	if db.leafGenerationManifestStore == nil || db.leafGenerationManifestStore.mode != leafGenerationManifestStable {
		if pack != nil {
			return result, update, fmt.Errorf("%w: stable manifest replacement mode unavailable", rootpublication.ErrNamespacePersistenceUnsupported)
		}
		return resources, update, nil
	}

	base := pack
	if pack == nil {
		authority, err := exactOrdinaryLeafManifestAuthority(resources)
		if err != nil {
			return result, update, err
		}
		if authority.revision == 0 {
			return resources, update, nil
		}
		// A compatibility replacement or an abandoned candidate may leave or
		// reuse a revision number. Only the inherited token gives coverage credit.
		if db.ordinaryLeafManifestCache.authority != authority {
			var exact *leafGenerationManifest
			if err := resources.WithScopedTokens(func(tokens []*rootpublication.StableResourceToken) error {
				var err error
				exact, _, err = loadSelectedDurableLeafGenerationManifestTokens(tokens)
				return err
			}); err != nil {
				return result, update, err
			}
			if exact == nil || exact.ManifestRevision != authority.revision {
				return result, update, fmt.Errorf("%w: missing inherited leaf-generation manifest revision %d", rootpublication.ErrResourceConflict, authority.revision)
			}
			db.ordinaryLeafManifestCache = ordinaryLeafManifestCache{authority: authority, view: newLeafGenerationView(exact)}
		}
		files := db.ordinaryLeafManifestCache.view.FileToGeneration
		missing, err := firstUncoveredOuterLeafFile(resources, func(fileID uint32) bool { return files[fileID] != 0 })
		if err != nil {
			return result, update, err
		}
		if missing == 0 {
			return result, update, nil
		}
		db.mu.RLock()
		base = db.leafGenerationManifest
		db.mu.RUnlock()
	}
	var err error
	update.staged, err = db.stagedLeafGenerationManifestWithPendingResultAndLimit(base, 0, commitSeq, opts.preparedLimits, packRawFileIDs...)
	if err != nil {
		return result, update, err
	}
	if pack != nil && !update.staged.changed {
		return result, update, fmt.Errorf("%w: packed publication did not advance the leaf-generation manifest", rootpublication.ErrResourceConflict)
	}
	missing, err := firstUncoveredOuterLeafFile(resources, func(fileID uint32) bool {
		return leafManifestContainsActiveFile(update.staged.manifest, fileID)
	})
	if err != nil {
		return result, update, err
	}
	if missing != 0 {
		return result, update, fmt.Errorf("%w: publication leaf-generation manifest omits leaf file %d", rootpublication.ErrUnresolvedResource, missing)
	}
	// The private roots are built, reuse is still open and resources retain the
	// candidate's exact files. The publication gate keeps the predecessor and
	// operational manifest fixed while the staged successor does its file and
	// directory syncs. No code may reacquire build locks under that gate.
	opts.releaseRootConstruction()
	update.prepared, update.staged.manifest, err = db.prepareLeafGenerationManifestStableCandidate(update.staged.manifest)
	if err != nil {
		return result, update, err
	}
	if pack != nil {
		if err := opts.packedRootPublication.afterManifestPreparation(); err != nil {
			return result, update, err
		}
	}
	manifestResources, err := update.prepared.TakeStableResources()
	if err != nil {
		return result, update, err
	}
	replacement, err := db.replaceDurableRootManifestResourcesV1(resources, manifestResources)
	if err = errors.Join(err, db.releaseCapturedRootResources(manifestResources)); err != nil {
		return result, update, errors.Join(err, db.releaseCapturedRootResources(replacement))
	}
	return replacement, update, db.releaseCapturedRootResources(resources)
}

func exactOrdinaryLeafManifestAuthority(resources *rootpublication.StableResourceSet) (ordinaryLeafManifestAuthority, error) {
	var authority ordinaryLeafManifestAuthority
	kinds := [...]rootpublication.ResourceKind{rootpublication.ResourceOuterLeafManifest}
	err := resources.WalkPhysicalResourceIdentities(kinds[:], func(_ rootpublication.ResourceKind, revision uint64, identity rootpublication.StableIdentity, digest [32]byte) error {
		if authority.revision != 0 || revision == 0 || identity == (rootpublication.StableIdentity{}) || digest == ([32]byte{}) {
			return fmt.Errorf("%w: publication requires one exact outer-leaf manifest", rootpublication.ErrResourceConflict)
		}
		authority = ordinaryLeafManifestAuthority{revision: revision, identity: identity, digest: digest}
		return nil
	})
	return authority, err
}

func firstUncoveredOuterLeafFile(resources *rootpublication.StableResourceSet, covered func(uint32) bool) (uint32, error) {
	var missing uint32
	kinds := [...]rootpublication.ResourceKind{rootpublication.ResourceOuterLeafLog, rootpublication.ResourceOuterLeafPack}
	err := resources.WalkPhysicalResourceIdentities(kinds[:], func(_ rootpublication.ResourceKind, generation uint64, _ rootpublication.StableIdentity, _ [32]byte) error {
		if generation > math.MaxUint32 {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		fileID, valid := rawLeafGenerationFileID(uint32(generation))
		if !valid {
			return fmt.Errorf("%w: invalid physical leaf file ID %d", rootpublication.ErrResourceConflict, generation)
		}
		if missing == 0 && !covered(fileID) {
			missing = fileID
		}
		return nil
	})
	return missing, err
}

func leafManifestContainsActiveFile(manifest *leafGenerationManifest, fileID uint32) bool {
	if manifest != nil {
		for _, gen := range manifest.Generations {
			if gen.State == leafGenerationStateDeleted || gen.State == leafGenerationStateRetiring {
				continue
			}
			for _, existing := range gen.FileIDs {
				if existing == fileID {
					return true
				}
			}
		}
	}
	return false
}
