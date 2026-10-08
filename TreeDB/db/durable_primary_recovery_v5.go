package db

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// primaryBankPageSource translates physical bank IDs only. DATA readers and
// allocator generation loading keep their separate original namespace.
type primaryBankPageSource struct {
	local  freelist.PageSource
	extent uint64
}

func (s primaryBankPageSource) ReadPage(id uint64) ([]byte, error) {
	if s.local == nil || !primaryarena.IsPage(id) || primaryarena.Local(id) < 2 || primaryarena.Local(id) >= s.extent {
		return nil, errors.New("primary bank outside exact extent")
	}
	return s.local.ReadPage(primaryarena.Local(id))
}

type durablePrimaryDirectoryValidatorV5 func(rootpublication.DurablePrimaryRootRecordV5) (*rootpublication.StableResourceSet, error)

// selectDurablePrimaryRootV5 admits both slots independently against the actual
// immutable DATA generation and complete primary projection. It reads exactly
// one parent record per slot for command-WAL frontier continuity; parents do
// not supply data, directory cells, allocation authority or replay.
func selectDurablePrimaryRootV5(data, localArena freelist.PageSource, dataPages, arenaPages uint64, uuid [16]byte, manifestValidator durableManifestValidatorV1, directoryValidator durablePrimaryDirectoryValidatorV5) (durableRootSelectionV1, error) {
	return selectDurablePrimaryRootWithMetadataV5(data, localArena, dataPages, arenaPages, uuid, manifestValidator, directoryValidator, nil)
}

// The selected arena owns admitted parser/output storage; generic callers keep
// the existing detached diagnostic contract. The physical selector is shared.
func selectDurablePrimaryRootWithMetadataV5(data, localArena freelist.PageSource, dataPages, arenaPages uint64, uuid [16]byte, manifestValidator durableManifestValidatorV1, directoryValidator durablePrimaryDirectoryValidatorV5, metadata *retainedalloc.Owner) (durableRootSelectionV1, error) {
	arena := primaryBankPageSource{local: localArena, extent: arenaPages}
	return selectDurableRootCandidates(data, func(candidate durableMetaCandidateV1) (durableRootSelectionV1, error) {
		image, e := arena.ReadPage(candidate.meta.RootRecordPageID)
		if e != nil {
			return durableRootSelectionV1{}, fmt.Errorf("primary record: %w", e)
		}
		v, e := rootpublication.DecodeDurablePrimaryRootRecordV5(image, candidate.meta.RootRecordPageID, candidate.meta.RootRecordDigest)
		if e != nil {
			return durableRootSelectionV1{}, fmt.Errorf("primary record: %w", e)
		}
		r, m := v.Record, candidate.meta
		if r.CommitSeq != m.CommitSeq || r.DurableSeq != m.DurableSeq || r.MetaProjectionDigest != m.MetaProjectionDigest {
			return durableRootSelectionV1{}, errors.New("primary record: meta projection mismatch")
		}
		generation, e := v.ValidatePhysicalProjectionV5(data, localArena, dataPages, arenaPages, uuid)
		if e != nil {
			return durableRootSelectionV1{}, fmt.Errorf("primary physical projection: %w", e)
		}
		selected := durableRootSelectionV1{Slot: candidate.slot, Meta: m, Record: r, Freelist: generation, Primary: v.Primary}
		selected.primaryPhysicalDigest[candidate.slot] = sha256.Sum256(image)
		parentOwned := true
		defer func() {
			if parentOwned {
				selected.ParentResources[candidate.slot].Release()
			}
		}()
		if r.ParentRecordPageID != 0 {
			// The parent extent is its own immutable claim. It may contain allocations
			// not selected by this child and is never substituted for the child extent.
			parentImage, e := arena.ReadPage(r.ParentRecordPageID)
			if e != nil {
				return durableRootSelectionV1{}, fmt.Errorf("primary parent: %w", e)
			}
			parent, e := rootpublication.DecodeDurablePrimaryRootRecordV5(parentImage, r.ParentRecordPageID, r.ParentRecordDigest)
			if e != nil {
				return durableRootSelectionV1{}, fmt.Errorf("primary parent: %w", e)
			}
			if parent.Primary.ArenaUUID != uuid || parent.Primary.ArenaHighWater > arenaPages || parent.Record.CommitSeq != r.ParentCommitSeq || parent.Record.DurableSeq == ^uint64(0) || parent.Record.DurableSeq+1 != r.DurableSeq || r.AppliedCommandLSN < parent.Record.AppliedCommandLSN {
				return durableRootSelectionV1{}, errors.New("primary parent: non-contiguous publication or regressed command-WAL frontier")
			}
			// Retained proof closure has its own actual DATA and physical authority.
			// It still cannot supply any operand to the independently complete child.
			if _, e = parent.ValidatePhysicalProjectionV5(data, localArena, dataPages, arenaPages, uuid); e != nil {
				return durableRootSelectionV1{}, fmt.Errorf("primary parent physical closure: %w", e)
			}
			if parent.Record.Directory.RootPageID != 0 {
				if directoryValidator == nil {
					return durableRootSelectionV1{}, errors.New("primary parent dependency validator unavailable")
				}
				selected.ParentResources[candidate.slot], e = directoryValidator(parent)
			} else {
				m, e2 := rootpublication.LoadDependencyManifestWithMetadataV1(arena, parent.Record.Manifest, metadata)
				e = e2
				if m != nil {
					defer m.ReleaseOwnedMetadataV1()
				}
				if e == nil && manifestValidator != nil {
					selected.ParentResources[candidate.slot], e = manifestValidator(m)
				}
			}
			if e != nil {
				return durableRootSelectionV1{}, fmt.Errorf("primary parent dependency closure: %w", e)
			}
			selected.ParentRecords[candidate.slot] = parent
			selected.parentPhysicalDigest[candidate.slot] = sha256.Sum256(parentImage)
		}
		var resources *rootpublication.StableResourceSet
		if r.Directory.RootPageID != 0 {
			if directoryValidator == nil {
				return durableRootSelectionV1{}, errors.New("primary dependency directory validator unavailable")
			}
			resources, e = directoryValidator(v)
		} else {
			manifest, loadErr := rootpublication.LoadDependencyManifestWithMetadataV1(arena, r.Manifest, metadata)
			e = loadErr
			if manifest != nil {
				defer manifest.ReleaseOwnedMetadataV1()
				selected.Manifest = manifest.DiagnosticProjectionV1()
			}
			if e == nil && manifestValidator != nil {
				resources, e = manifestValidator(manifest)
			}
		}
		if e != nil {
			resources.Release()
			return durableRootSelectionV1{}, fmt.Errorf("primary dependency metadata: %w", e)
		}
		selected.resources = resources
		parentOwned = false
		return selected, nil
	})
}

// recoveredPrimaryRootsV5 owns a finite independent set: both complete slots
// and their one-hop proof records. Exact duplicate identities share no invented
// ancestry; each slot/proof handle is acquired independently by Arena.Recover.
func recoveredPrimaryRootsV5(selected durableRootSelectionV1) (uint64, []primaryarena.RecoveredRoot) {
	extent, roots, _ := recoveredPrimaryRootsIntoV5(selected, make([]primaryarena.RecoveredRoot, 0, 4))
	return extent, roots
}

func recoveredPrimaryRootsIntoV5(selected durableRootSelectionV1, roots []primaryarena.RecoveredRoot) (uint64, []primaryarena.RecoveredRoot, error) {
	if len(roots) != 0 || cap(roots) < 4 {
		return 0, nil, rootpublication.ErrResourceOwnership
	}
	var extent uint64
	for slot := 0; slot < 2; slot++ {
		if selected.SlotCommits[slot] == 0 {
			continue
		}
		p := selected.SlotPrimary[slot]
		if p.ArenaHighWater > extent {
			extent = p.ArenaHighWater
		}
		m := selected.SlotMetas[slot]
		roots = append(roots, primaryarena.RecoveredRoot{PageID: m.RootRecordPageID, Digest: selected.primaryPhysicalDigest[slot], Class: primaryarena.Record})
		r := selected.SlotRecords[slot]
		if r.ParentRecordPageID != 0 {
			parent := selected.ParentRecords[slot]
			if parent.Primary.ArenaHighWater > extent {
				extent = parent.Primary.ArenaHighWater
			}
			roots = append(roots, primaryarena.RecoveredRoot{PageID: r.ParentRecordPageID, Digest: selected.parentPhysicalDigest[slot], Class: primaryarena.Record})
		}
	}
	return extent, roots, nil
}
