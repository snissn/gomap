package rootpublication

import (
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Completion carries only the exact original admission after the producer
// method returns. The directory clears its provider alias before consuming it.
type DependencyDirectoryReleaseCompletionV6 struct {
	Metadata *retainedalloc.Owner
	Charge   uint64
}

// A typed original producer reports its own consumed edge independently of
// earlier sibling errors. Failed physical ownership remains with that producer.
type DependencyDirectoryReleaseProviderV6 interface {
	ReleaseDependencyDirectoryV6() (consumed bool, completion DependencyDirectoryReleaseCompletionV6, err error)
}

// This stack receipt only carries original accounting across the return from
// the disposing method. It neither owns physical storage nor schedules cleanup.
type dependencyDirectoryRelease struct {
	directory       *DependencyDirectoryV2
	completion      DependencyDirectoryReleaseCompletionV6
	directoryOwner  *retainedalloc.Owner
	directoryCharge uint64
	phase           uint8
	bound, consumed bool
	err             error
}

func NewOwnedTypedLocalPrimaryDependencyDirectoryV6(p *pager.Pager, ref DependencyDirectoryRefV2, extent uint64, metadata *retainedalloc.Owner, release DependencyDirectoryReleaseProviderV6) (*DependencyDirectoryV2, error) {
	if release == nil {
		return nil, ErrResourceOwnership
	}
	// Retain the original canonical geometry and allocation admission.
	directory, err := NewOwnedLocalPrimaryDependencyDirectoryV5(p, ref, extent, metadata, unusedTypedDirectoryRelease)
	if err != nil {
		return nil, err
	}
	directory.ownedRelease = nil
	directory.ownedReleaseProvider = release
	return directory, nil
}

// DATA and PRIMARY use the same canonical constructor and independent receipt.
func NewOwnedTypedDependencyDirectoryV6(p *pager.Pager, ref DependencyDirectoryRefV2, extent uint64, metadata *retainedalloc.Owner, provider DependencyDirectoryReleaseProviderV6) (*DependencyDirectoryV2, error) {
	if provider == nil {
		return nil, ErrResourceOwnership
	}
	return newDependencyDirectoryV2(p, ref, extent, nil, metadata, provider)
}

func unusedTypedDirectoryRelease() (bool, error) { return false, ErrResourceOwnership }

func (s *dependencyDirectoryRelease) bind(directory *DependencyDirectoryV2) (bool, error) {
	if s.bound {
		return false, ErrResourceOwnership
	}
	if directory == nil {
		s.bound, s.consumed, s.phase = true, true, 3
		return true, nil
	}
	if directory.cleanupDone.Load() {
		s.bound, s.consumed, s.phase = true, directory.cleanupConsumed, 3
		if directory.cleanupFailure.causes[0] != nil {
			if s.consumed {
				s.err = directory.cleanupFailure.causes[0]
			} else {
				s.err = &directory.cleanupFailure
			}
		}
		return true, nil
	}
	last := false
	for refs := directory.refs.Load(); refs > 0; refs = directory.refs.Load() {
		if directory.refs.CompareAndSwap(refs, refs-1) {
			last = refs == 1
			s.bound = true
			break
		}
	}
	if !s.bound {
		return false, retainedalloc.ErrWorkRequired
	}
	if !last {
		s.phase, s.consumed = 3, true
		return true, nil
	}
	s.directory = directory
	return true, nil
}

func (s *dependencyDirectoryRelease) fail(directory *DependencyDirectoryV2, err error) (bool, bool, error) {
	if err == nil {
		err = ErrResourceOwnership
	}
	directory.cleanupFailure.original, directory.cleanupFailure.causes[0] = directory, err
	directory.cleanupConsumed = false
	if directory.metadata != nil {
		directory.metadata.CleanupFailed()
	}
	directory.cleanupDone.Store(true)
	s.err, s.phase = &directory.cleanupFailure, 3
	return false, true, s.err
}

func (s *dependencyDirectoryRelease) release() (bool, bool, error) {
	if !s.bound {
		return false, false, ErrResourceOwnership
	}
	if s.phase == 3 {
		return s.consumed, true, s.err
	}
	directory := s.directory
	if s.phase < 2 && directory == nil {
		return false, false, ErrResourceOwnership
	}
	if s.phase == 0 {
		if directory.tree != nil {
			// End the last physical reader alias before producer physical close.
			directory.tree.Reset(nil, nil, 0)
		}
		consumed := true
		var err error
		if directory.ownedReleaseProvider != nil {
			provider := directory.ownedReleaseProvider
			consumed, s.completion, err = provider.ReleaseDependencyDirectoryV6()
			provider = nil
		} else if directory.metadata != nil {
			callback := directory.ownedRelease
			consumed, err = callback()
			callback = nil
		} else {
			callback := directory.release
			callback()
			callback = nil
		}
		if !consumed {
			return s.fail(directory, err)
		}
		if s.completion.Charge != 0 && (s.completion.Metadata == nil || s.completion.Metadata != directory.metadata) {
			return s.fail(directory, ErrResourceOwnership)
		}
		s.err = err
		s.directoryOwner, s.directoryCharge = directory.metadata, directory.metadataCharge
		s.phase = 1
	}
	if s.phase == 1 {
		directory.tree, directory.release, directory.ownedRelease, directory.ownedReleaseProvider = nil, nil, nil, nil
		directory.metadata, directory.metadataCharge = nil, 0
		directory.cleanupConsumed = true
		directory.cleanupFailure.causes[0] = s.err
		directory.cleanupDone.Store(true)
		s.directory, directory = nil, nil
		s.phase = 2
		// Return through the actual disposing method before containing refunds.
		return false, false, nil
	}
	directory = nil
	if s.completion.Charge != 0 {
		s.completion.Metadata.RemovePending(s.completion.Charge)
		s.completion = DependencyDirectoryReleaseCompletionV6{}
	}
	if s.directoryCharge != 0 {
		s.directoryOwner.RemovePending(s.directoryCharge)
		s.directoryOwner, s.directoryCharge = nil, 0
	}
	s.phase, s.consumed = 3, true
	return true, true, s.err
}
