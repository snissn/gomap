package db

import (
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Original seal disposal stops at its first physical failure. All remaining
// operands and that same error receipt stay on this original seal.
func releasePrimaryPublicationSealV6(s *primaryPublicationSealV5) (bool, error) {
	if s == nil {
		return true, nil
	}
	s.disposalMu.Lock()
	defer s.disposalMu.Unlock()
	if s.disposalAttempted {
		return s.disposalErr == nil, s.disposalErr
	}
	if s.prepared == nil || s.prepared.Arena() == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	a := s.prepared.Arena()
	fail := func(err error) (bool, error) {
		s.disposalAttempted = true
		s.disposalFailure.payload = s
		s.disposalFailure.retainCause(0, err)
		s.disposalErr = &s.disposalFailure
		a.MetadataOwner().CleanupFailed()
		return false, s.disposalErr
	}
	drop := func(ref *primaryarena.Ref) error {
		if *ref == (primaryarena.Ref{}) {
			return nil
		}
		consumed, err := a.Drop(*ref, nil)
		if consumed {
			*ref = primaryarena.Ref{}
		}
		if err == nil && !consumed {
			err = rootpublication.ErrResourceOwnership
		}
		return err
	}
	if s.promotion != nil {
		for _, ref := range [4]*primaryarena.Ref{&s.promotion.Root, &s.promotion.Parent, &s.promotion.Closure, &s.promotion.ParentClosure} {
			if err := drop(ref); err != nil {
				return fail(err)
			}
		}
	}
	if err := drop(&s.proof); err != nil {
		return fail(err)
	}
	completion, ready, err := releasePrimaryBankConstructionV6(s.manifestBanks)
	if err != nil {
		return fail(err)
	}
	if !ready {
		return fail(rootpublication.ErrResourceOwnership)
	}
	s.manifestBanks = nil
	completion.complete()
	if s.proofResources != nil {
		if err := s.proofResources.ReleaseWithError(); err != nil {
			return fail(err)
		}
		s.proofResources = nil
	}
	if s.token != nil {
		if err := s.token.ReleaseWithError(); err != nil {
			return fail(err)
		}
		s.token = nil
	}
	if s.promotion != nil {
		s.promotion.Image = nil
		s.promotion.View = rootpublication.PrimaryCapsuleViewV6{}
	}
	s.promotion, s.prepared = nil, nil
	s.projection = rootpublication.PrimaryProjectionV5{}
	s.disposalAttempted = true
	return true, nil
}

func releaseRootPublicationSealV6(s *rootPublicationSealV1) (primaryRuntimeDisposalCompletionV6, bool, error) {
	var empty primaryRuntimeDisposalCompletionV6
	if s == nil {
		return empty, true, nil
	}
	s.disposalMu.Lock()
	defer s.disposalMu.Unlock()
	if s.released {
		return empty, s.disposalErr == nil, s.disposalErr
	}
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		s.released = true
		s.disposalFailure.payload = s
		s.disposalFailure.retainCause(0, err)
		s.disposalErr = &s.disposalFailure
		if s.metadata != nil {
			s.metadata.CleanupFailed()
		}
		return empty, false, s.disposalErr
	}
	ready, err := releasePrimaryPublicationSealV6(s.primary)
	if err != nil {
		return fail(err)
	}
	if !ready {
		return fail(rootpublication.ErrResourceOwnership)
	}
	s.primary = nil
	if s.resources != nil {
		if err := s.resources.ReleaseWithError(); err != nil {
			return fail(err)
		}
		s.resources = nil
	}
	if s.token != nil {
		if err := s.token.ReleaseWithError(); err != nil {
			return fail(err)
		}
		s.token = nil
	}
	if s.manifest != nil {
		completion, ready, err := s.manifest.ReleaseOwnedMetadataWithWorkV6(nil)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return fail(rootpublication.ErrResourceOwnership)
		}
		s.manifest = nil
		completion.Complete()
	}
	s.prefix, s.prepared = nil, nil
	s.base = rootPublicationSealBaseV1{}
	s.idx = nil
	completion := primaryRuntimeDisposalCompletionV6{s.metadata, s.metadataCharge}
	s.metadata, s.metadataCharge = nil, 0
	s.released = true
	return completion, true, nil
}
