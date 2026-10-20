package db

// Original seal disposal stops at its first physical failure. All remaining
// operands and that same error receipt stay on this original seal.

func releaseRootPublicationSealV6(s *rootPublicationSealV1) (metadataDisposalCompletionV6, bool, error) {
	var empty metadataDisposalCompletionV6
	if s == nil {
		return empty, true, nil
	}
	s.disposalMu.Lock()
	defer s.disposalMu.Unlock()
	if s.released {
		return empty, s.disposalErr == nil, s.disposalErr
	}
	fail := func(err error) (metadataDisposalCompletionV6, bool, error) {
		s.released = true
		s.disposalFailure.payload = s
		s.disposalFailure.retainCause(0, err)
		s.disposalErr = &s.disposalFailure
		if s.metadata != nil {
			s.metadata.CleanupFailed()
		}
		return empty, false, s.disposalErr
	}
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
		completion, err := s.manifest.ReleaseOwnedMetadataWithCompletionV6()
		if err != nil {
			return fail(err)
		}
		s.manifest = nil
		completion.Complete()
	}
	s.prefix, s.prepared = nil, nil
	s.base = rootPublicationSealBaseV1{}
	s.idx = nil
	completion := metadataDisposalCompletionV6{s.metadata, s.metadataCharge}
	s.metadata, s.metadataCharge = nil, 0
	s.released = true
	return completion, true, nil
}
