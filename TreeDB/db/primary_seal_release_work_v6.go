package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"unsafe"
)

// Both synchronous release and native Finish drain these SAME seal operands.
// The original seal retains phase, queue and first-error custody across refusal.
// Both drivers stop at the first failed physical operation. Later operands stay
// on this SAME original seal as debt; no later callback is attempted or replayed.
func continuePrimaryPublicationSealReleaseV6(s *primaryPublicationSealV5, w *iterator.OrdinalScanWork, release primaryResourceReleaseWithWorkV6) (bool, error) {
	if s == nil {
		return true, nil
	}
	if w != nil && !w.Reserve(3, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(s.disposalFailure))+2*uint64(unsafe.Sizeof(retainedalloc.Owner{}))) {
		return false, nil
	}
	s.disposalMu.Lock()
	defer s.disposalMu.Unlock()
	if s.disposalAttempted {
		return s.disposalErr == nil, s.disposalErr
	}
	if s.prepared == nil || (w != nil && release == nil) {
		return false, rootpublication.ErrResourceOwnership
	}
	a := s.prepared.Arena()
	if a == nil {
		return false, rootpublication.ErrResourceOwnership
	}
	admit := func(r, b uint64) bool { return w == nil || w.Reserve(r, b) }
	fail := func(err error) (bool, error) {
		s.disposalAttempted = true
		s.disposalFailure.payload = s
		if w != nil {
			s.disposalFailure.retainNativeCauseAdmittedV6(0, err)
		} else {
			s.disposalFailure.retainCause(0, err)
		}
		s.disposalErr = &s.disposalFailure
		a.MetadataOwner().CleanupFailed()
		return false, s.disposalErr
	}
	if s.disposalPhase == 0 {
		if !admit(1, 2*uint64(unsafe.Sizeof(*s))) {
			return false, nil
		}
		s.disposalNative = w != nil
		if s.disposalNative {
			a.InitializeReleaseQueue(&s.disposalQueue)
		}
		s.disposalPhase = 1
	}
	if s.disposalNative != (w != nil) {
		return false, rootpublication.ErrResourceOwnership
	}
	if s.disposalNative {
		if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
			return false, err
		}
		if ready, err := s.disposalChildCompletion.completeWithWork(&s.disposalRefund, w); !ready || err != nil {
			return false, err
		}
	}
	drop := func(ref *primaryarena.Ref) (bool, error) {
		if !admit(1, 2*uint64(unsafe.Sizeof(*ref))) {
			return false, nil
		}
		if *ref == (primaryarena.Ref{}) {
			return true, nil
		}
		var ready bool
		var err error
		if s.disposalNative {
			ready, err = a.DropToReleaseQueue(*ref, &s.disposalQueue, w)
		} else {
			ready, err = a.Drop(*ref, nil)
		}
		if ready {
			*ref = primaryarena.Ref{}
		}
		return ready, err
	}
	for s.disposalPhase == 1 && s.promotion != nil && s.disposalIndex < 4 {
		refs := [4]*primaryarena.Ref{&s.promotion.Root, &s.promotion.Parent, &s.promotion.Closure, &s.promotion.ParentClosure}
		ready, err := drop(refs[s.disposalIndex])
		if err != nil {
			return fail(err)
		}
		if !ready {
			return false, nil
		}
		s.disposalIndex++
	}
	if s.disposalPhase == 1 {
		s.disposalPhase = 3
	}
	if s.disposalPhase == 3 {
		ready, err := drop(&s.proof)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return false, nil
		}
		s.disposalPhase = 4
	}
	if s.disposalPhase == 4 {
		if s.disposalNative {
			for {
				ready, progress, err := a.ReleaseQueueStep(&s.disposalQueue, w)
				if err != nil {
					return fail(err)
				}
				if !ready {
					return false, nil
				}
				if !progress {
					break
				}
			}
		}
		s.disposalPhase = 5
	}
	if s.disposalPhase == 5 {
		completion, ready, err := continuePrimaryBankConstructionReleaseV6(s.manifestBanks, w)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return false, nil
		}
		s.manifestBanks = nil
		s.disposalChildCompletion = completion
		s.disposalPhase = 6
		if ready, err := s.disposalChildCompletion.completeWithWork(&s.disposalRefund, w); !ready || err != nil {
			return false, err
		}
	}
	if s.disposalPhase == 6 {
		if s.proofResources != nil {
			if !admit(1, 2*uint64(unsafe.Sizeof(s.proofResources))) {
				return false, nil
			}
			var completion rootpublication.StableResourceTokenReleaseCompletionV6
			var ready bool
			var err error
			if s.disposalNative {
				completion, ready, err = release(s.proofResources, w)
			} else {
				err = s.proofResources.ReleaseWithError()
				ready = err == nil
			}
			if err != nil {
				return fail(err)
			}
			if !ready {
				return false, nil
			}
			s.proofResources = nil
			if w == nil {
				completion.Complete()
			} else {
				s.disposalCompletion = completion
				if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
					return false, err
				}
			}
		}
		s.disposalPhase = 7
	}
	if s.disposalPhase == 7 {
		if s.token != nil {
			if !admit(1, 2*uint64(unsafe.Sizeof(s.token))) {
				return false, nil
			}
			var completion rootpublication.StableResourceTokenReleaseCompletionV6
			var ready bool
			var err error
			if s.disposalNative {
				completion, ready, err = s.token.ReleaseWithWorkV6(w)
			} else {
				err = s.token.ReleaseWithError()
				ready = err == nil
			}
			if err != nil {
				return fail(err)
			}
			if !ready {
				return false, nil
			}
			s.token = nil
			if w == nil {
				completion.Complete()
			} else {
				s.disposalCompletion = completion
				if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
					return false, err
				}
			}
		}
		s.disposalPhase = 8
	}
	if !admit(1, 2*uint64(unsafe.Sizeof(*s))) {
		return false, nil
	}
	if s.promotion != nil {
		s.promotion.Image = nil
		s.promotion.View = rootpublication.PrimaryCapsuleViewV6{}
	}
	s.promotion = nil
	s.prepared = nil
	s.projection = rootpublication.PrimaryProjectionV5{}
	s.disposalAttempted = true
	s.disposalPhase = 9
	return true, nil
}

func continueRootPublicationSealReleaseV6(s *rootPublicationSealV1, w *iterator.OrdinalScanWork, release primaryResourceReleaseWithWorkV6) (primaryRuntimeDisposalCompletionV6, bool, error) {
	var empty primaryRuntimeDisposalCompletionV6
	if s == nil {
		return empty, true, nil
	}
	if w != nil && !w.Reserve(3, 2*uint64(unsafe.Sizeof(*s))+2*uint64(unsafe.Sizeof(s.disposalFailure))+2*uint64(unsafe.Sizeof(retainedalloc.Owner{}))) {
		return empty, false, nil
	}
	s.disposalMu.Lock()
	defer s.disposalMu.Unlock()
	if s.released {
		return empty, s.disposalErr == nil, s.disposalErr
	}
	if w != nil && release == nil {
		return empty, false, rootpublication.ErrResourceOwnership
	}
	admit := func(r, b uint64) bool { return w == nil || w.Reserve(r, b) }
	fail := func(err error) (primaryRuntimeDisposalCompletionV6, bool, error) {
		s.released = true
		s.disposalFailure.payload = s
		if w != nil {
			s.disposalFailure.retainNativeCauseAdmittedV6(0, err)
		} else {
			s.disposalFailure.retainCause(0, err)
		}
		s.disposalErr = &s.disposalFailure
		if s.metadata != nil {
			s.metadata.CleanupFailed()
		}
		return empty, false, s.disposalErr
	}
	if s.disposalPhase == 0 {
		if !admit(1, 2*uint64(unsafe.Sizeof(*s))) {
			return empty, false, nil
		}
		s.disposalNative = w != nil
		s.disposalPhase = 1
	}
	if s.disposalNative != (w != nil) {
		return empty, false, rootpublication.ErrResourceOwnership
	}
	if s.disposalNative {
		if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
			return empty, false, err
		}
	}
	if s.disposalPhase == 1 {
		ready, err := abandonPrimarySealCommitStorageV6(s, w)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		ready, err = continuePrimaryPublicationSealReleaseV6(s.primary, w, release)
		if err != nil {
			return fail(err)
		}
		if !ready {
			return empty, false, nil
		}
		s.primary = nil
		s.disposalPhase = 2
	}
	if s.disposalPhase == 2 {
		if s.resources != nil {
			if !admit(1, 2*uint64(unsafe.Sizeof(s.resources))) {
				return empty, false, nil
			}
			var completion rootpublication.StableResourceTokenReleaseCompletionV6
			var ready bool
			var err error
			if s.disposalNative {
				completion, ready, err = release(s.resources, w)
			} else {
				err = s.resources.ReleaseWithError()
				ready = err == nil
			}
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			s.resources = nil
			if w == nil {
				completion.Complete()
			} else {
				s.disposalCompletion = completion
				if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
					return empty, false, err
				}
			}
		}
		s.disposalPhase = 3
	}
	if s.disposalPhase == 3 {
		if s.token != nil {
			if !admit(1, 2*uint64(unsafe.Sizeof(s.token))) {
				return empty, false, nil
			}
			var completion rootpublication.StableResourceTokenReleaseCompletionV6
			var ready bool
			var err error
			if s.disposalNative {
				completion, ready, err = s.token.ReleaseWithWorkV6(w)
			} else {
				err = s.token.ReleaseWithError()
				ready = err == nil
			}
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			s.token = nil
			if w == nil {
				completion.Complete()
			} else {
				s.disposalCompletion = completion
				if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
					return empty, false, err
				}
			}
		}
		s.disposalPhase = 4
	}
	if s.disposalPhase == 4 {
		if s.manifest != nil {
			if !admit(1, 2*uint64(unsafe.Sizeof(s.manifest))) {
				return empty, false, nil
			}
			completion, ready, err := s.manifest.ReleaseOwnedMetadataWithWorkV6(w)
			if err != nil {
				return fail(err)
			}
			if !ready {
				return empty, false, nil
			}
			s.manifest = nil
			if w == nil {
				completion.Complete()
			} else {
				s.disposalCompletion = completion
				if ready, err := s.disposalCompletion.CompleteWithWorkV6(&s.disposalRefund, w); !ready || err != nil {
					return empty, false, err
				}
			}
		}
		s.disposalPhase = 5
	}
	if !admit(1, 2*uint64(unsafe.Sizeof(*s))) {
		return empty, false, nil
	}
	s.prefix = nil
	s.prepared = nil
	s.base = rootPublicationSealBaseV1{}
	s.idx = nil
	completion := primaryRuntimeDisposalCompletionV6{s.metadata, s.metadataCharge}
	s.metadata = nil
	s.metadataCharge = 0
	s.released = true
	s.disposalPhase = 6
	return completion, true, nil
}
