package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// visibleTransactionSpecV6 is the single callback constructor for both ordinary
// and retained preparation. It binds the SAME member, original allocator and
// physical retirement operands; native work never copies a publisher.
func (runtime *rootPublicationRuntimeV1) visibleTransactionSpecV6(member *rootPublicationVisibleMemberV1, memberMetadata uint64, native bool) rootpublication.DurableRootTransactionSpec {
	install, next, primary, limits := member.install, member.next, member.primary, member.preparedLimits
	transactionCOW := member.prepared
	if primary != nil {
		transactionCOW = nil
	}
	spec := rootpublication.DurableRootTransactionSpec{
		PrimaryMetadataBytesV6: memberMetadata + install.metadataCharge,
		Lineage:                runtime.lineage, Sequence: next.CommitSeq, PreparedCOW: transactionCOW, PreparedPrimary: primary, PublicationProjection: member,
		Activate: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != transactionCOW || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			if limits != nil {
				runtime.mu.Lock()
				over := len(runtime.visibleMembers)-runtime.visibleHead >= limits.MaxVisibleMembers || len(runtime.debt) >= limits.MaxAllocatorDebt
				runtime.mu.Unlock()
				if over {
					return errors.New("prepared visible root runtime exceeds admission limits")
				}
			}
			if err := member.install.activate(func() error {
				if member.prepared != nil {
					return runtime.idx.allocator.ActivateCOWCandidateV1(member.prepared)
				}
				return nil
			}); err != nil {
				return err
			}
			runtime.mu.Lock()
			if member.primary != nil && member.primary.ReadCertificateV6() != nil {
				runtime.primaryDataCertificateV6 = member.primary.DataCertificateV6()
			}
			previousResources := runtime.visibleResources
			runtime.visibleResources = member.resources
			runtime.visibleMembers = append(runtime.visibleMembers, member)
			if member.prepared != nil {
				runtime.debt = append(runtime.debt, member.prepared)
			}
			runtime.mu.Unlock()
			previousResources.Release()
			return nil
		},
		Consume: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != transactionCOW || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			runtime.mu.Lock()
			runtime.removeVisibleMember(member.sequence)
			if member.primary != nil && member.primary.Bundle().Record != (primaryarena.Ref{}) && !member.primaryRecordTransferred {
				_, e := runtime.idx.primary.Drop(member.primary.Bundle().Record, nil)
				if e != nil {
					runtime.mu.Unlock()
					return e
				}
			}
			runtime.mu.Unlock()
			return nil
		},
		Abort: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != transactionCOW || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			if member.primary != nil {
				var e error
				if member.primary.Bundle().Record != (primaryarena.Ref{}) {
					_, e = runtime.idx.primary.Drop(member.primary.Bundle().Record, nil)
				}
				if e != nil {
					return e
				}
				if !member.install.activated {
					if _, e = runtime.idx.primary.Drop(member.primary.Bundle().Directory, nil); e != nil {
						return e
					}
				}
			}
			if member.prepared != nil {
				if err := runtime.idx.allocator.AbortCOWCandidateV1(member.prepared); err != nil {
					return err
				}
			}
			return nil
		},
		AbortComplete: func(input rootpublication.DurableRootCallbackInput) error {
			if input.PreparedCOW != transactionCOW || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			// Observe the SAME transaction's final physical abort, including its
			// original constructor root. No callback evidence is refunded earlier.
			if !member.install.activated {
				if err := member.resources.ReleaseWithError(); err != nil {
					return err
				}
				member.resources = nil
			}
			if err := member.install.abort(); err != nil {
				return err
			}
			return nil
		},
		Fail: func(input rootpublication.DurableRootCallbackInput, cause error) error {
			if input.PreparedCOW != transactionCOW || input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return rootpublication.ErrDurableRootOwnership
			}
			if member.prepared != nil {
				return runtime.idx.allocator.FailCOWCandidateV1(member.prepared, cause)
			}
			return nil
		},
	}
	if native {
		member.retirementReleaseSet = releasePrimaryResourceSetWithWorkV6
		spec.AbortWithWork = func(input rootpublication.DurableRootCallbackInput, work *iterator.OrdinalScanWork) (bool, error) {
			return runtime.abortNativeVisibleMemberV6(member, transactionCOW, input, work)
		}
		spec.AbortCompleteWithWork = func(input rootpublication.DurableRootCallbackInput, work *iterator.OrdinalScanWork) (bool, error) {
			return runtime.completeNativeVisibleMemberAbortV6(member, transactionCOW, input, work)
		}
		spec.ActivateWithWork = func(input rootpublication.DurableRootCallbackInput, work *iterator.OrdinalScanWork) (bool, error) {
			return runtime.activateNativeVisibleMemberV6(member, transactionCOW, input, work)
		}
		spec.ConsumeWithWork = func(input rootpublication.DurableRootCallbackInput, work *iterator.OrdinalScanWork) (bool, error) {
			return runtime.consumeNativeVisibleMemberV6(member, transactionCOW, input, work)
		}
		spec.RetireWithWork = func(input rootpublication.DurableRootCallbackInput, work *iterator.OrdinalScanWork) (bool, error) {
			if input.PreparedPrimary != member.primary || input.Sequence != member.sequence {
				return false, rootpublication.ErrDurableRootOwnership
			}
			return member.continueRetirementV6(runtime.idx.primary, work)
		}
	}
	return spec
}
