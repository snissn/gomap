package vectorpartition

import (
	"context"
	"errors"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

// ReplaceRequestV1 replaces an existing exact ID; it never inserts a missing ID.
// A later mutation needs a new idempotency key, even for identical content.
type ReplaceRequestV1 = InsertRequestV1

type DeleteRequestV1 struct {
	Version            uint32
	Generation         GenerationIDV1
	IdempotencyKey, ID []byte
	Deadline           time.Time
}

// MutationResponseV1 is the original durable outcome of a colocated mutation.
// Coverage and LiveRevision are a scoped read-your-write floor, not a global
// snapshot. A missing or unchanged target does not manufacture a live revision.
type MutationResponseV1 struct {
	VisibilityToken                       []byte
	Generation                            GenerationIDV1
	OwnerGroup                            string
	CommitTerm, CommitIndex, AppliedIndex uint64
	ProductionConsensus                   bool
	Coverage, LiveRevision                uint64
	Matched, Modified, Deleted            uint64
	Counters                              MutationCountersV1
}

// ColocatedMutationBackendV1 is optional so existing assembled backends retain
// their insert and search contracts. Implementations refuse unsupported layouts.
type ColocatedMutationBackendV1 interface {
	ReplaceVectorPartitionV1(context.Context, ReplaceRequestV1) (MutationResponseV1, error)
	DeleteVectorPartitionV1(context.Context, DeleteRequestV1) (MutationResponseV1, error)
}

func (s *ServiceV1) Replace(ctx context.Context, request ReplaceRequestV1) (MutationResponseV1, error) {
	if err := ValidateReplaceRequestV1(ctx, request); err != nil {
		return MutationResponseV1{}, err
	}
	return s.colocatedMutationV1(ctx, request.Generation, request.Deadline, false, func(ctx context.Context, backend ColocatedMutationBackendV1) (MutationResponseV1, error) {
		return backend.ReplaceVectorPartitionV1(ctx, cloneInsertRequestV1(request))
	})
}

func (s *ServiceV1) Delete(ctx context.Context, request DeleteRequestV1) (MutationResponseV1, error) {
	if err := ValidateDeleteRequestV1(ctx, request); err != nil {
		return MutationResponseV1{}, err
	}
	return s.colocatedMutationV1(ctx, request.Generation, request.Deadline, true, func(ctx context.Context, backend ColocatedMutationBackendV1) (MutationResponseV1, error) {
		request.ID, request.IdempotencyKey = slices.Clone(request.ID), slices.Clone(request.IdempotencyKey)
		return backend.DeleteVectorPartitionV1(ctx, request)
	})
}

func (s *ServiceV1) colocatedMutationV1(ctx context.Context, generation GenerationIDV1, deadline time.Time, deletion bool, apply func(context.Context, ColocatedMutationBackendV1) (MutationResponseV1, error)) (MutationResponseV1, error) {
	if s == nil {
		return MutationResponseV1{}, &ErrorV1{Code: ErrorUnavailableV1, Err: errors.New("colocated vector mutation service is unavailable")}
	}
	backend, ok := s.backend.(ColocatedMutationBackendV1)
	if !ok {
		return MutationResponseV1{}, &ErrorV1{Code: ErrorUnavailableV1, Err: errors.New("colocated vector mutation backend is unavailable")}
	}
	requestCtx, cancel := searchRequestContextV1(ctx, deadline)
	defer cancel()
	response, err := apply(requestCtx, backend)
	if err != nil {
		return MutationResponseV1{}, classifyErrorV1(requestCtx, err)
	}
	if err := ValidateMutationResponseV1(generation, deletion, response); err != nil {
		return MutationResponseV1{}, &ErrorV1{Code: ErrorCommitAmbiguousV1, Err: err}
	}
	response.VisibilityToken = slices.Clone(response.VisibilityToken)
	return response, nil
}

func ValidateReplaceRequestV1(ctx context.Context, r ReplaceRequestV1) error {
	if err := ValidateInsertRequestV1(ctx, r); err != nil {
		return err
	}
	if len(r.ID) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || len(r.IdempotencyKey) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || len(r.Document) > commitlog.ColocatedVectorMutationMaxDocumentBytesV1 {
		return invalidV1("colocated replacement exceeds bounded identity/document limits")
	}
	return nil
}

func ValidateDeleteRequestV1(ctx context.Context, r DeleteRequestV1) error {
	if err := validateGenerationV1(ctx, r.Generation); err != nil {
		return err
	}
	if r.Version != 1 || len(r.IdempotencyKey) == 0 || len(r.IdempotencyKey) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || string(r.IdempotencyKey) == raftentry.NoIdempotencyTokenV1 || len(r.ID) == 0 || len(r.ID) > commitlog.ColocatedVectorMutationMaxIdentityBytesV1 || !utf8.Valid(r.ID) {
		return invalidV1("version, generation, bounded idempotency key and valid UTF-8 stable id are required")
	}
	if !r.Deadline.IsZero() && !time.Now().Before(r.Deadline) {
		return &ErrorV1{Code: ErrorDeadlineExceededV1, Err: context.DeadlineExceeded}
	}
	return nil
}

func ValidateMutationResponseV1(generation GenerationIDV1, deletion bool, r MutationResponseV1) error {
	valid := r.Generation == generation && r.OwnerGroup != "" && r.CommitTerm != 0 && r.CommitIndex != 0 && r.AppliedIndex >= r.CommitIndex && r.ProductionConsensus && r.Coverage != 0 && len(r.VisibilityToken) != 0 && len(r.VisibilityToken) <= 8192
	valid = valid && r.Matched <= 1 && r.Modified <= r.Matched && r.Deleted <= 1
	if deletion {
		valid = valid && r.Matched == 0 && r.Modified == 0
	} else {
		valid = valid && r.Deleted == 0
	}
	if r.Modified != 0 || r.Deleted != 0 {
		valid = valid && r.LiveRevision != 0
	}
	c := r.Counters
	valid = valid && c.Routes == 1 && c.Forwards <= 1 && c.Commits == 1 && c.Replications == 1 && c.Applies == 1 && c.VisibilityProofs == 1
	if !valid {
		return &ErrorV1{Code: ErrorFailedV1, Err: errors.New("backend returned invalid colocated mutation proof")}
	}
	return nil
}

func (o *OperationsV1) Replace(ctx context.Context, request ReplaceRequestV1) (MutationResponseV1, error) {
	if err := o.admitInsertV1(ctx, request); err != nil {
		return MutationResponseV1{}, err
	}
	response, err := o.service.Replace(ctx, request)
	o.recordColocatedMutationV1(false, response, err)
	return response, err
}

func (o *OperationsV1) Delete(ctx context.Context, request DeleteRequestV1) (MutationResponseV1, error) {
	if err := o.enabled(); err != nil {
		return MutationResponseV1{}, err
	}
	if err := ValidateDeleteRequestV1(ctx, request); err != nil {
		return MutationResponseV1{}, err
	}
	if uint64(len(request.Generation.Index))+uint64(len(request.IdempotencyKey))+uint64(len(request.ID)) > o.config.MaxRequestBytes {
		o.mu.Lock()
		o.counts.CapRequestBytes++
		o.mu.Unlock()
		return MutationResponseV1{}, invalidV1("vector mutation exceeds configured operation limit")
	}
	response, err := o.service.Delete(ctx, request)
	o.recordColocatedMutationV1(true, response, err)
	return response, err
}

func (o *OperationsV1) recordColocatedMutationV1(deletion bool, r MutationResponseV1, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if deletion {
		o.counts.Deletes++
	} else {
		o.counts.Replaces++
	}
	if err != nil {
		o.counts.Failures++
		return
	}
	o.counts.MutationRoutes += r.Counters.Routes
	o.counts.MutationForwards += r.Counters.Forwards
	o.counts.MutationCommits += r.Counters.Commits
	o.counts.MutationReplications += r.Counters.Replications
	o.counts.MutationApplies += r.Counters.Applies
	o.counts.MutationVisibilityProofs += r.Counters.VisibilityProofs
}
