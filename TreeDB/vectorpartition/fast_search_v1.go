package vectorpartition

import (
	"context"
	"errors"
	"sync"
	"time"
)

// IndexedWriteTokenV1 is the opaque monotonic source position returned by an
// update pipeline and accepted as a local fast-read floor.
type IndexedWriteTokenV1 struct{ Sequence uint64 }

// FastSearchOptionsV1 bounds the local immutable snapshot accepted by SearchFast.
type FastSearchOptionsV1 struct {
	MaxIndexAge       time.Duration
	MinIndexedThrough uint64
}

// PinSearchSnapshotOptionsV1 bounds one explicitly closed snapshot session.
type PinSearchSnapshotOptionsV1 struct {
	FastSearchOptionsV1
	MaxSessionAge time.Duration
}

// FastSearchEvidenceV1 identifies the immutable snapshot actually served.
type FastSearchEvidenceV1 struct {
	Generation                 GenerationIDV1
	IndexedThrough             uint64
	PublishedAt                time.Time
	IndexAge                   time.Duration
	TopologyDigest             string
	AuthorizationOverlayDigest string
}

// SearchSnapshotBackendV1 is the bounded backend lease used by OperationsV1.
// Its implementation remains topology-owned.
type SearchSnapshotBackendV1 interface {
	SearchVectorPartitionV1(context.Context, SearchRequestV1) (SearchResponseV1, error)
	Close() error
}

type serviceSearchSnapshotV1 struct {
	backend   SearchSnapshotBackendV1
	evidence  FastSearchEvidenceV1
	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func validateFastSearchOptionsV1(options FastSearchOptionsV1) error {
	if options.MaxIndexAge <= 0 {
		return invalidV1("positive max index age is required")
	}
	return nil
}

func validatePinSearchSnapshotOptionsV1(options PinSearchSnapshotOptionsV1) error {
	if err := validateFastSearchOptionsV1(options.FastSearchOptionsV1); err != nil {
		return err
	}
	if options.MaxSessionAge <= 0 {
		return invalidV1("positive maximum session age is required")
	}
	return nil
}

// SearchFast serves the newest complete local snapshot satisfying options. It
// never falls back to Search.
func (s *ServiceV1) SearchFast(ctx context.Context, request SearchRequestV1, options FastSearchOptionsV1) (SearchResponseV1, FastSearchEvidenceV1, error) {
	if err := validateSearchRequestV1(ctx, request); err != nil {
		return SearchResponseV1{}, FastSearchEvidenceV1{}, err
	}
	if err := validateFastSearchOptionsV1(options); err != nil {
		return SearchResponseV1{}, FastSearchEvidenceV1{}, err
	}
	requestCtx, cancel := ctx, func() {}
	ctxDeadline, hasCtxDeadline := ctx.Deadline()
	if !request.Deadline.IsZero() && (!hasCtxDeadline || request.Deadline.Before(ctxDeadline)) {
		requestCtx, cancel = context.WithDeadline(ctx, request.Deadline)
	}
	defer cancel()
	response, evidence, err := s.backend.SearchVectorPartitionFastV1(requestCtx, cloneSearchRequestV1(request), options)
	if err != nil {
		return SearchResponseV1{}, FastSearchEvidenceV1{}, classifyErrorV1(ctx, err)
	}
	if err := validateFastSearchResultV1(request, response, evidence, options); err != nil {
		return SearchResponseV1{}, FastSearchEvidenceV1{}, err
	}
	response.Neighbors = cloneNeighborsV1(response.Neighbors)
	return response, evidence, nil
}

func (s *ServiceV1) pinSearchSnapshotV1(ctx context.Context, options PinSearchSnapshotOptionsV1) (*serviceSearchSnapshotV1, error) {
	if err := validatePinSearchSnapshotOptionsV1(options); err != nil {
		return nil, err
	}
	backend, evidence, err := s.backend.PinVectorPartitionSearchSnapshotV1(ctx, options)
	if err != nil {
		return nil, classifyErrorV1(ctx, err)
	}
	if backend == nil || evidence.Generation.Index == "" || evidence.Generation.Generation == 0 || evidence.IndexedThrough < options.MinIndexedThrough || evidence.IndexAge < 0 || evidence.IndexAge > options.MaxIndexAge || evidence.PublishedAt.IsZero() || evidence.TopologyDigest == "" || evidence.AuthorizationOverlayDigest == "" {
		if backend != nil {
			_ = backend.Close()
		}
		return nil, &ErrorV1{Code: ErrorFailedV1, Err: errors.New("backend returned invalid pinned search evidence")}
	}
	return &serviceSearchSnapshotV1{backend: backend, evidence: evidence}, nil
}

func (s *serviceSearchSnapshotV1) Search(ctx context.Context, request SearchRequestV1) (SearchResponseV1, error) {
	if s == nil {
		return SearchResponseV1{}, &ErrorV1{Code: ErrorUnavailableV1, Err: errors.New("pinned search snapshot is closed")}
	}
	if err := validateSearchRequestV1(ctx, request); err != nil {
		return SearchResponseV1{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.backend == nil || s.closed {
		return SearchResponseV1{}, &ErrorV1{Code: ErrorUnavailableV1, Err: errors.New("pinned search snapshot is closed")}
	}
	if request.Generation != s.evidence.Generation {
		return SearchResponseV1{}, &ErrorV1{Code: ErrorGenerationMismatchV1, Err: errors.New("request generation differs from pinned snapshot")}
	}
	response, err := s.backend.SearchVectorPartitionV1(ctx, cloneSearchRequestV1(request))
	if err != nil {
		return SearchResponseV1{}, classifyErrorV1(ctx, err)
	}
	if err := validateSearchResponseV1(request, response); err != nil {
		return SearchResponseV1{}, err
	}
	response.Neighbors = cloneNeighborsV1(response.Neighbors)
	return response, nil
}

func (s *serviceSearchSnapshotV1) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		if s.backend != nil {
			s.closeErr = s.backend.Close()
		}
	})
	return s.closeErr
}

func validateFastSearchResultV1(request SearchRequestV1, response SearchResponseV1, evidence FastSearchEvidenceV1, options FastSearchOptionsV1) error {
	if err := validateSearchResponseV1(request, response); err != nil {
		return err
	}
	if evidence.Generation != request.Generation || evidence.IndexedThrough < options.MinIndexedThrough || evidence.PublishedAt.IsZero() || evidence.IndexAge < 0 || evidence.IndexAge > options.MaxIndexAge || evidence.TopologyDigest == "" || evidence.AuthorizationOverlayDigest == "" {
		return &ErrorV1{Code: ErrorFailedV1, Err: errors.New("backend returned invalid fast search evidence")}
	}
	return nil
}

func cloneNeighborsV1(neighbors []NeighborV1) []NeighborV1 {
	return append([]NeighborV1(nil), neighbors...)
}
