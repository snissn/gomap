package documentservice

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
)

// FetchTypedDocuments returns owned full documents from one ordinary read view.
// It requires matching captured schema generation, but no graph admission.
func (s *Service) FetchTypedDocuments(ctx context.Context, index string, expectedGeneration uint64, ids [][]byte) (out collections.DocumentFetchResponse, err error) {
	if expectedGeneration == 0 {
		return out, serviceError(CodeInvalidRequest, "typed fetch requires expected generation")
	}
	col, _, err := s.openIndex(ctx, index, 0)
	if err != nil {
		return out, err
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		return out, err
	}
	defer func() { err = errors.Join(err, view.Close()) }()
	return fetchTypedDocumentsFromView(ctx, view, expectedGeneration, ids)
}

func fetchTypedDocumentsFromView(ctx context.Context, view *collections.CollectionReadView, expectedGeneration uint64, ids [][]byte) (collections.DocumentFetchResponse, error) {
	if err := ctxErr(ctx); err != nil {
		return collections.DocumentFetchResponse{}, err
	}
	meta, err := view.Meta()
	if err != nil {
		return collections.DocumentFetchResponse{}, err
	}
	info, err := indexInfoFromMeta(meta)
	if err != nil {
		return collections.DocumentFetchResponse{}, err
	}
	if !info.TypedInput {
		return collections.DocumentFetchResponse{}, serviceError(CodeUnsupported, "typed fetch requires declared typed input")
	}
	if expectedGeneration == 0 || info.Generation != expectedGeneration {
		return collections.DocumentFetchResponse{}, serviceErrorf(CodeIndexStale, "captured generation %d does not match expected_generation %d", info.Generation, expectedGeneration)
	}
	return view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{Context: ctx})
}
