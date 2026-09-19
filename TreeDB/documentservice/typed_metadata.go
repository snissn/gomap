package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// UpdateMetadataByID applies a metadata-only update without accepting or
// reconstructing vector/content input.
func (s *Service) UpdateMetadataByID(ctx context.Context, index string, req UpdateMetadataByIDRequest) (UpdateMetadataByIDResponse, error) {
	if s == nil {
		return UpdateMetadataByIDResponse{}, serviceError(CodeIndexUnavailable, "document service has no collection manager")
	}
	if req.ExpectedGeneration == 0 {
		return UpdateMetadataByIDResponse{}, serviceError(CodeInvalidRequest, "metadata update requires expected generation")
	}
	if err := validateMetadataUpdateShape(req); err != nil {
		return UpdateMetadataByIDResponse{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	col, info, err := s.openIndex(ctx, index, req.ExpectedGeneration)
	if err != nil {
		return UpdateMetadataByIDResponse{}, err
	}
	if !info.TypedInput {
		return UpdateMetadataByIDResponse{}, serviceError(CodeUnsupported, "metadata update requires declared typed input")
	}
	if err := validateMetadataUpdateSchema(info, req); err != nil {
		return UpdateMetadataByIDResponse{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return UpdateMetadataByIDResponse{}, err
	}
	ids := make([][]byte, len(req.IDs))
	for i := range req.IDs {
		ids[i] = []byte(req.IDs[i])
	}
	result, err := col.UpdateTypedMetadataByID(ids, req.Set, req.Unset, req.ExpectedGeneration)
	if err != nil {
		return UpdateMetadataByIDResponse{}, mapTypedMetadataUpdateError(err)
	}
	if result.MatchedCount < 0 || result.MatchedCount > len(ids) || result.ModifiedCount < 0 || result.ModifiedCount > result.MatchedCount {
		return UpdateMetadataByIDResponse{}, serviceError(CodeInternal, "metadata update returned inconsistent counts")
	}
	return UpdateMetadataByIDResponse{Index: info, MatchedCount: result.MatchedCount, ModifiedCount: result.ModifiedCount}, nil
}

func validateMetadataUpdateShape(req UpdateMetadataByIDRequest) error {
	if len(req.IDs) == 0 {
		return serviceError(CodeInvalidRequest, "metadata update requires at least one document ID")
	}
	if req.Set == nil || req.Unset == nil {
		return serviceError(CodeInvalidRequest, "metadata update requires set and unset")
	}
	if _, err := validateDocumentIDs(req.IDs); err != nil {
		return err
	}
	paths := make([]string, 0, len(req.Set)+len(req.Unset))
	for path := range req.Set {
		paths = append(paths, path)
	}
	paths = append(paths, req.Unset...)
	for i, path := range paths {
		if !validMetadataUpdatePath(path) {
			return serviceErrorf(CodeInvalidRequest, "metadata update path %q must be a dotted meta.* path", path)
		}
		for j := 0; j < i; j++ {
			if metadataPathsOverlap(paths[j], path) {
				return serviceErrorf(CodeInvalidRequest, "metadata update paths %q and %q overlap", paths[j], path)
			}
		}
	}
	if _, err := json.Marshal(req.Set); err != nil {
		return wrapServiceError(CodeInvalidRequest, "metadata update set values must be JSON-compatible", err)
	}
	return nil
}

func validMetadataUpdatePath(path string) bool {
	if !utf8.ValidString(path) || !strings.HasPrefix(path, "meta.") {
		return false
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "meta."), ".") {
		if part == "" {
			return false
		}
	}
	return true
}

func metadataPathsOverlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+".") || strings.HasPrefix(b, a+".")
}

func validateMetadataUpdateSchema(info IndexInfo, req UpdateMetadataByIDRequest) error {
	declared := make(map[string]struct{}, len(info.ScalarFields))
	for _, field := range info.ScalarFields {
		declared[field.Field] = struct{}{}
	}
	for path, value := range req.Set {
		if _, ok := declared[path]; ok {
			if _, ok := value.(string); !ok {
				return serviceErrorf(CodeInvalidRequest, "declared metadata field %q requires a string value", path)
			}
		}
	}
	for _, path := range req.Unset {
		if _, ok := declared[path]; ok {
			return serviceErrorf(CodeInvalidRequest, "required declared metadata field %q cannot be unset", path)
		}
	}
	return nil
}

func mapTypedMetadataUpdateError(err error) error {
	switch {
	case errors.Is(err, collections.ErrCommitAmbiguous):
		return wrapServiceError(CodeCommitAmbiguous, "metadata update commit is ambiguous; reopen before deciding whether to retry", err)
	case errors.Is(err, collections.ErrRecoveryRequired), errors.Is(err, backenddb.ErrRecoveryRequired):
		return wrapServiceError(CodeRecoveryRequired, "metadata update requires database recovery", err)
	case errors.Is(err, collections.ErrDurabilityUnavailable):
		return wrapServiceError(CodeIndexUnavailable, "metadata update durability is unavailable", err)
	case errors.Is(err, collections.ErrConcurrentMutation):
		return wrapServiceError(CodeConflict, "metadata update conflicted with a concurrent mutation", err)
	case errors.Is(err, collections.ErrHybridSearchStaleIndex):
		return wrapServiceError(CodeIndexStale, "metadata update schema generation changed", err)
	case errors.Is(err, collections.ErrHybridSearchUnsupported):
		return wrapServiceError(CodeUnsupported, "metadata update is unsupported for this index", err)
	case errors.Is(err, collections.ErrDuplicateDocumentID):
		return wrapServiceError(CodeInvalidRequest, "metadata update contains duplicate document IDs", err)
	case errors.Is(err, collections.ErrTypedMetadataInvalid):
		return wrapServiceError(CodeInvalidRequest, "metadata update request is invalid", err)
	default:
		return wrapServiceError(CodeInternal, "metadata update failed", err)
	}
}
