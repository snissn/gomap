package collections

import (
	"context"
	"fmt"
)

// NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2 prepares local
// search metadata for groupID from an already admitted generation manifest.
// This selection grants no lifecycle or placement authority and must not be
// treated as a complete global search plan. Callers still validate the exact
// generation and placement through the existing admission path.
//
// The first-red walking seam delegates to the existing generation constructor;
// owner-local loading is intentionally not implemented in this candidate.
func NewVectorPartitionGenerationOwnerSearchOpenPlanWithContextV2(ctx context.Context, manifest VectorPartitionManifestV1, groupID string) (*VectorPartitionGenerationSearchOpenPlanV1, error) {
	if groupID == "" {
		return nil, fmt.Errorf("%w: missing owner group", ErrVectorPartitionSearchUnavailable)
	}
	return NewVectorPartitionGenerationSearchOpenPlanWithContextV1(ctx, manifest)
}
