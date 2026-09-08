package collections

import "github.com/snissn/gomap/TreeDB/internal/workstats"

// Explicit internal limits bound epoch-cumulative attempted candidate output and
// appender opens, not existing disk inventory or encoder scratch. An open can
// leave an empty segment even when payload admission fails.
type typedGraphFoldAssetLimits struct {
	Bytes            int64
	AppenderAttempts int64
}

type typedGraphFoldAssetAdmission struct {
	coord *collectionSchemaCoordinator
}

func (c *collectionSchemaCoordinator) bindTypedGraphFoldAssetAdmission(limits typedGraphFoldAssetLimits) (*typedGraphFoldAssetAdmission, error) {
	if limits.Bytes <= 0 || limits.AppenderAttempts <= 0 {
		return nil, ErrVectorIndexSnapshotMismatch
	}
	c.typedPublicationDebtMu.Lock()
	defer c.typedPublicationDebtMu.Unlock()
	if c.typedGraphCandidateLimits == (typedGraphFoldAssetLimits{}) {
		c.typedGraphCandidateLimits = limits
	} else if c.typedGraphCandidateLimits != limits {
		return nil, ErrConcurrentMutation
	}
	return &typedGraphFoldAssetAdmission{coord: c}, nil
}

func (a *typedGraphFoldAssetAdmission) charge(bytes, opens int64) error {
	if a == nil {
		return nil // Existing ordinary producers have no candidate admission.
	}
	c := a.coord
	c.typedPublicationDebtMu.Lock()
	defer c.typedPublicationDebtMu.Unlock()
	if bytes < 0 || opens < 0 || bytes > c.typedGraphCandidateLimits.Bytes-c.typedGraphCandidateBytes || opens > c.typedGraphCandidateLimits.AppenderAttempts-c.typedGraphCandidateAttempts {
		return errTypedGraphOverlayFoldNeeded
	}
	// No within-epoch refund: partial writes, canceled/stale candidates and
	// handle release do not establish the explicit maintenance renewal boundary.
	workstats.Fold.CandidateBytesCharged.Add(uint64(bytes))
	workstats.Fold.AppenderAttemptsCharged.Add(uint64(opens))
	c.typedGraphCandidateBytes += bytes
	c.typedGraphCandidateAttempts += opens
	return nil
}
