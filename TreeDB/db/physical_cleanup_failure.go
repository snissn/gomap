package db

import "errors"

// Shared physical cleanup failures are embedded before producer admission.
type physicalCleanupFailureV6 struct{ causes [3]error }

func (failure *physicalCleanupFailureV6) Error() string   { return "physical cleanup failed" }
func (failure *physicalCleanupFailureV6) Unwrap() []error { return failure.causes[:] }

// Independent bounded physical helpers preserve their capability refusal.
var ErrNativeMVCCSnapshotWork = errors.New("native MVCC snapshot requires original work-bearing capture/cleanup")
