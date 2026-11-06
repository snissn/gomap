package db

import "github.com/snissn/gomap/TreeDB/internal/valuelog"

// NativeSemanticBasisRefusalDiagnostic is an optional diagnostic-only sibling.
// The exact original invocation supplies all labels explicitly; implementations
// must preserve original return/ownership semantics and retain no call context.
type NativeSemanticBasisRefusalDiagnostic interface {
	PrepareBasisRefreshDiagnostic(before, after *Snapshot, call valuelog.COWBasisCall) (NativeSemanticState, error)
}
