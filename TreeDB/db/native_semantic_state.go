package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

var ErrNativeSemanticUnsupported = errors.New("treedb: command kind has no native semantic preparation")

// NativeSemanticState is a native immutable semantic/catalog root set paired
// with the SAME raw COW cut and exact backend Snapshot. Implementations belong
// to higher-level packages; db/caching never import their implementation.
// Retain and Release are nonallocating constant work under the existing cut
// latch. Release returns the preborn original record only at the final edge.
type NativeSemanticState interface {
	Retain()
	Release() *rootpublication.SemanticCleanup
	SparseProjections() []rootpublication.ParticipantProjection
	// PrepareBuild uses this captured complete logical basis and original
	// teardown lease. It never recaptures current metadata or acquires P.
	PrepareBuild(*RootPublicationBuildGroup, *Snapshot, *StableResourceCaptureLease) error
	// PrepareHandoff prepares a successor against latest logical state using
	// only captured-T touched domains. Distinct suffix-domain headers/sequence
	// survive, and the exact accepted snapshot remains the physical basis.
	// All fallible allocation precedes the logical installation latch.
	PrepareHandoff(latest NativeSemanticState, accepted *Snapshot) (NativeSemanticState, error)
}

// NativePreparedPublication is admitted and owned before canonical assignment.
// The native producer holds Basis through the synchronous publication and
// transfers State plus sparse original Custody only on installation. Entries
// use the ordinary canonical raw preparation path; semantic-only changes may
// supply an empty slice. No whole EngineVersion is retained by queue custody.
// NativeSemanticCustody owns only the exact sparse native plans/assets. Last
// release detaches one preborn original cleanup record; physical work and errors
// belong exclusively to that record on the existing cleanup worker.
type NativeSemanticCustody interface {
	ReleaseNativeCustody() *rootpublication.SemanticCleanup
}

type NativePreparedPublication struct {
	Basis                       NativeSemanticState
	State                       NativeSemanticState
	Entries                     []batch.Entry
	Projections                 []rootpublication.ParticipantProjection
	Custody                     NativeSemanticCustody
	AdmittedBytes, CleanupBytes uint64
}
