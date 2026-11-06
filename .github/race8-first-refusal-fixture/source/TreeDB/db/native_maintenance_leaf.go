package db

import (
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

// NativeMaintenanceLeafReader is borrowed only during one cache-owned physical
// basis refresh. Its private implementation binds the exact paired Snapshots;
// callers cannot replace a snapshot or retain dictionary/provider authority.
// shape comes from the original validated inspector, not a fabricated header.
type NativeMaintenanceLeafReader interface {
	MatchesNativeMaintenanceSnapshots(before, after *Snapshot) bool
	ReadNativeMaintenanceDictionaryLeaf(snapshot *Snapshot, ptr page.LeafLogPtr, shape valuelog.COWRecordShape, dst []byte, call valuelog.COWBasisCall) ([]byte, error)
}

// NativeSemanticMaintenanceBasisRefresher adds the synchronous private reader to
// the original equivalent-basis protocol. It does not create a capture/root cut.
type NativeSemanticMaintenanceBasisRefresher interface {
	PrepareBasisRefreshWithMaintenanceLeaves(before, after *Snapshot, leaves NativeMaintenanceLeafReader, call valuelog.COWBasisCall) (NativeSemanticState, error)
}
