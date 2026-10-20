package rootpublication

import "github.com/snissn/gomap/TreeDB/internal/retainedalloc"

// Independent accounting receipt returned only after original physical disposal.
type primaryConstructionReleaseState struct {
	owner  *retainedalloc.Owner
	charge uint64
}
