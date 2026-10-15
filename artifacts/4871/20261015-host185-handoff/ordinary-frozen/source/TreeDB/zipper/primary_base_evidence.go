package zipper

import (
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/pager"
)

// unchangedPrimaryBaseEvidence is produced only by a successful sealed PRIMARY
// apply which did not materialize its base. Exact consumed component pointers
// are already represented by OldPointerRefs; no raw outer-leaf base disappeared.
// The caller must consume this borrowed evidence while its source root and the
// same output construction are still owned. It is not a persistent certificate.
type unchangedPrimaryBaseEvidence struct {
	pager         *pager.Pager
	arena         *primaryarena.Arena
	input, output uint64
	constructor   primaryarena.ReadRootConstructionV6
}

// PreservesMaterializedBaseV6 validates the private producer receipt against the
// exact apply and index incarnation. A retired/scrubbed construction invalidates
// it; a caller cannot assert this property by assigning a boolean.
func (z *Zipper) PreservesMaterializedBaseV6(result *ApplyResult, input, output uint64) bool {
	if z == nil || result == nil || !result.OldPointerRefsCollected || result.RootID != output {
		return false
	}
	e := &result.unchangedPrimaryBase
	return e.pager == z.pager && e.arena != nil && e.arena == z.primaryArena &&
		e.input == input && e.output == output && e.constructor != nil &&
		e.constructor.PrimaryConstructionReferenceV6().PageID == output
}
