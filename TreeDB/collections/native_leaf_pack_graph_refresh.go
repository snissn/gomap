package collections

import (
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
)

// This synchronous plan borrows one immutable collection. It neither retains a
// cut nor consumes a source. boundary is the unchanged tail AFTER the oldest
// graph marker that needs rebinding, not a captured-prefix removal boundary.
type nativeLeafPackGraphRefresh struct {
	primary               *nativeDomainState
	boundary              *nativeTypedDelta
	beforeRoot, afterRoot uint64
}

func (g nativeLeafPackGraphRefresh) touches(d *nativeDomainState) bool {
	return g.primary != nil && d == g.primary
}

func (s *nativeBasisRefreshScratch) leafPackGraphRefresh(c *nativeCollectionState, qualified bool) (nativeLeafPackGraphRefresh, error) {
	if !qualified {
		return nativeLeafPackGraphRefresh{}, nil
	}
	primary, ok := c.domains.Get("primary")
	if !ok || primary.typed == nil {
		return nativeLeafPackGraphRefresh{}, nil
	}
	manifest, ok := c.domains.Get("column/manifest")
	if !ok {
		return nativeLeafPackGraphRefresh{}, nil
	}
	root, err := s.root(c, manifest)
	if err != nil {
		return nativeLeafPackGraphRefresh{}, err
	}
	return prepareNativeLeafPackGraphRefresh(c, primary, manifest, root)
}

func prepareNativeLeafPackGraphRefresh(c *nativeCollectionState, primary, manifest *nativeDomainState, afterRoot uint64) (g nativeLeafPackGraphRefresh, err error) {
	if manifest.baseRoot == 0 || afterRoot == manifest.catalogRoot {
		return g, nil // a full replacement must keep its zero physical read base
	}
	if manifest.baseRoot != manifest.catalogRoot || afterRoot == 0 {
		return g, backenddb.ErrRootPublicationBasisMismatch
	}
	for d := primary.typed; d != nil; d = d.previous {
		if d.data == nil || d.data.graph == nil || !d.data.graph.ordinary {
			continue
		}
		recipe := d.data.graph
		if d.appliedLSN == 0 || d.graphSourceRoot != manifest.baseRoot || recipe.schema == nil || recipe.schema.meta == nil || !nativeOrdinaryGraphPendingSourceMetaEqual(*c.schema.meta, *recipe.schema.meta) {
			return nativeLeafPackGraphRefresh{}, backenddb.ErrRootPublicationBasisMismatch
		}
		g.primary, g.boundary = primary, d.previous
	}
	g.beforeRoot, g.afterRoot = manifest.baseRoot, afterRoot
	return g, nil
}

// Clone only the necessary header prefix, keeping every original row, recipe,
// LSN, ordinal, capacity and older source. Maintenance admits each header before
// allocation. A partial prefix is returned to the original private domain on
// failure and uses its existing off-gate release/refund path.
func (g nativeLeafPackGraphRefresh) clone(current *nativeTypedDelta, admit cowbtree.OwnedMapAdmit) (*nativeTypedDelta, error) {
	head, err := cloneNativeTypedSuffix(current, g.boundary, admit)
	if err != nil {
		return head, err
	}
	var tail *nativeTypedDelta
	for d := head; d != nil; d = d.previous {
		if d.data != nil && d.data.graph != nil && d.data.graph.ordinary {
			if d.graphSourceRoot != g.beforeRoot {
				return head, backenddb.ErrRootPublicationBasisMismatch
			}
			d.graphSourceRoot = g.afterRoot
		}
		tail = d
	}
	if tail == nil {
		return head, backenddb.ErrRootPublicationBasisMismatch
	}
	g.boundary.retain()
	tail.previous = g.boundary
	return head, nil
}
