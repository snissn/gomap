package db

import (
	"fmt"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// applyLeafResourceCapture is private to one COW Apply chain. Stable append APIs
// supply raw identity/frontier and dictionary/template authority; reading bytes
// or scanning the candidate cannot replace those producer proofs. Lane wrappers
// share this builder. Aborted/conflicting applies abandon it; successful applies
// transfer the frozen set through the existing finalize ownership boundary.
type applyLeafResourceCapture struct {
	mu      sync.Mutex
	builder *rootpublication.StableResourceSetBuilder
}

type applyLeafResourceLog struct {
	inner   LeafPageLog
	capture *applyLeafResourceCapture
}

func newApplyLeafResourceLog(appender bulk.LeafPageAppender) (*applyLeafResourceLog, error) {
	log, ok := appender.(LeafPageLog)
	if !ok {
		return nil, fmt.Errorf("%w: outer-leaf producer lacks flush authority", rootpublication.ErrUnresolvedResource)
	}
	if _, ok := log.(LeafPageStableLog); !ok {
		return nil, fmt.Errorf("%w: outer-leaf producer lacks stable append authority", rootpublication.ErrUnresolvedResource)
	}
	return &applyLeafResourceLog{inner: log, capture: &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}}, nil
}

func (l *applyLeafResourceLog) accept(ptrs []page.LeafLogPtr, resources *rootpublication.StableResourceSet, err error) error {
	if err == nil {
		err = validateLeafPageStableResources(ptrs, resources)
	}
	if err != nil {
		resources.Release()
		return err
	}
	if resources == nil {
		return nil
	}
	l.capture.mu.Lock()
	defer l.capture.mu.Unlock()
	if err := l.capture.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	return nil
}

func (l *applyLeafResourceLog) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	ptr, resources, err := l.inner.(LeafPageStableLog).AppendLeafPageWithStableResources(data)
	err = l.accept([]page.LeafLogPtr{ptr}, resources, err)
	return ptr, err
}

func (l *applyLeafResourceLog) AppendLeafPages(data [][]byte) ([]page.LeafLogPtr, error) {
	if stable, ok := l.inner.(LeafPageStableBatchLog); ok {
		ptrs, resources, err := stable.AppendLeafPagesWithStableResources(data)
		err = l.accept(ptrs, resources, err)
		return ptrs, err
	}
	ptrs := make([]page.LeafLogPtr, len(data))
	for i := range data {
		ptr, err := l.AppendLeafPage(data[i])
		if err != nil {
			return nil, err
		}
		ptrs[i] = ptr
	}
	return ptrs, nil
}

func (l *applyLeafResourceLog) PreparedLeafPageAppends() bool {
	_, ok := l.inner.(LeafPagePreparedStableLog)
	return ok
}

func (l *applyLeafResourceLog) PreparedLeafPageBatchAppends() bool {
	_, batch := l.inner.(LeafPagePreparedStableBatchLog)
	_, refs := l.inner.(LeafPagePreparedChildRefStableBatchLog)
	return batch || refs
}

func (l *applyLeafResourceLog) AppendPreparedLeafPage(data, payload []byte) (page.LeafLogPtr, error) {
	if stable, ok := l.inner.(LeafPagePreparedStableLog); ok {
		ptr, resources, err := stable.AppendPreparedLeafPageWithStableResources(data, payload)
		err = l.accept([]page.LeafLogPtr{ptr}, resources, err)
		return ptr, err
	}
	return l.AppendLeafPage(data)
}

func (l *applyLeafResourceLog) AppendPreparedLeafPages(data, payloads [][]byte) ([]page.LeafLogPtr, error) {
	if stable, ok := l.inner.(LeafPagePreparedStableBatchLog); ok {
		ptrs, resources, err := stable.AppendPreparedLeafPagesWithStableResources(data, payloads)
		err = l.accept(ptrs, resources, err)
		return ptrs, err
	}
	return l.AppendLeafPages(data)
}

func (l *applyLeafResourceLog) AppendPreparedLeafPageChildRefs(data, payloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	if stable, ok := l.inner.(LeafPagePreparedChildRefStableBatchLog); ok {
		out, resources, err := stable.AppendPreparedLeafPageChildRefsWithStableResources(data, payloads, refs)
		ptrs := make([]page.LeafLogPtr, len(out))
		for i := range out {
			if !out[i].IsLeafLog() {
				resources.Release()
				return nil, fmt.Errorf("%w: stable leaf append returned pager ref", rootpublication.ErrResourceConflict)
			}
			ptrs[i] = out[i].Log
		}
		err = l.accept(ptrs, resources, err)
		return out, err
	}
	ptrs, err := l.AppendPreparedLeafPages(data, payloads)
	if err != nil {
		return nil, err
	}
	refs = refs[:0]
	for _, ptr := range ptrs {
		refs = append(refs, page.LeafLogChildRef(ptr))
	}
	return refs, nil
}

func (l *applyLeafResourceLog) ConcurrentLeafPageAppends() bool {
	p, ok := l.inner.(LeafPageConcurrentAppendLog)
	return ok && p.ConcurrentLeafPageAppends()
}

func (l *applyLeafResourceLog) LeafPageLogLane(worker int) (LeafPageLog, bool) {
	provider, ok := l.inner.(LeafPageLogLaneProvider)
	if !ok {
		return nil, false
	}
	lane, ok := provider.LeafPageLogLane(worker)
	if !ok {
		return nil, false
	}
	if _, stable := lane.(LeafPageStableLog); !stable {
		return nil, false
	}
	return &applyLeafResourceLog{inner: lane, capture: l.capture}, true
}

func (l *applyLeafResourceLog) Flush() error { return l.inner.Flush() }
func (l *applyLeafResourceLog) Sync() error  { return l.inner.Sync() }

func (l *applyLeafResourceLog) abandon() {
	if l != nil && l.capture.builder != nil {
		l.capture.builder.Abandon()
		l.capture.builder = nil
	}
}

func (l *applyLeafResourceLog) freeze() (*rootpublication.StableResourceSet, error) {
	if l == nil {
		return nil, nil
	}
	resources, err := l.capture.builder.Freeze()
	if err != nil {
		l.capture.builder.Abandon()
	}
	l.capture.builder = nil
	return resources, err
}
