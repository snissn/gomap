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
	mu           sync.Mutex
	builder      *rootpublication.StableResourceSetBuilder
	dictionaries applyLeafDictionaryCapture
}

type applyLeafResourceLog struct {
	inner   LeafPageLog
	direct  LeafPageLog
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
	capture := &applyLeafResourceCapture{builder: rootpublication.NewStableResourceSetBuilder()}
	result, err := bindApplyLeafResourceLog(log, capture)
	if err != nil {
		capture.builder.Abandon()
		return nil, err
	}
	return result, nil
}

func bindApplyLeafResourceLog(log LeafPageLog, capture *applyLeafResourceCapture) (*applyLeafResourceLog, error) {
	result := &applyLeafResourceLog{inner: log, capture: capture}
	if factory, ok := log.(LeafPageLogApplyTokenProvider); ok {
		direct, supported, err := factory.LeafPageLogForApply(capture.acceptRaw, capture.acceptChild)
		if err != nil {
			return nil, err
		}
		if supported {
			if direct == nil {
				return nil, rootpublication.ErrResourceOwnership
			}
			result.direct = direct
		}
	}
	return result, nil
}

// acceptRaw consumes the producer's complete token inventory. All pointer,
// frontier and namespace checks precede ownership mutation. Add owns successes;
// this callback releases failures and the remaining inventory exactly once.
func (capture *applyLeafResourceCapture) acceptRaw(ptrs []page.ValuePtr, tokens []*rootpublication.StableResourceToken) error {
	remaining := tokens
	defer func() {
		for _, token := range remaining {
			token.Release()
		}
	}()
	if capture == nil {
		return rootpublication.ErrResourceOwnership
	}
	required := make(map[uint64]uint64, len(ptrs))
	for i, ptr := range ptrs {
		length := uint64(page.ValuePtrRecordLength(ptr))
		if ptr.FileID == 0 || length == 0 || ptr.Offset > ^uint64(0)-length {
			return fmt.Errorf("%w: direct leaf pointer frontier at %d", rootpublication.ErrUnresolvedResource, i)
		}
		end := ptr.Offset + length
		if end > required[uint64(ptr.FileID)] {
			required[uint64(ptr.FileID)] = end
		}
	}
	for _, token := range tokens {
		if token == nil || token.Kind() != rootpublication.ResourceOuterLeafLog || token.Reachability() != rootpublication.ReachabilityOuterLeafRawPointer {
			return fmt.Errorf("%w: direct leaf token kind/reachability", rootpublication.ErrResourceConflict)
		}
		end, ok := required[token.Generation()]
		if !ok {
			return fmt.Errorf("%w: direct leaf unreferenced generation", rootpublication.ErrResourceConflict)
		}
		if err := token.ValidateStableNamespace(); err != nil {
			return err
		}
		// A rotation can contribute an earlier active and a later closed token
		// for the same physical segment. As with builder coalescing, the maximum
		// immutable certificate must cover the append's last pointer, rather
		// than every earlier certificate individually covering that frontier.
		if token.Frontier().Bytes >= end {
			required[token.Generation()] = 0
		}
	}
	for generation, end := range required {
		if end != 0 {
			return fmt.Errorf("%w: direct leaf omitted/short generation %d", rootpublication.ErrUnresolvedResource, generation)
		}
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.builder == nil {
		return rootpublication.ErrResourceOwnership
	}
	for i, token := range tokens {
		if err := capture.builder.Add(token); err != nil {
			remaining = tokens[i:]
			return err
		}
		remaining = tokens[i+1:]
	}
	return nil
}

func (capture *applyLeafResourceCapture) acceptChild(resources *rootpublication.StableResourceSet) error {
	if capture == nil {
		return rootpublication.ErrResourceOwnership
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.builder == nil || resources == nil {
		return rootpublication.ErrResourceOwnership
	}
	return capture.builder.Merge(resources)
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
	if l.capture.builder == nil {
		resources.Release()
		return rootpublication.ErrResourceOwnership
	}
	if err := l.capture.builder.Merge(resources); err != nil {
		resources.Release()
		return err
	}
	return nil
}

func (l *applyLeafResourceLog) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	if l.direct != nil {
		return l.direct.AppendLeafPage(data)
	}
	ptr, resources, err := appendLeafPageWithDictionaryCapture(l.inner, &l.capture.dictionaries, data)
	err = l.accept([]page.LeafLogPtr{ptr}, resources, err)
	return ptr, err
}

func (l *applyLeafResourceLog) AppendLeafPages(data [][]byte) ([]page.LeafLogPtr, error) {
	if direct, ok := l.direct.(LeafPageBatchLog); ok {
		return direct.AppendLeafPages(data)
	}
	if _, ok := l.inner.(LeafPageStableBatchLog); ok {
		ptrs, resources, err := appendLeafPagesWithDictionaryCapture(l.inner, &l.capture.dictionaries, data)
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
	if _, ok := l.inner.(LeafPagePreparedStableLog); !ok {
		return false
	}
	if capability, ok := l.inner.(LeafPagePreparedAppendLog); ok {
		return capability.PreparedLeafPageAppends()
	}
	return true
}

func (l *applyLeafResourceLog) PreparedLeafPageBatchAppends() bool {
	_, batch := l.inner.(LeafPagePreparedStableBatchLog)
	_, refs := l.inner.(LeafPagePreparedChildRefStableBatchLog)
	if !batch && !refs {
		return false
	}
	if capability, ok := l.inner.(LeafPagePreparedBatchAppendLog); ok {
		return capability.PreparedLeafPageBatchAppends()
	}
	return true
}

func (l *applyLeafResourceLog) AppendPreparedLeafPage(data, payload []byte) (page.LeafLogPtr, error) {
	if direct, ok := l.direct.(LeafPagePreparedLog); ok {
		return direct.AppendPreparedLeafPage(data, payload)
	}
	if stable, ok := l.inner.(LeafPagePreparedStableLog); ok {
		ptr, resources, err := stable.AppendPreparedLeafPageWithStableResources(data, payload)
		err = l.accept([]page.LeafLogPtr{ptr}, resources, err)
		return ptr, err
	}
	return l.AppendLeafPage(data)
}

func (l *applyLeafResourceLog) AppendPreparedLeafPages(data, payloads [][]byte) ([]page.LeafLogPtr, error) {
	if direct, ok := l.direct.(LeafPagePreparedBatchLog); ok {
		return direct.AppendPreparedLeafPages(data, payloads)
	}
	if stable, ok := l.inner.(LeafPagePreparedStableBatchLog); ok {
		ptrs, resources, err := stable.AppendPreparedLeafPagesWithStableResources(data, payloads)
		err = l.accept(ptrs, resources, err)
		return ptrs, err
	}
	return l.AppendLeafPages(data)
}

func (l *applyLeafResourceLog) AppendPreparedLeafPageChildRefs(data, payloads [][]byte, refs []page.ChildRef) ([]page.ChildRef, error) {
	if direct, ok := l.direct.(LeafPagePreparedChildRefBatchLog); ok {
		return direct.AppendPreparedLeafPageChildRefs(data, payloads, refs)
	}
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
	source := l.inner
	if l.direct != nil {
		source = l.direct
	}
	p, ok := source.(LeafPageConcurrentAppendLog)
	return ok && p.ConcurrentLeafPageAppends()
}

func (l *applyLeafResourceLog) LeafPageLogLane(worker int) (LeafPageLog, bool) {
	source := l.inner
	if l.direct != nil {
		source = l.direct
	}
	provider, ok := source.(LeafPageLogLaneProvider)
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
	if l.direct != nil {
		return &applyLeafResourceLog{inner: lane, direct: lane, capture: l.capture}, true
	}
	result, err := bindApplyLeafResourceLog(lane, l.capture)
	if err != nil {
		return nil, false
	}
	return result, true
}

func (l *applyLeafResourceLog) LeafPageLogLaneAny(worker int) (any, bool) {
	return l.LeafPageLogLane(worker)
}

func (l *applyLeafResourceLog) Flush() error { return l.inner.Flush() }
func (l *applyLeafResourceLog) Sync() error  { return l.inner.Sync() }

func (l *applyLeafResourceLog) abandon() {
	if l != nil {
		l.capture.dictionaries.release()
	}
	if l != nil && l.capture.builder != nil {
		l.capture.builder.Abandon()
		l.capture.builder = nil
	}
}

func (l *applyLeafResourceLog) freeze() (*rootpublication.StableResourceSet, error) {
	if l == nil {
		return nil, nil
	}
	if l.capture.builder == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	defer l.capture.dictionaries.release()
	resources, err := l.capture.builder.Freeze()
	if err != nil {
		l.capture.builder.Abandon()
	}
	l.capture.builder = nil
	if err != nil {
		return nil, err
	}
	if l.capture.dictionaries.empty() {
		return resources, nil
	}
	// Known dictionary producers certify physical-generation fences inherited
	// by exact candidate views. Original snapshots end after composition, while
	// the retained physical representative protects maintenance through release.
	builder := rootpublication.NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err := l.capture.dictionaries.mergeInto(builder); err != nil {
		resources.Release()
		return nil, err
	}
	if err := builder.Merge(resources); err != nil {
		resources.Release()
		return nil, err
	}
	return builder.Freeze()
}
