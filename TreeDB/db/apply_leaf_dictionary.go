package db

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
)

// A definition is writer-owned immutable storage, shared by cloned lanes.
// Reconfiguration installs a new definition even when the logical ID is reused.
type rewriteLeafDictionaryDefinition struct {
	id    uint64
	bytes []byte
}

type applyLeafDictionaryKey struct {
	definition *rewriteLeafDictionaryDefinition
	provider   StableDictionaryResourceProvider
}

// This owner lives for one private COW Apply chain. Each original closure owns
// its provider's physical pins and snapshot lease until it transfers once into
// the final Apply builder. Private appends need no duplicate dictionary closure.
type applyLeafDictionaryCapture struct {
	mu        sync.Mutex
	resources map[applyLeafDictionaryKey]*rootpublication.StableResourceSet
}

// A successful nil result means this private scope owns the validated closure;
// fallback captures return their closure for the ordinary append builder.
func (scope *applyLeafDictionaryCapture) capture(ctx context.Context, writer *rewriteWriter, provider StableDictionaryResourceProvider, dictID uint64, dictionary []byte) (*rootpublication.StableResourceSet, error) {
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var definition *rewriteLeafDictionaryDefinition
	if writer != nil {
		definition = writer.leafDictionaryDefinition
	}
	// Only the known immutable encoder definition and an identifiable provider
	// can reuse authority. Arbitrary definitions/providers retain full validation.
	if scope == nil || definition == nil || provider == nil || !reflect.ValueOf(provider).Comparable() || definition.id != dictID || len(dictionary) == 0 || len(definition.bytes) != len(dictionary) || &definition.bytes[0] != &dictionary[0] {
		return captureStableDictionaryResources(ctx, provider, dictID, dictionary)
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	key := applyLeafDictionaryKey{definition: definition, provider: provider}
	resources := scope.resources[key]
	if resources == nil {
		var err error
		resources, err = captureStableDictionaryResources(ctx, provider, dictID, dictionary)
		if err != nil {
			return nil, err
		}
		if scope.resources == nil {
			scope.resources = make(map[applyLeafDictionaryKey]*rootpublication.StableResourceSet)
		}
		scope.resources[key] = resources
	}
	return nil, nil
}

func (scope *applyLeafDictionaryCapture) mergeInto(builder *rootpublication.StableResourceSetBuilder) error {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	for key, resources := range scope.resources {
		if err := builder.Merge(resources); err != nil {
			return err
		}
		delete(scope.resources, key)
	}
	return nil
}

func (scope *applyLeafDictionaryCapture) empty() bool {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return len(scope.resources) == 0
}

func (scope *applyLeafDictionaryCapture) release() {
	scope.mu.Lock()
	defer scope.mu.Unlock()
	for _, resources := range scope.resources {
		resources.Release()
	}
	scope.resources = nil
}

// The private forwarding view preserves the existing hint/lane/replay append
// boundaries. It does not replace a producer's provider or change shared state.
type dictionaryCaptureLeafLog struct {
	inner LeafPageLog
	scope *applyLeafDictionaryCapture
}

func (l dictionaryCaptureLeafLog) AppendLeafPage(data []byte) (page.LeafLogPtr, error) {
	return l.inner.AppendLeafPage(data)
}
func (l dictionaryCaptureLeafLog) Flush() error { return l.inner.Flush() }
func (l dictionaryCaptureLeafLog) Sync() error  { return l.inner.Sync() }
func (l dictionaryCaptureLeafLog) AppendLeafPageWithStableResources(data []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return appendLeafPageWithDictionaryCapture(l.inner, l.scope, data)
}
func (l dictionaryCaptureLeafLog) AppendLeafPagesWithStableResources(data [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	return appendLeafPagesWithDictionaryCapture(l.inner, l.scope, data)
}

func appendLeafPageWithDictionaryCapture(log LeafPageLog, scope *applyLeafDictionaryCapture, data []byte) (page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	switch log := log.(type) {
	case *rewriteWriter:
		return log.appendLeafPageWithDictionaryCapture(data, scope)
	case *leafPageLogWithRecordLengthHints:
		if log == nil || log.inner == nil {
			return log.AppendLeafPageWithStableResources(data)
		}
		if _, stable := log.inner.(LeafPageStableLog); scope == nil || !stable {
			return log.AppendLeafPageWithStableResources(data)
		}
		view := *log
		view.inner = dictionaryCaptureLeafLog{inner: log.inner, scope: scope}
		return view.AppendLeafPageWithStableResources(data)
	case *leafPageLogLaneGroup:
		return log.appendLeafPageWithDictionaryCaptureAt(0, data, scope)
	case *leafPageLogLaneHandle:
		if log == nil || log.group == nil {
			return log.AppendLeafPageWithStableResources(data)
		}
		return log.group.appendLeafPageWithDictionaryCaptureAt(log.index, data, scope)
	case replayInlineLeafPageLog:
		ptrs, resources, err := log.appender.appendLeafPagesWithDictionaryCapture([][]byte{data}, scope)
		if err != nil {
			return page.LeafLogPtr{}, nil, err
		}
		return ptrs[0], resources, nil
	default:
		if stable, ok := log.(LeafPageStableLog); ok {
			return stable.AppendLeafPageWithStableResources(data)
		}
		return page.LeafLogPtr{}, nil, fmt.Errorf("%w: leaf page log lacks stable append", rootpublication.ErrUnresolvedResource)
	}
}

func appendLeafPagesWithDictionaryCapture(log LeafPageLog, scope *applyLeafDictionaryCapture, data [][]byte) ([]page.LeafLogPtr, *rootpublication.StableResourceSet, error) {
	switch log := log.(type) {
	case *rewriteWriter:
		return log.appendLeafPagesWithDictionaryCapture(data, scope)
	case *leafPageLogWithRecordLengthHints:
		if log == nil || log.inner == nil {
			return log.AppendLeafPagesWithStableResources(data)
		}
		if _, stable := log.inner.(LeafPageStableBatchLog); scope == nil || !stable {
			return log.AppendLeafPagesWithStableResources(data)
		}
		view := *log
		view.inner = dictionaryCaptureLeafLog{inner: log.inner, scope: scope}
		return view.AppendLeafPagesWithStableResources(data)
	case *leafPageLogLaneGroup:
		return log.appendLeafPagesWithDictionaryCaptureAt(0, data, scope)
	case *leafPageLogLaneHandle:
		if log == nil || log.group == nil {
			return log.AppendLeafPagesWithStableResources(data)
		}
		return log.group.appendLeafPagesWithDictionaryCaptureAt(log.index, data, scope)
	case replayInlineLeafPageLog:
		return log.appender.appendLeafPagesWithDictionaryCapture(data, scope)
	default:
		if stable, ok := log.(LeafPageStableBatchLog); ok {
			return stable.AppendLeafPagesWithStableResources(data)
		}
		return nil, nil, fmt.Errorf("%w: leaf page log lacks stable batch append", rootpublication.ErrUnresolvedResource)
	}
}
