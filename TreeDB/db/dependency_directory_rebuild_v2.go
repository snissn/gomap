package db

import (
	"context"
	"errors"
	"iter"

	"github.com/snissn/gomap/TreeDB/internal/bulk"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// A pull adapter lets the existing bulk builder consume the validated stream
// without buffering a logical corpus or starting a background worker.
type dependencyDirectoryRecordIteratorV2 struct {
	next       func() ([2][]byte, bool)
	stop       func()
	key, value []byte
	valid      bool
	err        error
}

func newDependencyDirectoryRecordIteratorV2(resources *rootpublication.StableResourceSet) *dependencyDirectoryRecordIteratorV2 {
	return newDependencyDirectoryStreamIteratorV2(func(visit func([]byte, []byte) error) error {
		return rootpublication.WalkDependencyDirectoryRecordsV2(resources, visit)
	})
}

func newDependencyDirectoryStreamIteratorV2(walk func(func([]byte, []byte) error) error) *dependencyDirectoryRecordIteratorV2 {
	it := &dependencyDirectoryRecordIteratorV2{}
	stopped := errors.New("dependency directory rebuild stopped")
	it.next, it.stop = iter.Pull(func(yield func([2][]byte) bool) {
		err := walk(func(key, value []byte) error {
			if !yield([2][]byte{key, value}) {
				return stopped
			}
			return nil
		})
		if !errors.Is(err, stopped) {
			it.err = err
		}
	})
	it.Next()
	return it
}

func (it *dependencyDirectoryRecordIteratorV2) Next() {
	pair, valid := it.next()
	it.key, it.value, it.valid = pair[0], pair[1], valid
}
func (it *dependencyDirectoryRecordIteratorV2) Valid() bool  { return it.valid && it.err == nil }
func (it *dependencyDirectoryRecordIteratorV2) Error() error { return it.err }
func (it *dependencyDirectoryRecordIteratorV2) Close() error {
	it.stop()
	it.valid = false
	return it.err
}
func (it *dependencyDirectoryRecordIteratorV2) Domain() ([]byte, []byte) { return nil, nil }
func (it *dependencyDirectoryRecordIteratorV2) Seek([]byte) {
	it.err = errors.New("dependency directory rebuild is forward-only")
}
func (it *dependencyDirectoryRecordIteratorV2) UnsafeKey() []byte   { return it.key }
func (it *dependencyDirectoryRecordIteratorV2) UnsafeValue() []byte { return it.value }
func (it *dependencyDirectoryRecordIteratorV2) Key() []byte         { return it.key }
func (it *dependencyDirectoryRecordIteratorV2) Value() []byte       { return it.value }
func (it *dependencyDirectoryRecordIteratorV2) KeyCopy(dst []byte) []byte {
	return append(dst[:0], it.key...)
}
func (it *dependencyDirectoryRecordIteratorV2) ValueCopy(dst []byte) []byte {
	return append(dst[:0], it.value...)
}
func (it *dependencyDirectoryRecordIteratorV2) IsDeleted() bool { return false }
func (it *dependencyDirectoryRecordIteratorV2) UnsafeEntry() ([]byte, page.ValuePtr, byte) {
	return it.value, page.ValuePtr{}, node.FlagInline
}

func rebuildDependencyDirectoryV2(p *pager.Pager, allocator bulk.Allocator, resources *rootpublication.StableResourceSet) (rootpublication.DependencyDirectoryRefV2, error) {
	base, err := resources.DependencyDirectoryBaseV2()
	if err != nil || base == nil {
		return rootpublication.DependencyDirectoryRefV2{}, err
	}
	var reference rootpublication.DependencyDirectoryRefV2
	for _, descriptor := range resources.PhysicalDescriptors() {
		if !descriptor.LogicalObligationCountAvailable || descriptor.LogicalObligationCount > ^uint64(0)-reference.LogicalCount {
			return reference, rootpublication.ErrDependencyManifestFormat
		}
		reference.PhysicalCount++
		reference.LogicalCount += descriptor.LogicalObligationCount
	}
	it := newDependencyDirectoryRecordIteratorV2(resources)
	defer it.Close()
	reference.RootPageID, err = bulk.BuildWithOptions(it, allocator, p, bulk.BuildOptions{LeafPrefixCompression: true})
	if err == nil {
		err = it.Error()
	}
	return reference, err
}

// Replacement-index construction and explicit snapshot rebinding validate
// directory structure before host identities have their final installed names.
// Ordinary recovery instead uses the full physical/content admission callback.
func dependencyDirectoryStructureValidatorV2(p *pager.Pager) durableDirectoryValidatorV2 {
	return dependencyDirectoryStructureValidatorWithContextV2(nil, p)
}

func dependencyDirectoryStructureValidatorWithContextV2(ctx context.Context, p *pager.Pager) durableDirectoryValidatorV2 {
	return func(record rootpublication.DurableRootRecordV1) (*rootpublication.StableResourceSet, error) {
		directory, err := rootpublication.NewDependencyDirectoryV2(p, record.Directory, record.TotalPages, func() {})
		if err != nil {
			return nil, err
		}
		defer directory.Release()
		if ctx != nil {
			return nil, directory.Walk(func(_, _ []byte) error { return ctx.Err() })
		}
		return nil, directory.Walk(nil)
	}
}
