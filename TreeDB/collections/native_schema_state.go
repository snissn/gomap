package collections

import (
	"reflect"
	"strings"
	"sync/atomic"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// nativeSchemaState is shared until explicit schema/interpretation replacement.
// Ordinary domain mutations retain one descriptor, never copy this metadata.
type nativeSchemaState struct {
	refs          atomic.Int64
	meta          *CollectionMeta
	refund        *cowbtree.OwnedMapRefund
	canonicalBSON bool
	plannerOK     bool
	planner       insertBatchPlanner
}

func nativeSchemaDerivedPlannerCharge(meta *CollectionMeta) (uint64, error) {
	if meta == nil {
		return 0, nil
	}
	var total uint64
	addAlloc := func(bytes uint64) bool {
		charge := memtable.COWAllocationCharge(bytes)
		if charge > ^uint64(0)-total {
			return false
		}
		total += charge
		return true
	}
	addLen := func(a, b uint64) (uint64, bool) {
		if b > ^uint64(0)-a {
			return 0, false
		}
		return a + b, true
	}
	nameLen := uint64(len(meta.Name))
	for _, suffix := range [...]uint64{uint64(len("/primary")), uint64(len("/templates")), uint64(len("/index-state"))} {
		n, ok := addLen(nameLen, suffix)
		if !ok || !addAlloc(n) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	indexCount := uint64(len(meta.Indexes))
	if indexCount == 0 {
		return total, nil
	}
	if indexCount > ^uint64(0)/uint64(unsafe.Sizeof(indexDefinition{})) ||
		indexCount > ^uint64(0)/uint64(unsafe.Sizeof(indexRuntime{})) {
		return 0, memtable.ErrCOWCapacity
	}
	if !addAlloc(indexCount*uint64(unsafe.Sizeof(indexDefinition{}))) ||
		!addAlloc(indexCount*uint64(unsafe.Sizeof(indexRuntime{}))) {
		return 0, memtable.ErrCOWCapacity
	}
	if indexCount > 32 {
		// Transient duplicate-detection map backing in indexRuntimes().
		if indexCount > ^uint64(0)/64 || !addAlloc(indexCount*64+256) {
			return 0, memtable.ErrCOWCapacity
		}
	}
	strSize := uint64(unsafe.Sizeof(""))
	compSize := uint64(unsafe.Sizeof(IndexComponent{}))
	strSliceSize := uint64(unsafe.Sizeof([]string{}))
	for _, idx := range meta.Indexes {
		secLen, ok := addLen(nameLen, uint64(len("/index/")))
		if !ok {
			return 0, memtable.ErrCOWCapacity
		}
		secLen, ok = addLen(secLen, uint64(len(idx.Name)))
		if !ok || !addAlloc(secLen) {
			return 0, memtable.ErrCOWCapacity
		}
		if idx.Field != "" {
			depth := uint64(strings.Count(idx.Field, ".")) + 1
			if depth > ^uint64(0)/strSize || !addAlloc(depth*strSize) {
				return 0, memtable.ErrCOWCapacity
			}
		}
		if len(idx.Components) > 0 {
			compCount := uint64(len(idx.Components))
			if compCount > ^uint64(0)/compSize || compCount > ^uint64(0)/strSliceSize {
				return 0, memtable.ErrCOWCapacity
			}
			if !addAlloc(compCount*compSize) || !addAlloc(compCount*strSliceSize) {
				return 0, memtable.ErrCOWCapacity
			}
			for _, comp := range idx.Components {
				if comp.Field != "" {
					depth := uint64(strings.Count(comp.Field, ".")) + 1
					if depth > ^uint64(0)/strSize || !addAlloc(depth*strSize) {
						return 0, memtable.ErrCOWCapacity
					}
				}
			}
		}
	}
	return total, nil
}

func nativeSchemaTotalCharge(meta *CollectionMeta) (uint64, error) {
	backing, err := nativeSchemaBackingCharge(reflect.ValueOf(meta), 0)
	if err != nil {
		return 0, err
	}
	derived, err := nativeSchemaDerivedPlannerCharge(meta)
	if err != nil {
		return 0, err
	}
	base := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeSchemaState{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowbtree.OwnedMapRefund{})))
	if backing > ^uint64(0)-base {
		return 0, memtable.ErrCOWCapacity
	}
	total := base + backing
	if derived > ^uint64(0)-total {
		return 0, memtable.ErrCOWCapacity
	}
	return total + derived, nil
}

func nativeJSONInsertPlannerForSchema(schema *nativeSchemaState, meta CollectionMeta) (insertBatchPlanner, error) {
	if schema != nil && schema.plannerOK && schema.meta != nil && schema.meta.Name == meta.Name {
		return schema.planner, nil
	}
	return nativeJSONInsertPlanner(meta)
}

func nativeSchemaRootNames(schema *nativeSchemaState, name string) (primaryRoot, indexStateRoot string) {
	if schema != nil && schema.plannerOK && schema.meta != nil && schema.meta.Name == name {
		return schema.planner.primaryRoot, schema.planner.indexStateRoot
	}
	return "", ""
}

func newNativeSchemaState(meta *CollectionMeta, admit cowbtree.OwnedMapAdmit) (*nativeSchemaState, error) {
	if meta == nil || meta.Name == "" || admit == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	total, err := nativeSchemaTotalCharge(meta)
	if err != nil {
		return nil, err
	}
	lease, err := admit(total)
	if err != nil {
		return nil, err
	}
	refund := new(cowbtree.OwnedMapRefund)
	refund.Init(lease)
	// Keep the existing metadata copy implementation, which already owns all
	// slice/pointer structure. Clone immutable string backing as well so this
	// descriptor has no uncharged caller/catalog substring dependency.
	copied := meta.copy()
	nativeSchemaOwnStrings(reflect.ValueOf(copied))
	s := &nativeSchemaState{meta: copied, refund: refund, canonicalBSON: isCanonicalNativeBSONMeta(copied)}
	if planner, err := nativeJSONInsertPlanner(*copied); err == nil {
		if len(planner.indexes) == 0 {
			planner.cachedIndexRuntimes = emptyIndexRuntimes
			s.planner = planner
			s.plannerOK = true
		} else if runtimes, err := planner.indexRuntimes(); err == nil {
			planner.cachedIndexRuntimes = runtimes
			s.planner = planner
			s.plannerOK = true
		}
	}
	s.refs.Store(1)
	return s, nil
}

// This is a schema-only construction walk, never a commit/read/retain path.
// Collection metadata has a finite nonrecursive struct/pointer/slice shape;
// unknown dynamic kinds reject before admission instead of adding a registry.
func nativeSchemaBackingCharge(v reflect.Value, depth int) (uint64, error) {
	if depth > 32 {
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	var total uint64
	add := func(bytes uint64) bool {
		if bytes > ^uint64(0)-total {
			return false
		}
		total += bytes
		return true
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return 0, nil
		}
		total = memtable.COWAllocationCharge(uint64(v.Type().Elem().Size()))
		bytes, err := nativeSchemaBackingCharge(v.Elem(), depth+1)
		if err != nil {
			return 0, err
		}
		if !add(bytes) {
			return 0, memtable.ErrCOWCapacity
		}
	case reflect.Slice:
		if v.Len() == 0 {
			return 0, nil
		}
		size := uint64(v.Type().Elem().Size())
		if size != 0 && uint64(v.Len()) > ^uint64(0)/size {
			return 0, memtable.ErrCOWCapacity
		}
		total = memtable.COWAllocationCharge(uint64(v.Len()) * size)
		for i := 0; i < v.Len(); i++ {
			bytes, err := nativeSchemaBackingCharge(v.Index(i), depth+1)
			if err != nil {
				return 0, err
			}
			if !add(bytes) {
				return 0, memtable.ErrCOWCapacity
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			bytes, err := nativeSchemaBackingCharge(v.Field(i), depth+1)
			if err != nil {
				return 0, err
			}
			if !add(bytes) {
				return 0, memtable.ErrCOWCapacity
			}
		}
	case reflect.Array:
		for i := 0; i < v.Len(); i++ {
			bytes, err := nativeSchemaBackingCharge(v.Index(i), depth+1)
			if err != nil {
				return 0, err
			}
			if !add(bytes) {
				return 0, memtable.ErrCOWCapacity
			}
		}
	case reflect.String:
		return memtable.COWAllocationCharge(uint64(v.Len())), nil
	case reflect.Map:
		// Current text synonyms are an explicitly unsupported operation.
		// A nil/empty reserved configuration carries no dynamic backing.
		if v.Len() != 0 {
			return 0, backenddb.ErrNativeSemanticUnsupported
		}
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
	default:
		return 0, backenddb.ErrNativeSemanticUnsupported
	}
	return total, nil
}

func nativeSchemaOwnStrings(v reflect.Value) {
	switch v.Kind() {
	case reflect.Pointer:
		if !v.IsNil() {
			nativeSchemaOwnStrings(v.Elem())
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			nativeSchemaOwnStrings(v.Field(i))
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			nativeSchemaOwnStrings(v.Index(i))
		}
	case reflect.String:
		v.SetString(strings.Clone(v.String()))
	}
}

func (s *nativeSchemaState) retain() {
	if s == nil || s.refs.Add(1) <= 1 {
		panic("collections: retain released native schema")
	}
}
func (s *nativeSchemaState) release() cowbtree.OwnedMapCompletion {
	if s == nil {
		return cowbtree.OwnedMapCompletion{}
	}
	refs := s.refs.Add(-1)
	if refs < 0 {
		panic("collections: native schema released twice")
	}
	if refs != 0 {
		return cowbtree.OwnedMapCompletion{}
	}
	s.meta = nil
	s.planner = insertBatchPlanner{}
	s.plannerOK = false
	refund := s.refund
	s.refund = nil
	return refund.Completion()
}
