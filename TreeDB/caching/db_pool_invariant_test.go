package caching

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func typeHasPointers(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Array:
		return typeHasPointers(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if typeHasPointers(t.Field(i).Type) {
				return true
			}
		}
		return false
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan, reflect.Func, reflect.String, reflect.UnsafePointer:
		return true
	default:
		return false
	}
}

func TestValuePtrHasNoPointerFieldsForNoClearPool(t *testing.T) {
	if typeHasPointers(reflect.TypeOf(page.ValuePtr{})) {
		t.Fatalf("page.ValuePtr now contains pointer-bearing fields; update pool reuse logic before using putValueLogPtrsNoClear")
	}
}
