package rootpublication

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"reflect"
)

// StableFileMetadataCharge derives os.File's separately allocated private
// backing from the current target's type metadata, not an object graph walk.
// os.NewFile allocates the public wrapper and that embedded pointer's element;
// callers separately declare the exact newly allocated diagnostic-name bytes.
func StableFileMetadataCharge(nameBytes uint64) (uint64, error) {
	t := reflect.TypeOf(os.File{})
	if t.Kind() != reflect.Struct || t.NumField() != 1 || t.Field(0).Type.Kind() != reflect.Pointer {
		return 0, fmt.Errorf("%w: unsupported os.File ownership layout", ErrUnresolvedResource)
	}
	return retainedalloc.AllocationCharge(uint64(t.Size())) +
		retainedalloc.AllocationCharge(uint64(t.Field(0).Type.Elem().Size())) +
		retainedalloc.AllocationCharge(nameBytes), nil
}
