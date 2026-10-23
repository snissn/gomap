package rootpublication

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"reflect"
	"strings"
)

// A duplicated handle retains the same immutable diagnostic provenance. Once
// its role is present, repeating it only materializes another path-sized string.
// Every caller still admits the full name capacity independently before the
// duplication, so closing/refunding the source never subsidizes the new handle.
func stableDuplicateFileName(name, suffix string) string {
	if strings.HasSuffix(name, suffix) {
		return name
	}
	return name + suffix
}

// StableFileMetadataCharge derives os.File's separately allocated private
// backing from the current target's type metadata, not an object graph walk.
// os.NewFile allocates the public wrapper and that embedded pointer's element;
// callers separately admit a full diagnostic-name capacity, including when a
// duplicate can reuse an immutable name already retained by its source.
func StableFileMetadataCharge(nameBytes uint64) (uint64, error) {
	t := reflect.TypeOf(os.File{})
	if t.Kind() != reflect.Struct || t.NumField() != 1 || t.Field(0).Type.Kind() != reflect.Pointer {
		return 0, fmt.Errorf("%w: unsupported os.File ownership layout", ErrUnresolvedResource)
	}
	return retainedalloc.AllocationCharge(uint64(t.Size())) +
		retainedalloc.AllocationCharge(uint64(t.Field(0).Type.Elem().Size())) +
		retainedalloc.AllocationCharge(nameBytes), nil
}
