package mvcc

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Pure containing-layout evidence: the actual33 fixture is not executed.
func TestNativeFiniteCursorAdmissionSizesV6(t *testing.T) {
	size := uint64(unsafe.Sizeof(PruneCursor{}))
	t.Logf("containing-admission sourceSymbol=pruneNativeV6.PruneCursor size=%d charge=%d", size, retainedalloc.AllocationCharge(size))
}
