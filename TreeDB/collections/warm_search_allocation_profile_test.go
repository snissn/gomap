package collections

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
)

// The opt-in observer is used only in the selected isolated child. Profiles
// include observer and process background allocations, not only search work.
func beginWarmSearchAllocationProfile(t *testing.T) func() {
	t.Helper()
	dir := os.Getenv("TREEDB_WARM_SEARCH_ALLOC_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if os.Getenv(collectionsVectorAllocationGateEnv) != "native-search-warm-scratch" {
		t.Fatal("allocation profile requires the selected isolated child")
	}
	write := func(name string) {
		t.Helper()
		file, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Errorf("allocation profile create: %v", err)
			return
		}
		err = pprof.Lookup("allocs").WriteTo(file, 0)
		closeErr := file.Close()
		if err != nil {
			t.Errorf("allocation profile write: %v", err)
		}
		if closeErr != nil {
			t.Errorf("allocation profile close: %v", closeErr)
		}
	}
	oldRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	runtime.GC()
	write("before.pprof")
	return func() {
		defer func() { runtime.MemProfileRate = oldRate }()
		runtime.GC()
		write("after.pprof")
	}
}
