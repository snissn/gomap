package valuelog

import (
	"errors"
	"os"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Baseline-compatible boundaries. All writer/setup/retirement/retry work is
// outside timed Refresh loops. ColdRegistration separately times open+close.
func BenchmarkManagerRetirementBoundaries(b *testing.B) {
	for _, registry := range []bool{false, true} {
		pinsName := "nil"
		if registry {
			pinsName = "registry"
		}
		for _, boundary := range []string{"LiveRefresh", "PinnedZombieRefresh", "ClosedRetiredRefresh", "ColdRegistration"} {
			b.Run(pinsName+"/"+boundary, func(b *testing.B) {
				// Compiled wrapper widths, not OS/kernel FD-retention accounting.
				// The common fixture reports them on both original and candidate.
				defer func() {
					b.ReportMetric(float64(unsafe.Sizeof(File{})), "File-B")
					b.ReportMetric(float64(unsafe.Sizeof(os.File{})), "parent-wrapper-B")
					b.ReportMetric(float64(unsafe.Sizeof(rootpublication.StableIdentity{})), "identity-B")
				}()
				dir := b.TempDir()
				id, err := EncodeFileID(0, 1)
				if err != nil {
					b.Fatal(err)
				}
				writer, err := NewWriter(SegmentPath(dir, id), id)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := writer.Append(0, nil, 1, []byte("boundary value")); err != nil {
					b.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
				var pins *rootpublication.IdentityPinRegistry
				if registry {
					pins = rootpublication.NewIdentityPinRegistry()
				}
				open := func() *Manager {
					manager, err := NewManagerWithStableResourcePinRegistry(dir, pins)
					if err != nil {
						b.Fatal(err)
					}
					return manager
				}
				if boundary == "ColdRegistration" {
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						manager := open()
						if err := manager.Close(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					return
				}
				manager := open()
				defer manager.Close()
				var held *Set
				if boundary != "LiveRefresh" {
					held = manager.CurrentSetNoRefresh()
					file := held.Files[id]
					if err := manager.MarkZombie(id); err != nil {
						b.Fatal(err)
					}
					if boundary == "ClosedRetiredRefresh" {
						originalRemove := removeSegmentPath
						want := errors.New("closed retired boundary setup")
						removeSegmentPath = func(string, func(string) error) error { return want }
						err := manager.Release(held)
						removeSegmentPath = originalRemove
						held = nil
						if !errors.Is(err, want) || !file.closed.Load() {
							b.Fatalf("closed retired setup: %v", err)
						}
					}
					if manager.retiredCount != 1 || !file.IsZombie.Load() {
						b.Fatal("boundary lacks exact retired owner")
					}
					if held != nil {
						defer manager.Release(held)
					}
				}
				for range 10 {
					if err := manager.Refresh(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := manager.Refresh(); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
