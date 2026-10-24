package treedb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// Exercise actual public prepared-payload callers, including ordinary Write in
// the durable profile and explicit WriteSync in the relaxed profile. A captured
// physical sync must release both backend and public append admission first.
func TestPublicCommandWALContinuousPayloadHeldSync(t *testing.T) {
	for _, measured := range []bool{false, true} {
		for _, durableProfile := range []bool{false, true} {
			t.Run(fmt.Sprintf("measured=%t/durable_profile=%t", measured, durableProfile), func(t *testing.T) {
				opts := commandWALDurabilityProofOptions(t.TempDir())
				if !durableProfile {
					ApplyProfile(&opts, ProfileCommandWALRelaxed)
				}
				d, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer d.Close()
				d.publicBatchWriteSyncPhaseEnabled = measured
				entered, release, appended := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				var appends atomic.Int32
				var held sync.Once
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Resource != durabilitycut.ResourceCommandWAL {
						return nil
					}
					if e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 && appends.Add(1) == 2 {
						close(appended)
					}
					if e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 {
						held.Do(func() { close(entered); <-release })
					}
					return nil
				})
				defer restore()
				write := func(key string) error {
					b := d.NewBatch()
					defer b.Close()
					if err := b.Set([]byte(key), []byte("value")); err != nil {
						return err
					}
					if durableProfile && !measured {
						return b.Write()
					}
					return b.WriteSync()
				}
				first, second := make(chan error, 1), make(chan error, 1)
				go func() { first <- write("continuous-first") }()
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("payload did not reach captured physical sync")
				}
				go func() { second <- write("continuous-successor") }()
				select {
				case <-appended:
				case <-time.After(2 * time.Second):
					t.Fatal("successor blocked behind physical sync")
				}
				for _, done := range []chan error{first, second} {
					select {
					case err := <-done:
						t.Fatalf("ACK overtook captured sync: %v", err)
					default:
					}
				}
				if appends.Load() != 2 {
					t.Fatalf("held mutation appends=%d want2", appends.Load())
				}
				closed := make(chan error, 1)
				go func() { closed <- d.Close() }()
				select {
				case err := <-closed:
					t.Fatalf("Close overtook captured sync: %v", err)
				default:
				}
				unblock()
				for _, done := range []chan error{first, second, closed} {
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("payload completion/Close stalled")
					}
				}
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				requireRawKVValue(t, reopened, []byte("continuous-first"), []byte("value"))
				requireRawKVValue(t, reopened, []byte("continuous-successor"), []byte("value"))
			})
		}
	}
}
