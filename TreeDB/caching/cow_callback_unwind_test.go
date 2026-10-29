package caching

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

// The real cache writer must release both leases even when the installed command
// callback never returns. TryLock avoids leaking waiter goroutines on failure.
func TestCOWCommandCallbackUnwindLeases(t *testing.T) {
	for _, mode := range []string{"panic", "nil", "goexit", "error", "success"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			backend, err := backenddb.Open(backenddb.Options{Dir: filepath.Join(dir, "backend")})
			if err != nil {
				t.Fatal(err)
			}
			db, err := Open(dir, backend, Options{ExternalCommandWAL: true, MemtableMode: "cow_ordered", MemtableShards: 2, FlushThreshold: 1 << 30})
			if err != nil {
				backend.Close()
				t.Fatal(err)
			}
			b := db.NewBatch()
			if err := b.Set([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("callback unwind")
			done := make(chan struct{})
			var recovered any
			var result error
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				result = b.WriteAfterCommandWALAppend(false, func() error {
					if db.writeMu.TryLock() {
						db.writeMu.Unlock()
						panic("missing write lease")
					}
					if db.cow.writerMu.TryLock() {
						db.cow.writerMu.Unlock()
						panic("missing writer lease")
					}
					switch mode {
					case "panic":
						panic(sentinel)
					case "nil":
						panic(nil)
					case "goexit":
						runtime.Goexit()
					case "error":
						return sentinel
					}
					return backend.FinalizeRawKVEntryScanForCachedPublication(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1)
				})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("callback unwind blocked")
			}
			writeFree := db.writeMu.TryLock()
			if writeFree {
				db.writeMu.Unlock()
			}
			writerFree := db.cow.writerMu.TryLock()
			if writerFree {
				db.cow.writerMu.Unlock()
			}
			if !writeFree || !writerFree {
				t.Fatalf("retained leases: writeMu=%v writerMu=%v", !writeFree, !writerFree)
			}
			defer backend.Close()
			defer db.Close()
			defer b.Close()
			switch mode {
			case "panic":
				if returned || recovered != sentinel {
					t.Fatalf("returned=%v panic=%v", returned, recovered)
				}
			case "nil":
				if returned {
					t.Fatal("nil panic returned")
				}
				if recovered != nil {
					if _, ok := recovered.(*runtime.PanicNilError); !ok {
						t.Fatalf("panic=%T", recovered)
					}
				}
			case "goexit":
				if returned || recovered != nil {
					t.Fatalf("returned=%v panic=%v", returned, recovered)
				}
			case "error":
				if !returned || result != sentinel {
					t.Fatalf("returned=%v error=%v", returned, result)
				}
			case "success":
				if !returned || result != nil {
					t.Fatalf("returned=%v error=%v panic=%v", returned, result, recovered)
				}
			}
			next := db.NewBatch()
			defer next.Close()
			if err := next.Set([]byte("later"), []byte("usable")); err != nil {
				t.Fatal(err)
			}
			if err := next.Write(); err != nil {
				t.Fatal(err)
			}
			got, err := db.Get([]byte("later"))
			if err != nil || string(got) != "usable" {
				t.Fatalf("later=%q error=%v", got, err)
			}
		})
	}
}

// Native custody is bound before the append returns its assigned receipt. An
// unwind in that gap must poison storage while returning the borrowed custody edge to its
// producer for disposal; the absent receipt cannot establish storage rollback.
func TestCOWNativeCommandCallbackUnknownReceiptUnwind(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, point := range []durabilitycut.Point{durabilitycut.AfterCOWPreparation, durabilitycut.AfterDependencyAppend} {
		for _, mode := range []string{"nil", "goexit"} {
			t.Run(string(point)+"/"+mode, func(t *testing.T) {
				cache, backend := nativeBridgeFixture(t, true)
				tx, err := backend.BeginNativeConditionalTxn()
				if err != nil {
					t.Fatal(err)
				}
				publication, plan := nativeTxnPublication(t, tx, "uncertain")
				state := plan.state
				intent := nativeTxnIntent(t, backend, true)
				opening, basis := cache.cow.cut, cache.cow.cut.basis
				refs, basisRefs := opening.refs, basis.refs.Load()
				fired, returned := false, false
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Point == point {
						fired = true
						if mode == "goexit" {
							runtime.Goexit()
						}
						panic(nil)
					}
					return nil
				})
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { _ = recover() }()
					_, _ = tx.CommitNative(publication, intent, true)
					returned = true
				}()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("native unwind blocked")
				}
				restore()
				if !fired || returned || intent.AssignedLSN() != 0 {
					t.Fatalf("fired=%v returned=%v assigned=%d", fired, returned, intent.AssignedLSN())
				}
				if !cache.writeMu.TryLock() {
					t.Fatal("native write lease retained")
				}
				cache.writeMu.Unlock()
				if !cache.cow.writerMu.TryLock() {
					t.Fatal("native writer lease retained")
				}
				cache.cow.writerMu.Unlock()
				if plan.calls != 1 || cache.cow.cut != opening || opening.refs != refs-1 || basis.refs.Load() != basisRefs {
					t.Fatalf("native private aliases retained or publication changed: calls=%d refs=%d/%d basis=%d/%d", plan.calls, opening.refs, refs, basis.refs.Load(), basisRefs)
				}
				if err := backend.CheckStorageMaintenanceReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Fatalf("later maintenance=%v", err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				if err := cache.Close(); (err != nil && !errors.Is(err, backenddb.ErrRecoveryRequired)) || cache.cow.prefix.AcceptedCleanupFailed() {
					t.Fatalf("borrowed custody did not drain: Close=%v failed=%v", err, cache.cow.prefix.AcceptedCleanupFailed())
				}
				if state.refs.Load() != 0 || state.calls != 1 {
					t.Fatalf("state not disposed exactly once: refs=%d calls=%d", state.refs.Load(), state.calls)
				}
				if stats := cache.COWMemoryStats(); stats.TotalBytes != 0 {
					t.Fatalf("native debt=%+v", stats)
				}
			})
		}
	}
}

func TestCOWNativeBeforeCutSwapUnwind(t *testing.T) {
	registerNativeBridgeTestCommand()
	for _, wal := range []bool{false, true} {
		for _, point := range []durabilitycut.Point{durabilitycut.BeforeCOWCutSwap} {
			for _, mode := range []string{"panic", "nil", "goexit"} {
				t.Run(fmt.Sprintf("wal=%v/%s/%s", wal, point, mode), func(t *testing.T) {
					cache, backend := nativeBridgeFixture(t, wal)
					tx, err := backend.BeginNativeConditionalTxn()
					if err != nil {
						t.Fatal(err)
					}
					publication, plan := nativeTxnPublication(t, tx, "uncertain")
					state := plan.state
					intent := nativeTxnIntent(t, backend, wal)
					opening, basis := cache.cow.cut, cache.cow.cut.basis
					refs, basisRefs := opening.refs, basis.refs.Load()
					fired, returned := false, false
					var recovered any
					restore := durabilitycut.Install(func(e durabilitycut.Event) error {
						if e.Point == point {
							fired = true
							if mode == "goexit" {
								runtime.Goexit()
							}
							if mode == "panic" {
								panic("cut swap unwind")
							}
							panic(nil)
						}
						return nil
					})
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() { recovered = recover() }()
						_, _ = tx.CommitNative(publication, intent, true)
						returned = true
					}()
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("native unwind blocked")
					}
					restore()
					if mode == "panic" && recovered != "cut swap unwind" {
						t.Fatalf("panic changed: %v", recovered)
					}
					if mode == "goexit" && recovered != nil {
						t.Fatalf("Goexit became panic: %v", recovered)
					}
					if mode == "nil" && recovered != nil {
						if _, ok := recovered.(*runtime.PanicNilError); !ok {
							t.Fatalf("nil panic changed: %v", recovered)
						}
					}
					if !fired || returned || (wal && intent.AssignedLSN() == 0) {
						t.Fatalf("fired=%v returned=%v assigned=%d", fired, returned, intent.AssignedLSN())
					}
					if !cache.writeMu.TryLock() {
						t.Fatal("native write lease retained")
					}
					cache.writeMu.Unlock()
					if !cache.cow.writerMu.TryLock() {
						t.Fatal("native writer lease retained")
					}
					cache.cow.writerMu.Unlock()
					if plan.calls != 1 || cache.cow.cut != opening || opening.refs != refs-1 || basis.refs.Load() != basisRefs {
						t.Fatalf("native private aliases retained or publication changed: calls=%d refs=%d/%d basis=%d/%d", plan.calls, opening.refs, refs, basis.refs.Load(), basisRefs)
					}
					if err := backend.CheckStorageMaintenanceReady(); wal && !errors.Is(err, backenddb.ErrRecoveryRequired) {
						t.Fatalf("later maintenance=%v", err)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					// Assigned original queue custody is parked for recovery; private
					// state and borrowed captures still dispose exactly once.
					if err := cache.Close(); (err != nil && !errors.Is(err, backenddb.ErrRecoveryRequired)) || cache.cow.prefix.AcceptedCleanupFailed() != wal {
						t.Fatalf("borrowed custody did not drain: Close=%v failed=%v", err, cache.cow.prefix.AcceptedCleanupFailed())
					}
					if state.refs.Load() != 0 || state.calls != 1 {
						t.Fatalf("state not disposed exactly once: refs=%d calls=%d", state.refs.Load(), state.calls)
					}
					if stats := cache.COWMemoryStats(); !wal && stats.TotalBytes != 0 {
						t.Fatalf("native debt=%+v", stats)
					}
				})
			}
		}
	}
}

func TestCOWBeforeCutSwapUnwind(t *testing.T) {
	for _, wal := range []bool{false, true} {
		for _, mode := range []string{"panic", "nil", "goexit"} {
			t.Run(fmt.Sprintf("wal=%v/%s", wal, mode), func(t *testing.T) {
				cache, backend := nativeBridgeFixture(t, wal)
				b := cache.NewBatch()
				defer b.Close()
				if err := b.Set([]byte("private"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				opening, basis := cache.cow.cut, cache.cow.cut.basis
				refs, basisRefs := opening.refs, basis.refs.Load()
				fired, returned := false, false
				var prepared *cowBatchPreparation
				var recovered any
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Point == durabilitycut.BeforeCOWCutSwap {
						fired = true
						prepared = b.cowState.prepared
						switch mode {
						case "goexit":
							runtime.Goexit()
						case "panic":
							panic("cut swap unwind")
						default:
							panic(nil)
						}
					}
					return nil
				})
				done := make(chan struct{})
				go func() {
					defer close(done)
					defer func() { recovered = recover() }()
					if wal {
						if err := b.AuthorizeCOWAppend(cache.cow.txnOwner); err != nil {
							panic(err)
						}
						_ = b.WriteAfterCommandWALAppendWithPreparedRevision(false, func() error {
							lsn, err := backend.AppendRawKVCommandWALOrderedEntryScanWithHintPreparedFinalizedAndMode(b.PrepareExternalCommandWALPublication, b.finalizeCOWPublication, b.Replay, 1, backenddb.RawKVCommandWALAppendRelaxed)
							if receiptErr := b.RecordCOWAppend(cache.cow.txnOwner, lsn, err, false); err == nil {
								err = receiptErr
							}
							return err
						})
					} else {
						_ = b.Write()
					}
					returned = true
				}()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("unwind blocked")
				}
				restore()
				if mode == "panic" && recovered != "cut swap unwind" {
					t.Fatalf("panic changed: %v", recovered)
				}
				if mode == "goexit" && recovered != nil {
					t.Fatalf("Goexit became panic: %v", recovered)
				}
				if mode == "nil" && recovered != nil {
					if _, ok := recovered.(*runtime.PanicNilError); !ok {
						t.Fatalf("nil panic changed: %v", recovered)
					}
				}
				if !fired || returned {
					t.Fatalf("fired=%v returned=%v", fired, returned)
				}
				if !cache.writeMu.TryLock() {
					t.Fatal("write lease retained")
				}
				cache.writeMu.Unlock()
				if !cache.cow.writerMu.TryLock() {
					t.Fatal("writer lease retained")
				}
				cache.cow.writerMu.Unlock()
				if prepared == nil || prepared.cut != nil || prepared.scratch != nil || prepared.mutation.Valid() || prepared.resources != nil {
					t.Fatal("preparation owners retained")
				}
				if cache.cow.cut != opening || opening.refs != refs || basis.refs.Load() != basisRefs || b.cowState.prepared != nil {
					t.Fatalf("private preparation retained: cut=%v refs=%d/%d basis=%d/%d prepared=%v", cache.cow.cut == opening, opening.refs, refs, basis.refs.Load(), basisRefs, b.cowState.prepared != nil)
				}
				if b.cowState.receipt.Assigned != wal || b.cowState.receipt.Uncertain != wal {
					t.Fatalf("unexpected receipt: %+v", b.cowState.receipt)
				}
				if err := backend.CheckStorageMaintenanceReady(); (wal && !errors.Is(err, backenddb.ErrRecoveryRequired)) || (!wal && err != nil) {
					t.Fatalf("maintenance readiness=%v", err)
				}
				if err := b.Close(); err != nil {
					t.Fatal(err)
				}
				if err := cache.Close(); err != nil && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Fatal(err)
				}
				if stats := cache.COWMemoryStats(); !wal && stats.TotalBytes != 0 {
					t.Fatalf("debt=%+v", stats)
				}
			})
		}
	}
}
