package caching

import (
	"errors"
	"runtime"
	"testing"
)

// Exercise every callback installation route; the mixed path temporarily clears
// hasDeleteRanges during materialization, so custody must be selected earlier.
func TestCommandWALCallbackWriteLeaseCustody(t *testing.T) {
	for _, route := range []string{"plain", "prepared", "measured", "prepared-measured"} {
		for _, shape := range []string{"point", "mixed", "range"} {
			for _, mode := range []string{"success", "error", "panic", "nil", "goexit"} {
				t.Run(route+"/"+shape+"/"+mode, func(t *testing.T) {
					db, err := Open(t.TempDir(), NewMockBackend(), Options{ExternalCommandWAL: true, FlushThreshold: 1 << 30})
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					b := db.NewBatch()
					defer b.Close()
					if shape != "point" {
						if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
							t.Fatal(err)
						}
					}
					if shape != "range" {
						if err := b.Set([]byte("key"), []byte("value")); err != nil {
							t.Fatal(err)
						}
					}
					restored := false
					b.commandWALAppend = func() error { restored = true; return nil }
					sentinel := errors.New("callback error")
					calls, returned := 0, false
					var got error
					var recovered any
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() { recovered = recover() }()
						callback := func() error {
							calls++
							// Publication must still hold a write lease through the callback.
							if db.writeMu.TryLock() {
								db.writeMu.Unlock()
								panic("callback has no write lease")
							}
							switch mode {
							case "error":
								return sentinel
							case "panic":
								panic(sentinel)
							case "nil":
								panic(nil)
							case "goexit":
								runtime.Goexit()
							}
							return nil
						}
						switch route {
						case "plain":
							got = b.WriteAfterCommandWALAppend(false, callback)
						case "prepared":
							got = b.WriteAfterCommandWALAppendWithPreparedRevision(false, callback)
						case "measured":
							_, got = b.WriteAfterCommandWALAppendMeasured(false, callback)
						case "prepared-measured":
							_, got = b.WriteAfterCommandWALAppendWithPreparedRevisionMeasured(false, callback)
						}
						returned = true
					}()
					<-done
					if calls != 1 {
						t.Fatalf("calls=%d", calls)
					}
					if !db.writeMu.TryLock() {
						t.Fatal("write lease retained")
					}
					db.writeMu.Unlock()
					if b.commandWALAppend == nil {
						t.Fatal("prior callback lost")
					}
					_ = b.commandWALAppend()
					if !restored || b.commandWALPreparedRevision {
						t.Fatal("callback/revision state not restored")
					}
					switch mode {
					case "success":
						if !returned || got != nil || recovered != nil {
							t.Fatalf("returned=%v err=%v panic=%v", returned, got, recovered)
						}
						if shape != "range" {
							value, err := db.Get([]byte("key"))
							if err != nil || string(value) != "value" {
								t.Fatalf("value=%q err=%v", value, err)
							}
						}
					case "error":
						if !returned || got != sentinel || recovered != nil {
							t.Fatalf("returned=%v err=%v panic=%v", returned, got, recovered)
						}
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
								t.Fatalf("nil panic changed: %T %v", recovered, recovered)
							}
						}
					case "goexit":
						if returned || recovered != nil {
							t.Fatalf("returned=%v panic=%v", returned, recovered)
						}
					}
				})
			}
		}
	}
}

func TestCommandWALCallbackCustodyCOWExclusion(t *testing.T) {
	// COW publication owns the combined writer/cache lease; this wrapper must
	// propagate the unwind without releasing any part of that custody.
	db := &DB{cow: &cowCache{}}
	b := &Batch{db: db}
	db.writeMu.RLock()
	defer db.writeMu.RUnlock()
	sentinel := errors.New("COW callback panic")
	func() {
		defer func() {
			if recover() != sentinel {
				t.Error("panic identity changed")
			}
		}()
		b.commandWALAppendWithWriteLeaseCustody(func() error { panic(sentinel) })()
	}()
	if db.writeMu.TryLock() {
		db.writeMu.Unlock()
		t.Fatal("wrapper released COW lease")
	}
}
