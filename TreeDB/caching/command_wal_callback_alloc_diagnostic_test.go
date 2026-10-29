package caching

import (
	"errors"
	"testing"
)

// Diagnostic only: repeated warm ordinary callback paths, no timing acceptance.
func TestCommandWALCallbackWarmAllocationDiagnostic(t *testing.T) {
	for _, route := range []string{"plain", "prepared", "measured", "prepared-measured"} {
		for _, outcome := range []string{"success", "error"} {
			t.Run(route+"/"+outcome, func(t *testing.T) {
				db, err := Open(t.TempDir(), NewMockBackend(), Options{ExternalCommandWAL: true, FlushThreshold: 1 << 30, MemtableShards: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				b := db.NewBatch()
				defer b.Close()
				key, value := []byte("key"), []byte("value")
				sentinel := errors.New("callback error")
				cb := func() error {
					if outcome == "error" {
						return sentinel
					}
					return nil
				}
				run := func() {
					if err := b.Set(key, value); err != nil {
						panic(err)
					}
					var err error
					switch route {
					case "plain":
						err = b.WriteAfterCommandWALAppend(false, cb)
					case "prepared":
						err = b.WriteAfterCommandWALAppendWithPreparedRevision(false, cb)
					case "measured":
						_, err = b.WriteAfterCommandWALAppendMeasured(false, cb)
					case "prepared-measured":
						_, err = b.WriteAfterCommandWALAppendWithPreparedRevisionMeasured(false, cb)
					}
					if outcome == "error" {
						if err != sentinel {
							panic("lost error")
						}
					} else if err != nil {
						panic(err)
					}
					b.Reset()
				}
				for i := 0; i < 100; i++ {
					run()
				}
				t.Logf("warm allocs/op = %.3f", testing.AllocsPerRun(1000, run))
			})
		}
	}
}
