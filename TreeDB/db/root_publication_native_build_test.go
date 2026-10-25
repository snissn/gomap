package db

import (
	"bytes"
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestNativePhysicalAppendPinLifetime(t *testing.T) {
	for _, outcome := range []string{"publish", "abandon", "append_failure"} {
		t.Run(outcome, func(t *testing.T) {
			database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			appender, err := newReplayInlineAppender(database, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer appender.close()
			database.SetValueLogAppender(appender)
			defer database.SetValueLogAppender(nil)
			group, err := database.BeginRootPublicationBuildGroup()
			if err != nil {
				t.Fatal(err)
			}
			defer group.Close()
			ptrs, err := group.AppendNativeValueLogValues([][]byte{bytes.Repeat([]byte("large value"), 512)})
			if err != nil {
				t.Fatal(err)
			}
			database.pendingValueLogAppendMu.Lock()
			ptrCount, fileRefs := len(database.pendingValueLogAppendPtrRefs), database.pendingValueLogAppendFileIDRefs[ptrs[0].FileID]
			database.pendingValueLogAppendMu.Unlock()
			if ptrCount != 0 || fileRefs != 1 {
				t.Fatalf("physical group must own one segment pin and no pointer pin: ptrs=%d file refs=%d", ptrCount, fileRefs)
			}
			if outcome == "append_failure" {
				database.SetValueLogAppender(nil)
				if _, err := group.AppendNativeValueLogValues([][]byte{[]byte("failure")}); !errors.Is(err, ErrValueLogAppenderUnavailable) {
					t.Fatalf("append error=%v", err)
				}
			} else {
				delta := batch.New(nil, 1)
				defer delta.Close()
				if err := delta.SetPointer([]byte("native/doc"), ptrs[0]); err != nil {
					t.Fatal(err)
				}
				if err := group.BuildNativeSystemDelta(delta); err != nil {
					t.Fatal(err)
				}
				if outcome == "publish" {
					final := database.NewPhysicalBatch().(*Batch)
					defer final.Close()
					if err := final.SetRootPublicationBuildGroup(group, true); err != nil {
						t.Fatal(err)
					}
					if err := final.WriteSync(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := group.Close(); err != nil {
				t.Fatal(err)
			}
			database.pendingValueLogAppendMu.Lock()
			ptrCount, fileCount := len(database.pendingValueLogAppendPtrRefs), len(database.pendingValueLogAppendFileIDRefs)
			database.pendingValueLogAppendMu.Unlock()
			if ptrCount != 0 || fileCount != 0 {
				t.Fatalf("completed group leaked append pins: ptrs=%d files=%d", ptrCount, fileCount)
			}
		})
	}
}
