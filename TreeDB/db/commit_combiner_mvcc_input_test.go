package db

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/mvccadmission"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

// No worker runs: this is the exact queued request at the stop-return boundary.
// Its eventual accepted cut must consume only the combiner-owned key postimage.
func TestCommitCombinerQueuedStopOwnsQualifiedInput(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(map[bool]string{false: "qualified", true: "mismatched"}[mismatch], func(t *testing.T) {
			var a mvccadmission.Authority
			c := a.Issue()
			original, _ := mvcckey.Encode([]byte("key"), 10)
			inputKey := original
			if mismatch {
				inputKey, _ = mvcckey.Encode([]byte("other"), 10)
			}
			db := &DB{combineReqCh: make(chan *commitCombineReq, 1), combineStopCh: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				_, err := db.writeViaCommitCombinerWithMVCCInput(original, []byte("value"), false, false, c.Input(inputKey, 10, 0))
				done <- err
			}()
			var req *commitCombineReq
			select {
			case req = <-db.combineReqCh:
			case <-time.After(time.Second):
				t.Fatal("request not queued")
			}
			close(db.combineStopCh)
			select {
			case err := <-done:
				if !errors.Is(err, errCommitCombinerClosed) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("stop did not release caller")
			}
			for i := range original {
				original[i] = 0
			}
			cut := a.Begin()
			cut.Observe(req.key, req.input)
			cut.Commit()
			qualified := req.input.BindOwnedKey(req.key, req.key).Present()
			if qualified == mismatch {
				t.Fatalf("owned queue qualification=%v mismatch=%v", qualified, mismatch)
			}

		})
	}
}
