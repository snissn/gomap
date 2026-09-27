package nativewire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestReplacementNativeWorkerOwnsActualReturnV1(t *testing.T) {
	cfg := fixedPeerTestConfigsV1(t)[0]
	runtime, err := OpenFixedPeerTCPRuntimeV1(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	d := runtime.localDataV1(cfg.Groups[0].ID)
	if d == nil {
		t.Fatal("missing hosted group")
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseWork := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWork()
	var calls atomic.Int32
	work := func(ctx context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
		calls.Add(1)
		close(entered)
		<-release
		return nil, ctx.Err()
	}
	var first fixedPeerReplyV1
	if err := d.replacementWorkV1("op", "seed", work, &first); err != nil || !first.ReplacementPending {
		t.Fatalf("first: %+v %v", first, err)
	}
	<-entered
	// Requests can expire/disconnect independently; the one native call remains
	// owned, and another exact poll cannot run it again.
	for i := 0; i < 8; i++ {
		var poll fixedPeerReplyV1
		if err := d.replacementWorkV1("op", "seed", work, &poll); err != nil || !poll.ReplacementPending {
			t.Fatalf("poll: %+v %v", poll, err)
		}
	}
	var conflict fixedPeerReplyV1
	if err := d.replacementWorkV1("different", "seed", work, &conflict); err == nil {
		t.Fatal("different operation reused slot")
	}
	done := d.cancelReplacementWorkV1()
	select {
	case <-done:
		t.Fatal("cancellation released outstanding native work")
	default:
	}
	if err := d.replacementWorkV1("op", "seed", work, &conflict); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("closed slot: %v", err)
	}
	releaseWork()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("actual return not observed")
	}
	if calls.Load() != 1 {
		t.Fatalf("native calls=%d", calls.Load())
	}
	if !errors.Is(d.replacementWork.work.err, context.Canceled) {
		t.Fatalf("worker error=%v", d.replacementWork.work.err)
	}
}
