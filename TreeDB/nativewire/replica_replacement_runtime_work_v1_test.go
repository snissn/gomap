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
	cfg.ClusterID = "replacement-worker"
	cfg.Credentials = peerCredentialsFixtureV1(t, cfg.ClusterID, string(cfg.NodeID))
	runtime, err := fixedPeerOpenTestRuntimeV1(t, cfg)
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
	transport, ok := d.transport.(*peerRaftTransportV1)
	if !ok {
		t.Fatal("missing node admission transport")
	}
	saturated, err := transport.admission.work(transport.scope, peerSnapshotsV1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer saturated.release()
	var refused fixedPeerReplyV1
	if err := d.replacementWorkV1("op", "seed", work, &refused); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) {
		t.Fatalf("saturated seed admission: %v", err)
	}
	if d.replacementWork.work != nil || calls.Load() != 0 {
		t.Fatal("saturation cached or launched work")
	}
	saturated.release()
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
	if stats := runtime.PeerTransportV1().ResourceStatsV1(); stats.Current[peerSnapshotsV1] != 1 {
		t.Fatalf("poll acquired duplicate seed admission: %+v", stats)
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
	if stats := runtime.PeerTransportV1().ResourceStatsV1(); stats.Current[peerSnapshotsV1] != 0 {
		t.Fatalf("completed canceled work retained clean admission: %+v", stats)
	}
}

func TestReplacementNativeFailureRetainsSeedBudgetV1(t *testing.T) {
	cfg := fixedPeerTestConfigsV1(t)[0]
	cfg.ClusterID = "replacement-worker"
	cfg.Credentials = peerCredentialsFixtureV1(t, cfg.ClusterID, string(cfg.NodeID))
	runtime, err := fixedPeerOpenTestRuntimeV1(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	d := runtime.localDataV1(cfg.Groups[0].ID)
	if d == nil {
		t.Fatal("missing hosted group")
	}
	// This fresh group has no applied application command. Its submitted native
	// snapshot future cannot produce a valid cut. The replacement boundary
	// conservatively retains ownership for an opaque native future failure.
	var calls atomic.Int32
	work := func(ctx context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
		calls.Add(1)
		_, err := d.provider.SnapshotForReplacementV1(ctx)
		return nil, err
	}
	var reply fixedPeerReplyV1
	if err := d.replacementWorkV1("poisoned", "seed", work, &reply); err != nil {
		t.Fatal(err)
	}
	select {
	case <-d.replacementWork.work.done:
	case <-time.After(5 * time.Second):
		t.Fatal("native future did not complete")
	}
	if err := d.replacementWorkV1("poisoned", "seed", work, &reply); !raftcluster.ReplacementSnapshotCleanupRequiredV1(err) {
		t.Fatalf("native failure lost poisoned owner: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("poll repeated poisoned work")
	}
	for i := 0; i < 2; i++ {
		if err := runtime.Close(); !raftcluster.ReplacementSnapshotCleanupRequiredV1(err) {
			t.Fatalf("Close forgot unresolved native owner: %v", err)
		}
		if stats := runtime.PeerTransportV1().ResourceStatsV1(); stats.Current[peerSnapshotsV1] != 1 {
			t.Fatalf("Close released unknown native cleanup: %+v", stats)
		}
	}
}

func TestReplacementInstallWorkerRetriesOnlyProvenPreSendRefusalV1(t *testing.T) {
	for _, tc := range []struct {
		name      string
		firstErr  error
		retryable bool
	}{
		{"pre-send leadership loss", errors.Join(raftcluster.ErrReplacementInstallNotSentV1, raftcluster.ErrNotLeader), true},
		{"unknown post-send result", raftcluster.ErrCommitAmbiguous, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := fixedPeerTestConfigsV1(t)[0]
			cfg.ClusterID = "replacement-install-retry"
			cfg.Credentials = peerCredentialsFixtureV1(t, cfg.ClusterID, string(cfg.NodeID))
			runtime, err := fixedPeerOpenTestRuntimeV1(t, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			d := runtime.localDataV1(cfg.Groups[0].ID)
			var attempts atomic.Int32
			work := func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
				if attempts.Add(1) == 1 {
					return nil, tc.firstErr
				}
				return nil, nil
			}
			var reply fixedPeerReplyV1
			if err := d.replacementWorkV1("exact-operation", "install", work, &reply); err != nil || !reply.ReplacementPending {
				t.Fatalf("first work: %+v %v", reply, err)
			}
			<-d.replacementWork.work.done
			reply = fixedPeerReplyV1{}
			err = d.replacementWorkV1("exact-operation", "install", work, &reply)
			if tc.retryable {
				if !errors.Is(err, raftcluster.ErrReplacementInstallNotSentV1) || reply.ReplacementPending || attempts.Load() != 1 {
					t.Fatalf("pre-send refusal was not surfaced: %+v attempts=%d err=%v", reply, attempts.Load(), err)
				}
				reply = fixedPeerReplyV1{}
				if err := d.replacementWorkV1("exact-operation", "install", work, &reply); err != nil || !reply.ReplacementPending {
					t.Fatalf("pre-send exact retry: %+v %v", reply, err)
				}
				<-d.replacementWork.work.done
				reply = fixedPeerReplyV1{}
				if err := d.replacementWorkV1("exact-operation", "install", work, &reply); err != nil || reply.ReplacementPending {
					t.Fatalf("completed exact retry: %+v %v", reply, err)
				}
				if attempts.Load() != 2 {
					t.Fatalf("install attempts=%d", attempts.Load())
				}
			} else if !errors.Is(err, tc.firstErr) || attempts.Load() != 1 {
				t.Fatalf("ambiguous install repeated: attempts=%d err=%v", attempts.Load(), err)
			}
		})
	}
}

// The direct entry exercises worker ownership independently of catalog fixtures.
// Production callers enter through the atomic canonical-BEGIN boundary above.
func (d *fixedPeerDataV1) replacementWorkV1(operation, phase string, work func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error), reply *fixedPeerReplyV1) error {
	d.replacementWork.mu.Lock()
	defer d.replacementWork.mu.Unlock()
	return d.replacementWorkLockedV1(operation, phase, nil, work, reply)
}

func TestReplacementAuthorizedWorkerRejectsStalePublicationV1(t *testing.T) {
	cfg := fixedPeerTestConfigsV1(t)[0]
	cfg.ClusterID = "replacement-worker"
	cfg.Credentials = peerCredentialsFixtureV1(t, cfg.ClusterID, string(cfg.NodeID))
	runtime, err := fixedPeerOpenTestRuntimeV1(t, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	d := runtime.localDataV1(cfg.Groups[0].ID)
	old := replacementReceiverTestBeginV1(t)
	newer := old
	newer.ExpectedEpoch++ // The logical operation name alone is not identity.
	var calls atomic.Int32
	work := func(context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) { calls.Add(1); return nil, nil }
	var reply fixedPeerReplyV1
	if err := d.replacementAuthorizedWorkV1(old, "verify", work, &reply); err != nil {
		t.Fatal(err)
	}
	<-d.replacementWork.work.done
	if err := d.replacementAuthorizedWorkV1(newer, "verify", work, &reply); err != nil {
		t.Fatal(err)
	}
	<-d.replacementWork.work.done
	// Simulate an old request delayed across a newer committed authorization.
	// It must not publish work or poison the newer operation's cached result.
	if err := d.replacementAuthorizedWorkV1(old, "verify", work, &reply); err == nil {
		t.Fatal("stale publication accepted")
	}
	if err := d.replacementAuthorizedWorkV1(newer, "verify", work, &reply); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("work repeated: %d", calls.Load())
	}
}
