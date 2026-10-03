package nativewire

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// Done is evaluated only after lockSearchAdmissionV1 has failed TryRLock.
// Holding this evaluation schedules an already-waiting reader late, without
// a production hook, fabricated search result, or a sleep-based arrival guess.
type fixedPeerVectorAdmissionWaitContextV1 struct {
	context.Context
	waiting chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *fixedPeerVectorAdmissionWaitContextV1) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	select {
	case <-c.resume:
	case <-c.Context.Done():
	}
	return c.Context.Done()
}

// The existing real-Raft owner-search test proves native reader pins exclude
// publication. This regression tests the opposite arrival order: a reader has
// already observed a held writer, then a consecutive ordinary writer arrives.
func TestFixedPeerVectorWaitingSearchPrecedesNextWriterRealRaftV1(t *testing.T) {
	runFixedPeerVectorPrepareRealRaftV1(t, false, false, false, 3, false, func(t *testing.T, parent context.Context, nodes []*FixedPeerTCPRuntimeV1) {
		ctx, cancel := context.WithTimeout(parent, 30*time.Second)
		defer cancel()
		leader, err := nodes[0].client.leader(ctx, nodes[0].config.Groups[0])
		if err != nil {
			t.Fatal(err)
		}
		var owner *FixedPeerTCPRuntimeV1
		for _, node := range nodes {
			if node.config.NodeID == leader {
				owner = node
			}
		}
		if owner == nil || owner.vector == nil {
			t.Fatal("missing prepared owner")
		}
		writer, err := DialContext(ctx, "tcp", owner.servingVectorConfigV1().PublicAddresses[owner.config.NodeID])
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		waiting := &fixedPeerVectorAdmissionWaitContextV1{Context: ctx, waiting: make(chan struct{}), resume: make(chan struct{})}
		var resumeOnce, retireOnce sync.Once
		resume := func() { resumeOnce.Do(func() { close(waiting.resume) }) }
		retired := make(chan struct{})
		retire := func() { retireOnce.Do(func() { close(retired) }) }
		var workers sync.WaitGroup
		owner.vector.mutationMu.Lock()
		held := true
		defer func() {
			cancel()
			resume()
			retire()
			if held {
				owner.vector.mutationMu.Unlock()
			}
			workers.Wait()
		}()
		readAdmitted := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			err := owner.vector.lockSearchAdmissionV1(waiting)
			readAdmitted <- err
			if err == nil {
				select {
				case <-retired:
				case <-ctx.Done():
				}
				owner.vector.mutationMu.RUnlock()
			}
		}()
		select {
		case <-waiting.waiting:
		case <-ctx.Done():
			t.Fatal("search did not observe held writer admission")
		}
		generation := public.GenerationIDV1{Index: owner.config.VectorInitialization.IndexDefinition.Name, Generation: owner.config.VectorInitialization.Generation}
		request := public.InsertRequestV1{Version: 1, Generation: generation, IdempotencyKey: []byte("waiting-search-next-writer"), ID: []byte("waiting-search-minus-x-plus-y"), Vector: []float32{-1, 1}, Document: []byte("{\"embedding\":[-1,1],\"kind\":\"admission-progress\"}"), Deadline: time.Now().Add(30 * time.Second)}
		type reply struct {
			response public.InsertResponseV1
			err      error
		}
		inserted := make(chan reply, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			response, err := writer.VectorInsertV1(ctx, request)
			inserted <- reply{response, err}
		}()
		// Confirm the genuine next public request is admitted by the server,
		// while the prior writer still owns the mutation interval.
		fixedPeerWaitV1(t, ctx, func() bool { return owner.vector.server.inFlight.Load() != 0 })
		owner.vector.mutationMu.Unlock()
		held = false
		// An already waiting reader must reserve the next publication gap even
		// if its goroutine is scheduled late. This bounded observation is not a
		// claim that a queued reader can preempt a writer already holding Lock.
		observe := time.NewTimer(100 * time.Millisecond)
		select {
		case result := <-inserted:
			observe.Stop()
			t.Fatalf("next ordinary writer completed before waiting search admission: commit=%d revision=%d err=%v", result.response.CommitIndex, result.response.LiveRevision, result.err)
		case <-observe.C:
		case <-ctx.Done():
			observe.Stop()
			t.Fatal("admission order observation canceled")
		}
		resume()
		select {
		case err := <-readAdmitted:
			if err != nil {
				t.Fatal(err)
			}
		case result := <-inserted:
			t.Fatalf("writer overtook waiting reader: commit=%d err=%v", result.response.CommitIndex, result.err)
		case <-ctx.Done():
			t.Fatal("waiting search did not receive publication gap")
		}
		select {
		case result := <-inserted:
			t.Fatalf("writer published while admitted reader retained its pin: commit=%d err=%v", result.response.CommitIndex, result.err)
		default:
		}
		retire()
		select {
		case result := <-inserted:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if err := public.ValidateInsertResponseV1(request, result.response); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("next writer did not progress after reader retirement")
		}
	})
}

func TestFixedPeerVectorSearchAdmissionCancellationAndZeroAllocV1(t *testing.T) {
	vector := &fixedPeerVectorRuntimeV1{}
	ctx := context.Background()
	if got := testing.AllocsPerRun(1000, func() {
		if err := vector.lockSearchAdmissionV1(ctx); err != nil {
			panic(err)
		}
		vector.mutationMu.RUnlock()
	}); got != 0 {
		t.Fatalf("uncontended search admission allocated %g times", got)
	}
	vector.mutationMu.Lock()
	blocked, cancel := context.WithCancel(ctx)
	wait := &fixedPeerVectorAdmissionWaitContextV1{Context: blocked, waiting: make(chan struct{}), resume: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- vector.lockSearchAdmissionV1(wait) }()
	select {
	case <-wait.waiting:
	case <-time.After(time.Second):
		cancel()
		vector.mutationMu.Unlock()
		t.Fatal("contended search did not register its wait")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("contended cancellation: %v", err)
		}
	case <-time.After(time.Second):
		vector.mutationMu.Unlock()
		t.Fatal("cancellation waited for writer release")
	}
	vector.mutationMu.Unlock()
	if !vector.mutationMu.TryLock() {
		t.Fatal("canceled search leaked a reader pin")
	}
	vector.mutationMu.Unlock()
}

func TestFixedPeerVectorAdmissionIntentCancellationAndWriterProgressV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	vector := &fixedPeerVectorRuntimeV1{}
	vector.mutationMu.Lock()
	held := true
	readCtx, cancelRead := context.WithCancel(ctx)
	wait := &fixedPeerVectorAdmissionWaitContextV1{Context: readCtx, waiting: make(chan struct{}), resume: make(chan struct{})}
	readDone := make(chan error, 1)
	var workers sync.WaitGroup
	defer func() {
		cancelRead()
		cancel()
		if held {
			vector.mutationMu.Unlock()
		}
		workers.Wait()
	}()
	workers.Add(1)
	go func() {
		defer workers.Done()
		err := vector.lockSearchAdmissionV1(wait)
		if err == nil {
			vector.mutationMu.RUnlock()
		}
		readDone <- err
	}()
	select {
	case <-wait.waiting:
	case <-ctx.Done():
		t.Fatal("reader did not reach contended admission")
	}
	if got := vector.waitingSearches.Load(); got != 1 {
		t.Fatalf("contended reader intent=%d", got)
	}
	writeCtx, cancelWrite := context.WithCancel(ctx)
	writeWait := &fixedPeerVectorAdmissionWaitContextV1{Context: writeCtx, waiting: make(chan struct{}), resume: make(chan struct{})}
	writeDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		err := vector.lockMutationAdmissionV1(writeWait)
		if err == nil {
			vector.mutationMu.Unlock()
		}
		writeDone <- err
	}()
	select {
	case <-writeWait.waiting:
	case <-ctx.Done():
		t.Fatal("writer did not wait for registered reader intent")
	}
	cancelWrite()
	select {
	case err := <-writeDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("writer waiting for reader intent: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled intent-waiting writer waited for exclusive lock")
	}
	cancelRead()
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reader retirement: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("reader intent did not retire on cancellation")
	}
	if got := vector.waitingSearches.Load(); got != 0 {
		t.Fatalf("canceled reader retained %d waiting intents", got)
	}
	vector.mutationMu.Unlock()
	held = false
	if err := vector.lockMutationAdmissionV1(ctx); err != nil {
		t.Fatal(err)
	}
	vector.mutationMu.Unlock()
	if got := testing.AllocsPerRun(1000, func() {
		if err := vector.lockMutationAdmissionV1(context.Background()); err != nil {
			panic(err)
		}
		vector.mutationMu.Unlock()
	}); got != 0 {
		t.Fatalf("uncontended mutation admission allocated %g times", got)
	}
}

func TestFixedPeerVectorCanceledQueuedWriterReleasesAdmissionV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	vector := &fixedPeerVectorRuntimeV1{}
	vector.mutationMu.RLock()
	held := true
	writeCtx, cancelWrite := context.WithCancel(ctx)
	done := make(chan error, 1)
	defer func() {
		cancelWrite()
		if held {
			vector.mutationMu.RUnlock()
		}
	}()
	go func() {
		err := vector.lockMutationAdmissionV1(writeCtx)
		if err == nil {
			vector.mutationMu.Unlock()
		}
		done <- err
	}()
	// Unlike an intent-waiting writer, this writer has already queued in the
	// existing RWMutex; cancellation is checked immediately after acquisition.
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for vector.mutationMu.TryRLock() {
		vector.mutationMu.RUnlock()
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("writer did not queue behind retained reader pin")
		}
	}
	cancelWrite()
	vector.mutationMu.RUnlock()
	held = false
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled queued writer: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled queued writer did not retire with reader pin")
	}
	if !vector.mutationMu.TryLock() {
		t.Fatal("canceled queued writer leaked exclusive admission")
	}
	vector.mutationMu.Unlock()
}

func TestFixedPeerVectorNewReaderCannotPreemptQueuedWriterV1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	vector := &fixedPeerVectorRuntimeV1{}
	vector.mutationMu.RLock()
	held := true
	writeRelease := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writeRelease) }) }
	var workers sync.WaitGroup
	defer func() {
		cancel()
		if held {
			vector.mutationMu.RUnlock()
		}
		release()
		workers.Wait()
	}()
	writerAcquired := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		err := vector.lockMutationAdmissionV1(ctx)
		writerAcquired <- err
		if err == nil {
			select {
			case <-writeRelease:
			case <-ctx.Done():
			}
			vector.mutationMu.Unlock()
		}
	}()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for vector.mutationMu.TryRLock() {
		vector.mutationMu.RUnlock()
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("writer did not queue behind current reader")
		}
	}
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()
	wait := &fixedPeerVectorAdmissionWaitContextV1{Context: readCtx, waiting: make(chan struct{}), resume: make(chan struct{})}
	readDone := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		err := vector.lockSearchAdmissionV1(wait)
		if err == nil {
			vector.mutationMu.RUnlock()
		}
		readDone <- err
	}()
	select {
	case <-wait.waiting:
	case <-ctx.Done():
		t.Fatal("later reader did not wait behind queued writer")
	}
	vector.mutationMu.RUnlock()
	held = false
	select {
	case err := <-writerAcquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("later reader intent prevented already queued writer progress")
	}
	cancelRead()
	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("later reader: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("later reader did not cancel")
	}
	if got := vector.waitingSearches.Load(); got != 0 {
		t.Fatalf("later reader retained %d intents", got)
	}
	release()
}
