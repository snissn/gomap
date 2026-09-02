package rootpublication

import (
	"context"
	"testing"
	"time"
)

func TestFakeClockDueTimersUseDeadlineThenCreationOrder(t *testing.T) {
	clock := NewFakeClock(time.Unix(1, 0))
	tiedFirst := clock.NewTimer(3 * time.Millisecond).(*fakeTimer)
	early := clock.NewTimer(time.Millisecond).(*fakeTimer)
	tiedSecond := clock.NewTimer(3 * time.Millisecond).(*fakeTimer)
	later := clock.NewTimer(4 * time.Millisecond).(*fakeTimer)

	clock.mu.Lock()
	due := clock.takeDueTimersLocked(clock.now.Add(3 * time.Millisecond))
	clock.mu.Unlock()

	want := []*fakeTimer{early, tiedFirst, tiedSecond}
	if len(due) != len(want) {
		t.Fatalf("due timer count=%d, want %d", len(due), len(want))
	}
	for i := range want {
		if due[i] != want[i] {
			t.Fatalf("due timer %d=%p, want %p", i, due[i], want[i])
		}
	}
	if !later.active {
		t.Fatal("future timer was consumed")
	}
}

func TestStaleStoppedTimerFiringCannotReplaceCurrentTimer(t *testing.T) {
	clock := NewFakeClock(time.Unix(2, 0))
	c := &Coordinator{clock: clock, firstPendingAt: clock.Now(), wakeReason: WakeNone}

	c.installTimerLocked()
	staleTimer := c.timer
	staleGeneration := c.timerGeneration
	c.clearPublishRequestLocked()
	c.installTimerLocked()
	currentTimer := c.timer
	currentGeneration := c.timerGeneration
	if currentTimer == staleTimer || currentGeneration == staleGeneration {
		t.Fatal("replacement timer did not receive a distinct identity")
	}

	c.handleTimerFiredLocked(staleGeneration)
	if c.timer != currentTimer || c.timerGeneration != currentGeneration {
		t.Fatal("stale timer firing replaced the current timer")
	}
	if c.publishNow || c.wakeReason != WakeNone {
		t.Fatalf("stale timer firing requested publication: now=%v reason=%v", c.publishNow, c.wakeReason)
	}

	c.handleTimerFiredLocked(currentGeneration)
	if c.timer != nil || !c.publishNow || c.wakeReason != WakeTimer {
		t.Fatalf("current timer firing was ignored: timer=%v now=%v reason=%v", c.timer, c.publishNow, c.wakeReason)
	}
}

func TestFixedPublishDelayOverridesAdaptiveDelayWithinProductionBounds(t *testing.T) {
	for _, delay := range []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		c := &Coordinator{fixedPublishDelay: delay, ewmaService: time.Second}
		if got := c.publishDelayLocked(); got != delay {
			t.Fatalf("fixed delay=%s got=%s", delay, got)
		}
	}
	for _, delay := range []time.Duration{time.Millisecond, 101 * time.Millisecond} {
		c, err := New(Options{
			Publisher: PublisherFunc(func(_ context.Context, candidate *PreparedRootCandidate) PublishResult {
				return PublishResult{Outcome: PublishSucceeded, DurableCommitSeq: candidate.Frontier().CommitSeq()}
			}),
			FixedPublishDelay: delay,
		})
		if err == nil {
			stopClean(t, c)
			t.Fatalf("fixed delay %s unexpectedly accepted", delay)
		}
	}
}
