package rootpublication

import (
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
