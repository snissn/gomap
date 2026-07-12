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
