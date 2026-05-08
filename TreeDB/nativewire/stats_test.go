package nativewire

import (
	"testing"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func TestCountersNilReceiverNoop(t *testing.T) {
	var c *counters
	c.add("custom", 1)
	c.inc("custom")
	c.incFramesIn()
	c.incFramesOut()
	c.addBytesIn(1)
	c.addBytesOut(1)
	c.incRequestsStarted()
	c.incRequestsCompleted()
	c.incRequestsFailed()
	c.incRequestsCanceled()
	c.addDispatchNanos(1)
	c.incErrorsTotal()
	c.incErrorCode(iwire.ErrInvalidCommand)
	c.incCommandRequest(iwire.CommandStats, "stats")
	c.incCommandError(iwire.CommandStats, "stats")
	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("nil counters snapshot=%v want empty", got)
	}
}
