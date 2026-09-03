package nativewire

import (
	"strconv"
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

func TestCountersErrorCodeSnapshotUsesTrackedAndFallbackCounters(t *testing.T) {
	var c counters
	c.incErrorCode(iwire.ErrInvalidCommand)
	c.incErrorCode(iwire.ErrorCode(maxTrackedErrorCode + 1))
	got := c.snapshot()
	if got["errors.code."+strconv.FormatUint(uint64(iwire.ErrInvalidCommand), 10)] != 1 {
		t.Fatalf("tracked error snapshot=%v", got)
	}
	if got["errors.code."+strconv.Itoa(maxTrackedErrorCode+1)] != 1 {
		t.Fatalf("fallback error snapshot=%v", got)
	}
	if _, ok := got["errors.code."+strconv.FormatUint(uint64(iwire.ErrMalformedFrame), 10)]; ok {
		t.Fatalf("zero tracked error code was emitted: %v", got)
	}
}

func TestCountersDenseCommandUsesAtomicStorage(t *testing.T) {
	var c counters
	c.incCommandRequest(iwire.CommandDenseVectorSearch, "dense_vector_search")
	c.incCommandError(iwire.CommandDenseVectorSearch, "dense_vector_search")
	requests := c.commands[iwire.CommandDenseVectorSearch].requests.Load()
	errors := c.commands[iwire.CommandDenseVectorSearch].errors.Load()
	if requests != 1 || errors != 1 {
		t.Fatalf("dense atomic counters requests=%d errors=%d", requests, errors)
	}
	if c.values != nil {
		t.Fatalf("dense counters used fallback map: %v", c.values)
	}
	got := c.snapshot()
	if got["commands.dense_vector_search.requests_total"] != 1 || got["commands.dense_vector_search.errors_total"] != 1 {
		t.Fatalf("dense counter snapshot=%v", got)
	}
}
