package valuelog

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/page"
)

type stepClock struct {
	now  time.Time
	step time.Duration
}

func (c *stepClock) Now() time.Time {
	if c.step == 0 {
		return c.now
	}
	c.now = c.now.Add(c.step)
	return c.now
}

func TestWriterEncodeNsSamplingVirtualTime(t *testing.T) {
	dict, err := buildFallbackBenchDict(1)
	if err != nil || len(dict) == 0 {
		t.Fatalf("buildFallbackBenchDict: %v", err)
	}

	w := newWriterWithSink(ioDiscard{}, 1)
	w.SetClock(&stepClock{now: time.Unix(0, 0), step: time.Microsecond})
	w.SetEncodeSampleStride(1)

	records := []Record{
		{RID: 1, Value: bytes.Repeat([]byte("compressible-"), 512)},
	}
	var ptrScratch [1]page.ValuePtr
	_, stats, err := w.AppendFrameWithStatsInto(1, dict, records, ptrScratch[:])
	if err != nil {
		t.Fatalf("AppendFrameWithStatsInto: %v", err)
	}
	if !stats.Attempted || !stats.Kept {
		t.Fatalf("expected attempted+kept compression")
	}
	if stats.EncodeNs <= 0 {
		t.Fatalf("expected EncodeNs to be sampled")
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
