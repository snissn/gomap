package nativewire

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const fixedPeerElectionLogBytesV1 = 64 << 10

// The existing Raft/raft-net loggers share this instance-owned memory tail.
// Write never performs file I/O or calls testing.T from a consensus goroutine.
// The tail can evict the original cause; the header reports all discarded bytes.
type fixedPeerElectionLogCaptureV1 struct {
	mu      sync.Mutex
	started time.Time
	bytes   [fixedPeerElectionLogBytesV1]byte
	next    int
	used    int
	total   uint64
}

func newFixedPeerElectionLogCaptureV1() *fixedPeerElectionLogCaptureV1 {
	return &fixedPeerElectionLogCaptureV1{started: time.Now()}
}

type fixedPeerElectionGroupLogV1 struct {
	capture *fixedPeerElectionLogCaptureV1
	group   raftcluster.GroupID
}

func (c *fixedPeerElectionLogCaptureV1) groupWriter(group raftcluster.GroupID) io.Writer {
	return fixedPeerElectionGroupLogV1{capture: c, group: group}
}

func (w fixedPeerElectionGroupLogV1) Write(p []byte) (int, error) {
	c := w.capture
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appendLocked([]byte(fmt.Sprintf("elapsed=%s group=%s ", time.Since(c.started), fixedPeerElectionTextV1(string(w.group), 128))))
	c.appendLocked(p)
	return len(p), nil
}

func (c *fixedPeerElectionLogCaptureV1) appendLocked(p []byte) {
	c.total += uint64(len(p))
	if len(p) >= len(c.bytes) {
		copy(c.bytes[:], p[len(p)-len(c.bytes):])
		c.next, c.used = 0, len(c.bytes)
		return
	}
	n := copy(c.bytes[c.next:], p)
	copy(c.bytes[:], p[n:])
	c.next = (c.next + len(p)) % len(c.bytes)
	c.used = min(len(c.bytes), c.used+len(p))
}

func (c *fixedPeerElectionLogCaptureV1) snapshot() ([]byte, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	raw := make([]byte, c.used)
	start := (c.next - c.used + len(c.bytes)) % len(c.bytes)
	n := copy(raw, c.bytes[start:min(start+c.used, len(c.bytes))])
	copy(raw[n:], c.bytes[:c.used-n])
	return raw, c.total - uint64(c.used)
}

func (c *fixedPeerElectionLogCaptureV1) dump(out io.Writer, label string) {
	raw, omitted := c.snapshot()
	// Emission occurs after releasing the writer lock. A tail may start in the
	// middle of a library message; do not mistake it for a complete RPC trace.
	fmt.Fprintf(out, "election log tail %s retained_bytes=%d omitted_bytes=%d capacity=%d\n%s\n", label, len(raw), omitted, fixedPeerElectionLogBytesV1, raw)
}

func fixedPeerOpenTestRuntimeWithElectionLogV1(t *testing.T, config FixedPeerTCPConfigV1, round string) (*FixedPeerTCPRuntimeV1, error) {
	t.Helper()
	capture := newFixedPeerElectionLogCaptureV1()
	t.Cleanup(func() {
		if t.Failed() {
			raw, omitted := capture.snapshot()
			t.Logf("election log tail node=%s round=%s retained_bytes=%d omitted_bytes=%d capacity=%d\n%s", config.NodeID, round, len(raw), omitted, fixedPeerElectionLogBytesV1, raw)
		}
	})
	return openFixedPeerTCPRuntimeWithRaftLogV1(config, fixedPeerTakeTestListenersV1(config), capture.groupWriter)
}

// Only existing fixture Status calls feed this recorder, on the test goroutine.
// Store transitions, not every identical poll; still retain call counts and the
// longest call to expose gaps without adding requests or changing wait bounds.
type fixedPeerElectionStatusHistoryV1 struct {
	started     time.Time
	phase       string
	entries     [128]string
	next, used  int
	omitted     uint64
	calls       uint64
	longestCall time.Duration
	last        map[raftcluster.NodeID]string
}

func newFixedPeerElectionStatusHistoryV1() *fixedPeerElectionStatusHistoryV1 {
	return &fixedPeerElectionStatusHistoryV1{started: time.Now(), phase: "status", last: make(map[raftcluster.NodeID]string)}
}

func fixedPeerElectionTextV1(s string, limit int) string {
	if len(s) > limit {
		return s[:limit] + "[truncated]"
	}
	return s
}

func (h *fixedPeerElectionStatusHistoryV1) setPhase(phase string) {
	h.phase = fixedPeerElectionTextV1(phase, 64)
}

func fixedPeerElectionGroupStatusV1(s raftcluster.RuntimeStatusV1) string {
	return fmt.Sprintf("group=%s state=%s term=%d leader=%s commit=%d applied=%d last=%d", fixedPeerElectionTextV1(string(s.GroupID), 64), fixedPeerElectionTextV1(s.State, 32), s.Term, fixedPeerElectionTextV1(string(s.LeaderID), 64), s.CommitIndex, s.RaftAppliedIndex, s.LastIndex)
}

func (h *fixedPeerElectionStatusHistoryV1) observe(node raftcluster.NodeID, started time.Time, status FixedPeerTCPStatusV1, err error) {
	elapsed := time.Since(started)
	h.calls++
	h.longestCall = max(h.longestCall, elapsed)
	var summary string
	if err != nil {
		summary = "error=" + fixedPeerElectionTextV1(err.Error(), 512)
	} else {
		groups := make([]string, 0, min(len(status.Groups), 4))
		for _, group := range status.Groups[:min(len(status.Groups), 4)] {
			groups = append(groups, fixedPeerElectionGroupStatusV1(group.RuntimeStatusV1))
		}
		summary = fmt.Sprintf("epoch=%d catalog={%s} data={%s} unrecorded_groups=%d", status.Catalog.Epoch, fixedPeerElectionGroupStatusV1(status.CatalogRaft), strings.Join(groups, "; "), max(0, len(status.Groups)-4))
	}
	summary = fixedPeerElectionTextV1(h.phase+" "+summary, 1536)
	if previous, ok := h.last[node]; ok && previous == summary {
		return
	}
	if _, ok := h.last[node]; ok || len(h.last) < 128 {
		h.last[node] = summary
	}
	entry := fmt.Sprintf("elapsed=%s call=%s node=%s phase=%s", time.Since(h.started), elapsed, fixedPeerElectionTextV1(string(node), 64), summary)
	h.entries[h.next] = entry
	h.next = (h.next + 1) % len(h.entries)
	if h.used < len(h.entries) {
		h.used++
	} else {
		h.omitted++
	}
}

func (h *fixedPeerElectionStatusHistoryV1) report(t testing.TB) {
	t.Helper()
	if !t.Failed() {
		return
	}
	t.Logf("fixture election observations phase=%s calls=%d longest_call=%s retained_transitions=%d omitted_transitions=%d capacity=%d", h.phase, h.calls, h.longestCall, h.used, h.omitted, len(h.entries))
	for i := 0; i < h.used; i++ {
		t.Log(h.entries[(h.next-h.used+i+len(h.entries))%len(h.entries)])
	}
}

func TestFixedPeerElectionLogCaptureBoundsV1(t *testing.T) {
	capture := newFixedPeerElectionLogCaptureV1()
	writer := capture.groupWriter("meta")
	large := strings.Repeat("a", fixedPeerElectionLogBytesV1+19) + "tail"
	n, err := writer.Write([]byte(large))
	raw, omitted := capture.snapshot()
	if err != nil || n != len(large) || len(raw) != fixedPeerElectionLogBytesV1 || !strings.HasSuffix(string(raw), "tail") || omitted < 23 {
		t.Fatalf("bounded write: n=%d err=%v retained=%d omitted=%d", n, err, len(raw), omitted)
	}
	if _, err := writer.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	raw, laterOmitted := capture.snapshot()
	if len(raw) != fixedPeerElectionLogBytesV1 || !strings.HasSuffix(string(raw), "next") || laterOmitted <= omitted {
		t.Fatalf("wrapped tail retained=%d omitted=%d previous=%d", len(raw), laterOmitted, omitted)
	}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				_, _ = writer.Write([]byte("concurrent\n"))
				_, _ = capture.snapshot()
			}
		}()
	}
	workers.Wait()
	// Captures do not alias the mutable ring after the lock is released.
	frozen, _ := capture.snapshot()
	want := string(frozen)
	_, _ = writer.Write([]byte(strings.Repeat("z", fixedPeerElectionLogBytesV1)))
	if string(frozen) != want {
		t.Fatal("snapshot aliases mutable ring")
	}
}

func TestFixedPeerElectionStatusHistoryBoundsV1(t *testing.T) {
	history := newFixedPeerElectionStatusHistoryV1()
	history.setPhase("initial-catalog-leader")
	status := FixedPeerTCPStatusV1{}
	for term := uint64(1); term <= 140; term++ {
		status.CatalogRaft.Term = term
		history.observe("ingress", time.Now(), status, nil)
	}
	history.observe("ingress", time.Now(), status, nil)
	if history.calls != 141 || history.used != 128 || history.omitted != 12 {
		t.Fatalf("history counts: calls=%d retained=%d omitted=%d", history.calls, history.used, history.omitted)
	}
	history.setPhase("catalog-publication")
	history.observe("ingress", time.Now(), status, context.DeadlineExceeded)
	if history.used != 128 || history.omitted != 13 || !strings.Contains(history.entries[(history.next+127)%128], "catalog-publication error=context deadline exceeded") {
		t.Fatal("phase/error transition not retained")
	}
}
