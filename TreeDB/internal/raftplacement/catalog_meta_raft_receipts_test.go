package raftplacement

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// The Raft logger writes one complete event per Write. Keep each peer's
// recent events, including their native timestamps, across provider restarts.
type catalogMetaRaftLogV1 struct {
	mu        sync.Mutex
	writes    uint64
	bytesIn   uint64
	truncated uint64
	events    [32]struct {
		prefix    [480]byte
		length    int
		truncated bool
	}
}

func (l *catalogMetaRaftLogV1) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	event := &l.events[l.writes%uint64(len(l.events))]
	line := bytes.TrimSuffix(p, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	event.length = copy(event.prefix[:], line)
	event.truncated = event.length < len(line)
	if event.truncated {
		l.truncated++
	}
	l.writes++
	l.bytesIn += uint64(n)
	return n, nil
}

func (l *catalogMetaRaftLogV1) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	first := l.writes - min(l.writes, uint64(len(l.events)))
	var out strings.Builder
	fmt.Fprintf(&out, "writes=%d bytes=%d overwritten=%d truncated=%d", l.writes, l.bytesIn, first, l.truncated)
	for i := first; i < l.writes; i++ {
		event := &l.events[i%uint64(len(l.events))]
		out.WriteByte('\n')
		out.Write(event.prefix[:event.length])
		if event.truncated {
			out.WriteString(" [truncated]")
		}
	}
	return out.String()
}

func TestCatalogMetaRaftLogRetainsBoundedChronologicalEventsV1(t *testing.T) {
	var log catalogMetaRaftLogV1
	var bytesIn int
	write := func(s string) {
		t.Helper()
		n, err := log.Write([]byte(s))
		if n != len(s) || err != nil {
			t.Fatalf("write returned %d, %v for %d bytes", n, err, len(s))
		}
		bytesIn += n
	}
	write("")
	write("evicted\n")
	width := len(log.events[0].prefix)
	long := strings.Repeat("x", width+20)
	write(long + "\r\n")
	wantLines := []string{long[:width] + " [truncated]"}
	for i := 1; i < len(log.events); i++ {
		line := fmt.Sprintf("raft event %d", i)
		write(line + "\r\n")
		wantLines = append(wantLines, line)
	}
	header := fmt.Sprintf("writes=%d bytes=%d overwritten=1 truncated=1", len(log.events)+1, bytesIn)
	if got, want := log.String(), header+"\n"+strings.Join(wantLines, "\n"); got != want {
		t.Fatalf("raft receipt:\n%s\nwant:\n%s", got, want)
	}
}
