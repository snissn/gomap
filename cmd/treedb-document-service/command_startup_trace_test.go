package main

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

const commandPerfStartupRecordLimit = 4096

// Command HTTP/native readiness accounting only. These diagnostic observations
// include instrumentation overhead and do not establish Mongo qualification or
// isolate database-open time. No extra probes or changes to polling are made.
type commandPerfStartupTrace struct {
	ProbeBoundary string                             `json:"probe_boundary"`
	Unavailable   []string                           `json:"unavailable"`
	Evidence      string                             `json:"evidence"`
	Events        []commandPerfStartupEvent          `json:"events"`
	Totals        map[string]commandPerfStartupTotal `json:"totals"`
	Truncated     bool                               `json:"truncated"`
	Status        string                             `json:"status"`
	origin        time.Time
	now           func() time.Time
}
type commandPerfStartupEvent struct {
	Kind             string `json:"kind"`
	StartNanoseconds int64  `json:"start_ns"`
	EndNanoseconds   int64  `json:"end_ns"`
	Error            string `json:"error,omitempty"`
}
type commandPerfStartupTotal struct {
	Count       int   `json:"count"`
	Failures    int   `json:"failures"`
	Nanoseconds int64 `json:"nanoseconds"`
}

func (r *commandPerfReport) newStartupTrace(phase string) *commandPerfStartupTrace {
	if !r.startupTraceEnabled {
		return nil
	}
	if r.StartupTrace == nil {
		r.StartupTrace = make(map[string]*commandPerfStartupTrace)
	}
	tr := &commandPerfStartupTrace{origin: time.Now(), now: time.Now, Status: "incomplete", Totals: make(map[string]commandPerfStartupTotal), Unavailable: []string{"database_open_duration", "cross_process_monotonic_milestones"}, Evidence: "Command HTTP/native startup diagnostic; instrumentation overhead included; terminal includes failure cleanup; not Mongo qualification or causal attribution"}
	r.StartupTrace[phase] = tr
	return tr
}
func (tr *commandPerfStartupTrace) begin() time.Time {
	if tr == nil {
		return time.Time{}
	}
	return tr.now()
}
func (tr *commandPerfStartupTrace) mark(kind string) {
	if tr == nil {
		return
	}
	now := tr.now()
	tr.record(kind, now, now, nil)
}
func (tr *commandPerfStartupTrace) end(kind string, start time.Time, err error) {
	if tr == nil {
		return
	}
	tr.record(kind, start, tr.now(), err)
}
func (tr *commandPerfStartupTrace) record(kind string, start, end time.Time, err error) {
	total := tr.Totals[kind]
	total.Count++
	total.Nanoseconds += end.Sub(start).Nanoseconds()
	if err != nil {
		total.Failures++
	}
	tr.Totals[kind] = total
	if len(tr.Events) >= commandPerfStartupRecordLimit {
		tr.Truncated = true
		return
	}
	event := commandPerfStartupEvent{Kind: kind, StartNanoseconds: start.Sub(tr.origin).Nanoseconds(), EndNanoseconds: end.Sub(tr.origin).Nanoseconds()}
	if err != nil {
		event.Error = err.Error()
		if len(event.Error) > 256 {
			event.Error = event.Error[:256]
		}
	}
	tr.Events = append(tr.Events, event)
}
func (tr *commandPerfStartupTrace) finish(err error) {
	if tr == nil {
		return
	}
	tr.Status = "ready"
	if err != nil {
		tr.Status = "failed"
	}
	tr.end("terminal", tr.origin, err)
}

func TestCommandStartupTraceAccounting(t *testing.T) {
	base := time.Unix(0, 0)
	now := base
	tr := &commandPerfStartupTrace{origin: base, now: func() time.Time { return now }, Status: "incomplete", Totals: make(map[string]commandPerfStartupTotal)}
	advance := func(kind string, duration time.Duration, err error) {
		start := tr.begin()
		now = now.Add(duration)
		tr.end(kind, start, err)
	}
	tr.mark("prepared")
	advance("spawn", 3*time.Millisecond, nil)
	advance("native_hello", 2*time.Millisecond, errors.New("hello refused"))
	advance("poll_sleep", 11*time.Millisecond, nil)
	advance("native_hello", time.Millisecond, nil)
	tr.finish(nil)
	if tr.Status != "ready" || tr.Truncated || len(tr.Events) != 6 {
		t.Fatalf("bad trace: %+v", tr)
	}
	if got := tr.Totals["native_hello"]; got.Count != 2 || got.Failures != 1 || got.Nanoseconds != 3000000 {
		t.Fatalf("probe accounting: %+v", got)
	}
	if got := tr.Totals["poll_sleep"]; got.Count != 1 || got.Nanoseconds != 11000000 {
		t.Fatalf("actual sleep: %+v", got)
	}
	if got := tr.Totals["terminal"]; got.Nanoseconds != 17000000 || got.Failures != 0 {
		t.Fatalf("terminal: %+v", got)
	}
	for i, event := range tr.Events {
		if event.StartNanoseconds > event.EndNanoseconds || i > 0 && event.EndNanoseconds < tr.Events[i-1].EndNanoseconds {
			t.Fatalf("event order: %+v", tr.Events)
		}
	}
}

func TestCommandStartupTracePartialFailures(t *testing.T) {
	for _, kind := range []string{"prepared", "spawn", "http_health", "native_hello"} {
		t.Run(kind, func(t *testing.T) {
			r := commandPerfReport{startupTraceEnabled: true}
			tr := r.newStartupTrace("reopen")
			err := errors.New("synthetic failure")
			tr.end(kind, tr.begin(), err)
			tr.finish(err)
			if r.StartupTrace["reopen"] != tr || tr.Status != "failed" || tr.Totals[kind].Failures != 1 || tr.Totals["terminal"].Failures != 1 {
				t.Fatalf("partial failure lost: %+v", tr)
			}
			raw, e := json.Marshal(r)
			if e != nil || !json.Valid(raw) {
				t.Fatalf("receipt: %s %v", raw, e)
			}
		})
	}
}

func TestCommandStartupTraceBoundAndDisabled(t *testing.T) {
	var disabled commandPerfReport
	tr := disabled.newStartupTrace("seed")
	tr.mark("prepared")
	tr.end("spawn", tr.begin(), errors.New("ignored"))
	tr.finish(nil)
	if tr != nil || disabled.StartupTrace != nil {
		t.Fatal("disabled tracing allocated state")
	}
	raw, err := json.Marshal(disabled)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, exists := fields["startup_trace"]; exists {
		t.Fatal("disabled trace serialized")
	}
	enabled := commandPerfReport{startupTraceEnabled: true}
	tr = enabled.newStartupTrace("serve")
	for i := 0; i < commandPerfStartupRecordLimit+3; i++ {
		tr.end("http_health", tr.begin(), errors.New("non-200"))
	}
	tr.finish(errors.New("deadline"))
	if !tr.Truncated || len(tr.Events) != commandPerfStartupRecordLimit || tr.Status != "failed" {
		t.Fatalf("bound: %+v", tr)
	}
	if got := tr.Totals["http_health"]; got.Count != commandPerfStartupRecordLimit+3 || got.Failures != got.Count {
		t.Fatalf("truncated totals: %+v", got)
	}
	if tr.Totals["terminal"].Failures != 1 {
		t.Fatal("truncation lost terminal failure")
	}
}
