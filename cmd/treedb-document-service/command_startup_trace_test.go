package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/nativewire"
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

// This subprocess serves only readiness protocols; it never opens a database.
func TestCommandStartupHelperProcess(t *testing.T) {
	mode := os.Getenv("GOMAP_COMMAND_STARTUP_TEST_HELPER")
	if mode == "" {
		t.Skip("integration helper only")
	}
	var addr string
	for i, arg := range os.Args {
		if arg == "-addr" && i+1 < len(os.Args) {
			addr = os.Args[i+1]
		}
	}
	if addr == "" {
		t.Fatal("missing helper address")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	ctx, limit := context.WithTimeout(ctx, 10*time.Second)
	defer limit()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var done = make(chan struct{})
	switch mode {
	case "http", "http-retry":
		var requests atomic.Int32
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/health" {
				http.NotFound(w, r)
				return
			}
			if requests.Add(1) == 1 && mode == "http-retry" {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			fmt.Fprint(w, "readiness body")
		})}
		go func() { defer close(done); _ = server.Serve(listener) }()
		<-ctx.Done()
		_ = server.Close()
	case "native":
		server := nativewire.NewServer(nativewire.ServerOptions{})
		go func() { defer close(done); _ = server.Serve(ctx, listener) }()
		<-ctx.Done()
		_ = listener.Close()
		_ = server.Close()
	case "native-no-hello":
		go func() {
			defer close(done)
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				fmt.Println("native-no-hello accepted connection")
				_ = conn.Close()
			}
		}()
		<-ctx.Done()
		_ = listener.Close()
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	<-done
}

func TestCommandStartupRealPreStartFailures(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, stage := range []string{"log-open", "spawn"} {
			t.Run(fmt.Sprintf("enabled=%v/%s", enabled, stage), func(t *testing.T) {
				dir := t.TempDir()
				log := filepath.Join(dir, "startup.log")
				if stage == "log-open" {
					if err := os.WriteFile(log, []byte("preserve"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				report := commandPerfReport{startupTraceEnabled: enabled}
				tr := report.newStartupTrace("reopen")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				process, err := startCommandPerf(ctx, log, filepath.Join(dir, "missing-executable"), dir, true, false, tr)
				if err == nil || process != nil {
					t.Fatalf("expected pre-start failure: %v %+v", err, process)
				}
				if !enabled {
					if report.StartupTrace != nil {
						t.Fatal("disabled trace created")
					}
					return
				}
				if tr.Status != "failed" || tr.Totals["terminal"].Failures != 1 {
					t.Fatalf("lost failure: %+v", tr)
				}
				if stage == "log-open" {
					if len(tr.Events) != 1 {
						t.Fatalf("unexpected pre-spawn records: %+v", tr.Events)
					}
					contents, e := os.ReadFile(log)
					if e != nil || string(contents) != "preserve" {
						t.Fatal("existing log changed")
					}
				} else if tr.Totals["spawn"].Count != 1 || tr.Totals["spawn"].Failures != 1 {
					t.Fatalf("missing spawn failure: %+v", tr)
				}
			})
		}
	}
}

func TestCommandStartupRealReadiness(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("readiness helper uses /bin/sh; accounting and pre-start failure tests remain portable")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		mode          string
		enabled, fail bool
	}{
		{"http", false, false}, {"http-retry", true, false}, {"native", false, false}, {"native", true, false}, {"native-no-hello", true, true},
	} {
		t.Run(fmt.Sprintf("%s/trace=%v", test.mode, test.enabled), func(t *testing.T) {
			t.Setenv("GOMAP_COMMAND_STARTUP_TEST_HELPER", test.mode)
			dir := t.TempDir()
			wrapper := filepath.Join(dir, "helper")
			// exec preserves direct-child ownership; production args follow -- so the
			// test binary's flag parser leaves the command's flags for the helper.
			script := "#!/bin/sh\nexec '" + strings.ReplaceAll(binary, "'", "'\"'\"'") + "' -test.run=^TestCommandStartupHelperProcess$ -- \"$@\"\n"
			if err := os.WriteFile(wrapper, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			report := commandPerfReport{startupTraceEnabled: test.enabled}
			tr := report.newStartupTrace("serve")
			timeout := 5 * time.Second
			if test.fail {
				timeout = 3 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			process, err := startCommandPerf(ctx, filepath.Join(dir, "start.log"), wrapper, dir, strings.HasPrefix(test.mode, "http"), false, tr)
			if test.fail {
				if err == nil || process != nil || tr.Status != "failed" || tr.Totals["native_hello"].Failures == 0 {
					t.Fatalf("TCP without Hello accepted: %v %+v", err, tr)
				}
				raw, readErr := os.ReadFile(filepath.Join(dir, "start.log"))
				if readErr != nil || !strings.Contains(string(raw), "native-no-hello accepted connection") {
					t.Fatalf("no accepted TCP connection: %v %s", readErr, raw)
				}
				return
			}
			if err != nil {
				raw, _ := os.ReadFile(filepath.Join(dir, "start.log"))
				t.Fatalf("start: %v\n%s", err, raw)
			}
			if err := process.stop(); err != nil {
				t.Fatalf("helper cleanup: %v", err)
			}
			if !test.enabled {
				if tr != nil || report.StartupTrace != nil {
					t.Fatal("disabled trace created")
				}
				return
			}
			kind := "native_hello"
			if strings.HasPrefix(test.mode, "http") {
				kind = "http_health"
			}
			total := tr.Totals[kind]
			if tr.Status != "ready" || total.Count <= total.Failures || tr.Totals["terminal"].Failures != 0 {
				t.Fatalf("readiness accounting: %+v", tr)
			}
			if test.mode == "http-retry" {
				observed503 := false
				for _, event := range tr.Events {
					if event.Kind == "http_health" && event.Error == "health status503" {
						observed503 = true
					}
				}
				if !observed503 || total.Failures == 0 || tr.Totals["poll_sleep"].Count == 0 {
					t.Fatalf("503 retry absent: %+v", tr)
				}
			}
		})
	}
}
