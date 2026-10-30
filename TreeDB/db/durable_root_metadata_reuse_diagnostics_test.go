package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// One predeadline receipt locates a stall in the original fixture. Phase and
// completed-publication count are observations, not a durability frontier. This
// observer never calls the DB or acquires its locks. The byte cap does not bound
// runtime.Stack or writer latency, and termination can prevent any receipt.
type metadataReuseCutWitness4627 struct {
	phase     atomic.Pointer[string]
	completed atomic.Int32
}

func (w *metadataReuseCutWitness4627) mark(phase string) { w.phase.Store(&phase) }

func (w *metadataReuseCutWitness4627) start(mode string, at time.Time, out io.Writer) func() {
	w.mark("child-start")
	started := time.Now()
	stop, done := make(chan struct{}), make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		timer := time.NewTimer(time.Until(at))
		defer timer.Stop()
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		select {
		case <-stop:
			return
		default:
		}
		stack := make([]byte, (64<<10)-512)
		n := runtime.Stack(stack, true)
		header := fmt.Sprintf("metadata-reuse pre-deadline mode=%.32s phase=%.64s completed_publications=%d elapsed=%s stack_truncated=%t\n", mode, *w.phase.Load(), w.completed.Load(), time.Since(started), n == len(stack))
		_, _ = out.Write(append([]byte(header), stack[:n]...))
	}()
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}

type metadataReuseCutReceipt4627 chan []byte

func (out metadataReuseCutReceipt4627) Write(buf []byte) (int, error) {
	out <- buf // The sole write owns this immutable receipt; the slot is buffered.
	return len(buf), nil
}

func TestMetadataReuseCutWitness4627(t *testing.T) {
	t.Run("stopped", func(t *testing.T) {
		var w metadataReuseCutWitness4627
		var out strings.Builder
		stop := w.start("control", time.Now().Add(time.Hour), &out)
		stop()
		stop()
		if out.Len() != 0 {
			t.Fatalf("stopped witness emitted %d bytes", out.Len())
		}
	})
	t.Run("emitted", func(t *testing.T) {
		var w metadataReuseCutWitness4627
		w.completed.Store(37)
		out := make(metadataReuseCutReceipt4627, 1)
		stop := w.start("control", time.Now().Add(-time.Second), out)
		defer stop()
		select {
		case receipt := <-out:
			if len(receipt) > 64<<10 || !strings.Contains(string(receipt), "phase=child-start completed_publications=37") || !strings.Contains(string(receipt), "stack_truncated=") || !strings.Contains(string(receipt), "\ngoroutine ") {
				t.Fatalf("invalid receipt (%d bytes): %s", len(receipt), receipt)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("past timer did not emit")
		}
		stop()
		select {
		case <-out:
			t.Fatal("witness emitted twice")
		default:
		}
	})
}

func formatMetadataReuseCutFailure4627(processErr, contextErr error, elapsed time.Duration, state, cancellation string, output []byte) string {
	return fmt.Sprintf("child did not hit cut: process=%v context=%v elapsed=%s state=%s cancellation={%s}\n%s", processErr, contextErr, elapsed, state, cancellation, output)
}

// Observe the existing CommandContext cancellation callback without changing its
// return value or adding another kill. Calling Kill successfully is a request,
// not proof of why the process exited. The parent reports ProcessState separately.
type metadataReuseCutCancellation4627 struct {
	mu       sync.Mutex
	called   bool
	returned bool
	elapsed  time.Duration
	err      error
}

func (o *metadataReuseCutCancellation4627) observe(cancel func() error, elapsed func() time.Duration) func() error {
	return func() error {
		o.mu.Lock()
		o.called, o.elapsed = true, elapsed()
		o.mu.Unlock()
		err := cancel()
		o.mu.Lock()
		o.returned, o.err = true, err
		o.mu.Unlock()
		return err
	}
}

func (o *metadataReuseCutCancellation4627) snapshot() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return fmt.Sprintf("called=%t returned=%t elapsed=%s error=%v", o.called, o.returned, o.elapsed, o.err)
}

func TestMetadataReuseCutDiagnosticCancellation4627(t *testing.T) {
	for _, cancelErr := range []error{nil, errors.New("kill refused")} {
		t.Run(fmt.Sprint(cancelErr), func(t *testing.T) {
			var observation metadataReuseCutCancellation4627
			if got := observation.snapshot(); got != "called=false returned=false elapsed=0s error=<nil>" {
				t.Fatal(got)
			}
			calls := 0
			cancel := observation.observe(func() error { calls++; return cancelErr }, func() time.Duration { return 20 * time.Second })
			if got := cancel(); got != cancelErr || calls != 1 {
				t.Fatalf("callback result=%v calls=%d", got, calls)
			}
			want := fmt.Sprintf("called=true returned=true elapsed=20s error=%v", cancelErr)
			if got := observation.snapshot(); got != want {
				t.Fatalf("observation=%q want=%q", got, want)
			}
		})
	}
}

func TestMetadataReuseCutDiagnosticFailureObservations4627(t *testing.T) {
	for _, contextErr := range []error{nil, context.DeadlineExceeded} {
		// A deadline observed after Wait is reported alongside, never substituted
		// for the process error or asserted to have caused it.
		got := formatMetadataReuseCutFailure4627(errors.New("exit status 1"), contextErr, 21*time.Second, "exit status 1", "called=false returned=false elapsed=0s error=<nil>", []byte("last child phase\n"))
		for _, want := range []string{"process=exit status 1", fmt.Sprintf("context=%v", contextErr), "elapsed=21s", "state=exit status 1", "cancellation={called=false returned=false elapsed=0s error=<nil>}", "\nlast child phase\n"} {
			if !strings.Contains(got, want) {
				t.Fatalf("missing %q in %q", want, got)
			}
		}
	}
}

// This control exercises CommandContext's real cancellation path. It does not
// reproduce the database failure or establish why a previous child exited.
func TestMetadataReuseCutDiagnosticProcessTermination4627(t *testing.T) {
	const childEnv = "TREEDB_METADATA_REUSE_DIAGNOSTIC_CHILD4627"
	if mode := os.Getenv(childEnv); mode != "" {
		fmt.Fprintln(os.Stderr, "metadata-reuse diagnostic child ready")
		if mode == "exit-one" {
			os.Exit(1)
		}
		time.Sleep(time.Minute) // The parent must terminate this blocked child.
		os.Exit(2)
	}
	for _, mode := range []string{"exit-one", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMetadataReuseCutDiagnosticProcessTermination4627$")
			cmd.Env = append(os.Environ(), childEnv+"="+mode)
			started := time.Now()
			var cancellation metadataReuseCutCancellation4627
			cmd.Cancel = cancellation.observe(cmd.Cancel, func() time.Duration { return time.Since(started) })
			output, err := cmd.CombinedOutput()
			observation := formatMetadataReuseCutFailure4627(err, ctx.Err(), time.Since(started), fmt.Sprint(cmd.ProcessState), cancellation.snapshot(), output)
			t.Log(observation)
			exit, ok := err.(*exec.ExitError)
			if !ok || !strings.Contains(string(output), "metadata-reuse diagnostic child ready") {
				t.Fatalf("control child did not run: %s", observation)
			}
			if mode == "exit-one" {
				if exit.ExitCode() != 1 || ctx.Err() != nil || cancellation.called {
					t.Fatalf("independent exit control: %s", observation)
				}
			} else if ctx.Err() != context.DeadlineExceeded || !cancellation.called || !cancellation.returned || cancellation.err != nil {
				t.Fatalf("deadline termination control: %s", observation)
			}
		})
	}
}
