package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

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
