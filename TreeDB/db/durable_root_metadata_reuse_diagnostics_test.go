package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// The caller emits two records for each of the fixed 64 publications. A start
// without its completion bounds the last visible operation; it is not proof
// that operation caused a deadline or that an unreported phase was not reached.
func reportMetadataReuseCutPhase4627(w io.Writer, mode, phase string, elapsed, publication time.Duration) {
	fmt.Fprintf(w, "metadata-reuse process-cut mode=%s phase=%s elapsed=%s publication=%s\n", mode, phase, elapsed, publication)
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

func TestMetadataReuseCutDiagnosticPublicationTiming4627(t *testing.T) {
	var output bytes.Buffer
	for i := 1; i <= 64; i++ {
		reportMetadataReuseCutPhase4627(&output, "before-seal", fmt.Sprintf("publication-%02d-start", i), time.Duration(i)*time.Second, 0)
		reportMetadataReuseCutPhase4627(&output, "before-seal", fmt.Sprintf("publication-%02d-complete", i), time.Duration(i)*time.Second+time.Millisecond, time.Millisecond)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 128 || !strings.Contains(lines[0], "publication-01-start elapsed=1s publication=0s") || !strings.Contains(lines[127], "publication-64-complete elapsed=1m4.001s publication=1ms") {
		t.Fatalf("fixed publication records=%d first=%q last=%q", len(lines), lines[0], lines[len(lines)-1])
	}
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
