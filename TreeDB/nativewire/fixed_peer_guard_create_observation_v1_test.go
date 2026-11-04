package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http/httptrace"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type fixedPeerGuardCreateObservationKeyV1 struct{}

// This test-only receipt observes the first original guard-create Submit. Later
// original definite-precommit retries are counted, never added or retried here.
// No transport, response body, request bytes, or returned error is retained.
type fixedPeerGuardCreateReceiptV1 struct {
	ScopeStartUnixNanos, ScopeElapsedNanos      int64
	ScopeDeadline, ScopeCanceled                bool
	Schema                                      uint32
	Attempts                                    uint32
	RequestTimeoutNanos                         int64
	OuterDeadlineUnixNanos, OuterRemainingNanos int64
	SubmitStartUnixNanos, SubmitElapsedNanos    int64
	WroteCallbacks, FirstByteCallbacks          uint32
	WroteRequest, WroteRequestOK                bool
	WroteRequestElapsedNanos                    int64
	FirstResponseByte, Returned                 bool
	FirstResponseByteElapsedNanos               int64
	ResultAck                                   uint8
	CommittedRecoverable, CommittedApplied      bool
	Ambiguous, Deadline, Canceled, BodyDeadline bool
	OuterDeadline, OuterCanceled, ResultError   bool
	WroteDeadline, WroteCanceled, WroteError    bool
}

type fixedPeerGuardCreateObservationV1 struct {
	mu      sync.Mutex
	receipt fixedPeerGuardCreateReceiptV1
	start   time.Time
	scope   time.Time
}

func newFixedPeerGuardCreateObservationV1(ctx context.Context, timeout time.Duration) *fixedPeerGuardCreateObservationV1 {
	now := time.Now()
	o := &fixedPeerGuardCreateObservationV1{scope: now, receipt: fixedPeerGuardCreateReceiptV1{Schema: 1, ScopeStartUnixNanos: now.UnixNano(), RequestTimeoutNanos: int64(timeout)}}
	if deadline, ok := ctx.Deadline(); ok {
		o.receipt.OuterDeadlineUnixNanos = deadline.UnixNano()
		o.receipt.OuterRemainingNanos = int64(deadline.Sub(now))
	}
	return o
}

func fixedPeerGuardCreateSubmitContextV1(ctx context.Context, id iwire.CommandID, key string) (context.Context, *fixedPeerGuardCreateObservationV1) {
	o, _ := ctx.Value(fixedPeerGuardCreateObservationKeyV1{}).(*fixedPeerGuardCreateObservationV1)
	if o == nil || id != iwire.CommandCreateCollection || key != "guard-create" {
		return ctx, nil
	}
	o.mu.Lock()
	o.receipt.Attempts++
	if o.receipt.Attempts != 1 {
		o.mu.Unlock()
		return ctx, nil
	}
	o.start = time.Now()
	o.receipt.SubmitStartUnixNanos = o.start.UnixNano()
	if deadline, ok := ctx.Deadline(); ok {
		o.receipt.OuterDeadlineUnixNanos = deadline.UnixNano()
		o.receipt.OuterRemainingNanos = int64(deadline.Sub(o.start))
	}
	o.mu.Unlock()
	// WithClientTrace composes with any existing trace. The production call's
	// own WroteRequest callback is composed later and remains authoritative.
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			o.mu.Lock()
			defer o.mu.Unlock()
			o.receipt.WroteCallbacks++
			o.receipt.WroteRequest = true
			o.receipt.WroteRequestOK = info.Err == nil
			o.receipt.WroteError = info.Err != nil
			o.receipt.WroteDeadline = errors.Is(info.Err, context.DeadlineExceeded)
			o.receipt.WroteCanceled = errors.Is(info.Err, context.Canceled)
			o.receipt.WroteRequestElapsedNanos = int64(time.Since(o.start))
		},
		GotFirstResponseByte: func() {
			o.mu.Lock()
			defer o.mu.Unlock()
			o.receipt.FirstByteCallbacks++
			o.receipt.FirstResponseByte = true
			o.receipt.FirstResponseByteElapsedNanos = int64(time.Since(o.start))
		},
	}), o
}

func (o *fixedPeerGuardCreateObservationV1) returned(ctx context.Context, result raftcluster.SubmitResultV1, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.receipt.Returned = true
	o.receipt.SubmitElapsedNanos = int64(time.Since(o.start))
	o.receipt.ResultAck = uint8(result.ActualAck)
	o.receipt.CommittedRecoverable = result.CommittedRecoverable
	o.receipt.CommittedApplied = result.CommittedApplied
	o.receipt.ResultError = err != nil
	o.receipt.Ambiguous = errors.Is(err, raftcluster.ErrCommitAmbiguous)
	o.receipt.Deadline = errors.Is(err, context.DeadlineExceeded)
	o.receipt.Canceled = errors.Is(err, context.Canceled)
	// Go 1.26.10 cancelTimerBody.Read adds this exact text only after Do has
	// returned a response and the original response-body read fails at timeout.
	// This is evidence of that client boundary, never server commit/apply state.
	var timeout net.Error
	o.receipt.BodyDeadline = errors.As(err, &timeout) && timeout.Timeout() && strings.Contains(err.Error(), "(Client.Timeout or context cancellation while reading body)")
	o.receipt.OuterDeadline = errors.Is(ctx.Err(), context.DeadlineExceeded)
	o.receipt.OuterCanceled = errors.Is(ctx.Err(), context.Canceled)
}

func (o *fixedPeerGuardCreateObservationV1) finish(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.receipt.ScopeElapsedNanos = int64(time.Since(o.scope))
	o.receipt.ScopeDeadline = errors.Is(ctx.Err(), context.DeadlineExceeded)
	o.receipt.ScopeCanceled = errors.Is(ctx.Err(), context.Canceled)
}

func (o *fixedPeerGuardCreateObservationV1) snapshot() fixedPeerGuardCreateReceiptV1 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.receipt
}

func TestFixedPeerGuardCreateObservationPassThroughV1(t *testing.T) {
	t.Logf("guard-create-observation-size-v1: receipt=%d observer=%d; context/trace composition adds diagnostic-only allocations", unsafe.Sizeof(fixedPeerGuardCreateReceiptV1{}), unsafe.Sizeof(fixedPeerGuardCreateObservationV1{}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	o := newFixedPeerGuardCreateObservationV1(ctx, 2*time.Second)
	ctx = context.WithValue(ctx, fixedPeerGuardCreateObservationKeyV1{}, o)
	oldWrites, oldHeaders := 0, 0
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest:         func(httptrace.WroteRequestInfo) { oldWrites++ },
		GotFirstResponseByte: func() { oldHeaders++ },
	})
	observed, active := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create")
	if active != o || observed.Err() != ctx.Err() {
		t.Fatal("observer missing or context state changed")
	}
	d1, ok1 := ctx.Deadline()
	d2, ok2 := observed.Deadline()
	if ok1 != ok2 || !d1.Equal(d2) {
		t.Fatal("observer changed deadline")
	}
	productionWrites := 0
	observed = httptrace.WithClientTrace(observed, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { productionWrites++ }})
	trace := httptrace.ContextClientTrace(observed)
	trace.WroteRequest(httptrace.WroteRequestInfo{})
	trace.GotFirstResponseByte()
	result := raftcluster.SubmitResultV1{ActualAck: iwire.AckRaftCommitted, CommittedRecoverable: true, CommittedApplied: true}
	o.returned(ctx, result, nil)
	r := o.snapshot()
	if oldWrites != 1 || oldHeaders != 1 || productionWrites != 1 || !r.WroteRequestOK || !r.FirstResponseByte || !r.Returned || r.ResultError || r.ResultAck != uint8(result.ActualAck) || !r.CommittedRecoverable || !r.CommittedApplied || r.RequestTimeoutNanos != int64(2*time.Second) {
		t.Fatalf("trace composition/result receipt: %+v old=%d/%d production=%d", r, oldWrites, oldHeaders, productionWrites)
	}
	if plain, active := fixedPeerGuardCreateSubmitContextV1(context.Background(), iwire.CommandCreateCollection, "guard-create"); plain != context.Background() || active != nil {
		t.Fatal("unobserved context changed")
	}
	for _, item := range []struct {
		id  iwire.CommandID
		key string
	}{{iwire.CommandInsertBatch, "guard-create"}, {iwire.CommandCreateCollection, "other"}} {
		if plain, active := fixedPeerGuardCreateSubmitContextV1(ctx, item.id, item.key); plain != ctx || active != nil {
			t.Fatal("non-target call observed")
		}
	}
	if again, active := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create"); again != ctx || active != nil || o.snapshot().Attempts != 2 {
		t.Fatal("later original attempt changed or uncounted")
	}
}

func TestFixedPeerGuardCreateObservationErrorsV1(t *testing.T) {
	for _, tc := range []struct {
		name                                    string
		err                                     error
		wrote, header, body, deadline, canceled bool
	}{
		{"pre-send", io.ErrClosedPipe, false, false, false, false, false},
		{"header-deadline", errors.Join(raftcluster.ErrCommitAmbiguous, context.DeadlineExceeded), true, false, false, true, false},
		{"partial-header-deadline", errors.Join(raftcluster.ErrCommitAmbiguous, context.DeadlineExceeded), true, true, false, true, false},
		{"body-deadline", errors.Join(raftcluster.ErrCommitAmbiguous, fixedPeerGuardCreateBodyTimeoutV1{}), true, true, true, true, false},
		{"body-text-without-timeout-interface", errors.New("context deadline exceeded (Client.Timeout or context cancellation while reading body)"), true, true, false, false, false},
		{"canceled", errors.Join(raftcluster.ErrCommitAmbiguous, context.Canceled), true, false, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			o := newFixedPeerGuardCreateObservationV1(context.Background(), 2*time.Second)
			ctx = context.WithValue(ctx, fixedPeerGuardCreateObservationKeyV1{}, o)
			traced, _ := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create")
			trace := httptrace.ContextClientTrace(traced)
			if tc.wrote {
				trace.WroteRequest(httptrace.WroteRequestInfo{Err: tc.err})
			}
			if tc.header {
				trace.GotFirstResponseByte()
			}
			cancel()
			o.returned(ctx, raftcluster.SubmitResultV1{}, tc.err)
			r := o.snapshot()
			if r.WroteRequest != tc.wrote || r.FirstResponseByte != tc.header || r.BodyDeadline != tc.body || r.Deadline != tc.deadline || r.Canceled != tc.canceled || !r.ResultError || !r.OuterCanceled || r.WroteRequestOK || r.CommittedApplied || r.CommittedRecoverable {
				t.Fatalf("original error/context classification: %+v", r)
			}
		})
	}
}

func TestFixedPeerGuardCreateObservationConcurrentTraceV1(t *testing.T) {
	o := newFixedPeerGuardCreateObservationV1(context.Background(), 2*time.Second)
	ctx := context.WithValue(context.Background(), fixedPeerGuardCreateObservationKeyV1{}, o)
	traced, _ := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create")
	trace := httptrace.ContextClientTrace(traced)
	var joined sync.WaitGroup
	joined.Add(2)
	go func() { defer joined.Done(); trace.WroteRequest(httptrace.WroteRequestInfo{}) }()
	go func() { defer joined.Done(); trace.GotFirstResponseByte() }()
	o.returned(ctx, raftcluster.SubmitResultV1{}, io.EOF)
	_ = o.snapshot()
	joined.Wait()
	r := o.snapshot()
	if !r.WroteRequestOK || !r.FirstResponseByte || !r.Returned {
		t.Fatalf("lost joined trace receipt: %+v", r)
	}
}

// This control error models the documented net/http timeout error interface;
// it is not a network result or evidence that a server command committed.
type fixedPeerGuardCreateBodyTimeoutV1 struct{}

func (fixedPeerGuardCreateBodyTimeoutV1) Error() string {
	return "context deadline exceeded (Client.Timeout or context cancellation while reading body)"
}
func (fixedPeerGuardCreateBodyTimeoutV1) Timeout() bool   { return true }
func (fixedPeerGuardCreateBodyTimeoutV1) Temporary() bool { return true }
func (fixedPeerGuardCreateBodyTimeoutV1) Is(err error) bool {
	return err == context.DeadlineExceeded
}

func TestFixedPeerGuardCreateObservationUnwindV1(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		o := newFixedPeerGuardCreateObservationV1(context.Background(), 2*time.Second)
		ctx := context.WithValue(context.Background(), fixedPeerGuardCreateObservationKeyV1{}, o)
		closed := false
		var caught any
		func() {
			defer func() { caught = recover() }()
			defer func() { closed = true }()
			defer func() {
				o.finish(ctx)
				_ = o.snapshot()
			}()
			traced, _ := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create")
			httptrace.ContextClientTrace(traced).WroteRequest(httptrace.WroteRequestInfo{})
			panic("original observer caller panic")
		}()
		if caught != "original observer caller panic" || !closed || o.snapshot().Returned {
			t.Fatalf("panic/defer behavior changed: panic=%v closed=%v receipt=%+v", caught, closed, o.snapshot())
		}
	})
	t.Run("goexit", func(t *testing.T) {
		o := newFixedPeerGuardCreateObservationV1(context.Background(), 2*time.Second)
		ctx := context.WithValue(context.Background(), fixedPeerGuardCreateObservationKeyV1{}, o)
		joined := make(chan struct{})
		closed := false
		go func() {
			defer close(joined)
			defer func() { closed = true }()
			defer func() {
				o.finish(ctx)
				_ = o.snapshot()
			}()
			traced, _ := fixedPeerGuardCreateSubmitContextV1(ctx, iwire.CommandCreateCollection, "guard-create")
			httptrace.ContextClientTrace(traced).WroteRequest(httptrace.WroteRequestInfo{})
			runtime.Goexit()
		}()
		// Join unconditionally. The owning finite Go test timeout is the outer guard.
		<-joined
		if !closed || o.snapshot().Returned || !o.snapshot().WroteRequestOK {
			t.Fatalf("Goexit/defer behavior changed: closed=%v receipt=%+v", closed, o.snapshot())
		}
	})
}
