package nativewire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func TestErrorCodeForNativeSemanticUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want iwire.ErrorCode
	}{
		{"unsupported", backenddb.ErrNativeSemanticUnsupported, iwire.ErrUnsupportedFeature},
		{"metadata_wrapped", metadataWrap(backenddb.ErrNativeSemanticUnsupported), iwire.ErrUnsupportedFeature},
		{"ambiguous", errors.Join(backenddb.ErrNativeSemanticUnsupported, collections.ErrCommitAmbiguous), iwire.ErrCommitAmbiguous},
		{"raft_ambiguous", errors.Join(backenddb.ErrNativeSemanticUnsupported, raftcluster.ErrCommitAmbiguous), iwire.ErrCommitAmbiguous},
		{"recovery", errors.Join(backenddb.ErrNativeSemanticUnsupported, collections.ErrRecoveryRequired), iwire.ErrDurabilityUnavailable},
		{"backend_recovery", errors.Join(backenddb.ErrNativeSemanticUnsupported, backenddb.ErrRecoveryRequired), iwire.ErrDurabilityUnavailable},
		{"documentservice_recovery", &documentservice.Error{Code: documentservice.CodeRecoveryRequired, Err: backenddb.ErrNativeSemanticUnsupported}, iwire.ErrDurabilityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorCodeFor(tc.err); got != tc.want {
				t.Errorf("errorCodeFor(%v)=%d want=%d", tc.err, got, tc.want)
			}
			if retryableError(tc.err, tc.want) {
				t.Errorf("unsupported or uncertain mutation outcome must not be retryable: %v", tc.err)
			}
		})
	}
}

func TestErrorCodeForMutationOutcomeOutranksContext(t *testing.T) {
	if got := errorCodeFor(errors.Join(collections.ErrCommitAmbiguous, context.Canceled)); got != iwire.ErrCommitAmbiguous {
		t.Fatalf("ambiguous+canceled code=%v want=%v", got, iwire.ErrCommitAmbiguous)
	}
	if got := errorCodeFor(errors.Join(collections.ErrRecoveryRequired, context.DeadlineExceeded)); got != iwire.ErrDurabilityUnavailable {
		t.Fatalf("recovery+deadline code=%v want=%v", got, iwire.ErrDurabilityUnavailable)
	}
	if got := errorCodeFor(errors.Join(backenddb.ErrRecoveryRequired, context.Canceled)); got != iwire.ErrDurabilityUnavailable {
		t.Fatalf("backend recovery+canceled code=%v want=%v", got, iwire.ErrDurabilityUnavailable)
	}
}

func TestRetryableErrorKeepsRecoveryRequiredNonRetryable(t *testing.T) {
	for _, err := range []error{
		&documentservice.Error{Code: documentservice.CodeRecoveryRequired, Err: context.Canceled},
		errors.Join(collections.ErrRecoveryRequired, context.DeadlineExceeded),
		errors.Join(backenddb.ErrRecoveryRequired, context.Canceled),
	} {
		if code := errorCodeFor(err); code != iwire.ErrDurabilityUnavailable {
			t.Fatalf("errorCodeFor(%v)=%v want=%v", err, code, iwire.ErrDurabilityUnavailable)
		} else if retryableError(err, code) {
			t.Fatalf("retryableError(%v, %v)=true want false", err, code)
		}
	}
	if err := collections.ErrDurabilityUnavailable; !retryableError(err, errorCodeFor(err)) {
		t.Fatal("ordinary durability-unavailable error must remain retryable")
	}
}

func TestErrorCodeForPreservesWrappedDocumentServiceContextError(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want iwire.ErrorCode
	}{
		{name: "canceled", ctx: canceledContext(), want: iwire.ErrCanceled},
		{name: "deadline", ctx: expiredContext(), want: iwire.ErrTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := documentservice.New(nil).SearchDenseVector(tc.ctx, "docs", documentservice.DenseVectorSearchRequest{})
			if got := errorCodeFor(err); got != tc.want {
				t.Fatalf("errorCodeFor(%v)=%v want %v", err, got, tc.want)
			}
		})
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	cancel()
	return ctx
}
