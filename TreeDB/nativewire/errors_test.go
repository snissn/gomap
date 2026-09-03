package nativewire

import (
	"context"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/documentservice"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

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
