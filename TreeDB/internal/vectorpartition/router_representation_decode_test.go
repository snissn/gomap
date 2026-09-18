package vectorpartition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// The syntax error lies beyond several reader blocks. Cancellation during
// parsing must win before the parser reaches it, not only in model validation.
func TestRouterRepresentationDecodeCancelsDuringParsing(t *testing.T) {
	raw := []byte(`{"nodes":[` + strings.Repeat(`{"leaf_members":[0]},`, 4096) + `invalid]}`)
	ctx := &representationPollContext{Context: context.Background(), limit: 3}
	m, err := DecodeRouterRepresentationV1(ctx, raw, 1<<20)
	if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(m, RouterRepresentationModelV1{}) {
		t.Fatalf("parse cancellation polls=%d model nodes=%d err=%v", ctx.polls, len(m.Nodes), err)
	}
}

func TestRouterRepresentationDecodeRejectsTrailingWithoutPartialModel(t *testing.T) {
	p, o := representationFixtureV1()
	m, err := BuildRouterRepresentationForDiagnosticsV1(t.Context(), p, o)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	for _, tail := range []string{` {}`, `[` + strings.Repeat(`0,`, 100000) + `0]`, "\v", `"unterminated`} {
		got, err := DecodeRouterRepresentationV1(t.Context(), append(append([]byte(nil), raw...), tail...), 1<<20)
		if err == nil || !reflect.DeepEqual(got, RouterRepresentationModelV1{}) {
			t.Fatalf("trailing data returned model nodes=%d err=%v", len(got.Nodes), err)
		}
	}
	// Only JSON's four whitespace bytes are legal after the first object.
	got, err := DecodeRouterRepresentationV1(nil, append(raw, []byte(" \t\r\n")...), 1<<20)
	if err != nil || !reflect.DeepEqual(got, m) {
		t.Fatal("valid whitespace/nil context", err)
	}
}

func TestRouterRepresentationDecodeReaderBoundsAndTrailingCancellation(t *testing.T) {
	ctx := &representationPollContext{Context: context.Background(), limit: 3}
	r := &representationContextReaderV1{ctx: ctx, reader: bytes.NewReader(make([]byte, 128<<10))}
	buf := make([]byte, 128<<10)
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		if err != nil || n != 16<<10 {
			t.Fatalf("unbounded read n=%d err=%v", n, err)
		}
	}
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read n=%d err=%v", n, err)
	}
	if _, err := io.ReadAll(&representationContextReaderV1{reader: bytes.NewReader([]byte("x"))}); err != nil {
		t.Fatal("nil context", err)
	}
	ctx = &representationPollContext{Context: context.Background(), limit: 4}
	if err := representationJSONTrailingV1(ctx, []byte(strings.Repeat(" ", 64<<10))); !errors.Is(err, context.Canceled) {
		t.Fatal("whitespace scan bypassed cancellation", err)
	}
	ctx = &representationPollContext{Context: context.Background(), limit: 4}
	if err := representationJSONTrailingV1(ctx, []byte("["+strings.Repeat("0,", 100000)+"0]")); err == nil || errors.Is(err, context.Canceled) || ctx.polls > 2 {
		t.Fatal("trailing value was scanned instead of rejecting its first byte", ctx.polls, err)
	}
}
