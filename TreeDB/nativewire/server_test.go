package nativewire

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func servePipe(t *testing.T, server *Server) (*Client, <-chan error) {
	t.Helper()
	left, right := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeConn(context.Background(), right)
	}()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
		_ = server.Close()
	})
	return NewClient(left), errCh
}

func TestServerControlHelloPingStatsGoaway(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, errCh := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	stats, err := client.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	for _, key := range []string{
		"treedb.native_wire.connections.opened_total",
		"treedb.native_wire.frames.in_total",
		"treedb.native_wire.frames.out_total",
		"treedb.native_wire.requests.started_total",
		"treedb.native_wire.requests.completed_total",
		"treedb.native_wire.commands.stats.requests_total",
	} {
		if _, ok := stats[key]; !ok {
			t.Fatalf("stats missing %s in %#v", key, stats)
		}
	}
	goawayHeader, goawayBody, err := client.roundTrip(ctx, iwire.FrameGoaway, nil, iwire.FrameGoaway)
	if err != nil {
		t.Fatalf("Goaway: %v", err)
	}
	sections, err := iwire.DecodeSections(goawayBody, iwire.DefaultLimits())
	if err != nil {
		t.Fatalf("decode goaway body: %v", err)
	}
	rawMeta, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		t.Fatalf("goaway response_meta: %v", err)
	}
	if !ok {
		t.Fatal("goaway missing response_meta")
	}
	meta, err := decodeStringMap(rawMeta)
	if err != nil {
		t.Fatalf("decode goaway response_meta: %v", err)
	}
	if got, want := meta["last_accepted_request_id"], strconv.FormatUint(goawayHeader.RequestID, 10); got != want {
		t.Fatalf("last_accepted_request_id=%q want %q", got, want)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeConn returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after goaway")
	}
}

func TestServerRequestUnsupportedCommandReturnsWireError(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	body, err := appendCommandRequestBody(nil, iwire.CommandOpenScan, iwire.Section{ID: iwire.SectionCollectionRef, Bytes: []byte("orders")})
	if err != nil {
		t.Fatalf("append request: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("error=%v want unsupported feature", err)
	}
}

func TestServerMalformedRequestReturnsWireError(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, _, err := client.roundTrip(ctx, iwire.FrameRequest, []byte{0xff}, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrMalformedFrame) {
		t.Fatalf("error=%v want malformed frame", err)
	}
}

func TestServerRejectsRequestBeforeHello(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, err := appendCommandRequestBody(nil, iwire.CommandStats)
	if err != nil {
		t.Fatalf("append request: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("error=%v want invalid command", err)
	}
}

func TestClientDetectsResponseRequestIDMismatch(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := NewClient(left)
	errCh := make(chan error, 1)
	go func() {
		header, _, err := readFrame(right, iwire.DefaultLimits())
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeFrame(right, iwire.Header{Type: iwire.FramePong, RequestID: header.RequestID + 1}, nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Ping(ctx)
	if err == nil || !strings.Contains(err.Error(), "request_id") {
		t.Fatalf("Ping error=%v want request_id mismatch", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server goroutine: %v", err)
	}
}

func TestClientRejectsUnsupportedResponseVersion(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := NewClient(left)
	errCh := make(chan error, 1)
	go func() {
		header, _, err := readFrame(right, iwire.DefaultLimits())
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeFrame(right, iwire.Header{
			Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0 + 1},
			Type:      iwire.FramePong,
			RequestID: header.RequestID,
		}, nil)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Ping(ctx)
	if code, ok := iwire.ErrorCodeOf(err); !ok || code != iwire.ErrUnsupportedVersion {
		t.Fatalf("Ping error=%v want unsupported version", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server goroutine: %v", err)
	}
}

func TestClientDetectsErrorResponseRequestIDMismatch(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := NewClient(left)
	errCh := make(chan error, 1)
	go func() {
		header, _, err := readFrame(right, iwire.DefaultLimits())
		if err != nil {
			errCh <- err
			return
		}
		body, err := iwire.AppendSection(nil, iwire.Section{
			ID:    iwire.SectionError,
			Bytes: appendErrorPayload(nil, iwire.ErrInvalidCommand, false, "wrong request"),
		})
		if err != nil {
			errCh <- err
			return
		}
		errCh <- writeFrame(right, iwire.Header{Type: iwire.FrameError, RequestID: header.RequestID + 1}, body)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Ping(ctx)
	if err == nil || !strings.Contains(err.Error(), "request_id") {
		t.Fatalf("Ping error=%v want request_id mismatch", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server goroutine: %v", err)
	}
}
