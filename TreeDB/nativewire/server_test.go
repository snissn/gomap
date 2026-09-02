package nativewire

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func servePipe(t testing.TB, server *Server) (*Client, <-chan error) {
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

func TestServerServeConnNormalizesNilContext(t *testing.T) {
	server := NewServer(ServerOptions{})
	left, right := net.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeConn(nil, right)
	}()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
		_ = server.Close()
	})

	client := NewClient(left)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if err := client.Goaway(ctx); err != nil {
		t.Fatalf("Goaway: %v", err)
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
		"treedb.native_wire.commands.stats.errors_total",
		"treedb.native_wire.errors.code.1",
		"treedb.native_wire.errors.code.22",
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

func TestServerReadCommandWithoutCollectionManagerReturnsWireError(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}

	body, err := appendCommandRequestBody(nil, iwire.CommandOpenScan, collectionNameRef("orders"))
	if err != nil {
		t.Fatalf("append request: %v", err)
	}
	_, _, err = client.roundTrip(ctx, iwire.FrameRequest, body, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrInvalidCommand) {
		t.Fatalf("error=%v want invalid command", err)
	}
}

func TestServerMalformedRequestReturnsWireError(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, errCh := servePipe(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Hello(ctx); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	_, _, err := client.roundTrip(ctx, iwire.FrameRequest, []byte{0xff}, iwire.FrameResponse)
	if !isRemoteError(err, iwire.ErrMalformedFrame) {
		t.Fatalf("error=%v want malformed frame", err)
	}
	if err := client.Ping(ctx); err == nil {
		t.Fatal("Ping after malformed request succeeded; connection should be closed")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeConn returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not close after malformed request")
	}
}

func TestServerRejectsMalformedHelloBody(t *testing.T) {
	server := NewServer(ServerOptions{})
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

	body := []byte{0x80}
	if err := writeFrame(left, iwire.Header{Type: iwire.FrameHello, RequestID: 1}, body); err != nil {
		t.Fatalf("write malformed hello: %v", err)
	}
	header, response, err := readFrame(left, iwire.DefaultLimits())
	if err != nil {
		t.Fatalf("read hello error: %v", err)
	}
	if header.Type != iwire.FrameError {
		t.Fatalf("malformed hello response type=%d want error", header.Type)
	}
	if !isRemoteError(decodeWireError(response, iwire.DefaultLimits()), iwire.ErrMalformedFrame) {
		t.Fatalf("malformed hello body did not decode as malformed frame")
	}

	request, err := appendCommandRequestBody(nil, iwire.CommandStats)
	if err != nil {
		t.Fatalf("append stats request: %v", err)
	}
	if err := writeFrame(left, iwire.Header{Type: iwire.FrameRequest, RequestID: 2}, request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	header, response, err = readFrame(left, iwire.DefaultLimits())
	if err != nil {
		t.Fatalf("read request error: %v", err)
	}
	if header.Type != iwire.FrameError || !isRemoteError(decodeWireError(response, iwire.DefaultLimits()), iwire.ErrInvalidCommand) {
		t.Fatalf("request after bad hello response type=%d body=%x", header.Type, response)
	}
	_ = left.Close()
	select {
	case <-errCh:
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not return after client close")
	}
}

func TestServerRejectsCriticalUnknownHelloSection(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)
	body, err := iwire.AppendSection(nil, iwire.Section{ID: 9000, Flags: iwire.SectionFlagCritical})
	if err != nil {
		t.Fatalf("append hello section: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, err = client.roundTrip(ctx, iwire.FrameHello, body, iwire.FrameHelloOK)
	if !isRemoteError(err, iwire.ErrUnsupportedFeature) {
		t.Fatalf("critical hello err=%v want unsupported feature", err)
	}
}

func TestPayloadDecodersRejectMalformedScalars(t *testing.T) {
	if _, err := decodeStringMap([]byte{0x80, 0x00}); codeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeStringMap non-minimal count err=%v code=%d", err, codeOf(err))
	}

	payload := appendErrorPayload(nil, iwire.ErrInvalidCommand, false, "bad")
	payload[uvarintLen(uint64(iwire.ErrInvalidCommand))] = 2
	if _, _, _, err := decodeErrorPayload(payload); codeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeErrorPayload retryable err=%v code=%d", err, codeOf(err))
	}

	if _, _, _, err := decodeErrorPayload([]byte{0x80, 0x00, 0, 0}); codeOf(err) != iwire.ErrMalformedFrame {
		t.Fatalf("decodeErrorPayload non-minimal code err=%v code=%d", err, codeOf(err))
	}
}

func TestServerClosesPostHandshakeUnsupportedVersion(t *testing.T) {
	server := NewServer(ServerOptions{})
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

	if err := writeFrame(left, iwire.Header{Type: iwire.FrameHello, RequestID: 1}, nil); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	header, _, err := readFrame(left, iwire.DefaultLimits())
	if err != nil {
		t.Fatalf("read hello_ok: %v", err)
	}
	if header.Type != iwire.FrameHelloOK {
		t.Fatalf("hello response type=%d want hello_ok", header.Type)
	}
	if err := writeFrame(left, iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0 + 1},
		Type:      iwire.FramePing,
		RequestID: 2,
	}, nil); err != nil {
		t.Fatalf("write bad-version ping: %v", err)
	}
	header, body, err := readFrame(left, iwire.DefaultLimits())
	if err != nil {
		t.Fatalf("read version error: %v", err)
	}
	if header.Type != iwire.FrameError {
		t.Fatalf("bad-version response type=%d want error", header.Type)
	}
	if !isRemoteError(decodeWireError(body, iwire.DefaultLimits()), iwire.ErrUnsupportedVersion) {
		t.Fatalf("bad-version body did not decode as unsupported version")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ServeConn returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ServeConn did not close after post-handshake version error")
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

func TestClientPingAllowsNilContext(t *testing.T) {
	server := NewServer(ServerOptions{})
	client, _ := servePipe(t, server)
	if err := client.Hello(nil); err != nil {
		t.Fatalf("Hello nil context: %v", err)
	}
	if err := client.Ping(nil); err != nil {
		t.Fatalf("Ping nil context: %v", err)
	}
}

func TestClientClosesAfterProtocolReadError(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := NewClientWithMaxFrameSize(left, 64)
	errCh := make(chan error, 1)
	go func() {
		header, _, err := readFrame(right, iwire.DefaultLimits())
		if err != nil {
			errCh <- err
			return
		}
		var headerBuf [iwire.FrameHeaderLenV1]byte
		frameHeader, err := iwire.AppendHeader(headerBuf[:0], iwire.Header{
			Type:      iwire.FramePong,
			RequestID: header.RequestID,
			BodyLen:   client.limits.MaxFrameSize + 1,
		})
		if err != nil {
			errCh <- err
			return
		}
		if err := writeAll(right, frameHeader); err != nil {
			errCh <- err
			return
		}
		_, _, err = readFrame(right, iwire.DefaultLimits())
		if err == nil {
			errCh <- errors.New("client connection remained open after protocol read error")
			return
		}
		errCh <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx); codeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("Ping err=%v code=%d want resource exhausted", err, codeOf(err))
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

func TestClientRoundTripHonorsCancellationWithoutDeadline(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := NewClient(left)
	started := make(chan struct{})
	serverErr := make(chan error, 1)
	go func() {
		_, _, err := readFrame(right, iwire.DefaultLimits())
		close(started)
		if err != nil {
			serverErr <- err
			return
		}
		_, _, err = readFrame(right, iwire.DefaultLimits())
		serverErr <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- client.Ping(ctx)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server did not receive ping")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Ping err=%v want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Ping did not return after cancel")
	}
	select {
	case err := <-serverErr:
		if err == nil {
			t.Fatal("server read after cancellation succeeded; client connection was reused")
		}
	case <-time.After(time.Second):
		t.Fatal("server did not observe client close after cancel")
	}
}

func TestClientCancelDeadlineStopWaitsForRunningCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &blockingDeadlineConn{
		entered: make(chan time.Time, 1),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	client := &Client{conn: conn}
	stop := client.interruptDeadlineOnContextCancel(ctx)
	cancel()
	select {
	case deadline := <-conn.entered:
		if deadline.IsZero() {
			t.Fatal("cancel callback set zero deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel callback did not set deadline")
	}
	stopDone := make(chan struct{})
	go func() {
		stop()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("stop returned before running cancel callback completed")
	case <-time.After(25 * time.Millisecond):
	}
	close(conn.release)
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("stop did not return after cancel callback completed")
	}
	select {
	case <-conn.done:
	case <-time.After(time.Second):
		t.Fatal("cancel callback did not complete")
	}
}

type blockingDeadlineConn struct {
	net.Conn
	entered chan time.Time
	release chan struct{}
	done    chan struct{}
}

func (c *blockingDeadlineConn) SetDeadline(t time.Time) error {
	if !t.IsZero() {
		c.entered <- t
		<-c.release
		close(c.done)
	}
	return nil
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

func TestRequestBodySizingRejectsIntOverflow(t *testing.T) {
	if _, err := addRequestBodyLen(1, maxInt); codeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("addRequestBodyLen err=%v want %d", err, iwire.ErrResourceExhausted)
	}
	if _, err := growRequestBody(make([]byte, 1), maxInt); codeOf(err) != iwire.ErrResourceExhausted {
		t.Fatalf("growRequestBody err=%v want %d", err, iwire.ErrResourceExhausted)
	}
}

func codeOf(err error) iwire.ErrorCode {
	code, _ := iwire.ErrorCodeOf(err)
	return code
}
