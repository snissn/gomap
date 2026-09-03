package nativewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// ErrNilListener is returned when Serve is called with a nil listener. It wraps
// net.ErrClosed so callers that classify listener shutdown keep working.
var ErrNilListener = fmt.Errorf("nativewire: nil listener: %w", net.ErrClosed)

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		return ErrServerClosed
	}
	if ln == nil {
		return ErrNilListener
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.registerListener(ln) {
		_ = ln.Close()
		return ErrServerClosed
	}
	defer s.unregisterListener(ln)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if s.closed.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !s.registerConn(conn) {
			_ = conn.Close()
			if s.closed.Load() {
				return nil
			}
			continue
		}
		go func(conn net.Conn) {
			_ = s.serveRegisteredConn(ctx, conn)
		}(conn)
	}
}

func DialContext(ctx context.Context, network, address string) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	client := NewClient(conn)
	if err := client.Hello(ctx); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func NewInProcessClient(ctx context.Context, server *Server) (*Client, func() error, error) {
	if server == nil || server.closed.Load() {
		return nil, nil, ErrServerClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	local, ok := newLocalEndpoint(server)
	if !ok {
		return nil, nil, ErrServerClosed
	}
	client := &Client{local: local, limits: server.limits}
	if err := client.Hello(ctx); err != nil {
		_ = local.close()
		return nil, nil, err
	}
	cleanup := func() error {
		return client.Close()
	}
	return client, cleanup, nil
}

type localEndpoint struct {
	server *Server
	state  *connState
	done   chan struct{}
	closed atomic.Bool
	mu     sync.Mutex
	frame  []byte
}

func newLocalEndpoint(server *Server) (*localEndpoint, bool) {
	state := &connState{id: uint64(server.nextConn.Add(1))}
	done := make(chan struct{})
	endpoint := &localEndpoint{server: server, state: state, done: done}
	if !server.registerLocalEndpoint(endpoint) {
		close(done)
		endpoint.closed.Store(true)
		return nil, false
	}
	server.startCursorReaper()
	return endpoint, true
}

func (e *localEndpoint) close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.closeLocked()
}

func (e *localEndpoint) closeLocked() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	if e.done != nil {
		close(e.done)
	}
	if e.server != nil {
		_ = e.state.closeVectorPinned()
		e.server.killCursorsForOwner(e.state.id)
		e.server.unregisterLocalEndpoint(e)
	}
	return nil
}

func (e *localEndpoint) Write(p []byte) (int, error) {
	if e == nil || e.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	e.frame = append(e.frame, p...)
	return len(p), nil
}

func (e *localEndpoint) roundTrip(ctx context.Context, streamID uint64, typ iwire.FrameType, requestID uint64, body []byte, want iwire.FrameType, limits iwire.Limits, responseDst []byte, copyResponse bool) (iwire.Header, []byte, error) {
	if e == nil {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.server == nil || e.closed.Load() || e.server.closed.Load() {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx != nil && ctx.Err() != nil {
		return iwire.Header{}, nil, ctx.Err()
	}
	frameLen := uint64(iwire.FrameHeaderLenV1) + uint64(len(body))
	if frameLen < uint64(len(body)) {
		return iwire.Header{}, nil, protocolError(iwire.ErrMalformedFrame, "request frame length overflow")
	}
	if frameLen > e.server.limits.MaxFrameSize {
		return iwire.Header{}, nil, protocolError(iwire.ErrResourceExhausted, "request frame length %d exceeds limit %d", frameLen, e.server.limits.MaxFrameSize)
	}
	e.frame = e.frame[:0]
	request := iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0},
		Type:      typ,
		StreamID:  streamID,
		RequestID: requestID,
	}
	err := e.server.handleFrame(ctx, e, e.state, request, body)
	if errors.Is(err, errGoaway) {
		_ = e.closeLocked()
		err = nil
	}
	if err != nil {
		return iwire.Header{}, nil, err
	}
	if len(e.frame) < int(iwire.FrameHeaderLenV1) {
		return iwire.Header{}, nil, protocolError(iwire.ErrMalformedFrame, "response frame too short: %d", len(e.frame))
	}
	header, err := iwire.DecodeHeader(e.frame[:iwire.FrameHeaderLenV1], limits)
	if err != nil {
		return iwire.Header{}, nil, err
	}
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		return header, nil, err
	}
	response := e.frame[iwire.FrameHeaderLenV1:]
	if uint64(len(response)) != header.BodyLen {
		return header, nil, protocolError(iwire.ErrMalformedFrame, "response body len %d want %d", len(response), header.BodyLen)
	}
	if copyResponse && len(response) > 0 {
		if len(response) <= cap(responseDst) {
			responseDst = responseDst[:len(response)]
		} else {
			responseDst = make([]byte, len(response))
		}
		copy(responseDst, response)
		response = responseDst
	}
	e.frame = retainSmallPayloadScratch(e.frame)
	if header.Type == iwire.FrameError {
		return header, response, decodeWireError(response, limits)
	}
	if header.Type != want {
		return header, response, protocolError(iwire.ErrMalformedFrame, "response frame type %d want %d", header.Type, want)
	}
	if header.RequestID != requestID {
		return header, response, protocolError(iwire.ErrMalformedFrame, "response request_id %d want %d", header.RequestID, requestID)
	}
	return header, response, nil
}
