package nativewire

// This file provides the M8 production-topology transport boundary for M5
// shard search.  It deliberately carries the existing validated M5 envelope
// rather than introducing a second shard protocol.  The coordinator still
// owns fanout, retry, and no-partial-result semantics.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const vectorPartitionShardSearchTCPMaxFrameBytesV1 uint32 = 64 << 20

// VectorPartitionShardSearchTCPDispatcherV1 is a bounded, length-framed TCP
// dispatcher for distinct M5 service endpoints.  It is intentionally not an
// in-process registry: every request and response crosses a serialized socket
// boundary before the coordinator can consume it.
type VectorPartitionShardSearchTCPDispatcherV1 struct {
	endpoints map[raftcluster.GroupID]string
	dial      func(context.Context, string, string) (net.Conn, error)
	maxFrame  uint32
}

// NewVectorPartitionShardSearchTCPDispatcherV1 validates and copies one TCP
// endpoint per group.  Endpoints are normally loopback addresses for the M8 CI
// topology and may be separate hosts in a deeper deployment.
func NewVectorPartitionShardSearchTCPDispatcherV1(endpoints map[raftcluster.GroupID]string) (*VectorPartitionShardSearchTCPDispatcherV1, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("nativewire: M5 TCP dispatcher requires endpoints")
	}
	copyEndpoints := make(map[raftcluster.GroupID]string, len(endpoints))
	for group, endpoint := range endpoints {
		if group == "" || endpoint == "" {
			return nil, errors.New("nativewire: M5 TCP dispatcher endpoint is incomplete")
		}
		copyEndpoints[group] = endpoint
	}
	dialer := &net.Dialer{}
	return &VectorPartitionShardSearchTCPDispatcherV1{
		endpoints: copyEndpoints,
		dial:      dialer.DialContext,
		maxFrame:  vectorPartitionShardSearchTCPMaxFrameBytesV1,
	}, nil
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) DispatchVectorPartitionShardSearchV1(ctx context.Context, request VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error) {
	if d == nil || d.dial == nil || d.maxFrame == 0 {
		return VectorPartitionShardSearchResponseV1{}, errors.New("nativewire: M5 TCP dispatcher is not configured")
	}
	endpoint, ok := d.endpoints[request.TargetGroupID]
	if !ok {
		return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorUnknownOwnerV1, GroupID: request.TargetGroupID, Err: ErrVectorPartitionShardSearchRouteMismatch}
	}
	conn, err := d.dial(ctx, "tcp", endpoint)
	if err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, err)
	}
	defer conn.Close()
	stopCancelIO := vectorPartitionShardSearchTCPInterruptOnCancelV1(ctx, conn)
	defer stopCancelIO()
	if err := vectorPartitionShardSearchTCPDeadlineV1(ctx, request.DeadlineUnixNano, conn); err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, err)
	}
	frame := vectorPartitionShardSearchTCPFrameV1{Request: &request}
	if err := writeVectorPartitionShardSearchTCPFrameV1(conn, frame, d.maxFrame); err != nil {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, err)
	}
	frame, err = readVectorPartitionShardSearchTCPFrameV1(conn, d.maxFrame)
	if err != nil {
		if request.DeadlineUnixNano != 0 && !time.Now().Before(time.Unix(0, request.DeadlineUnixNano)) {
			return VectorPartitionShardSearchResponseV1{}, &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: request.TargetGroupID, Err: context.DeadlineExceeded}
		}
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, err)
	}
	if frame.Request != nil || (frame.Response == nil) == (frame.Error == nil) {
		return VectorPartitionShardSearchResponseV1{}, vectorPartitionShardSearchTCPTransportErrorV1(ctx, request.TargetGroupID, errors.New("ambiguous M5 response frame"))
	}
	if frame.Error != nil {
		return VectorPartitionShardSearchResponseV1{}, frame.Error.toError()
	}
	return *frame.Response, nil
}

// VectorPartitionShardSearchTCPServerV1 serves one M5 service over the same
// bounded framing contract used by the dispatcher.
type VectorPartitionShardSearchTCPServerV1 struct {
	Service        VectorPartitionShardSearchHandlerV1
	MaxFrame       uint32
	InitialTimeout time.Duration
}

// VectorPartitionShardSearchHandlerV1 is the narrow server dependency needed
// by the TCP boundary.  The production service satisfies it directly.
type VectorPartitionShardSearchHandlerV1 interface {
	Search(context.Context, VectorPartitionShardSearchRequestV1) (VectorPartitionShardSearchResponseV1, error)
}

func (s VectorPartitionShardSearchTCPServerV1) ServeConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	maxFrame := s.MaxFrame
	if maxFrame == 0 {
		maxFrame = vectorPartitionShardSearchTCPMaxFrameBytesV1
	}
	initialTimeout := s.InitialTimeout
	if initialTimeout < 0 {
		return
	}
	if initialTimeout == 0 {
		initialTimeout = 5 * time.Second
	}
	_ = conn.SetReadDeadline(time.Now().Add(initialTimeout))
	frame, err := readVectorPartitionShardSearchTCPFrameV1(conn, maxFrame)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil || frame.Request == nil || frame.Response != nil || frame.Error != nil {
		// A peer that sends no frame (or never reads) must not strand this server
		// goroutine while we try to report the bounded framing failure.
		_ = conn.SetWriteDeadline(time.Now().Add(initialTimeout))
		_ = writeVectorPartitionShardSearchTCPFrameV1(conn, vectorPartitionShardSearchTCPFrameV1{Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorInvalidRequestV1, Message: "invalid M5 TCP request"}}, maxFrame)
		return
	}
	if s.Service == nil {
		s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Error: &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: frame.Request.TargetGroupID, Message: "M5 service is unavailable"}}, maxFrame, time.Now().Add(initialTimeout))
		return
	}
	requestCtx, cancel := vectorPartitionShardSearchTCPRequestContextV1(ctx, frame.Request.DeadlineUnixNano)
	defer cancel()
	stopPeerMonitor := vectorPartitionShardSearchTCPMonitorPeerDisconnectV1(conn, cancel)
	response, err := s.Service.Search(requestCtx, *frame.Request)
	stopPeerMonitor()
	writeDeadline := time.Now().Add(initialTimeout)
	if deadline, ok := requestCtx.Deadline(); ok {
		writeDeadline = deadline
	}
	if err != nil {
		s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Error: vectorPartitionShardSearchTCPErrorFromErrorV1(err)}, maxFrame, writeDeadline)
		return
	}
	s.writeFrame(conn, vectorPartitionShardSearchTCPFrameV1{Response: &response}, maxFrame, writeDeadline)
}

func (s VectorPartitionShardSearchTCPServerV1) writeFrame(conn net.Conn, frame vectorPartitionShardSearchTCPFrameV1, maxFrame uint32, deadline time.Time) {
	_ = conn.SetWriteDeadline(deadline)
	_ = writeVectorPartitionShardSearchTCPFrameV1(conn, frame, maxFrame)
}

type vectorPartitionShardSearchTCPFrameV1 struct {
	Request  *VectorPartitionShardSearchRequestV1  `json:"request,omitempty"`
	Response *VectorPartitionShardSearchResponseV1 `json:"response,omitempty"`
	Error    *vectorPartitionShardSearchTCPErrorV1 `json:"error,omitempty"`
}

type vectorPartitionShardSearchTCPErrorV1 struct {
	Code       VectorPartitionShardSearchErrorCodeV1 `json:"code"`
	GroupID    raftcluster.GroupID                   `json:"group_id,omitempty"`
	LeaderHint raftcluster.NodeID                    `json:"leader_hint,omitempty"`
	Message    string                                `json:"message"`
}

func vectorPartitionShardSearchTCPErrorFromErrorV1(err error) *vectorPartitionShardSearchTCPErrorV1 {
	var shardErr *VectorPartitionShardSearchErrorV1
	if errors.As(err, &shardErr) {
		return &vectorPartitionShardSearchTCPErrorV1{Code: shardErr.Code, GroupID: shardErr.GroupID, LeaderHint: shardErr.LeaderHint, Message: shardErr.Error()}
	}
	return &vectorPartitionShardSearchTCPErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, Message: err.Error()}
}

func (e vectorPartitionShardSearchTCPErrorV1) toError() error {
	if e.Code == "" {
		e.Code = VectorPartitionShardSearchErrorGroupUnavailableV1
	}
	return &VectorPartitionShardSearchErrorV1{Code: e.Code, GroupID: e.GroupID, LeaderHint: e.LeaderHint, Err: errors.New(e.Message)}
}

func writeVectorPartitionShardSearchTCPFrameV1(w io.Writer, frame vectorPartitionShardSearchTCPFrameV1, max uint32) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if len(raw) == 0 || uint64(len(raw)) > uint64(max) {
		return fmt.Errorf("M5 TCP frame is outside %d-byte bound", max)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(raw)))
	if err := writeVectorPartitionShardSearchTCPAllV1(w, prefix[:]); err != nil {
		return err
	}
	return writeVectorPartitionShardSearchTCPAllV1(w, raw)
}

func writeVectorPartitionShardSearchTCPAllV1(w io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := w.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}

func readVectorPartitionShardSearchTCPFrameV1(r io.Reader, max uint32) (vectorPartitionShardSearchTCPFrameV1, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	size := binary.BigEndian.Uint32(prefix[:])
	if size == 0 || size > max {
		return vectorPartitionShardSearchTCPFrameV1{}, fmt.Errorf("M5 TCP frame size %d is outside %d-byte bound", size, max)
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(r, raw); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	var frame vectorPartitionShardSearchTCPFrameV1
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&frame); err != nil {
		return vectorPartitionShardSearchTCPFrameV1{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return vectorPartitionShardSearchTCPFrameV1{}, errors.New("M5 TCP frame has trailing JSON values")
	}
	return frame, nil
}

func vectorPartitionShardSearchTCPDeadlineV1(ctx context.Context, deadlineUnixNano int64, conn net.Conn) error {
	deadline := time.Time{}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if value, ok := ctx.Deadline(); ok {
			deadline = value
		}
	}
	if deadlineUnixNano != 0 {
		requestDeadline := time.Unix(0, deadlineUnixNano)
		if deadline.IsZero() || requestDeadline.Before(deadline) {
			deadline = requestDeadline
		}
	}
	if !deadline.IsZero() {
		return conn.SetDeadline(deadline)
	}
	return nil
}

func vectorPartitionShardSearchTCPInterruptOnCancelV1(ctx context.Context, conn net.Conn) func() {
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			// A deadline wakes both Read and Write without racing the deferred Close.
			_ = conn.SetDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}

func vectorPartitionShardSearchTCPRequestContextV1(ctx context.Context, deadlineUnixNano int64) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if deadlineUnixNano == 0 {
		return context.WithCancel(ctx)
	}
	deadline := time.Unix(0, deadlineUnixNano)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(ctx, deadline)
}

// vectorPartitionShardSearchTCPMonitorPeerDisconnectV1 is safe because the
// framing contract carries exactly one request per connection.  After the
// request has been decoded no further peer bytes are valid, so a bounded read
// loop can turn an EOF/reset into cancellation of remote M5 work. Stopping the
// monitor interrupts its outstanding read before waiting, so an ordinary
// successful response is not delayed by the polling deadline. Read deadlines
// are cleared when the normal request completes and do not affect the response
// write deadline.
func vectorPartitionShardSearchTCPMonitorPeerDisconnectV1(conn net.Conn, cancel context.CancelFunc) func() {
	if conn == nil || cancel == nil {
		return func() {}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	var deadlineMu sync.Mutex
	var stopOnce sync.Once
	go func() {
		defer close(done)
		var discard [1]byte
		for {
			deadlineMu.Lock()
			select {
			case <-stop:
				deadlineMu.Unlock()
				return
			default:
			}
			_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			deadlineMu.Unlock()
			_, err := conn.Read(discard[:])
			if err == nil {
				// Extra input is invalid under the one-request contract. Treat it
				// as a disconnected/abandoned caller rather than extending work.
				cancel()
				return
			}
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				cancel()
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				select {
				case <-stop:
					return
				default:
					continue
				}
			}
			cancel()
			return
		}
	}()
	return func() {
		stopOnce.Do(func() {
			close(stop)
			deadlineMu.Lock()
			// Wake a currently blocked Read immediately; otherwise waiting for done
			// can impose the full peer-poll timeout on a successful response.
			_ = conn.SetReadDeadline(time.Now())
			deadlineMu.Unlock()
			<-done
			_ = conn.SetReadDeadline(time.Time{})
		})
	}
}

func vectorPartitionShardSearchTCPTransportErrorV1(ctx context.Context, groupID raftcluster.GroupID, err error) error {
	if ctx != nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if errors.Is(err, context.Canceled) {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorCanceledV1, GroupID: groupID, Err: err}
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: groupID, Err: err}
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorDeadlineV1, GroupID: groupID, Err: err}
	}
	return &VectorPartitionShardSearchErrorV1{Code: VectorPartitionShardSearchErrorGroupUnavailableV1, GroupID: groupID, Err: err}
}
