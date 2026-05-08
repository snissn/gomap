package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

type Client struct {
	conn                  net.Conn
	limits                iwire.Limits
	nextReq               atomic.Uint64
	catalogVersionPlusOne atomic.Uint64
	mu                    sync.Mutex
}

func NewClient(conn net.Conn) *Client {
	return &Client{conn: conn, limits: iwire.DefaultLimits()}
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) Hello(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, iwire.FrameHello, nil, iwire.FrameHelloOK)
	return err
}

func (c *Client) Ping(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, iwire.FramePing, nil, iwire.FramePong)
	return err
}

func (c *Client) Goaway(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, iwire.FrameGoaway, nil, iwire.FrameGoaway)
	return err
}

func (c *Client) Stats(ctx context.Context) (map[string]string, error) {
	sections, err := c.commandSections(ctx, iwire.CommandStats)
	if err != nil {
		return nil, err
	}
	payload, ok, err := singletonSection(sections, iwire.SectionResponseMeta)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, protocolError(iwire.ErrMalformedFrame, "stats response missing response_meta")
	}
	return decodeStringMap(payload)
}

func (c *Client) commandSections(ctx context.Context, commandID iwire.CommandID, sections ...iwire.Section) ([]iwire.Section, error) {
	return c.commandSectionsOnStream(ctx, 0, commandID, sections...)
}

func (c *Client) commandSectionsOnStream(ctx context.Context, streamID uint64, commandID iwire.CommandID, sections ...iwire.Section) ([]iwire.Section, error) {
	body, err := appendCommandRequestBody(nil, commandID, sections...)
	if err != nil {
		return nil, err
	}
	_, response, err := c.roundTripStream(ctx, streamID, iwire.FrameRequest, body, iwire.FrameResponse)
	if err != nil {
		return nil, err
	}
	return iwire.DecodeSections(response, c.limits)
}

func (c *Client) roundTrip(ctx context.Context, typ iwire.FrameType, body []byte, want iwire.FrameType) (iwire.Header, []byte, error) {
	return c.roundTripStream(ctx, 0, typ, body, want)
}

func (c *Client) roundTripStream(ctx context.Context, streamID uint64, typ iwire.FrameType, body []byte, want iwire.FrameType) (iwire.Header, []byte, error) {
	if c == nil || c.conn == nil {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	if ctx != nil && ctx.Err() != nil {
		return iwire.Header{}, nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ctx != nil && ctx.Err() != nil {
		return iwire.Header{}, nil, ctx.Err()
	}
	requestID := c.nextReq.Add(1)
	if deadline, ok := ctxDeadline(ctx); ok {
		_ = c.conn.SetDeadline(deadline)
		defer func() { _ = c.conn.SetDeadline(noDeadline) }()
	}
	stopCancel := c.interruptDeadlineOnContextCancel(ctx)
	defer stopCancel()
	if err := writeFrame(c.conn, iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0},
		Type:      typ,
		StreamID:  streamID,
		RequestID: requestID,
	}, body); err != nil {
		return iwire.Header{}, nil, errorOrContext(ctx, err)
	}
	header, response, err := readFrame(c.conn, c.limits)
	if err != nil {
		return iwire.Header{}, nil, errorOrContext(ctx, err)
	}
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		return header, response, err
	}
	if header.RequestID != requestID {
		return header, response, protocolError(iwire.ErrMalformedFrame, "response request_id %d want %d", header.RequestID, requestID)
	}
	if header.Type == iwire.FrameError {
		return header, response, decodeWireError(response, c.limits)
	}
	if header.Type != want {
		return header, response, protocolError(iwire.ErrMalformedFrame, "response frame type %d want %d", header.Type, want)
	}
	return header, response, nil
}

func (c *Client) interruptDeadlineOnContextCancel(ctx context.Context) func() {
	if c == nil || c.conn == nil || ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = c.conn.SetDeadline(time.Now())
		case <-done:
		}
	}()
	return func() { close(done) }
}

func errorOrContext(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func decodeWireError(body []byte, limits iwire.Limits) error {
	sections, err := iwire.DecodeSections(body, limits)
	if err != nil {
		return err
	}
	payload, ok, err := singletonSection(sections, iwire.SectionError)
	if err != nil {
		return err
	}
	if !ok {
		return protocolError(iwire.ErrMalformedFrame, "error frame missing error section")
	}
	code, retryable, message, err := decodeErrorPayload(payload)
	if err != nil {
		return err
	}
	return &WireError{Code: code, Retryable: retryable, Message: message}
}

func ctxDeadline(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}, false
	}
	return deadline, true
}

var noDeadline time.Time

func isRemoteError(err error, code iwire.ErrorCode) bool {
	var wireErr *WireError
	return errors.As(err, &wireErr) && wireErr.Code == code
}
