package nativewire

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

// Client serializes requests on one native-wire connection.
//
// Byte slices returned by result APIs are immutable views into the client's
// response buffer. They remain valid until the next round trip on the same
// client; callers that need to keep them longer must copy them.
type Client struct {
	conn                          net.Conn
	local                         *localEndpoint
	limits                        iwire.Limits
	nextReq                       atomic.Uint64
	catalogVersionPlusOne         atomic.Uint64
	mu                            sync.Mutex
	requestBody                   []byte
	writeBody                     []byte
	readBody                      []byte
	vectorSections                []iwire.Section
	denseRequest                  []byte
	denseIDs                      [][]byte
	denseDocuments                [][]byte
	denseResults                  []DenseVectorSearchResult
	denseTypedNegotiated          bool
	denseTypedQuantizedNegotiated bool
}

// NewClient returns a native-wire client that owns conn until Close.
func NewClient(conn net.Conn) *Client {
	return &Client{conn: conn, limits: iwire.DefaultLimits()}
}

// NewClientWithMaxFrameSize returns a native-wire client with an explicit frame bound.
func NewClientWithMaxFrameSize(conn net.Conn, maxFrameSize uint64) *Client {
	client := NewClient(conn)
	if maxFrameSize != 0 {
		client.limits.MaxFrameSize = maxFrameSize
	}
	return client
}

// Close closes the underlying client connection.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if c.local != nil {
		return c.local.close()
	}
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Hello performs the native-wire hello handshake.
func (c *Client) Hello(ctx context.Context) error {
	if c == nil {
		return io.ErrClosedPipe
	}
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.denseTypedNegotiated = false
	c.denseTypedQuantizedNegotiated = false
	_, response, err := c.roundTripLocked(ctx, iwire.FrameHello, nil, iwire.FrameHelloOK)
	if err != nil {
		return err
	}
	sections, err := iwire.DecodeSections(response, c.limits)
	if err != nil {
		return err
	}
	raw, found, err := singletonSection(sections, iwire.SectionCapabilitySet)
	if err != nil || !found {
		return err
	}
	caps, err := decodeStringMap(raw)
	if err != nil {
		return err
	}
	for _, version := range strings.Split(caps["dense_vector_search_versions"], ",") {
		if version == "2" {
			c.denseTypedNegotiated = true
		}
		if version == "3" {
			c.denseTypedQuantizedNegotiated = true
		}
	}
	return nil
}

// Ping sends a ping frame and waits for a pong response.
func (c *Client) Ping(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, iwire.FramePing, nil, iwire.FramePong)
	return err
}

// Goaway asks the server to close the connection gracefully.
func (c *Client) Goaway(ctx context.Context) error {
	_, _, err := c.roundTrip(ctx, iwire.FrameGoaway, nil, iwire.FrameGoaway)
	return err
}

// Stats fetches the server stats map over the native-wire stats command.
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
	if c == nil {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx != nil && ctx.Err() != nil {
		return iwire.Header{}, nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.roundTripLockedStream(ctx, streamID, typ, body, want, false)
}

func (c *Client) roundTripLocked(ctx context.Context, typ iwire.FrameType, body []byte, want iwire.FrameType) (iwire.Header, []byte, error) {
	return c.roundTripLockedStream(ctx, 0, typ, body, want, false)
}

func (c *Client) roundTripLockedStream(ctx context.Context, streamID uint64, typ iwire.FrameType, body []byte, want iwire.FrameType, denseWorkAllowed bool) (iwire.Header, []byte, error) {
	version := uint64(0)
	if denseWorkAllowed {
		version = iwire.DenseVectorSearchTypedVersion
	}
	return c.roundTripLockedStreamVersion(ctx, streamID, typ, body, want, version)
}

func (c *Client) roundTripLockedStreamVersion(ctx context.Context, streamID uint64, typ iwire.FrameType, body []byte, want iwire.FrameType, denseVersion uint64) (iwire.Header, []byte, error) {
	if c == nil {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	c.clearBorrowedResponseViews()
	c.readBody = retainSmallPayloadScratch(c.readBody)
	if c.local != nil {
		header, response, err := c.local.roundTripVersion(ctx, streamID, typ, c.nextReq.Add(1), body, want, c.limits, c.readBody, true, denseVersion)
		c.readBody = response[:0]
		return header, response, err
	}
	if c.conn == nil {
		return iwire.Header{}, nil, io.ErrClosedPipe
	}
	if ctx != nil && ctx.Err() != nil {
		return iwire.Header{}, nil, ctx.Err()
	}
	requestID := c.nextReq.Add(1)
	if deadline, ok := ctxDeadline(ctx); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	stopCancel := c.interruptDeadlineOnContextCancel(ctx)
	defer func() {
		stopCancel()
		_ = c.conn.SetDeadline(noDeadline)
	}()
	onWire := true
	var err error
	c.writeBody, err = writeFrameBuffered(c.conn, iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0},
		Type:      typ,
		StreamID:  streamID,
		RequestID: requestID,
	}, body, c.writeBody)
	if err != nil {
		return iwire.Header{}, nil, c.closeOnProtocolError(c.errorOrCanceledOnWire(ctx, onWire, err))
	}
	header, response, err := readFrameInto(c.conn, c.limits, c.readBody)
	if err != nil {
		return iwire.Header{}, nil, c.closeOnProtocolError(c.errorOrCanceledOnWire(ctx, onWire, err))
	}
	c.readBody = response[:0]
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		return header, response, c.closeOnProtocolError(err)
	}
	if header.RequestID != requestID {
		return header, response, c.closeOnProtocolError(protocolError(iwire.ErrMalformedFrame, "response request_id %d want %d", header.RequestID, requestID))
	}
	if header.Type == iwire.FrameError {
		return header, response, c.closeOnProtocolError(decodeWireErrorVersion(response, c.limits, denseVersion))
	}
	if header.Type != want {
		return header, response, c.closeOnProtocolError(protocolError(iwire.ErrMalformedFrame, "response frame type %d want %d", header.Type, want))
	}
	return header, response, nil
}

func (c *Client) roundTripLockedDiscardResponse(ctx context.Context, typ iwire.FrameType, body []byte, want iwire.FrameType) error {
	if c == nil {
		return io.ErrClosedPipe
	}
	c.clearBorrowedResponseViews()
	c.readBody = retainSmallPayloadScratch(c.readBody)
	if c.local != nil {
		_, _, err := c.local.roundTrip(ctx, 0, typ, c.nextReq.Add(1), body, want, c.limits, nil, false, false)
		return err
	}
	if c.conn == nil {
		return io.ErrClosedPipe
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	requestID := c.nextReq.Add(1)
	if deadline, ok := ctxDeadline(ctx); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	stopCancel := c.interruptDeadlineOnContextCancel(ctx)
	defer func() {
		stopCancel()
		_ = c.conn.SetDeadline(noDeadline)
	}()
	onWire := true
	var err error
	c.writeBody, err = writeFrameBuffered(c.conn, iwire.Header{
		Version:   iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0},
		Type:      typ,
		RequestID: requestID,
	}, body, c.writeBody)
	if err != nil {
		return c.errorOrCanceledOnWire(ctx, onWire, err)
	}
	header, response, err := readFrameInto(c.conn, c.limits, c.readBody)
	if err != nil {
		return c.closeOnProtocolError(c.errorOrCanceledOnWire(ctx, onWire, err))
	}
	c.readBody = response[:0]
	if err := iwire.ValidateHeaderVersion(header, iwire.Version{Major: iwire.ProtocolMajorV1, Minor: iwire.ProtocolMinorV0}); err != nil {
		return c.closeOnProtocolError(err)
	}
	if header.RequestID != requestID {
		return c.closeOnProtocolError(protocolError(iwire.ErrMalformedFrame, "response request_id %d want %d", header.RequestID, requestID))
	}
	if header.Type == iwire.FrameError {
		return decodeWireError(response, c.limits, false)
	}
	if header.Type != want {
		return c.closeOnProtocolError(protocolError(iwire.ErrMalformedFrame, "response frame type %d want %d", header.Type, want))
	}
	return nil
}

func (c *Client) clearBorrowedResponseViews() {
	clear(c.vectorSections[:cap(c.vectorSections)])
	c.vectorSections = c.vectorSections[:0]
	clear(c.denseIDs[:cap(c.denseIDs)])
	if cap(c.denseIDs) > maxRetainedGetManyScratchItems {
		c.denseIDs = nil
	} else {
		c.denseIDs = c.denseIDs[:0]
	}
	clear(c.denseDocuments[:cap(c.denseDocuments)])
	if cap(c.denseDocuments) > maxRetainedGetManyScratchItems {
		c.denseDocuments = nil
	} else {
		c.denseDocuments = c.denseDocuments[:0]
	}
	clear(c.denseResults[:cap(c.denseResults)])
	if cap(c.denseResults) > maxRetainedGetManyScratchItems {
		c.denseResults = nil
	} else {
		c.denseResults = c.denseResults[:0]
	}
}

func (c *Client) interruptDeadlineOnContextCancel(ctx context.Context) func() {
	if c == nil || c.conn == nil || ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(done)
		_ = c.conn.SetDeadline(time.Now())
	})
	return func() {
		if stop() {
			close(done)
		}
		<-done
	}
}

func (c *Client) errorOrCanceledOnWire(ctx context.Context, onWire bool, err error) error {
	if ctx != nil {
		ctxErr := ctx.Err()
		if ctxErr == nil {
			if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
				ctxErr = context.DeadlineExceeded
			}
		}
		if ctxErr != nil {
			if onWire && c != nil && c.conn != nil {
				_ = c.conn.Close()
			}
			return ctxErr
		}
	}
	return err
}

func (c *Client) closeOnProtocolError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := iwire.ErrorCodeOf(err); ok && c != nil && c.conn != nil {
		_ = c.conn.Close()
	}
	return err
}

func decodeWireError(body []byte, limits iwire.Limits, denseWorkAllowed bool) error {
	version := uint64(0)
	if denseWorkAllowed {
		version = iwire.DenseVectorSearchTypedVersion
	}
	return decodeWireErrorVersion(body, limits, version)
}

func decodeWireErrorVersion(body []byte, limits iwire.Limits, denseVersion uint64) error {
	sections, err := iwire.DecodeSections(body, limits)
	if err != nil {
		return err
	}
	for _, section := range sections {
		denseWorkAllowed := denseVersion >= iwire.DenseVectorSearchTypedVersion
		if section.ID == iwire.SectionDenseSearchWork && !denseWorkAllowed {
			return protocolError(iwire.ErrMalformedFrame, "dense error work is unavailable for this call")
		}
		if section.ID == iwire.SectionDenseSearchScorePlaneProof && denseVersion != iwire.DenseVectorSearchTypedQuantizedVersion {
			return protocolError(iwire.ErrMalformedFrame, "dense score-plane proof is unavailable for this call")
		}
		if denseWorkAllowed && section.ID != iwire.SectionError && section.ID != iwire.SectionDenseSearchWork && section.ID != iwire.SectionDenseSearchScorePlaneProof && section.Flags&iwire.SectionFlagCritical != 0 {
			return protocolError(iwire.ErrUnsupportedFeature, "unknown critical dense error section")
		}
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
	out := &WireError{Code: code, Retryable: retryable, Message: message}
	workRaw, workFound, workErr := singletonSection(sections, iwire.SectionDenseSearchWork)
	if workErr == nil && workFound {
		work, err := decodeDenseWork(workRaw)
		workErr = err
		if workErr == nil {
			out.DenseWork = &work
		}
	}
	scoreRaw, scoreFound, scoreErr := singletonSection(sections, iwire.SectionDenseSearchScorePlaneProof)
	if scoreErr == nil && scoreFound {
		if denseVersion != iwire.DenseVectorSearchTypedQuantizedVersion {
			scoreErr = protocolError(iwire.ErrMalformedFrame, "dense score-plane proof does not match command version")
		} else {
			proof, err := decodeDenseScorePlane(scoreRaw, limits)
			scoreErr = err
			if scoreErr == nil {
				out.ScorePlane = &proof
			}
		}
	}
	if workErr != nil {
		return &DenseVectorSearchDecodeError{Err: workErr, ScorePlane: out.ScorePlane}
	}
	if scoreErr != nil {
		return &DenseVectorSearchDecodeError{Err: scoreErr, DenseWork: out.DenseWork}
	}
	return out
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
