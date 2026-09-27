package nativewire

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/go-msgpack/v2/codec"
	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const peerRaftMessageMaxV1 = (8 << 20) + (128 << 10)

// Preserve HashiCorp's MessagePack bytes, but bound one complete message before
// its decoder sees it. Snapshot archives stay streamed. This is framing and
// resource admission only; HashiCorp still interprets every consensus message.
type peerRaftWireConnV1 struct {
	net.Conn
	mu                sync.Mutex
	admission         *peerNodeAdmissionV1
	scope             string
	server            bool
	timeout           time.Duration
	base              peerResourceLeaseV1
	request           peerResourceLeaseV1
	parts             []peerResourceLeaseV1
	buffer            []byte
	offset            int
	snapshotRemaining int64
	closed            bool
}

func newPeerRaftWireConnV1(conn net.Conn, admission *peerNodeAdmissionV1, scope string, server bool, timeout time.Duration) (net.Conn, error) {
	if admission == nil {
		return conn, nil
	}
	// Includes upstream bufio/codec initial buffers, small decoded containers,
	// and the bounded response frame. Reserve before handing out the socket.
	base, err := admission.acquire(scope, peerBytesV1, 1<<20)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &peerRaftWireConnV1{Conn: conn, admission: admission, scope: scope, server: server, timeout: timeout, base: base}, nil
}

func (c *peerRaftWireConnV1) releaseMessage() {
	for i := range c.parts {
		c.parts[i].release()
	}
	c.parts = nil
	c.request.release()
	c.buffer = nil
	c.offset = 0
}

func (c *peerRaftWireConnV1) Close() error {
	err := c.Conn.Close() // interrupt a blocked scanner before taking its mutex
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.releaseMessage()
		c.base.release()
	}
	return err
}

func (c *peerRaftWireConnV1) Write(p []byte) (int, error) {
	if c.server {
		c.mu.Lock()
		// NetworkTransport writes its response after the RPC consumer responds.
		// The decoder and FSM have finished using this message at this point.
		c.releaseMessage()
		c.snapshotRemaining = 0
		c.mu.Unlock()
	}
	return c.Conn.Write(p)
}

func (c *peerRaftWireConnV1) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	if c.timeout > 0 && c.server {
		_ = c.Conn.SetReadDeadline(time.Now().Add(c.timeout))
	}
	if c.offset < len(c.buffer) {
		n := copy(p, c.buffer[c.offset:])
		c.offset += n
		return n, nil
	}
	if c.snapshotRemaining > 0 {
		limit := min(int64(len(p)), c.snapshotRemaining, int64(64<<10))
		n, err := c.Conn.Read(p[:limit])
		c.snapshotRemaining -= int64(n)
		return n, err
	}
	c.releaseMessage()
	maxBytes := uint64(128 << 10)
	if c.server {
		maxBytes = peerRaftMessageMaxV1
	}
	scanner := peerMsgpackScannerV1{conn: c, max: maxBytes}
	var kind byte
	if c.server {
		// An idle connection consumes its buffer/connection reservation, but
		// not an active request slot. TLS and this fixed one-byte read are
		// covered by the connection reservation before any decoder is exposed.
		var first [1]byte
		if _, err := io.ReadFull(c.Conn, first[:]); err != nil {
			return 0, err
		}
		kind = first[0]
		if kind > 4 {
			return 0, fmt.Errorf("%w: unknown Raft RPC", raftcluster.ErrRouteTargetUnsupported)
		}
		var err error
		c.request, err = c.admission.acquire(c.scope, peerRequestsV1, 1)
		if err != nil {
			return 0, err
		}
		lease, err := c.admission.acquire(c.scope, peerBytesV1, 4*(64<<10))
		if err != nil {
			return 0, err
		}
		c.parts = append(c.parts, lease)
		c.buffer = append(c.buffer, kind)
		scanner.charged = 64 << 10
		if kind == 2 {
			scanner.max = 128 << 10
		}
	}
	if err := scanner.value(0); err != nil {
		return 0, err
	}
	if !c.server {
		// The upstream response is an error string followed by the typed reply.
		if err := scanner.value(0); err != nil {
			return 0, err
		}
	} else if kind == 2 {
		var snapshot hraft.InstallSnapshotRequest
		if err := codec.NewDecoderBytes(c.buffer[1:], &codec.MsgpackHandle{}).Decode(&snapshot); err != nil {
			return 0, err
		}
		if snapshot.Size < 0 {
			return 0, raftcluster.ErrRouteTargetUnsupported
		}
		c.snapshotRemaining = snapshot.Size
	}
	n := copy(p, c.buffer)
	c.offset = n
	return n, nil
}

// The scanner accepts MessagePack scalars, blobs and bounded containers. It
// rejects declared lengths/counts before allocating or waiting for their body.
type peerMsgpackScannerV1 struct {
	conn    *peerRaftWireConnV1
	max     uint64
	charged uint64
}

func (s *peerMsgpackScannerV1) take(n uint64) ([]byte, error) {
	c := s.conn
	if n > s.max-uint64(len(c.buffer)) {
		return nil, raftcluster.ErrAdmissionUnavailable
	}
	end := uint64(len(c.buffer)) + n
	if c.server && end > s.charged {
		// One lease per 64 KiB tranche, not one per scalar. Four copies cover
		// scanner capacity growth and the decoder's command/data copies.
		charge := min(s.max, (end+(64<<10)-1)&^uint64((64<<10)-1))
		lease, err := c.admission.acquire(c.scope, peerBytesV1, int64(charge-s.charged)*4)
		if err != nil {
			return nil, err
		}
		c.parts = append(c.parts, lease)
		s.charged = charge
	}
	start := len(c.buffer)
	c.buffer = append(c.buffer, make([]byte, int(n))...)
	part := c.buffer[start:]
	_, err := io.ReadFull(c.Conn, part)
	return part, err
}

func (s *peerMsgpackScannerV1) length(bytes uint64) (uint64, error) {
	p, err := s.take(bytes)
	if err != nil {
		return 0, err
	}
	switch bytes {
	case 1:
		return uint64(p[0]), nil
	case 2:
		return uint64(binary.BigEndian.Uint16(p)), nil
	case 4:
		return uint64(binary.BigEndian.Uint32(p)), nil
	}
	return 0, raftcluster.ErrRouteTargetUnsupported
}

func (s *peerMsgpackScannerV1) value(depth int) error {
	if depth > 16 {
		return raftcluster.ErrAdmissionUnavailable
	}
	p, err := s.take(1)
	if err != nil {
		return err
	}
	kind := p[0]
	var payload, count, lengthBytes uint64
	isMap, isContainer, extension := false, false, false
	switch {
	case kind <= 0x7f || kind >= 0xe0 || kind == 0xc0 || kind == 0xc2 || kind == 0xc3:
		return nil
	case kind >= 0xa0 && kind <= 0xbf:
		payload = uint64(kind & 31)
	case kind >= 0x90 && kind <= 0x9f:
		count, isContainer = uint64(kind&15), true
	case kind >= 0x80 && kind <= 0x8f:
		count, isContainer, isMap = uint64(kind&15), true, true
	default:
		switch kind {
		case 0xc4, 0xd9:
			lengthBytes = 1
		case 0xc5, 0xda:
			lengthBytes = 2
		case 0xc6, 0xdb:
			lengthBytes = 4
		case 0xc7:
			lengthBytes, extension = 1, true
		case 0xc8:
			lengthBytes, extension = 2, true
		case 0xc9:
			lengthBytes, extension = 4, true
		case 0xca, 0xce, 0xd2:
			payload = 4
		case 0xcb, 0xcf, 0xd3:
			payload = 8
		case 0xcc, 0xd0:
			payload = 1
		case 0xcd, 0xd1:
			payload = 2
		case 0xd4:
			payload = 2
		case 0xd5:
			payload = 3
		case 0xd6:
			payload = 5
		case 0xd7:
			payload = 9
		case 0xd8:
			payload = 17
		case 0xdc:
			lengthBytes, isContainer = 2, true
		case 0xdd:
			lengthBytes, isContainer = 4, true
		case 0xde:
			lengthBytes, isContainer, isMap = 2, true, true
		case 0xdf:
			lengthBytes, isContainer, isMap = 4, true, true
		default:
			return raftcluster.ErrRouteTargetUnsupported
		}
	}
	if lengthBytes != 0 {
		length, err := s.length(lengthBytes)
		if err != nil {
			return err
		}
		if isContainer {
			count = length
		} else {
			payload = length
		}
	}
	if isContainer {
		if count > 1024 || (isMap && count > 64) {
			return raftcluster.ErrAdmissionUnavailable
		}
		if isMap {
			count *= 2
		}
		for range count {
			if err := s.value(depth + 1); err != nil {
				return err
			}
		}
		return nil
	}
	if extension {
		payload++
	}
	_, err = s.take(payload)
	return err
}
