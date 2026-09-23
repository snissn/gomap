package nativewire

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

const (
	vectorPartitionShardSearchTCPGlobalConnectionsV1 = 64
	vectorPartitionShardSearchTCPGlobalRequestsV1    = 256
	vectorPartitionShardSearchTCPGroupRequestsV1     = 32
)

// One token covers a dial in progress or a socket's entire lifetime, including
// time spent idle. Per-endpoint active-call slots remain a separate bound.
type peerShardBudgetConnV1 struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
	err   error
}

func (c *peerShardBudgetConnV1) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		<-c.slots
	})
	return c.err
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) dialBounded(ctx context.Context, network, address string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case d.connectionSlots <- struct{}{}:
	default:
		if !d.evictIdleConnection() {
			return nil, fmt.Errorf("%w: shard socket budget exhausted (%d/%d), no idle socket", raftcluster.ErrAdmissionUnavailable, len(d.connectionSlots), cap(d.connectionSlots))
		}
		// Another dial may win the freed token. Never queue unbounded callers
		// or evict arbitrary additional sockets in a retry loop.
		select {
		case d.connectionSlots <- struct{}{}:
		default:
			return nil, fmt.Errorf("%w: shard socket budget consumed during idle eviction", raftcluster.ErrAdmissionUnavailable)
		}
	}
	dialCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(d.lifetime, cancel)
	defer func() {
		stop()
		cancel()
	}()
	conn, err := d.dial(dialCtx, network, address)
	if err != nil {
		<-d.connectionSlots
		return nil, err
	}
	return &peerShardBudgetConnV1{Conn: conn, slots: d.connectionSlots}, nil
}

func (d *VectorPartitionShardSearchTCPDispatcherV1) evictIdleConnection() bool {
	var conn net.Conn
	d.mu.Lock()
	for _, pool := range d.pools {
		pool.mu.Lock()
		if n := len(pool.idle); n != 0 {
			conn = pool.idle[n-1]
			pool.idle[n-1] = nil
			pool.idle = pool.idle[:n-1]
			delete(pool.all, conn)
		}
		pool.mu.Unlock()
		if conn != nil {
			break
		}
	}
	d.mu.Unlock()
	if conn == nil {
		return false
	}
	// Close outside pool/dispatcher locks; active borrowers can only acquire
	// sockets still present in idle, so eviction cannot close a live request.
	_ = conn.Close()
	return true
}
