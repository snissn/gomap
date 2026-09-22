package nativewire

import (
	"context"
	"errors"
	"net"
	"sync"
)

// Serve authenticates a shard listener and admits each accepted socket before
// starting its goroutine. Cancellation closes this listener and its sockets;
// other groups sharing the node transport continue running.
func (s VectorPartitionShardSearchTCPServerV1) Serve(ctx context.Context, listener net.Listener) error {
	if listener == nil { return ErrNilListener }
	if s.PeerTransport == nil || s.PeerTransport.admission == nil || !s.PeerTransport.groups[s.PeerGroupID][s.PeerTransport.node] { return errPeerAuthenticationV1 }
	var err error
	listener, err = s.PeerTransport.admission.listener(listener)
	if err != nil { return err }
	if ctx == nil { ctx = context.Background() }
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { listener.Close() })
	defer stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	for {
		raw, err := listener.Accept()
		if err != nil {
			stopped := ctx.Err() != nil || errors.Is(err, net.ErrClosed)
			cancel()
			if stopped { return nil }; return err
		}
		conn, err := s.PeerTransport.admission.accept(raw, "shard:"+string(s.PeerGroupID))
		if err != nil { if errors.Is(err, net.ErrClosed) { cancel(); return nil }; continue }
		workers.Add(1)
		go func() {
			defer workers.Done()
			stop := context.AfterFunc(ctx, func() { conn.Close() })
			defer stop()
			s.serveAdmittedConnV1(ctx, conn)
		}()
	}
}
