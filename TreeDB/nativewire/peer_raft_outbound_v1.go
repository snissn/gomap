package nativewire

import (
	"context"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

func (t *peerRaftTransportV1) appendWork(args *hraft.AppendEntriesRequest) (peerWorkLeaseV1, error) {
	if args == nil || len(args.Entries) > 1 {
		return peerWorkLeaseV1{}, raftcluster.ErrRouteTargetUnsupported
	}
	bytes := int64(64 << 10)
	for _, entry := range args.Entries {
		if entry == nil || len(entry.Data) > fixedPeerMaxRPCBytesV1 || len(entry.Extensions) > 64<<10 {
			return peerWorkLeaseV1{}, raftcluster.ErrRouteTargetUnsupported
		}
		bytes += int64(len(entry.Data)+len(entry.Extensions)) * 4
	}
	return t.admission.work(t.scope, peerRequestsV1, bytes)
}

func (t *peerRaftTransportV1) AppendEntries(id hraft.ServerID, target hraft.ServerAddress, args *hraft.AppendEntriesRequest, response *hraft.AppendEntriesResponse) error {
	work, err := t.appendWork(args)
	if err != nil {
		return err
	}
	defer work.release()
	return t.NetworkTransport.AppendEntries(id, target, args, response)
}

func (t *peerRaftTransportV1) RequestVote(id hraft.ServerID, target hraft.ServerAddress, args *hraft.RequestVoteRequest, response *hraft.RequestVoteResponse) error {
	work, err := t.admission.work(t.scope, peerRequestsV1, 64<<10)
	if err != nil {
		return err
	}
	defer work.release()
	return t.NetworkTransport.RequestVote(id, target, args, response)
}

func (t *peerRaftTransportV1) RequestPreVote(id hraft.ServerID, target hraft.ServerAddress, args *hraft.RequestPreVoteRequest, response *hraft.RequestPreVoteResponse) error {
	work, err := t.admission.work(t.scope, peerRequestsV1, 64<<10)
	if err != nil {
		return err
	}
	defer work.release()
	return t.NetworkTransport.RequestPreVote(id, target, args, response)
}

func (t *peerRaftTransportV1) TimeoutNow(id hraft.ServerID, target hraft.ServerAddress, args *hraft.TimeoutNowRequest, response *hraft.TimeoutNowResponse) error {
	work, err := t.admission.work(t.scope, peerRequestsV1, 64<<10)
	if err != nil {
		return err
	}
	defer work.release()
	return t.NetworkTransport.TimeoutNow(id, target, args, response)
}

type peerAppendFutureV1 struct {
	hraft.AppendFuture
	done chan struct{}
	once sync.Once
	err  error
	work peerWorkLeaseV1
}

func (f *peerAppendFutureV1) Error() error { <-f.done; return f.err }
func (f *peerAppendFutureV1) complete(err error) {
	f.once.Do(func() { f.err = err; f.work.release(); close(f.done) })
}

type peerAppendPipelineV1 struct {
	hraft.AppendPipeline
	owner    *peerRaftTransportV1
	mu       sync.Mutex
	pending  map[hraft.AppendFuture]*peerAppendFutureV1
	consumer chan hraft.AppendFuture
	ctx      context.Context
	cancel   context.CancelFunc
	closed   bool
}

func (t *peerRaftTransportV1) AppendEntriesPipeline(id hraft.ServerID, target hraft.ServerAddress) (hraft.AppendPipeline, error) {
	underlying, err := t.NetworkTransport.AppendEntriesPipeline(id, target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(t.ctx)
	p := &peerAppendPipelineV1{AppendPipeline: underlying, owner: t, pending: make(map[hraft.AppendFuture]*peerAppendFutureV1), consumer: make(chan hraft.AppendFuture, 2), ctx: ctx, cancel: cancel}
	go p.responses()
	return p, nil
}

func (p *peerAppendPipelineV1) AppendEntries(args *hraft.AppendEntriesRequest, response *hraft.AppendEntriesResponse) (hraft.AppendFuture, error) {
	work, err := p.owner.appendWork(args)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.ctx.Err() != nil {
		work.release()
		return nil, hraft.ErrTransportShutdown
	}
	original, err := p.AppendPipeline.AppendEntries(args, response)
	if err != nil {
		work.release()
		return nil, err
	}
	future := &peerAppendFutureV1{AppendFuture: original, done: make(chan struct{}), work: work}
	p.pending[original] = future
	return future, nil
}

func (p *peerAppendPipelineV1) Consumer() <-chan hraft.AppendFuture { return p.consumer }

func (p *peerAppendPipelineV1) responses() {
	// HashiCorp consumers expect this channel to remain open on Close; their
	// shutdown/stop channel terminates the receive loop. A closed channel would
	// yield a nil future to the upstream replication decoder.
	defer p.Close()
	for {
		select {
		case <-p.ctx.Done():
			return
		case original, ok := <-p.AppendPipeline.Consumer():
			if !ok {
				return
			}
			p.mu.Lock()
			future := p.pending[original]
			delete(p.pending, original)
			p.mu.Unlock()
			if future == nil {
				continue
			}
			// Only this goroutine calls upstream Error; that implementation is
			// not safe for concurrent callers. Our future publishes one result.
			future.complete(original.Error())
			select {
			case p.consumer <- future:
			case <-p.ctx.Done():
				return
			}
		}
	}
}

func (p *peerAppendPipelineV1) Close() error {
	// Close the socket before waiting for an AppendEntries write's mutex.
	err := p.AppendPipeline.Close()
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		for original, future := range p.pending {
			future.complete(hraft.ErrTransportShutdown)
			delete(p.pending, original)
		}
	}
	return err
}

// Assert that the wrapper preserves the complete future API without exposing
// the upstream non-concurrent Error method.
var _ interface {
	Error() error
	Start() time.Time
	Request() *hraft.AppendEntriesRequest
	Response() *hraft.AppendEntriesResponse
} = (*peerAppendFutureV1)(nil)
