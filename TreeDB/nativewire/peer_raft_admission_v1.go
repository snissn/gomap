package nativewire

import (
	"context"
	"io"
	"sync"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// Keep HashiCorp's consensus and wire transport. This adapter admits decoded
// RPCs and streaming snapshots before handing them to the real Raft consumer.
// A bounded inbox lets the adapter refuse a second snapshot even while the
// group's Raft goroutine is occupied installing the first one.
type peerRaftTransportV1 struct {
	*hraft.NetworkTransport
	admission *peerNodeAdmissionV1
	scope     string
	catalog   bool
	inbox     chan hraft.RPC
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error
}

func newPeerRaftTransportV1(network *hraft.NetworkTransport, admission *peerNodeAdmissionV1, scope string, catalog bool) *peerRaftTransportV1 {
	ctx, cancel := context.WithCancel(context.Background())
	t := &peerRaftTransportV1{NetworkTransport: network, admission: admission, scope: scope, catalog: catalog, inbox: make(chan hraft.RPC, 32), ctx: ctx, cancel: cancel}
	go t.receive()
	return t
}

func (t *peerRaftTransportV1) Consumer() <-chan hraft.RPC { return t.inbox }

func (t *peerRaftTransportV1) snapshotBytes(size int64) (int64, error) {
	if size < 0 || (t.catalog && size > fixedPeerMaxRPCBytesV1) {
		return 0, raftcluster.ErrRouteTargetUnsupported
	}
	if t.catalog {
		return 4*size + (64 << 10), nil
	}
	// The existing data installer streams its files and bounds the archive
	// header at 1 MiB. It never buffers a whole database snapshot.
	return 4*int64(raftcluster.RaftSnapshotArchiveHeaderMaxBytes) + (64 << 10), nil
}

func (t *peerRaftTransportV1) receive() {
	for {
		select {
		case <-t.ctx.Done():
			return
		case rpc, ok := <-t.NetworkTransport.Consumer():
			if !ok {
				return
			}
			var snapshot peerWorkLeaseV1
			var err error
			if command, ok := rpc.Command.(*hraft.InstallSnapshotRequest); ok {
				var bytes int64
				bytes, err = t.snapshotBytes(command.Size)
				if err == nil {
					snapshot, err = t.admission.work(t.scope, peerSnapshotsV1, bytes)
				}
			}
			if err != nil {
				snapshot.release()
				rpc.Respond(nil, err)
				continue
			}
			response := make(chan hraft.RPCResponse, 1)
			forward := rpc
			forward.RespChan = response
			select {
			case t.inbox <- forward:
				// The stream request lease bounds these response waiters. Hold snapshot
				// capacity until the FSM actually finishes, including restore errors.
				go func(original hraft.RPC, snapshot peerWorkLeaseV1) {
					defer snapshot.release()
					select {
					case result := <-response:
						original.Respond(result.Response, result.Error)
					case <-t.ctx.Done():
						original.Respond(nil, hraft.ErrTransportShutdown)
					}
				}(rpc, snapshot)
			default:
				snapshot.release()
				rpc.Respond(nil, raftcluster.ErrAdmissionUnavailable)
			}
		}
	}
}

func (t *peerRaftTransportV1) InstallSnapshot(id hraft.ServerID, target hraft.ServerAddress, args *hraft.InstallSnapshotRequest, response *hraft.InstallSnapshotResponse, data io.Reader) error {
	bytes, err := t.snapshotBytes(args.Size)
	if err != nil {
		return err
	}
	work, err := t.admission.work(t.scope, peerSnapshotsV1, bytes)
	if err != nil {
		return err
	}
	defer work.release()
	return t.NetworkTransport.InstallSnapshot(id, target, args, response, data)
}

func (t *peerRaftTransportV1) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = t.NetworkTransport.Close()
		t.NetworkTransport.CloseStreams()
		t.cancel()
	})
	return t.closeErr
}
