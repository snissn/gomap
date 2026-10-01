package nativewire

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// PrepareReplicaReplacementOwnerEndpointV1 opens only an operation-owned shard
// listener on the prepared NONVOTER. It grants no public route, READY or vote.
func (c *FixedPeerTCPClientV1) PrepareReplicaReplacementOwnerEndpointV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	if err := c.WarmReplicaReplacementOwnerV1(ctx, command); err != nil {
		return err
	}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	_, err = c.call(ctx, command.NewPeer.ID, "replacement-owner-endpoint", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil {
		return err
	}
	// Ordinary ProbeShardEndpointV1 retains its member check. The successful exact
	// operation control is followed by the same credentialed probe framing.
	endpoint := c.config.Vector.ShardAddresses[command.GroupID][command.NewPeer.ID]
	identity, err := c.peerTransport.probeAuthenticatedShardEndpointV1(ctx, endpoint, command.NewPeer.ID, command.GroupID)
	if err == nil && (identity.Version != 1 || identity.GroupID != string(command.GroupID) || identity.InstanceIdentity != c.digest) {
		err = errPeerAuthenticationV1
	}
	return err
}

type replacementOwnerEndpointV1 struct {
	vectorPartitionShardConnectionsV1
	listener  net.Listener
	closeOnce sync.Once
	closeErr  error
}

func (e *replacementOwnerEndpointV1) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		var errs []error
		if err := e.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
		for conn := range e.conns {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				errs = append(errs, err)
			}
		}
		e.mu.Unlock()
		e.wg.Wait()
		e.closeErr = errors.Join(errs...)
	})
	return e.closeErr
}

func (r *FixedPeerTCPRuntimeV1) prepareReplacementOwnerEndpointV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	d := r.localDataV1(command.GroupID)
	if _, _, err := r.replacementOwnerCatalogV1(ctx, command, d); err != nil {
		if d != nil && ctx.Err() == nil {
			err = errors.Join(err, d.retireReplacementOwnerSourceV1())
		}
		return err
	}
	if err := r.replacementOwnerTailV1(ctx, command); err != nil {
		if ctx.Err() == nil {
			err = errors.Join(err, d.retireReplacementOwnerSourceV1())
		}
		return err
	}
	slot := &d.replacementWork
	slot.mu.Lock()
	source, database, existingEndpoint, closed := slot.ownerSource, slot.ownerDB, slot.ownerEndpoint, slot.closed
	slot.mu.Unlock()
	if closed || source == nil || database == nil || !d.fsm.HasCurrentDBV1(database) {
		return raftcluster.ErrAdmissionUnavailable
	}
	if existingEndpoint != nil {
		return nil
	}
	// Retained source identity is checked before and after every fresh admission.
	// Refusal closes that request; an explicit preparation retry retires the cache.
	admission := func(ctx context.Context) error {
		slot.mu.Lock()
		current := !slot.closed && slot.ownerSource == source && slot.ownerDB == database
		slot.mu.Unlock()
		if !current || r.draining.Load() || !d.fsm.HasCurrentDBV1(database) {
			return ErrFixedPeerVectorProofStaleV1
		}
		if _, _, err := r.replacementOwnerCatalogV1(ctx, command, d); err != nil {
			return err
		}
		if err := r.replacementOwnerTailV1(ctx, command); err != nil {
			return err
		}
		slot.mu.Lock()
		current = !slot.closed && slot.ownerSource == source && slot.ownerDB == database
		slot.mu.Unlock()
		if !current || !d.fsm.HasCurrentDBV1(database) {
			return ErrFixedPeerVectorProofStaleV1
		}
		return nil
	}
	resolved, err := raftplacement.Validate(r.config.Vector.Catalog)
	if err != nil {
		return err
	}
	coordinator, err := raftcluster.NewGroupRoutedReadIndexCoordinator([]raftcluster.GroupReadIndexCoordinatorV1{{
		GroupID: command.GroupID, NodeID: r.config.NodeID, ReadIndexProvider: d.provider, AppliedIndexWaiter: d.fsm,
	}})
	if err != nil {
		return err
	}
	service, err := newVectorPartitionShardSearchServiceV1(VectorPartitionShardSearchServiceOptionsV1{
		Catalog: resolved, Placement: r.config.Vector.Placement, LocalNodeID: r.config.NodeID, LocalGroupID: command.GroupID,
		ReadCoordinator: coordinator, GenerationSource: source, Limits: DefaultVectorPartitionShardSearchLimitsV1(),
	}, admission)
	if err != nil {
		return err
	}
	service.postSearchGuard = func() error {
		if !d.fsm.HasCurrentDBV1(database) {
			return ErrFixedPeerVectorProofStaleV1
		}
		return nil
	}
	address := r.config.Vector.ShardAddresses[command.GroupID][r.config.NodeID]
	if address == "" {
		return errPeerAuthenticationV1
	}
	responseBound, err := vectorPartitionShardSearchTCPResponseFrameBoundV1(DefaultVectorPartitionShardSearchLimitsV1())
	if err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.closed || slot.ownerSource != source || slot.ownerDB != database || slot.work != nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	if slot.ownerEndpoint != nil {
		return nil
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	if !d.fsm.HasCurrentDBV1(database) {
		return errors.Join(ErrFixedPeerVectorProofStaleV1, listener.Close())
	}
	endpoint := &replacementOwnerEndpointV1{listener: listener, vectorPartitionShardConnectionsV1: vectorPartitionShardConnectionsV1{conns: make(map[net.Conn]struct{})}}
	slot.ownerEndpoint = endpoint
	server := VectorPartitionShardSearchTCPServerV1{
		PeerTransport: r.PeerTransportV1(), PeerGroupID: command.GroupID, Service: service, preparedOwnerAdmission: admission,
		EndpointIdentity: VectorPartitionShardEndpointIdentityV1{Version: 1, GroupID: string(command.GroupID), InstanceIdentity: r.client.digest},
		MaxFrame:         uint32(DefaultVectorPartitionShardSearchLimitsV1().MaxRequestBytes), MaxResponseFrame: responseBound, InitialTimeout: r.config.RequestTimeout,
	}
	endpoint.serveV1(listener, server, DefaultVectorPartitionCoordinatorLimitsV1().MaxRequests, nil)
	return nil
}
