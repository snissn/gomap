package nativewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type FixedPeerConfigIdentityV1 struct {
	NodeID                    raftcluster.NodeID
	ClusterID                 string
	SharedSHA256, LocalSHA256 string
	Authenticated             bool
	ResourceLimits            PeerNodeLimitsV1
}

// InspectFixedPeerTCPConfigV1 validates and hashes configuration without
// opening stores, listeners, credentials or cloud resources.
func InspectFixedPeerTCPConfigV1(config FixedPeerTCPConfigV1) (FixedPeerConfigIdentityV1, error) {
	config, shared, err := validateFixedPeerConfigV1(config)
	if err != nil {
		return FixedPeerConfigIdentityV1{}, err
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return FixedPeerConfigIdentityV1{}, err
	}
	local := sha256.Sum256(raw)
	identity := FixedPeerConfigIdentityV1{NodeID: config.NodeID, ClusterID: config.ClusterID, SharedSHA256: shared, LocalSHA256: hex.EncodeToString(local[:]), Authenticated: config.Credentials != nil}
	if identity.Authenticated {
		var limits PeerNodeLimitsV1
		if config.ResourceLimits != nil {
			limits = *config.ResourceLimits
		}
		identity.ResourceLimits, err = normalizePeerNodeLimitsV1(limits)
		if err != nil {
			return identity, err
		}
		admission, err := newPeerNodeAdmissionV1(config)
		if err != nil {
			return identity, err
		}
		admission.close()
	}
	return identity, nil
}

type FixedPeerGroupReadinessV1 struct {
	GroupID                                       raftcluster.GroupID
	LeaderID                                      raftcluster.NodeID
	Term, RequiredAppliedIndex, LocalAppliedIndex uint64
	Ready                                         bool
	Error                                         string `json:",omitempty"`
}

// FixedPeerReadinessV1 is a fresh operational observation, never a reusable
// route/read capability. ANN generation readiness remains the product's gate.
type FixedPeerReadinessV1 struct {
	Live, Ready, Draining bool
	NodeID                raftcluster.NodeID
	CatalogEpoch          uint64
	Groups                []FixedPeerGroupReadinessV1
	Error                 string `json:",omitempty"`
}

// BeginDrainV1 refuses new public work while leaving consensus/control reads
// available for already admitted work and orchestration. Close performs the
// existing bounded HTTP drain and then closes all shared transport resources.
func (r *FixedPeerTCPRuntimeV1) BeginDrainV1() {
	if r != nil {
		r.draining.Store(true)
		if r.client != nil && r.client.peerTransport != nil {
			r.client.peerTransport.admission.draining.Store(true)
		}
	}
}

func (r *FixedPeerTCPRuntimeV1) ReadinessV1(ctx context.Context) (FixedPeerReadinessV1, error) {
	if r == nil {
		return FixedPeerReadinessV1{}, raftcluster.ErrAdmissionUnavailable
	}
	select {
	case r.diagnostics <- struct{}{}:
		defer func() { <-r.diagnostics }()
	default:
		return FixedPeerReadinessV1{}, raftcluster.ErrAdmissionUnavailable
	}
	return r.readinessV1(ctx)
}

func (r *FixedPeerTCPRuntimeV1) readinessV1(ctx context.Context) (FixedPeerReadinessV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	defer cancel()
	report := FixedPeerReadinessV1{Live: !r.closed.Load(), NodeID: r.config.NodeID, Draining: r.draining.Load()}
	if !report.Live || report.Draining {
		report.Error = "node is draining or closed"
		return report, raftcluster.ErrAdmissionUnavailable
	}
	var catalog raftplacement.CatalogMetaStatusV1
	var err error
	if r.meta == nil {
		// Consumers have no local catalog authority to fence. Resolve the
		// leader afresh and require its request-scoped quorum/applied fence,
		// just as consumer routing does. Never install or cache this reply.
		var reply fixedPeerReplyV1
		reply, err = r.catalogConsumerCall(ctx, "catalog-read", fixedPeerRequestV1{})
		catalog = reply.Catalog
	} else {
		catalog, err = r.catalogFence(ctx)
	}
	if err != nil {
		report.Error = err.Error()
		return report, err
	}
	report.CatalogEpoch = catalog.Epoch
	if catalog.Epoch == 0 {
		report.Error = "catalog is not initialized"
		return report, raftcluster.ErrAdmissionUnavailable
	}
	var failures []error
	for _, group := range r.config.Groups {
		local := r.data[group.ID]
		if local == nil {
			continue
		}
		item := FixedPeerGroupReadinessV1{GroupID: group.ID}
		leader, err := r.client.leader(ctx, group)
		item.LeaderID = leader
		var proof raftcluster.ReadIndexProof
		if err == nil {
			if leader == r.config.NodeID {
				proof, err = local.provider.ReadIndex(ctx, raftcluster.ReadIndexBarrier{NodeID: leader, GroupID: group.ID})
			} else {
				var reply fixedPeerReplyV1
				reply, err = r.client.call(ctx, leader, "group-read-proof", fixedPeerRequestV1{Metadata: raftentry.RequestMetadataV1{ClusterRouteGroupID: string(group.ID)}}, false)
				if err == nil {
					if reply.ReadProof == nil {
						err = raftcluster.ErrReadBarrierNotSatisfied
					} else {
						proof = *reply.ReadProof
					}
				}
			}
		}
		if err == nil {
			err = (raftcluster.ReadIndexBarrier{NodeID: leader, GroupID: group.ID}).Check(proof)
		}
		if err == nil {
			item.Term, item.RequiredAppliedIndex = proof.Term, proof.Index
			var status raftcluster.RuntimeStatusV1
			status, err = local.provider.RuntimeStatusV1(ctx)
			item.LocalAppliedIndex = status.Applied.Index
			if err == nil && (!status.Applied.HasApplied || status.Applied.Index < proof.Index || status.RaftAppliedIndex < proof.Index) {
				err = raftcluster.ErrReadBarrierNotSatisfied
			}
		}
		item.Ready = err == nil
		if err != nil {
			item.Error = err.Error()
			failures = append(failures, fmt.Errorf("%s: %w", group.ID, err))
		}
		report.Groups = append(report.Groups, item)
	}
	if r.draining.Load() {
		report.Draining = true
		failures = append(failures, raftcluster.ErrAdmissionUnavailable)
	}
	err = errors.Join(failures...)
	report.Ready = err == nil
	if err != nil {
		report.Error = err.Error()
	}
	return report, err
}

func (c *FixedPeerTCPClientV1) ReadinessV1(ctx context.Context, node raftcluster.NodeID) (FixedPeerReadinessV1, error) {
	reply, err := c.call(ctx, node, "readiness", fixedPeerRequestV1{}, false)
	if reply.Readiness == nil {
		return FixedPeerReadinessV1{}, errors.Join(err, raftcluster.ErrAdmissionUnavailable)
	}
	return *reply.Readiness, err
}
