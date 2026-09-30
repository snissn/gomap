package nativewire

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// WarmReplicaReplacementOwnerV1 retains private ANN packs on an already prepared
// nonvoter. It grants no endpoint, READY, membership or lifecycle authority.
func (c *FixedPeerTCPClientV1) WarmReplicaReplacementOwnerV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	_, err = c.pollReplacementV1(ctx, command.NewPeer.ID, "replacement-owner-warm", raw)
	return err
}

// Every admission and cache guard reads the exact operation and ACTIVE decision;
// neither a completed worker nor a retained pack is an authority lease.
func (r *FixedPeerTCPRuntimeV1) replacementOwnerCatalogV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, d *fixedPeerDataV1) (raftplacement.CatalogMetaStatusV1, raftplacement.VectorPartitionLifecycleRecordV1, error) {
	var status raftplacement.CatalogMetaStatusV1
	var record raftplacement.VectorPartitionLifecycleRecordV1
	if err := r.validateImmutableOwnerPreparationV1(command); err != nil {
		return status, record, err
	}
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return status, record, err
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil ||
		command.NewPeer.ID != r.config.NodeID || d == nil || d.fsm == nil || d.startErr != nil || d.replacementID != command.OperationID ||
		d.prejoin == nil || d.replacementReceiver == nil {
		return status, record, raftcluster.ErrAdmissionUnavailable
	}
	d.replacementWork.mu.Lock()
	database := d.replacementWork.ownerDB
	d.replacementWork.mu.Unlock()
	if database != nil && !d.fsm.HasCurrentDBV1(database) {
		return status, record, ErrFixedPeerVectorProofStaleV1
	}
	reply, err := r.catalogConsumerCall(ctx, "vector-catalog-read", fixedPeerRequestV1{VectorLifecycle: &fixedPeerVectorLifecycleRequestV1{Action: fixedPeerVectorCatalogActiveV1}})
	if err != nil {
		return status, record, err
	}
	if reply.VectorCatalog == nil {
		return status, record, ErrFixedPeerVectorProofStaleV1
	}
	if err := r.validateImmutableVectorCatalogDecisionV1(reply.Catalog, *reply.VectorCatalog, fixedPeerVectorCatalogActiveV1); err != nil {
		return status, record, err
	}
	if database != nil && !d.fsm.HasCurrentDBV1(database) {
		return status, record, ErrFixedPeerVectorProofStaleV1
	}
	return reply.Catalog, *reply.VectorCatalog, nil
}

func (r *FixedPeerTCPRuntimeV1) replacementOwnerWarmV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, reply *fixedPeerReplyV1) error {
	d := r.localDataV1(command.GroupID)
	if _, _, err := r.replacementOwnerCatalogV1(ctx, command, d); err != nil {
		if d != nil && ctx.Err() == nil {
			err = errors.Join(err, d.retireReplacementOwnerSourceV1())
		}
		return err
	}
	d.replacementWork.mu.Lock()
	if err := d.authorizeReplacementWorkLockedV1(command); err != nil {
		d.replacementWork.mu.Unlock()
		return err
	}
	if current := d.replacementWork.work; current != nil {
		select {
		case <-current.done:
			if current.phase == "owner-warm" {
				// Retain the completed slot through its fresh admission check, so
				// another poll cannot start a worker while completion is checked.
				d.replacementWork.mu.Unlock()
				err := current.err
				if err == nil {
					// An abandoned poll cannot grant a later warm from old assets/tail.
					err = r.replacementOwnerTailV1(ctx, command)
					if err == nil {
						_, _, err = r.replacementOwnerCatalogV1(ctx, command, d)
					}
				}
				if err != nil && ctx.Err() == nil {
					err = errors.Join(err, d.retireReplacementOwnerSourceV1())
				}
				d.replacementWork.mu.Lock()
				if err == nil && (d.replacementWork.closed || d.replacementWork.work != current || d.replacementWork.ownerSource == nil) {
					err = raftcluster.ErrAdmissionUnavailable
				}
				if d.replacementWork.work == current {
					d.replacementWork.work = nil
				}
				d.replacementWork.mu.Unlock()
				return err
			}
			if current.phase == "verify" && current.err == nil && current.cleanupErr == nil {
				d.replacementWork.work = nil
			}
		default:
		}
	}
	defer d.replacementWork.mu.Unlock()
	return d.replacementWorkLockedV1(command.OperationID, "owner-warm", nil, func(workCtx context.Context) (*raftcluster.ReplacementSnapshotSeedV1, error) {
		err := errors.Join(r.warmReplacementOwnerSourceV1(workCtx, command, d), workCtx.Err())
		if err != nil {
			err = errors.Join(err, d.closeReplacementOwnerSourceV1())
		}
		return nil, err
	}, reply)
}

// Detach before closing. A running worker owns its source until cancellation
// returns; Close is never called from inside a source load/authority callback.
func (d *fixedPeerDataV1) retireReplacementOwnerSourceV1() error {
	slot := &d.replacementWork
	slot.mu.Lock()
	if slot.work != nil {
		select {
		case <-slot.work.done:
			if slot.work.phase == "owner-warm" {
				// Retiring the cache also discards its completed process-local result.
				slot.work = nil
			}
		default:
			if slot.work.phase == "owner-warm" {
				slot.work.cancel()
			}
			slot.mu.Unlock()
			return nil
		}
	}
	source := slot.ownerSource
	slot.ownerSource, slot.ownerDB = nil, nil
	slot.mu.Unlock()
	if source != nil {
		return source.Close()
	}
	return nil
}

// Only the actual worker or shutdown after worker return may call this directly.
func (d *fixedPeerDataV1) closeReplacementOwnerSourceV1() error {
	slot := &d.replacementWork
	slot.mu.Lock()
	source := slot.ownerSource
	slot.ownerSource, slot.ownerDB = nil, nil
	slot.mu.Unlock()
	if source != nil {
		return source.Close()
	}
	return nil
}

func (r *FixedPeerTCPRuntimeV1) replacementOwnerTailV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1) error {
	state, err := r.replacementStateAuthorityV1(ctx, command)
	if err != nil {
		return err
	}
	if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
		return raftcluster.ErrAdmissionUnavailable
	}
	group, err := r.replacementGroupV1(state, false)
	if err != nil {
		return err
	}
	leader, err := r.client.leader(ctx, group)
	if err != nil {
		return err
	}
	raw, err := raftplacement.EncodeReplicaReplacementBeginV1(command)
	if err != nil {
		return err
	}
	reply, err := r.client.call(ctx, leader, "replacement-tail", fixedPeerRequestV1{Entry: raw}, false)
	if err != nil {
		return err
	}
	if reply.ReplacementTail == nil || reply.ReplacementTail.GroupID != command.GroupID ||
		reply.ReplacementTail.Progress.CommandDigest == (raftentry.CommandDigestV1{}) ||
		reply.ReplacementTail.Progress.Result.CommandDigest != reply.ReplacementTail.Progress.CommandDigest ||
		reply.ReplacementTail.Progress.Result.ResultDigest == (raftentry.CommandDigestV1{}) {
		return raftcluster.ErrReadBarrierNotSatisfied
	}
	return reply.ReplacementTail.Validate()
}

func (r *FixedPeerTCPRuntimeV1) warmReplacementOwnerSourceV1(ctx context.Context, command raftplacement.ReplicaReplacementBeginV1, d *fixedPeerDataV1) (err error) {
	// This leader fence retains the old voter, proves target NONVOTER membership,
	// and checks the installed seed/assets plus the actual current semantic tail.
	if err = r.replacementOwnerTailV1(ctx, command); err != nil {
		return err
	}
	if _, _, err = r.replacementOwnerCatalogV1(ctx, command, d); err != nil {
		return err
	}
	slot := &d.replacementWork
	slot.mu.Lock()
	source := slot.ownerSource
	slot.mu.Unlock()
	if source == nil {
		state, stateErr := r.replacementStateAuthorityV1(ctx, command)
		if stateErr != nil {
			return stateErr
		}
		if state.Phase != raftplacement.ReplicaReplacementAddIntentV1 || state.Seed == nil {
			return raftcluster.ErrAdmissionUnavailable
		}
		collection, database, openErr := d.fsm.OpenCollectionForRaftSourceFromCurrentDBV1(ctx, raftcluster.AppliedIndexReadBarrier{NodeID: r.config.NodeID, GroupID: command.GroupID, MinAppliedIndex: state.Seed.Manifest.LastIncludedIndex}, command.OwnerPreparation.Index.Collection.Collection)
		if openErr != nil {
			return openErr
		}
		identity := *command.OwnerPreparation
		scoped := &fixedPeerVectorScopedOwnerAuthorityV1{identity: identity, hosted: command.GroupID,
			currentDB: func() error {
				if !d.fsm.HasCurrentDBV1(database) {
					return ErrFixedPeerVectorProofStaleV1
				}
				return nil
			},
			readCatalog: func(ctx context.Context) (raftplacement.CatalogMetaStatusV1, raftplacement.VectorPartitionLifecycleRecordV1, error) {
				return r.replacementOwnerCatalogV1(ctx, command, d)
			},
		}
		source, err = NewCollectionVectorPartitionGenerationSourceForScopedOwnerV1(collection, identity.Index.Collection,
			&replacementOwnerLifecycleV1{scoped: scoped}, scoped, command.GroupID)
		if err != nil {
			return err
		}
		slot.mu.Lock()
		slot.ownerSource, slot.ownerDB = source, database
		slot.mu.Unlock()
	}
	// The worker alone owns this cache. Its caller retires failures after these
	// request leases return, outside collection/FSM storage locks.
	pin, err := source.PinVectorPartitionGenerationV1(ctx, command.OwnerPreparation.Index.IndexName, command.OwnerPreparation.Generation)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, pin.Close()) }()
	manifest := r.config.Vector.Manifest
	offsets, layoutErr := vectorPartitionCoordinatorDomainPackOffsetsV1(manifest)
	if layoutErr != nil {
		return layoutErr
	}
	placements := pin.Manifest().Placements
	if len(placements) != int(manifest.PartitionCount) {
		return ErrFixedPeerVectorProofStaleV1
	}
	domainGraphs := vectorPartitionCoordinatorUsesDomainGraphsV1(manifest)
	for domain := 0; domain+1 < len(offsets); domain++ {
		packs := manifest.DomainPacks[offsets[domain]:offsets[domain+1]]
		anchor := packs[0].PackID
		owner := placements[anchor].GroupID
		for _, mapping := range packs {
			if placements[mapping.PackID].PartitionID != mapping.PackID || placements[mapping.PackID].GroupID != owner {
				return ErrFixedPeerVectorWrongOwnerV1
			}
		}
		if owner != string(command.GroupID) {
			continue
		}
		// A domain searcher retains all colocated physical sections through its
		// first pack ID. Legacy per-pack graphs still open each physical pack.
		if domainGraphs {
			packs = packs[:1]
		}
		for _, mapping := range packs {
			lease, openErr := pin.OpenPartition(ctx, mapping.PackID)
			if openErr != nil {
				return openErr
			}
			if closeErr := lease.Close(); closeErr != nil {
				return closeErr
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	// Completion polls obtain the final tail/asset fence; this worker checks
	// ACTIVE/currentDB after searcher open without retaining the request's lifetime.
	_, _, err = r.replacementOwnerCatalogV1(ctx, command, d)
	return err
}

type replacementOwnerLifecycleV1 struct {
	scoped *fixedPeerVectorScopedOwnerAuthorityV1
}

func (a *replacementOwnerLifecycleV1) ValidateVectorPartitionGenerationSearchV1(ctx context.Context, collection raftplacement.CollectionRefV1, index string, generation uint64, definition string, sourceGeneration, sourceChecksum, sourceSchemaHash, sourceRowCount uint64) (string, error) {
	identity := a.scoped.identity
	if collection != identity.Index.Collection || index != identity.Index.IndexName || generation != identity.Generation ||
		definition != identity.Index.IndexDefinitionDigest || sourceGeneration != identity.Source.Generation ||
		sourceChecksum != identity.Source.Checksum || sourceSchemaHash != identity.Source.SchemaHash || sourceRowCount != identity.Source.RowCount {
		return "", ErrFixedPeerVectorProofStaleV1
	}
	if err := a.scoped.currentDB(); err != nil {
		return "", err
	}
	_, record, err := a.scoped.readCatalog(ctx)
	if err != nil {
		return "", err
	}
	if err := a.scoped.currentDB(); err != nil {
		return "", err
	}
	return record.ReadySetDigest, nil
}
