package main

import (
	"context"
	"encoding/binary"
	"errors"
	"sort"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// localRaftCluster preserves the M5 command's evidence shape while delegating
// all real three-node HashiCorp Raft construction to the reusable M8 harness.
type localRaftCluster struct {
	harness *raftcluster.ThreeNodeHarness
	groupID raftcluster.GroupID
	nodes   []raftcluster.NodeID
	leader  raftcluster.NodeID
}

func openLocalRaftCluster(ctx context.Context, group string) (*localRaftCluster, error) {
	harness, err := raftcluster.OpenThreeNodeHarness(ctx, raftcluster.GroupID(group))
	if err != nil {
		return nil, err
	}
	return &localRaftCluster{
		harness: harness,
		groupID: harness.GroupID(),
		nodes:   harness.NodeIDs(),
		leader:  harness.LeaderID(),
	}, nil
}

func (c *localRaftCluster) readCoordinator() raftcluster.RoutedReadIndexCoordinator {
	if c == nil || c.harness == nil {
		return nil
	}
	return c.harness.ReadCoordinator()
}

func (c *localRaftCluster) commitProofCommand(ctx context.Context) (raftcluster.CommitCommandEntryV1Result, error) {
	if c == nil || c.harness == nil {
		return raftcluster.CommitCommandEntryV1Result{}, errors.New("local Raft leader is unavailable")
	}
	entry, err := proofCommandEntry()
	if err != nil {
		return raftcluster.CommitCommandEntryV1Result{}, err
	}
	return c.harness.CommitAndProve(ctx, entry)
}

func proofCommandEntry() ([]byte, error) {
	sections := []iwire.Section{
		{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: iwire.CommandInsertBatch, Version: 1})},
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte("m5-real-raft-proof-v1")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, 1)},
		{ID: iwire.SectionCollectionRef, Bytes: append([]byte{1}, "m5_benchmark_proof"...)},
		{ID: iwire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(iwire.DocumentFormatJSON))},
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("proof"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"proof":true}`))},
	}
	command, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	return iwire.AppendDeterministicEntry(nil, command)
}

func (c *localRaftCluster) Close() error {
	if c == nil || c.harness == nil {
		return nil
	}
	err := c.harness.Close()
	c.harness = nil
	return err
}

func sortedNodeIDs(nodes []raftcluster.NodeID) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node))
	}
	sort.Strings(ids)
	return ids
}
