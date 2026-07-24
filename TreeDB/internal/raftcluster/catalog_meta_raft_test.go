package raftcluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
)

func TestCatalogMetaRaftProviderCommitsOnlyAfterLeaderApplyAndSnapshots(t *testing.T) {
	state := &catalogMetaRaftTestState{}
	_, transport := hraft.NewInmemTransport("node-a")
	defer transport.Close()
	p, err := OpenCatalogMetaRaftProviderV1(CatalogMetaRaftProviderOptionsV1{Cluster: Config{Dir: t.TempDir(), NodeID: "node-a", GroupID: "meta", Peers: []Peer{{ID: "node-a", Address: "node-a"}}}, State: state, Transport: transport, Bootstrap: true})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		status, e := p.ClusterAdmissionStatus(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if status.Leader {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	term, index, err := p.SubmitCatalogMetaCommandV1(ctx, []byte("generation-1"))
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 || index == 0 || !bytes.Equal(state.command, []byte("generation-1")) || state.index != index {
		t.Fatalf("term/index/state=%d/%d/%q/%d", term, index, state.command, state.index)
	}
	if err := p.SnapshotCatalogMetaV1(ctx); err != nil {
		t.Fatal(err)
	}
	if len(state.snapshot) == 0 {
		t.Fatal("snapshot did not export state")
	}
	if _, _, err := p.SubmitCatalogMetaCommandV1(context.Background(), []byte("generation-2")); err != nil {
		t.Fatal(err)
	}
	if err := state.InstallCatalogMetaSnapshotBytesV1(state.snapshot); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.command, []byte("generation-1")) {
		t.Fatalf("restore command=%q", state.command)
	}
}

func TestCatalogMetaRaftProviderFollowerAndCancellationFailClosed(t *testing.T) {
	p := &CatalogMetaRaftProviderV1{}
	if _, _, err := p.SubmitCatalogMetaCommandV1(context.Background(), []byte("x")); !errors.Is(err, ErrInvalidHashicorpRaftProvider) {
		t.Fatalf("err=%v", err)
	}
}

type catalogMetaRaftTestState struct {
	command, snapshot []byte
	index             uint64
}

func (s *catalogMetaRaftTestState) ApplyCatalogMetaCommittedV1(b []byte, index uint64) error {
	s.command = bytes.Clone(b)
	s.index = index
	return nil
}
func (s *catalogMetaRaftTestState) ExportCatalogMetaSnapshotBytesV1() ([]byte, error) {
	s.snapshot = bytes.Clone(s.command)
	return bytes.Clone(s.command), nil
}
func (s *catalogMetaRaftTestState) InstallCatalogMetaSnapshotBytesV1(b []byte) error {
	s.command = bytes.Clone(b)
	return nil
}

var _ io.Closer = (*catalogMetaNoopCloser)(nil)

type catalogMetaNoopCloser struct{}

func (*catalogMetaNoopCloser) Close() error { return nil }
