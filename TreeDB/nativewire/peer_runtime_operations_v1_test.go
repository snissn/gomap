package nativewire

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestPeerSecurityReadinessNeedsQuorumAndDrainPreservesStatusV1(t *testing.T) {
	for _, ownsData := range []bool{false, true} {
		name := "storage-free-consumer"
		if ownsData {
			name = "data-owner-consumer"
		}
		t.Run(name, func(t *testing.T) { peerSecurityReadinessSparseV1(t, ownsData) })
	}
}

func peerSecurityReadinessSparseV1(t *testing.T, ownsData bool) {
	fixtureStarted := time.Now()
	configs := sparseCatalogTestConfigsV1(t)
	if ownsData {
		group := configs[0].Groups[0]
		// Move this fixed test group's owner to the existing catalog consumer.
		// This happens before opening any stores; it is not live reconfiguration.
		group.Peers = append([]raftcluster.Peer(nil), group.Peers...)
		group.Peers[0].ID = configs[3].NodeID
		group.BootstrapNode = configs[3].NodeID
		address := configs[0].RaftListen[group.ID]
		delete(configs[0].RaftListen, group.ID)
		configs[3].RaftListen[group.ID] = address
		for i := range configs {
			configs[i].Groups[0] = group
		}
	}
	// Keep only observations from the existing calls. Failure reporting does
	// not issue RPCs or change the fixture's request order or deadline.
	opens := make([]struct {
		started  time.Time
		duration time.Duration
		err      error
	}, len(configs))
	probes := make([]struct {
		attempts            uint64
		started             time.Time
		duration            time.Duration
		status              FixedPeerTCPStatusV1
		err                 error
		lastSuccess         FixedPeerTCPStatusV1
		lastSuccessStarted  time.Time
		lastSuccessDuration time.Duration
		lastFailure         error
		lastFailureStarted  time.Time
		lastFailureDuration time.Duration
	}, len(configs))
	var contextCreated, contextDeadline, readinessStarted time.Time
	var readinessDuration time.Duration
	var readinessReport FixedPeerReadinessV1
	var readinessErr error
	var pollAttempts uint64
	defer func() {
		if !t.Failed() {
			return
		}
		t.Logf("readiness fixture observations: started=%s context_created=%s context_deadline=%s poll_attempts=%d", fixtureStarted.Format(time.RFC3339Nano), contextCreated.Format(time.RFC3339Nano), contextDeadline.Format(time.RFC3339Nano), pollAttempts)
		for i, config := range configs {
			open, probe := opens[i], probes[i]
			t.Logf("readiness fixture node=%s request_timeout=%s raft_timeout=%s open_started=%s open_duration=%s open_error=%v status_attempts=%d status_started=%s status_duration=%s status=%+v status_error=%v", config.NodeID, config.RequestTimeout, config.RaftTimeout, open.started.Format(time.RFC3339Nano), open.duration, open.err, probe.attempts, probe.started.Format(time.RFC3339Nano), probe.duration, probe.status, probe.err)
			t.Logf("readiness fixture node=%s last_success_started=%s last_success_duration=%s last_success_status=%+v last_failure_started=%s last_failure_duration=%s last_failure_error=%v", config.NodeID, probe.lastSuccessStarted.Format(time.RFC3339Nano), probe.lastSuccessDuration, probe.lastSuccess, probe.lastFailureStarted.Format(time.RFC3339Nano), probe.lastFailureDuration, probe.lastFailure)
		}
		t.Logf("readiness fixture initial refusal: started=%s duration=%s report=%+v error=%v", readinessStarted.Format(time.RFC3339Nano), readinessDuration, readinessReport, readinessErr)
	}()
	ca := newPeerCAFixtureV1(t)
	nodes := make([]*FixedPeerTCPRuntimeV1, len(configs))
	for i := range configs {
		configs[i].ClusterID = "readiness-conformance"
		configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	}
	for i := range configs {
		var err error
		opens[i].started = time.Now()
		nodes[i], err = fixedPeerOpenTestRuntimeV1(t, configs[i])
		opens[i].duration = time.Since(opens[i].started)
		opens[i].err = err
		if err != nil {
			t.Fatal(err)
		}
		defer nodes[i].Close()
	}
	contextCreated = time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	contextDeadline, _ = ctx.Deadline()
	defer cancel()
	client := nodes[0].client
	readinessStarted = time.Now()
	readinessReport, readinessErr = client.ReadinessV1(ctx, configs[0].NodeID)
	readinessDuration = time.Since(readinessStarted)
	if readinessErr == nil || readinessReport.Ready {
		t.Fatalf("uninitialized catalog advertised readiness: %+v %v", readinessReport, readinessErr)
	}
	var leader raftcluster.NodeID
	fixedPeerWaitV1(t, ctx, func() bool {
		pollAttempts++
		for i, config := range configs {
			probe := &probes[i]
			probe.attempts++
			probe.started = time.Now()
			status, err := client.Status(ctx, config.NodeID)
			probe.duration = time.Since(probe.started)
			probe.status, probe.err = status, err
			if err == nil {
				probe.lastSuccess = status
				probe.lastSuccessStarted = probe.started
				probe.lastSuccessDuration = probe.duration
			} else {
				probe.lastFailure = err
				probe.lastFailureStarted = probe.started
				probe.lastFailureDuration = probe.duration
			}
			if err == nil && status.CatalogRaft.State == "Leader" {
				leader = config.NodeID
				return true
			}
		}
		return false
	})
	catalog := raftplacement.CatalogV1{}
	for _, group := range configs[0].Groups {
		var members []raftcluster.NodeID
		for _, peer := range group.Peers {
			members = append(members, peer.ID)
		}
		catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group.ID, Members: members})
	}
	for _, group := range configs[0].Groups {
		catalog.Placements = append(catalog.Placements, raftplacement.CollectionPlacementV1{Collection: raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: string(group.ID)}, GroupID: group.ID})
	}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PublishCatalog(ctx, leader, command); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, node := range nodes {
			status, err := node.Status(ctx)
			if err != nil || (node.meta != nil && status.Catalog.Epoch != 1) {
				return false
			}
			for _, group := range status.Groups {
				if group.LeaderID == "" {
					return false
				}
			}
		}
		return true
	})
	for _, group := range configs[0].Groups {
		request := ClusterRouteRequest{Database: "default", Catalog: "default", Collection: string(group.ID), Shape: ClusterRouteShapeCollection}
		route, err := client.Route(ctx, configs[0].NodeID, request)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := client.leader(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		status, err := client.Status(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		var version uint64
		for _, local := range status.Groups {
			if local.GroupID == group.ID {
				version = local.CatalogVersion
			}
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		if result, err := client.Submit(ctx, configs[0].NodeID, fixedPeerCreateEntryV1(t, request.Collection, version), metadata); err != nil || !result.CommittedRecoverable {
			t.Fatalf("bootstrap real durable group: %+v %v", result, err)
		}
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		for _, config := range configs[:3] {
			report, err := client.ReadinessV1(ctx, config.NodeID)
			if err != nil || !report.Live || !report.Ready {
				return false
			}
		}
		return true
	})
	if report, err := client.ReadinessV1(ctx, configs[3].NodeID); err != nil || !report.Ready || report.CatalogEpoch != 1 {
		t.Fatalf("healthy catalog consumer never ready: %+v %v", report, err)
	}
	consumer := nodes[3]
	if consumer.meta != nil || consumer.authority != nil {
		t.Fatal("consumer installed local catalog authority")
	}
	if !ownsData {
		if _, err := os.Stat(configs[3].DataRoot); !os.IsNotExist(err) {
			t.Fatalf("consumer opened data root: %v", err)
		}
	}
	nodes[0].BeginDrainV1()
	observer := nodes[1].client
	if _, err := observer.Status(ctx, configs[0].NodeID); err != nil {
		t.Fatalf("drain blocked status: %v", err)
	}
	if report, err := observer.ReadinessV1(ctx, configs[0].NodeID); err == nil || !report.Live || report.Ready || !report.Draining {
		t.Fatalf("drain readiness: %+v %v", report, err)
	}
	if _, err := observer.Submit(ctx, configs[0].NodeID, nil, ClusterRequestMetadata{}); !errors.Is(err, raftcluster.ErrAdmissionUnavailable) || errors.Is(err, raftcluster.ErrCommitAmbiguous) {
		t.Fatalf("drained mutation not definitely refused: %v", err)
	}
	// Two-voter group-b loses quorum, while the catalog retains two of three
	// voters. A running process and a known leader must not become readiness.
	if err := nodes[2].Close(); err != nil {
		t.Fatal(err)
	}
	if report, err := observer.ReadinessV1(ctx, configs[1].NodeID); err == nil || !report.Live || report.Ready {
		t.Fatalf("missing data quorum advertised readiness: %+v %v", report, err)
	}
	// Readiness is local-group scoped: a healthy consumer must not inherit a
	// different data group's outage while the catalog still has quorum.
	// The stopped data voter can also be the catalog leader. Join its genuine
	// failover/quorum fence before checking the surviving consumer's readiness.
	fixedPeerWaitV1(t, ctx, func() bool {
		report, err := observer.ReadinessV1(ctx, configs[3].NodeID)
		return err == nil && report.Ready && report.CatalogEpoch == 1
	})
	if report, err := observer.ReadinessV1(ctx, configs[3].NodeID); err != nil || !report.Ready {
		t.Fatalf("healthy sparse consumer: %+v %v", report, err)
	}
	if err := nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	if report, err := observer.ReadinessV1(ctx, configs[3].NodeID); err == nil || !report.Live || report.Ready {
		t.Fatalf("consumer cached catalog readiness after quorum loss: %+v %v", report, err)
	}
	if consumer.meta != nil || consumer.authority != nil {
		t.Fatal("readiness manufactured local catalog authority")
	}

}
