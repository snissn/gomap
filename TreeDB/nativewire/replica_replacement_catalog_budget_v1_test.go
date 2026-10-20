package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// This control proves an incidental response budget can fail after real commit.
// It does not identify the scheduler, transport, or storage delay in a past run.
func TestReplacementCatalogPublishDelayedResponseV1(t *testing.T) {
	for _, tc := range []struct {
		name          string
		complete      bool
		callerTimeout time.Duration
		hold          time.Duration
		wantDeadline  bool
	}{
		{"ordinary RPC budget", false, 0, 3 * time.Second, true},
		{"completion RPC budget", true, 0, 3 * time.Second, false},
		{"shorter caller deadline", true, 4 * time.Second, 5 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configs := replacementPublicInstallTestConfigsV1(t, tc.complete)
			ca := newPeerCAFixtureV1(t)
			now := time.Now()
			runtimes := make([]*FixedPeerTCPRuntimeV1, len(configs))
			for i := range configs {
				// Hold election geometry equal; only the fixture RPC budget varies.
				configs[i].RaftTimeout = time.Second
				configs[i].ClusterID = "replacement-catalog-budget"
				configs[i].Credentials = ca.issue(t, configs[i].ClusterID, string(configs[i].NodeID), now.Add(-time.Hour), now.Add(time.Hour))
				runtime, err := fixedPeerOpenTestRuntimeV1(t, configs[i])
				if err != nil {
					t.Fatal(err)
				}
				runtimes[i] = runtime
				t.Cleanup(func() {
					if err := runtime.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			client, err := NewFixedPeerTCPClientV1(configs[0])
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			leader := fixedPeerWaitCatalogLeaderV1(t, ctx, runtimes)
			catalog := raftplacement.CatalogV1{}
			for _, group := range configs[0].Groups {
				members := make([]raftcluster.NodeID, len(group.Peers))
				for i, peer := range group.Peers {
					members[i] = peer.ID
				}
				catalog.Groups = append(catalog.Groups, raftplacement.GroupV1{ID: group.ID, Members: members})
			}
			record, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
			if err != nil {
				t.Fatal(err)
			}
			command, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
			if err != nil {
				t.Fatal(err)
			}
			base := client.http.Transport.(*http.Transport)
			var publishes atomic.Int32
			var committed atomic.Bool
			client.http.Transport = splitInsertRoundTripperV1{Transport: base, roundTrip: func(request *http.Request) (*http.Response, error) {
				if request.URL.Path != "/v1/catalog-publish" {
					return base.RoundTrip(request)
				}
				publishes.Add(1)
				response, err := base.RoundTrip(request)
				if err != nil {
					return response, err
				}
				raw, readErr := io.ReadAll(response.Body)
				if err := errors.Join(readErr, response.Body.Close()); err != nil {
					return nil, err
				}
				var reply fixedPeerReplyV1
				if err := json.Unmarshal(raw, &reply); err != nil {
					return nil, err
				}
				if reply.Error != "" || reply.Catalog.Epoch != record.Epoch || reply.Catalog.Digest != record.Digest {
					return nil, fmt.Errorf("publish did not commit before response gate: %+v", reply)
				}
				committed.Store(true)
				// Keep the real reply past the old RPC budget. No mutation is replayed.
				timer := time.NewTimer(tc.hold)
				defer timer.Stop()
				select {
				case <-request.Context().Done():
					return nil, request.Context().Err()
				case <-timer.C:
					response.Body = io.NopCloser(bytes.NewReader(raw))
					return response, nil
				}
			}}
			caller := ctx
			if tc.callerTimeout != 0 {
				var cancelCaller context.CancelFunc
				caller, cancelCaller = context.WithTimeout(ctx, tc.callerTimeout)
				defer cancelCaller()
			}
			status, err := client.PublishCatalog(caller, configs[leader].NodeID, command)
			if !committed.Load() {
				t.Fatalf("publication did not reach the postcommit response gate: %v", err)
			}
			if tc.wantDeadline {
				if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, raftcluster.ErrCommitAmbiguous) {
					t.Fatalf("held committed reply must remain deadline/commit ambiguous: %v", err)
				}
			} else if err != nil || status.Epoch != record.Epoch || status.Digest != record.Digest {
				t.Fatalf("completion fixture lost committed reply: %+v %v", status, err)
			}
			if tc.callerTimeout != 0 && !errors.Is(caller.Err(), context.DeadlineExceeded) {
				t.Fatalf("short caller deadline did not expire: %v", caller.Err())
			}
			if publishes.Load() != 1 {
				t.Fatalf("catalog publication was retried: %d", publishes.Load())
			}
			// Read-only observation after either outcome proves all three voters
			// applied the same real catalog. A timeout is never treated as rollback.
			fixedPeerWaitV1(t, ctx, func() bool {
				for _, config := range configs {
					status, err := client.Status(ctx, config.NodeID)
					if err != nil || status.Catalog.Epoch != record.Epoch || status.Catalog.Digest != record.Digest {
						return false
					}
				}
				return true
			})
		})
	}
}
