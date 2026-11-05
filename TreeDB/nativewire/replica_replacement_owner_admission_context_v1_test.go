package nativewire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

// replicaOwnerOperationErrorV1 observes only local state after an original error.
// It makes no RPC, acquires no blocking lock, and grants no authority. Queue
// occupancy is an instantaneous observation, not the state at the refused call.
func replicaOwnerOperationErrorV1(ctx context.Context, client *FixedPeerTCPClientV1, target *FixedPeerTCPRuntimeV1, command raftplacement.ReplicaReplacementBeginV1, operation string, err error) string {
	contextCanceled, contextDeadline := false, false
	if ctx != nil {
		ctxErr := ctx.Err()
		contextCanceled = errors.Is(ctxErr, context.Canceled)
		contextDeadline = errors.Is(ctxErr, context.DeadlineExceeded)
	}
	clientKnown := client != nil
	calls, callCapacity, reads, readCapacity := 0, 0, 0, 0
	if clientKnown {
		calls, callCapacity = len(client.calls), cap(client.calls)
		reads, readCapacity = len(client.readCalls), cap(client.readCalls)
	}
	runtimeKnown := target != nil
	draining, closed := false, false
	groupObserved, groupPresent, startFailed := false, false, false
	operationMatches, fsmPresent, prejoinPresent, receiverPresent := false, false, false, false
	if runtimeKnown {
		draining, closed = target.draining.Load(), target.closed.Load()
		// The same groupsMu protects map publication and preparation's startErr.
		// A contended lock means UNKNOWN; observation must not wait for progress.
		if target.groupsMu.TryRLock() {
			groupObserved = true
			if local := target.data[command.GroupID]; local != nil {
				groupPresent = true
				startFailed = local.startErr != nil
				operationMatches = local.replacementID == command.OperationID
				fsmPresent, prejoinPresent = local.fsm != nil, local.prejoin != nil
				receiverPresent = local.replacementReceiver != nil
			}
			target.groupsMu.RUnlock()
		}
	}
	return fmt.Sprintf("operation=%s observation=post-error admission_unavailable=%t context_canceled=%t context_deadline=%t client_known=%t calls=%d/%d reads=%d/%d runtime_known=%t draining=%t closed=%t group_observed=%t group_present=%t start_failed=%t operation_matches=%t fsm_present=%t prejoin_present=%t receiver_present=%t original_error=%v",
		operation, errors.Is(err, raftcluster.ErrAdmissionUnavailable), contextCanceled, contextDeadline,
		clientKnown, calls, callCapacity, reads, readCapacity, runtimeKnown, draining, closed,
		groupObserved, groupPresent, startFailed, operationMatches, fsmPresent, prejoinPresent, receiverPresent, err)
}

func TestReplicaOwnerOperationErrorContextV1(t *testing.T) {
	command := raftplacement.ReplicaReplacementBeginV1{GroupID: "private-group", OperationID: "private-operation"}
	t.Run("nil-inputs-original-error", func(t *testing.T) {
		got := replicaOwnerOperationErrorV1(nil, nil, nil, command, "target-reopen", raftcluster.ErrAdmissionUnavailable)
		for _, want := range []string{"operation=target-reopen", "observation=post-error", "admission_unavailable=true", "client_known=false", "runtime_known=false", "group_observed=false", "original_error=raftcluster: admission unavailable"} {
			if !strings.Contains(got, want) {
				t.Fatalf("missing %q: %s", want, got)
			}
		}
		if strings.Contains(got, "private-group") || strings.Contains(got, "private-operation") {
			t.Fatal("observation retained command identity")
		}
	})
	t.Run("live-local-prerequisites", func(t *testing.T) {
		client := &FixedPeerTCPClientV1{calls: make(chan struct{}, 1), readCalls: make(chan struct{}, 2)}
		client.calls <- struct{}{}
		local := &fixedPeerDataV1{}
		local.startErr = raftcluster.ErrAdmissionUnavailable
		local.replacementID = command.OperationID
		local.prejoin = &replacementPrejoinTransportV1{}
		local.replacementReceiver = &replacementReceiverOwnerV1{}
		target := &FixedPeerTCPRuntimeV1{data: map[raftcluster.GroupID]*fixedPeerDataV1{command.GroupID: local}}
		target.draining.Store(true)
		got := replicaOwnerOperationErrorV1(context.Background(), client, target, command, "restart-replacement-prepare", raftcluster.ErrAdmissionUnavailable)
		for _, want := range []string{"calls=1/1", "reads=0/2", "draining=true", "closed=false", "group_observed=true", "group_present=true", "start_failed=true", "operation_matches=true", "prejoin_present=true", "receiver_present=true"} {
			if !strings.Contains(got, want) {
				t.Fatalf("missing %q: %s", want, got)
			}
		}
		if len(client.calls) != 1 || !target.draining.Load() || target.closed.Load() {
			t.Fatal("observation changed local state")
		}
	})
	t.Run("contended-state-is-unknown", func(t *testing.T) {
		target := &FixedPeerTCPRuntimeV1{}
		target.groupsMu.Lock()
		defer target.groupsMu.Unlock()
		got := replicaOwnerOperationErrorV1(context.Background(), nil, target, command, "target-close", raftcluster.ErrAdmissionUnavailable)
		if !strings.Contains(got, "group_observed=false") || !strings.Contains(got, "runtime_known=true") {
			t.Fatal("contended state was reported as observed: " + got)
		}
	})
	t.Run("context-and-original-joined-error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := errors.Join(raftcluster.ErrAdmissionUnavailable, errors.New("original-marker"))
		got := replicaOwnerOperationErrorV1(ctx, nil, nil, command, "warm-restart-cold", err)
		if !strings.Contains(got, "context_canceled=true") || !strings.Contains(got, "context_deadline=false") || !strings.HasSuffix(got, "original_error="+err.Error()) {
			t.Fatal("context or original error lost: " + got)
		}
	})
}
