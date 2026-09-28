// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"

	"github.com/hashicorp/go-hclog"
)

// A restarted follower may have the installed snapshot at index 3 and a
// configuration log at index 4 while the log store no longer contains 3.
// Replication must use the installed snapshot's term for PrevLogEntry=3.
func TestRaft_AppendEntriesAtCompactedSnapshotBoundary(t *testing.T) {
	newFollower := func(t *testing.T, retainedDivergentBoundary bool) (*Raft, *InmemStore, *Log) {
		t.Helper()
		_, transport := NewInmemTransport("follower")
		config := DefaultConfig()
		config.LocalID = "follower"
		configuration := Configuration{Servers: []Server{{ID: "follower", Address: "follower", Suffrage: Voter}}}
		configurationLog := &Log{Index: 4, Term: 2, Type: LogConfiguration, Data: EncodeConfiguration(configuration)}
		logs := NewInmemStore()
		retainedLog := configurationLog
		if retainedDivergentBoundary {
			retainedLog = &Log{Index: 3, Term: 1, Type: LogNoop}
		}
		if err := logs.StoreLog(retainedLog); err != nil {
			t.Fatal(err)
		}
		follower := &Raft{
			protocolVersion: config.ProtocolVersion,
			trans:           transport,
			localID:         config.LocalID,
			localAddr:       "follower",
			logger:          hclog.NewNullLogger(),
			logs:            logs,
			stable:          logs,
			fsmMutateCh:     make(chan interface{}, 1),
			shutdownCh:      make(chan struct{}),
		}
		follower.conf.Store(*config)
		follower.setState(Follower)
		follower.setCurrentTerm(2)
		follower.setLastSnapshot(3, 2)
		follower.setLastLog(retainedLog.Index, retainedLog.Term)
		follower.setLastApplied(3)
		configurationIndex := uint64(4)
		if retainedDivergentBoundary {
			configurationIndex = 3
		}
		follower.setLatestConfiguration(configuration, configurationIndex)
		follower.setCommittedConfiguration(configuration, 3)
		var boundary Log
		err := logs.GetLog(3, &boundary)
		if retainedDivergentBoundary {
			if err != nil || boundary.Term == 2 {
				t.Fatalf("divergent snapshot boundary must remain in log store: log=%+v err=%v", boundary, err)
			}
		} else if err != ErrLogNotFound {
			t.Fatalf("snapshot boundary must be absent from log store: %v", err)
		}
		return follower, logs, configurationLog
	}
	appendTo := func(t *testing.T, follower *Raft, request *AppendEntriesRequest) *AppendEntriesResponse {
		t.Helper()
		responses := make(chan RPCResponse, 1)
		follower.appendEntries(RPC{RespChan: responses}, request)
		result := <-responses
		if result.Error != nil {
			t.Fatalf("append RPC error: %v", result.Error)
		}
		response, ok := result.Response.(*AppendEntriesResponse)
		if !ok {
			t.Fatalf("append response type %T", result.Response)
		}
		return response
	}
	request := func(previousIndex, previousTerm uint64, entries ...*Log) *AppendEntriesRequest {
		return &AppendEntriesRequest{
			RPCHeader:    RPCHeader{ID: []byte("leader"), Addr: []byte("leader")},
			Term:         2,
			PrevLogEntry: previousIndex,
			PrevLogTerm:  previousTerm,
			Entries:      entries,
		}
	}

	t.Run("exact installed boundary and subsequent command", func(t *testing.T) {
		follower, logs, configurationLog := newFollower(t, false)
		if response := appendTo(t, follower, request(3, 2, configurationLog)); !response.Success {
			t.Fatalf("identical configuration after installed snapshot refused: %+v", response)
		}
		command := &Log{Index: 5, Term: 2, Type: LogCommand, Data: []byte("document-command")}
		commit := request(4, 2, command)
		commit.LeaderCommitIndex = 5
		if response := appendTo(t, follower, commit); !response.Success {
			t.Fatalf("post-snapshot command refused: %+v", response)
		}
		var stored Log
		if err := logs.GetLog(5, &stored); err != nil || string(stored.Data) != string(command.Data) {
			t.Fatalf("post-snapshot command not stored: log=%+v err=%v", stored, err)
		}
		if follower.getCommitIndex() != 5 || follower.getLastApplied() != 5 {
			t.Fatalf("post-snapshot command not advanced: commit=%d applied=%d", follower.getCommitIndex(), follower.getLastApplied())
		}
		select {
		case mutation := <-follower.fsmMutateCh:
			batch, ok := mutation.([]*commitTuple)
			if !ok || len(batch) != 2 || batch[1].log.Index != 5 {
				t.Fatalf("unexpected FSM batch: %#v", mutation)
			}
		default:
			t.Fatal("committed command was not sent to the FSM")
		}
	})

	t.Run("retained divergent log at installed boundary", func(t *testing.T) {
		follower, logs, configurationLog := newFollower(t, true)
		if response := appendTo(t, follower, request(3, 2, configurationLog)); !response.Success {
			t.Fatalf("installed snapshot boundary term refused in favor of stale log: %+v", response)
		}
		if follower.getLastIndex() != 4 {
			t.Fatalf("configuration after installed snapshot was not appended: last=%d", follower.getLastIndex())
		}
		var stored Log
		if err := logs.GetLog(4, &stored); err != nil || stored.Term != configurationLog.Term {
			t.Fatalf("post-snapshot configuration not stored: log=%+v err=%v", stored, err)
		}
	})

	t.Run("retained divergent log does not authorize stale term", func(t *testing.T) {
		follower, _, configurationLog := newFollower(t, true)
		response := appendTo(t, follower, request(3, 1, configurationLog))
		if response.Success || !response.NoRetryBackoff || follower.getLastIndex() != 3 {
			t.Fatalf("stale retained boundary term accepted: response=%+v last=%d", response, follower.getLastIndex())
		}
	})

	for _, test := range []struct {
		name          string
		previousIndex uint64
		previousTerm  uint64
	}{
		{name: "wrong installed term", previousIndex: 3, previousTerm: 1},
		{name: "older unknown predecessor", previousIndex: 2, previousTerm: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			follower, logs, configurationLog := newFollower(t, false)
			response := appendTo(t, follower, request(test.previousIndex, test.previousTerm, configurationLog))
			if response.Success || !response.NoRetryBackoff {
				t.Fatalf("invalid predecessor accepted: %+v", response)
			}
			if follower.getLastIndex() != 4 || follower.getCommitIndex() != 0 || follower.getLastApplied() != 3 {
				t.Fatalf("invalid predecessor advanced state: last=%d commit=%d applied=%d", follower.getLastIndex(), follower.getCommitIndex(), follower.getLastApplied())
			}
			if first, err := logs.FirstIndex(); err != nil || first != 4 {
				t.Fatalf("invalid predecessor changed log floor: first=%d err=%v", first, err)
			}
		})
	}
}
