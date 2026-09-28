package nativewire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"sync"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type replacementReceiverPhaseV1 string

const (
	replacementReceiverPreparedV1   replacementReceiverPhaseV1 = "prepared"
	replacementReceiverInstallingV1 replacementReceiverPhaseV1 = "installing"
	replacementReceiverInstalledV1  replacementReceiverPhaseV1 = "installed"
	replacementReceiverAddIntentV1  replacementReceiverPhaseV1 = "add-intent"
)

// replacementPrejoinTransportV1 is present even when optional resource
// admission is disabled. Its state is restored from the exact operation record
// before NewRaft can register its Consumer or heartbeat callback. The two
// function values belong to that record's durable owner; they are not caller
// assertions of install success.
//
// Shutdown of the network is not native install completion. The runtime calls
// providerStopped only after the real provider Shutdown future has completed.
// Until then an accepted install owns its fence, even if its response is lost.
type replacementPrejoinTransportV1 struct {
	hraft.Transport
	mu       sync.Mutex
	seed     raftcluster.ReplacementSnapshotSeedV1
	phase    replacementReceiverPhaseV1
	inflight bool
	verified bool
	persist  func(replacementReceiverPhaseV1) error
	verify   func(raftcluster.ReplacementSnapshotSeedV1) error
	inbox    chan hraft.RPC
	stopped  chan struct{}
	stopOnce sync.Once
	workers  sync.WaitGroup
}

func newReplacementPrejoinTransportV1(inner hraft.Transport, seed raftcluster.ReplacementSnapshotSeedV1, phase replacementReceiverPhaseV1, persist func(replacementReceiverPhaseV1) error, verify func(raftcluster.ReplacementSnapshotSeedV1) error) (*replacementPrejoinTransportV1, error) {
	if inner == nil || seed.Validate() != nil || persist == nil || verify == nil {
		return nil, raftcluster.ErrInvalidConfig
	}
	switch phase {
	case replacementReceiverPreparedV1, replacementReceiverInstallingV1, replacementReceiverInstalledV1, replacementReceiverAddIntentV1:
	default:
		return nil, raftcluster.ErrInvalidConfig
	}
	t := &replacementPrejoinTransportV1{Transport: inner, seed: seed, phase: phase, persist: persist, verify: verify, inbox: make(chan hraft.RPC, 32), stopped: make(chan struct{})}
	t.workers.Add(1)
	go t.receive()
	return t, nil
}

func (t *replacementPrejoinTransportV1) Consumer() <-chan hraft.RPC { return t.inbox }

func (t *replacementPrejoinTransportV1) ordinaryAllowed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phase == replacementReceiverAddIntentV1 && !t.inflight
}

func (t *replacementPrejoinTransportV1) SetHeartbeatHandler(callback func(hraft.RPC)) {
	// Native NetworkTransport bypasses Consumer for this path.
	t.Transport.SetHeartbeatHandler(func(rpc hraft.RPC) {
		if !t.ordinaryAllowed() {
			rpc.Respond(nil, raftcluster.ErrAdmissionUnavailable)
			return
		}
		callback(rpc)
	})
}

func (t *replacementPrejoinTransportV1) RequestPreVote(id hraft.ServerID, target hraft.ServerAddress, args *hraft.RequestPreVoteRequest, response *hraft.RequestPreVoteResponse) error {
	if transport, ok := t.Transport.(hraft.WithPreVote); ok {
		return transport.RequestPreVote(id, target, args, response)
	}
	return hraft.ErrTransportShutdown
}

func (t *replacementPrejoinTransportV1) beginInstall(request *hraft.InstallSnapshotRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase == replacementReceiverAddIntentV1 {
		// This floor is permanent. Native Raft does not reject an incoming
		// snapshot merely because its index is below lastApplied.
		if request.LastLogIndex <= t.seed.Index {
			return raftcluster.ErrAdmissionUnavailable
		}
		return nil
	}
	if t.phase != replacementReceiverPreparedV1 || t.inflight || !t.seed.MatchesRequestV1(request) {
		return raftcluster.ErrAdmissionUnavailable
	}
	if err := t.persist(replacementReceiverInstallingV1); err != nil {
		// A failed namespace sync may have published the intent. Never reopen
		// installation from a cached old phase after an ambiguous write.
		t.phase = replacementReceiverInstallingV1
		return err
	}
	t.phase, t.inflight = replacementReceiverInstallingV1, true
	return nil
}

func (t *replacementPrejoinTransportV1) completeInstall(result hraft.RPCResponse, digest string) hraft.RPCResponse {
	t.mu.Lock()
	if !t.inflight {
		t.mu.Unlock()
		return result
	}
	t.mu.Unlock()
	// The fence stays held while validation streams durable state. Do not hold
	// mu through that work: quarantined heartbeats must refuse promptly.
	response, ok := result.Response.(*hraft.InstallSnapshotResponse)
	if result.Error == nil && ok && response.Success {
		if digest != t.seed.ArchiveSHA256 {
			result.Error = raftcluster.ErrInvalidSnapshotManifest
		} else {
			result.Error = t.verify(t.seed)
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() { t.inflight = false }()
	if result.Error != nil || !ok || !response.Success {
		return result
	}
	if err := t.persist(replacementReceiverInstalledV1); err != nil {
		result.Error = err
		return result
	}
	t.phase, t.verified = replacementReceiverInstalledV1, true
	return result
}

// reconcileInstalled verifies an interrupted native completion after restart.
// Ordinary traffic stays quarantined throughout, and absence is never treated
// as permission to replay an ambiguous seed. Native startup has completed before
// the runtime calls this, so its persisted snapshot and recovered FSM are the
// authority; a cached receipt by itself does not reopen replication.
func (t *replacementPrejoinTransportV1) reconcileInstalled() error {
	t.mu.Lock()
	if t.inflight || (t.phase != replacementReceiverInstallingV1 && t.phase != replacementReceiverInstalledV1) {
		t.mu.Unlock()
		return raftcluster.ErrAdmissionUnavailable
	}
	t.inflight = true
	t.mu.Unlock()
	err := t.verify(t.seed)
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() { t.inflight = false }()
	if err != nil {
		return err
	}
	if err := t.persist(replacementReceiverInstalledV1); err != nil {
		return err
	}
	t.phase, t.verified = replacementReceiverInstalledV1, true
	return nil
}

// allowEnrollment is invoked only after the committed operation has recorded
// enrollment intent and actual native completion has a recoverable receipt.
func (t *replacementPrejoinTransportV1) allowEnrollment() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight {
		return raftcluster.ErrAdmissionUnavailable
	}
	if t.phase == replacementReceiverAddIntentV1 {
		return nil
	}
	if t.phase != replacementReceiverInstalledV1 || !t.verified {
		return raftcluster.ErrAdmissionUnavailable
	}
	if err := t.persist(replacementReceiverAddIntentV1); err != nil {
		return err
	}
	t.phase = replacementReceiverAddIntentV1
	return nil
}

func (t *replacementPrejoinTransportV1) receive() {
	defer t.workers.Done()
	for {
		select {
		case <-t.stopped:
			return
		case rpc, ok := <-t.Transport.Consumer():
			if !ok {
				return
			}
			request, snapshot := rpc.Command.(*hraft.InstallSnapshotRequest)
			if !snapshot {
				if !t.ordinaryAllowed() {
					rpc.Respond(nil, raftcluster.ErrAdmissionUnavailable)
					continue
				}
				select {
				case t.inbox <- rpc:
				case <-t.stopped:
					rpc.Respond(nil, hraft.ErrTransportShutdown)
				}
				continue
			}
			if err := t.beginInstall(request); err != nil {
				rpc.Respond(nil, err)
				continue
			}
			// Later ordinary native snapshots need only the permanent floor.
			// No seed hashing or prejoin completion owner survives enrollment.
			if t.ordinaryAllowed() {
				select {
				case t.inbox <- rpc:
				case <-t.stopped:
					rpc.Respond(nil, hraft.ErrTransportShutdown)
				}
				continue
			}
			response := make(chan hraft.RPCResponse, 1)
			forward := rpc
			forward.RespChan = response
			digest := sha256.New()
			forward.Reader = &replacementSeedHashReaderV1{Reader: rpc.Reader, digest: digest}
			select {
			case t.inbox <- forward:
				t.workers.Add(1)
				go func(original hraft.RPC, response <-chan hraft.RPCResponse, digest hash.Hash) {
					defer t.workers.Done()
					select {
					case result := <-response:
						result = t.completeInstall(result, hex.EncodeToString(digest.Sum(nil)))
						original.Respond(result.Response, result.Error)
					case <-t.stopped:
						original.Respond(nil, hraft.ErrTransportShutdown)
					}
				}(rpc, response, digest)
			case <-t.stopped:
				rpc.Respond(nil, hraft.ErrTransportShutdown)
			}
		}
	}
}

type replacementSeedHashReaderV1 struct {
	io.Reader
	digest hash.Hash
}

func (r *replacementSeedHashReaderV1) Read(p []byte) (int, error) {
	if r.Reader == nil {
		return 0, errors.New("replacement snapshot reader is missing")
	}
	n, err := r.Reader.Read(p)
	if n != 0 {
		_, _ = r.digest.Write(p[:n])
	}
	return n, err
}

func (t *replacementPrejoinTransportV1) providerStopped() {
	t.stopOnce.Do(func() { close(t.stopped) })
	t.workers.Wait()
}
