package nativewire

import (
	"context"
	"net"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

type peerRequestModeV1 uint8

const (
	peerRequestIngressV1 peerRequestModeV1 = iota
	peerRequestDescendantV1
	peerRequestInternalV1
)

type peerRequestContextKeyV1 struct{}

// This capability is process-local, owner-bound and revoked when its original
// request finishes. It is never serialized or accepted from a peer.
type peerRequestLifetimeV1 struct {
	owner  *peerNodeAdmissionV1
	ctx    context.Context
	cancel context.CancelFunc
	stop   func() bool
}

func (a *peerNodeAdmissionV1) request(ctx context.Context, scope string, bytes int64, mode peerRequestModeV1) (peerWorkLeaseV1, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return peerWorkLeaseV1{}, err
	}
	if a == nil {
		return peerWorkLeaseV1{ctx: ctx}, nil
	}
	a.requestMu.Lock()
	defer a.requestMu.Unlock()
	if a.requestsFrozen {
		return peerWorkLeaseV1{}, net.ErrClosed
	}
	origin, _ := ctx.Value(peerRequestContextKeyV1{}).(*peerRequestLifetimeV1)
	valid := origin != nil && origin.owner == a && origin.ctx.Err() == nil
	if valid {
		_, valid = a.requests[origin]
	}
	if a.draining.Load() && mode != peerRequestInternalV1 && !(mode == peerRequestDescendantV1 && valid) {
		return peerWorkLeaseV1{}, raftcluster.ErrAdmissionUnavailable
	}
	work, err := a.work(scope, peerRequestsV1, bytes)
	if err != nil {
		return work, err
	}
	lifetime := &peerRequestLifetimeV1{owner: a}
	lifetime.ctx, lifetime.cancel = context.WithCancel(ctx)
	if mode != peerRequestIngressV1 && valid {
		lifetime.stop = context.AfterFunc(origin.ctx, lifetime.cancel)
	} else {
		origin = lifetime
	}
	a.requests[lifetime] = struct{}{}
	work.lifetime = lifetime
	work.ctx = context.WithValue(lifetime.ctx, peerRequestContextKeyV1{}, origin)
	return work, nil
}

func (l *peerRequestLifetimeV1) release() {
	if l == nil {
		return
	}
	a := l.owner
	a.requestMu.Lock()
	delete(a.requests, l)
	close(a.requestChanged)
	a.requestChanged = make(chan struct{})
	a.requestMu.Unlock()
	if l.stop != nil {
		l.stop()
	}
	l.cancel()
}

func (a *peerNodeAdmissionV1) beginDrain() {
	a.requestMu.Lock()
	a.draining.Store(true)
	a.requestMu.Unlock()
}

func (a *peerNodeAdmissionV1) freezeRequests() {
	a.requestMu.Lock()
	a.requestsFrozen = true
	cancel := make([]context.CancelFunc, 0, len(a.requests))
	for request := range a.requests {
		cancel = append(cancel, request.cancel)
	}
	a.requestMu.Unlock()
	for _, stop := range cancel {
		stop()
	}
}

// Keep internal dependencies available until all admitted work finishes. The
// transition to frozen is atomic with admission, including arriving shard RPCs.
func (a *peerNodeAdmissionV1) drainRequests(ctx context.Context) error {
	for {
		a.requestMu.Lock()
		if len(a.requests) == 0 {
			a.requestsFrozen = true
			a.requestMu.Unlock()
			return nil
		}
		changed := a.requestChanged
		a.requestMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			a.freezeRequests()
			return ctx.Err()
		}
	}
}
