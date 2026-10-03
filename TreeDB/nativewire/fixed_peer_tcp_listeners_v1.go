package nativewire

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"
)

func fixedPeerTCPListenAddressesV1(config FixedPeerTCPConfigV1) []string {
	addresses := []string{config.ListenAddress}
	for _, address := range config.RaftListen {
		addresses = append(addresses, address)
	}
	if config.Vector != nil && !fixedPeerImmutableVectorStandbyV1(config) {
		addresses = append(addresses, config.Vector.PublicAddresses[config.NodeID])
		for group, peers := range config.Vector.ShardAddresses {
			if _, hosted := config.RaftListen[group]; hosted && peers[config.NodeID] != "" {
				addresses = append(addresses, peers[config.NodeID])
			}
		}
	} else if config.Vector == nil && config.VectorInitialization != nil {
		addresses = append(addresses, config.VectorInitialization.PublicAddresses[config.NodeID])
		for group, peers := range config.VectorInitialization.ShardAddresses {
			if _, hosted := config.RaftListen[group]; hosted && peers[config.NodeID] != "" {
				addresses = append(addresses, peers[config.NodeID])
			}
		}
	}
	slices.Sort(addresses)
	return slices.Compact(addresses)
}

// Supplied listeners transfer ownership on entry, including failure. The
// public opener supplies none; fixtures can retain the exact reserved sockets.
func bindFixedPeerTCPListenersV1(config FixedPeerTCPConfigV1, supplied map[string]net.Listener) (map[string]net.Listener, error) {
	owned := make(map[string]net.Listener)
	for address, listener := range supplied {
		owned[address] = listener
	}
	fail := func(err error) (map[string]net.Listener, error) {
		for _, listener := range owned {
			if listener != nil {
				_ = listener.Close()
			}
		}
		return nil, err
	}
	addresses := fixedPeerTCPListenAddressesV1(config)
	for address, listener := range owned {
		if !slices.Contains(addresses, address) || listener == nil || listener.Addr().Network() != "tcp" || listener.Addr().String() != address {
			return fail(fmt.Errorf("invalid fixed-peer listener %q", address))
		}
	}
	for _, address := range addresses {
		if owned[address] != nil {
			continue
		}
		listener, err := net.Listen("tcp", address)
		if err != nil {
			return fail(fmt.Errorf("fixed-peer listen %s: %w", address, err))
		}
		owned[address] = listener
	}
	return owned, nil
}

// Cold vector roles must refuse traffic promptly while retaining the socket.
// Otherwise their TCP backlog hides backend activation errors until timeout.
func reserveFixedPeerDormantListenersV1(config FixedPeerTCPConfigV1, owned map[string]net.Listener) {
	if config.Vector != nil {
		for _, peers := range config.Vector.ShardAddresses {
			address := peers[config.NodeID]
			if listener := owned[address]; listener != nil {
				owned[address] = reserveFixedPeerTCPListenerV1(listener)
			}
		}
	} else if config.VectorInitialization != nil {
		address := config.VectorInitialization.PublicAddresses[config.NodeID]
		if listener := owned[address]; listener != nil {
			owned[address] = reserveFixedPeerTCPListenerV1(listener)
		}
		for group, peers := range config.VectorInitialization.ShardAddresses {
			if _, hosted := config.RaftListen[group]; !hosted {
				continue
			}
			if listener := owned[peers[config.NodeID]]; listener != nil {
				owned[peers[config.NodeID]] = reserveFixedPeerTCPListenerV1(listener)
			}
		}
	}
}

// A reservation rejects connections until the existing serving owner takes it.
// Take interrupts only the temporary Accept, clears its deadline, and returns
// the same TCP listener; serving traffic has no extra goroutine or wrapper.
type fixedPeerTCPReservationV1 struct {
	net.Listener
	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

func reserveFixedPeerTCPListenerV1(listener net.Listener) net.Listener {
	if _, ok := listener.(*fixedPeerTCPReservationV1); ok {
		return listener
	}
	r := &fixedPeerTCPReservationV1{Listener: listener, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		var retryDelay time.Duration
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-r.stop:
					return
				default:
				}
				var netErr net.Error
				if errors.Is(err, net.ErrClosed) || !errors.As(err, &netErr) || !netErr.Temporary() {
					return
				}
				if retryDelay == 0 {
					retryDelay = 5 * time.Millisecond
				} else {
					retryDelay = min(2*retryDelay, time.Second)
				}
				timer := time.NewTimer(retryDelay)
				select {
				case <-r.stop:
					timer.Stop()
					return
				case <-timer.C:
				}
				continue
			}
			retryDelay = 0
			_ = conn.Close()
		}
	}()
	return r
}

func (r *fixedPeerTCPReservationV1) takeV1() (net.Listener, error) {
	r.stopOnce.Do(func() { close(r.stop) })
	deadline, ok := r.Listener.(interface{ SetDeadline(time.Time) error })
	if !ok {
		_ = r.Close()
		return nil, fmt.Errorf("fixed-peer reserved listener has no deadline support")
	}
	if err := deadline.SetDeadline(time.Now()); err != nil {
		_ = r.Close()
		return nil, err
	}
	<-r.done
	if err := deadline.SetDeadline(time.Time{}); err != nil {
		_ = r.Listener.Close()
		return nil, err
	}
	return r.Listener, nil
}

func (r *fixedPeerTCPReservationV1) Close() error {
	r.stopOnce.Do(func() { close(r.stop) })
	err := r.Listener.Close()
	<-r.done
	return err
}

func (r *FixedPeerTCPRuntimeV1) takeListenerV1(address string) (net.Listener, error) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()
	if r.draining.Load() {
		return nil, net.ErrClosed
	}
	if listener := r.listeners[address]; listener != nil {
		delete(r.listeners, address)
		if reservation, ok := listener.(*fixedPeerTCPReservationV1); ok {
			return reservation.takeV1()
		}
		return listener, nil
	}
	// A backend can retire and reopen its endpoint after bootstrap.
	return net.Listen("tcp", address)
}
