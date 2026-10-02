package nativewire

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// This witness also runs against the old allocator: every successful competing
// bind is a real stolen advertised role, rather than an inferred lease gap.
func TestFixedPeerFixtureRetainsBootstrapListenersV1(t *testing.T) {
	configs := fixedPeerVectorTestConfigsV1(t, fixedPeerVectorSeedV1(t))
	for _, config := range configs {
		roles := map[string]string{config.ListenAddress: "control", config.Vector.PublicAddresses[config.NodeID]: "vector-public"}
		for group, address := range config.RaftListen {
			roles[address] = "raft:" + string(group)
		}
		for group, peers := range config.Vector.ShardAddresses {
			if address := peers[config.NodeID]; address != "" {
				roles[address] = "vector-shard:" + string(group)
			}
		}
		for address, role := range roles {
			stolen, err := net.Listen("tcp", address)
			if err == nil {
				t.Errorf("stolen advertised socket node=%s role=%s address=%s owner_pid=%d", config.NodeID, role, address, os.Getpid())
				if raw, err := stolen.(*net.TCPListener).SyscallConn(); err == nil {
					_ = raw.Control(func(fd uintptr) {
						socket, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), fd))
						t.Logf("stolen role=%s fd=%d socket=%s", role, fd, socket)
					})
				}
				_ = stolen.Close()
			}
		}
	}
}

func TestFixedPeerListenerOwnershipFailureCleanupV1(t *testing.T) {
	address := fixedPeerReserveTestAddressV1(t, nil)
	config := fixedPeerTestConfigsV1(t)[0]
	config.DataRoot = "invalid-relative-root"
	supplied := fixedPeerTakeTestListenersV1(FixedPeerTCPConfigV1{ListenAddress: address})
	if _, err := openFixedPeerTCPRuntimeV1(config, supplied); err == nil {
		t.Fatal("invalid config admitted")
	}
	assertFree := func(address string) {
		t.Helper()
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("owned listener leaked %s: %v", address, err)
		}
		listener.Close()
	}
	assertFree(address)
	if _, err := bindFixedPeerTCPListenersV1(FixedPeerTCPConfigV1{ListenAddress: address}, map[string]net.Listener{address: nil}); err == nil {
		t.Fatal("nil listener admitted")
	}
	first := fixedPeerReserveTestAddressV1(t, nil)
	blocked := fixedPeerReserveTestAddressV1(t, nil)
	owned := fixedPeerTakeTestListenersV1(FixedPeerTCPConfigV1{ListenAddress: first})
	if _, err := bindFixedPeerTCPListenersV1(FixedPeerTCPConfigV1{ListenAddress: first, RaftListen: map[raftcluster.GroupID]string{"blocked": blocked}}, owned); err == nil {
		t.Fatal("occupied role admitted")
	}
	assertFree(first)
	config = fixedPeerTestConfigsV1(t)[0]
	config.Nodes, config.Catalog.Peers, config.Groups = config.Nodes[:1], config.Catalog.Peers[:1], config.Groups[:1]
	config.RaftRoot = filepath.Join(t.TempDir(), "raft")
	if err := os.MkdirAll(config.RaftRoot, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config.RaftRoot, "fixed-peer-v1.json"), []byte("mismatch"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := fixedPeerOpenTestRuntimeV1(t, config); err == nil {
		t.Fatal("persisted mismatch admitted")
	}
	for _, address := range fixedPeerTCPListenAddressesV1(config) {
		assertFree(address)
	}
	r := &FixedPeerTCPRuntimeV1{}
	r.draining.Store(true)
	if _, err := r.takeListenerV1(address); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("drained runtime reopened endpoint: %v", err)
	}
}

var fixedPeerTestListenersV1 = struct {
	sync.Mutex
	listeners map[string]net.Listener
}{listeners: make(map[string]net.Listener)}

func TestFixedPeerDormantListenerRefusalTakeoverV1(t *testing.T) {
	for i := 0; i < 20; i++ {
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		reservation := reserveFixedPeerTCPListenerV1(raw).(*fixedPeerTCPReservationV1)
		address := raw.Addr().String()
		conn, err := net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, err = conn.Read(make([]byte, 1))
		conn.Close()
		if err == nil {
			t.Fatal("dormant role accepted serving traffic")
		}
		if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
			t.Fatalf("dormant role blackholed request: %v", err)
		}
		r := &FixedPeerTCPRuntimeV1{listeners: map[string]net.Listener{address: reservation}}
		serving, err := r.takeListenerV1(address)
		if err != nil {
			t.Fatal(err)
		}
		if serving != raw {
			t.Fatal("takeover replaced the reserved socket")
		}
		select {
		case <-reservation.done:
		default:
			t.Fatal("reservation pump survived takeover")
		}
		accepted := make(chan error, 1)
		go func() {
			conn, err := serving.Accept()
			if err == nil {
				_, err = conn.Write([]byte{42})
				conn.Close()
			}
			accepted <- err
		}()
		conn, err = net.Dial("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		value := make([]byte, 1)
		_, err = io.ReadFull(conn, value)
		conn.Close()
		if err != nil || value[0] != 42 {
			t.Fatalf("takeover did not restore serving deadline: value=%v err=%v", value, err)
		}
		if err := <-accepted; err != nil {
			t.Fatal(err)
		}
		serving.Close()
	}
}

type fixedPeerCountedListenerV1 struct {
	*net.TCPListener
	closes       atomic.Int32
	failDeadline bool
}

func (l *fixedPeerCountedListenerV1) Close() error { l.closes.Add(1); return l.TCPListener.Close() }
func (l *fixedPeerCountedListenerV1) SetDeadline(deadline time.Time) error {
	if l.failDeadline {
		return errors.New("controlled deadline failure")
	}
	return l.TCPListener.SetDeadline(deadline)
}

func TestFixedPeerDormantListenerFailureCleanupV1(t *testing.T) {
	for _, takeFailure := range []bool{false, true} {
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		counted := &fixedPeerCountedListenerV1{TCPListener: raw.(*net.TCPListener), failDeadline: takeFailure}
		reservation := reserveFixedPeerTCPListenerV1(counted).(*fixedPeerTCPReservationV1)
		if takeFailure {
			if _, err := reservation.takeV1(); err == nil {
				t.Fatal("failed transfer admitted")
			}
		} else if err := reservation.Close(); err != nil {
			t.Fatal(err)
		}
		if counted.closes.Load() != 1 {
			t.Fatalf("socket closed %d times", counted.closes.Load())
		}
		select {
		case <-reservation.done:
		default:
			t.Fatal("closed reservation pump leaked")
		}
		probe, err := net.Listen("tcp", raw.Addr().String())
		if err != nil {
			t.Fatalf("closed reservation retained address: %v", err)
		}
		probe.Close()
	}
}

func TestFixedPeerDormantListenerResourceBoundsV1(t *testing.T) {
	const count = 32
	var sockets [count]net.Listener
	for i := range sockets {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		sockets[i] = listener
	}
	fdCount := func() int {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return -1
		}
		return len(entries)
	}
	beforeFD, beforeG := fdCount(), runtime.NumGoroutine()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i, socket := range sockets {
		sockets[i] = reserveFixedPeerTCPListenerV1(socket)
	}
	runtime.ReadMemStats(&after)
	dormantG, dormantFD := runtime.NumGoroutine(), fdCount()
	for _, socket := range sockets {
		if err := socket.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for range 100 {
		if runtime.NumGoroutine() <= beforeG {
			break
		}
		runtime.Gosched()
	}
	if dormantG-beforeG > count+2 {
		t.Fatalf("unbounded dormant goroutines: before=%d dormant=%d roles=%d", beforeG, dormantG, count)
	}
	if beforeFD >= 0 && dormantFD != beforeFD {
		t.Fatalf("reservation duplicated descriptors: before=%d dormant=%d", beforeFD, dormantFD)
	}
	extraFD := -1
	if beforeFD >= 0 && dormantFD >= 0 {
		extraFD = dormantFD - beforeFD
	}
	t.Logf("dormant_listener_resources roles=%d added_goroutines=%d extra_fds=%d total_alloc_bytes=%d total_allocations=%d closed_goroutines=%d", count, dormantG-beforeG, extraFD, after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, runtime.NumGoroutine()-beforeG)
}

func TestFixedPeerListenerChildStartFailureCleanupV1(t *testing.T) {
	config := fixedPeerTestConfigsV1(t)[0]
	command := exec.Command(filepath.Join(t.TempDir(), "missing-child"))
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	release, err := fixedPeerPassTestListenersV1(t, command, input, config)
	defer release()
	if err == nil {
		if err := command.Start(); err == nil {
			command.Process.Kill()
			command.Wait()
			t.Fatal("controlled missing child started")
		}
	} else {
		t.Logf("child setup rejected held ownership before exec: %v", err)
	}
	release()
	for _, address := range fixedPeerTCPListenAddressesV1(config) {
		probe, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("failed child retained advertised role %s: %v", address, err)
		}
		probe.Close()
	}
}

func TestFixedPeerConsumedListenerCleanupV1(t *testing.T) {
	for _, reject := range []bool{false, true} {
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		counted := &fixedPeerCountedListenerV1{TCPListener: raw.(*net.TCPListener)}
		r := &FixedPeerTCPRuntimeV1{listeners: map[string]net.Listener{raw.Addr().String(): counted}}
		listener, err := r.takeListenerV1(raw.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		var admission *peerNodeAdmissionV1
		if reject {
			admission = &peerNodeAdmissionV1{}
			admission.stats.Closed = true
		}
		transport, stream, err := newFixedPeerTCPTransportListenerV1(listener, raw.Addr(), time.Second, nil, nil, admission, "raft:test")
		if reject {
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("closed admission accepted consumed socket: %v", err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			transport.Close()
			stream.Close()
		}
		if counted.closes.Load() != 1 || len(r.listeners) != 0 {
			t.Fatalf("consumed ownership closes=%d pool=%d", counted.closes.Load(), len(r.listeners))
		}
	}
}

func fixedPeerRawTestListenerV1(listener net.Listener) net.Listener {
	for {
		switch l := listener.(type) {
		case *fixedPeerTCPReservationV1:
			listener = l.Listener
		case *peerSecureListenerV1:
			listener = l.Listener
		case *peerNodeListenerV1:
			listener = l.Listener
		default:
			return listener
		}
	}
}

func fixedPeerReservedTestListenerV1(r *FixedPeerTCPRuntimeV1, address string) net.Listener {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()
	return fixedPeerRawTestListenerV1(r.listeners[address])
}

func fixedPeerReserveTestAddressV1(t testing.TB, excluded map[string]bool) string {
	t.Helper()
	for {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		fixedPeerTestListenersV1.Lock()
		fixedPeerTestListenersV1.listeners[address] = listener
		fixedPeerTestListenersV1.Unlock()
		t.Cleanup(func() {
			fixedPeerTestListenersV1.Lock()
			if fixedPeerTestListenersV1.listeners[address] == listener {
				delete(fixedPeerTestListenersV1.listeners, address)
				_ = listener.Close()
			}
			fixedPeerTestListenersV1.Unlock()
		})
		if !excluded[address] {
			return address
		}
	}
}

func fixedPeerTakeTestListenersV1(config FixedPeerTCPConfigV1) map[string]net.Listener {
	// A Nodes-only immutable standby deliberately has no public serving socket.
	if config.Vector != nil && fixedPeerImmutableVectorStandbyV1(config) {
		fixedPeerReleaseTestAddressV1(config.Vector.PublicAddresses[config.NodeID])
	}
	fixedPeerTestListenersV1.Lock()
	defer fixedPeerTestListenersV1.Unlock()
	listeners := make(map[string]net.Listener)
	for _, address := range fixedPeerTCPListenAddressesV1(config) {
		if listener := fixedPeerTestListenersV1.listeners[address]; listener != nil {
			listeners[address] = listener
			delete(fixedPeerTestListenersV1.listeners, address)
		}
	}
	return listeners
}

func fixedPeerOpenTestRuntimeV1(t testing.TB, config FixedPeerTCPConfigV1) (*FixedPeerTCPRuntimeV1, error) {
	t.Helper()
	listeners := fixedPeerTakeTestListenersV1(config)
	return openFixedPeerTCPRuntimeV1(config, listeners)
}

func fixedPeerReleaseTestAddressV1(address string) {
	fixedPeerTestListenersV1.Lock()
	defer fixedPeerTestListenersV1.Unlock()
	if listener := fixedPeerTestListenersV1.listeners[address]; listener != nil {
		_ = listener.Close()
		delete(fixedPeerTestListenersV1.listeners, address)
	}
}

// Future replacement endpoints stay reserved without granting serving authority.
func fixedPeerAdoptTestAddressV1(r *FixedPeerTCPRuntimeV1, address string) {
	fixedPeerTestListenersV1.Lock()
	listener := fixedPeerTestListenersV1.listeners[address]
	delete(fixedPeerTestListenersV1.listeners, address)
	fixedPeerTestListenersV1.Unlock()
	if listener != nil {
		r.listenersMu.Lock()
		r.listeners[address] = reserveFixedPeerTCPListenerV1(listener)
		r.listenersMu.Unlock()
	}
}

func init() {
	fixedPeerColdResourceReleaseV1 = func(configs []FixedPeerTCPConfigV1) {
		for _, config := range configs {
			for _, address := range fixedPeerTCPListenAddressesV1(config) {
				fixedPeerReleaseTestAddressV1(address)
			}
		}
	}
	sparseCatalogBenchmarkConfigsSetupV1 = func(t testing.TB, sparse bool, inventory int) []FixedPeerTCPConfigV1 {
		allocate := fixedPeerSparseSubprocessAllocatorV1(t)
		return sparseCatalogBenchmarkConfigsFromV1(t, sparse, inventory, fixedPeerTestConfigsV1(t, allocate), allocate)
	}
	sparseCatalogBenchmarkOpenV1 = func(config FixedPeerTCPConfigV1) (*FixedPeerTCPRuntimeV1, error) {
		listeners, err := fixedPeerChildListenersV1()
		if err != nil {
			return nil, err
		}
		if os.Getenv("GOMAP_FIXED_PEER_TEST_STAGE") != "" {
			config, err = fixedPeerReadTestConfigV1(os.Getenv("GOMAP_FIXED_PEER_TEST_CONFIG_FILE"))
			if err != nil {
				for _, listener := range listeners {
					_ = listener.Close()
				}
				return nil, err
			}
		}
		return openFixedPeerTCPRuntimeV1(config, listeners)
	}
	sparseCatalogBenchmarkStartV1 = func(t testing.TB, command *exec.Cmd, input io.WriteCloser, log *os.File, config FixedPeerTCPConfigV1) *fixedPeerTestProcessV1 {
		if staged := fixedPeerClaimStagedTestProcessV1(t, config); staged != nil {
			_ = input.Close()
			_ = log.Close()
			return staged
		}
		release, err := fixedPeerPassTestListenersV1(t, command, input, config)
		defer release()
		if err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		if err := fixedPeerFinishTestListenerTransferV1(t, command, input); err != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatal(err)
		}
		return &fixedPeerTestProcessV1{command: command, input: input, log: log}
	}
}

func fixedPeerTestAllocatorV1(t testing.TB, supplied []func(raftcluster.NodeID) string) func(raftcluster.NodeID) string {
	if len(supplied) > 0 {
		return supplied[0]
	}
	return func(raftcluster.NodeID) string { return fixedPeerReserveTestAddressV1(t, nil) }
}
