//go:build !windows

package nativewire

import (
	"encoding/json"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

func fixedPeerPassTestListenersV1(t testing.TB, command *exec.Cmd, _ io.WriteCloser, config FixedPeerTCPConfigV1) (func(), error) {
	listeners, err := bindFixedPeerTCPListenersV1(config, fixedPeerTakeTestListenersV1(config))
	var files []*os.File
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, file := range files {
				_ = file.Close()
			}
			for _, listener := range listeners {
				_ = listener.Close()
			}
		})
	}
	if err != nil {
		return release, err
	}
	addresses := fixedPeerTCPListenAddressesV1(config)
	for _, address := range addresses {
		file, err := listeners[address].(*net.TCPListener).File()
		if err != nil {
			release()
			return func() {}, err
		}
		files = append(files, file)
	}
	path := filepath.Join(t.TempDir(), "listener-addresses.json")
	raw, err := json.Marshal(addresses)
	if err == nil {
		err = os.WriteFile(path, raw, 0600)
	}
	if err != nil {
		release()
		return func() {}, err
	}
	command.ExtraFiles = files
	command.Env = append(command.Env, "GOMAP_FIXED_PEER_TEST_LISTENERS="+path)
	return release, nil
}

func fixedPeerFinishTestListenerTransferV1(_ testing.TB, _ *exec.Cmd, _ io.WriteCloser) error {
	return nil
}

func fixedPeerChildListenersV1() (map[string]net.Listener, error) {
	path := os.Getenv("GOMAP_FIXED_PEER_TEST_LISTENERS")
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var addresses []string
	if err := json.Unmarshal(raw, &addresses); err != nil {
		return nil, err
	}
	listeners := make(map[string]net.Listener)
	for i, address := range addresses {
		file := os.NewFile(uintptr(3+i), "fixed-peer-listener")
		listener, err := net.FileListener(file)
		_ = file.Close()
		if err != nil {
			for _, listener := range listeners {
				_ = listener.Close()
			}
			return nil, err
		}
		listeners[address] = listener
	}
	return listeners, nil
}

func fixedPeerSubprocessAllocatorV1(t testing.TB) func(raftcluster.NodeID) string {
	return fixedPeerTestAllocatorV1(t, nil)
}
func fixedPeerClaimStagedTestProcessV1(testing.TB, FixedPeerTCPConfigV1) *fixedPeerTestProcessV1 {
	return nil
}

func fixedPeerSparseSubprocessAllocatorV1(t testing.TB) func(raftcluster.NodeID) string {
	return fixedPeerSubprocessAllocatorV1(t)
}
