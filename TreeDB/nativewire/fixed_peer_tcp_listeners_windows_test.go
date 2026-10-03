package nativewire

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
)

// The eventual Windows runtime child selects and owns each socket. Go's public
// APIs cannot safely adopt a raw WSADuplicateSocket handle as a net.Listener.
// Staging is confined to subprocess fixtures; serving still starts only when
// the existing start helper sends the finalized config.
type fixedPeerWindowsStageV1 struct {
	process   *fixedPeerTestProcessV1
	path      string
	sequence  int
	activated bool
	addresses []string
}
type fixedPeerWindowsStageCommandV1 struct {
	Sequence int
	Activate bool
}
type fixedPeerWindowsStageReplyV1 struct {
	Address string
	Error   string
}

var fixedPeerWindowsStagesV1 = struct {
	sync.Mutex
	byAddress map[string]*fixedPeerWindowsStageV1
}{byAddress: make(map[string]*fixedPeerWindowsStageV1)}

func fixedPeerSubprocessAllocatorV1(t testing.TB) func(raftcluster.NodeID) string {
	return fixedPeerWindowsAllocatorV1(t, "TestFixedPeerTCPRuntimeProcessV1")
}
func fixedPeerWindowsAllocatorV1(t testing.TB, entry string) func(raftcluster.NodeID) string {
	stages := make(map[raftcluster.NodeID]*fixedPeerWindowsStageV1)
	return func(id raftcluster.NodeID) string {
		stage := stages[id]
		if stage == nil {
			if len(stages) >= 32 {
				t.Fatal("subprocess fixture exceeds child budget")
			}
			path := filepath.Join(t.TempDir(), "node-config.json")
			log, err := os.CreateTemp(t.TempDir(), "node-log-")
			if err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^"+entry+"$", "-test.v")
			command.Env = append(os.Environ(), "GOMAP_FIXED_PEER_TEST_CONFIG_FILE="+path, "GOMAP_FIXED_PEER_TEST_STAGE="+path, "GOMAP_SPARSE_CATALOG_BENCH_CONFIG={}")
			command.Stdout, command.Stderr = log, log
			input, err := command.StdinPipe()
			if err != nil {
				log.Close()
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				input.Close()
				log.Close()
				t.Fatal(err)
			}
			stage = &fixedPeerWindowsStageV1{process: &fixedPeerTestProcessV1{command: command, input: input, log: log}, path: path}
			stages[id] = stage
			t.Cleanup(func() {
				fixedPeerWindowsStagesV1.Lock()
				for _, address := range stage.addresses {
					if fixedPeerWindowsStagesV1.byAddress[address] == stage {
						delete(fixedPeerWindowsStagesV1.byAddress, address)
					}
				}
				fixedPeerWindowsStagesV1.Unlock()
				if !stage.activated && !stage.process.stopped {
					stage.process.stopped = true
					_ = stage.process.input.Close()
					_ = stage.process.command.Process.Kill()
					_ = stage.process.command.Wait()
					_ = stage.process.log.Close()
				}
			})
		}
		if len(stage.addresses) >= 128 {
			t.Fatal("subprocess fixture exceeds socket budget")
		}
		stage.sequence++
		if err := json.NewEncoder(stage.process.input).Encode(fixedPeerWindowsStageCommandV1{Sequence: stage.sequence}); err != nil {
			t.Fatal(err)
		}
		reply, err := fixedPeerWindowsWaitReplyV1(stage, stage.sequence)
		if err != nil {
			t.Fatal(err)
		}
		stage.addresses = append(stage.addresses, reply.Address)
		fixedPeerWindowsStagesV1.Lock()
		fixedPeerWindowsStagesV1.byAddress[reply.Address] = stage
		fixedPeerWindowsStagesV1.Unlock()
		t.Logf("selected listener node=%s address=%s owner_pid=%d", id, reply.Address, stage.process.command.Process.Pid)
		return reply.Address
	}
}
func fixedPeerWindowsWaitReplyV1(stage *fixedPeerWindowsStageV1, sequence int) (fixedPeerWindowsStageReplyV1, error) {
	path := fmt.Sprintf("%s.reply-%d", stage.path, sequence)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := fixedPeerReadWindowsReplyV1(path)
		if err == nil {
			var reply fixedPeerWindowsStageReplyV1
			if err := json.Unmarshal(raw, &reply); err != nil {
				return reply, err
			}
			if reply.Error != "" {
				return reply, errors.New(reply.Error)
			}
			return reply, nil
		}
		if !os.IsNotExist(err) {
			return fixedPeerWindowsStageReplyV1{}, err
		}
		time.Sleep(time.Millisecond)
	}
	raw, _ := os.ReadFile(stage.process.log.Name())
	return fixedPeerWindowsStageReplyV1{}, fmt.Errorf("child socket ownership acknowledgement deadline: %s", raw)
}
func fixedPeerClaimStagedTestProcessV1(t testing.TB, config FixedPeerTCPConfigV1) *fixedPeerTestProcessV1 {
	fixedPeerWindowsStagesV1.Lock()
	stage := fixedPeerWindowsStagesV1.byAddress[config.ListenAddress]
	fixedPeerWindowsStagesV1.Unlock()
	if stage == nil || stage.activated {
		return nil
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > fixedPeerMaxConfigBytesV1 {
		t.Fatal("child configuration exceeds byte budget")
	}
	if err := os.WriteFile(stage.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	stage.sequence++
	if err := json.NewEncoder(stage.process.input).Encode(fixedPeerWindowsStageCommandV1{Sequence: stage.sequence, Activate: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixedPeerWindowsWaitReplyV1(stage, stage.sequence); err != nil {
		t.Fatal(err)
	}
	stage.activated = true
	t.Cleanup(func() { stage.process.stop(t) })
	return stage.process
}
func fixedPeerPassTestListenersV1(_ testing.TB, _ *exec.Cmd, _ io.WriteCloser, config FixedPeerTCPConfigV1) (func(), error) {
	// Restart has no fixture-held sockets: the new child binds the stored exact
	// addresses. A failed exec still releases any supplied fixture inventory.
	listeners := fixedPeerTakeTestListenersV1(config)
	var once sync.Once
	release := func() {
		once.Do(func() {
			for _, listener := range listeners {
				_ = listener.Close()
			}
		})
	}
	if len(listeners) > 0 {
		return release, errors.New("Windows subprocess fixture must allocate sockets in its staged child")
	}
	return release, nil
}
func fixedPeerFinishTestListenerTransferV1(_ testing.TB, _ *exec.Cmd, _ io.WriteCloser) error {
	return nil
}
func fixedPeerChildListenersV1() (listeners map[string]net.Listener, childErr error) {
	path := os.Getenv("GOMAP_FIXED_PEER_TEST_STAGE")
	if path == "" {
		config, err := fixedPeerReadTestConfigV1(os.Getenv("GOMAP_FIXED_PEER_TEST_CONFIG_FILE"))
		if err != nil {
			return nil, err
		}
		return bindFixedPeerTCPListenersV1(config, nil)
	}
	return fixedPeerReadStagedListenersV1(path, os.Stdin)
}

func fixedPeerReadStagedListenersV1(path string, input io.Reader) (listeners map[string]net.Listener, childErr error) {
	owned := make(map[string]net.Listener)
	listeners = owned
	defer func() {
		if childErr != nil {
			for _, listener := range owned {
				_ = listener.Close()
			}
		}
	}()
	decoder := json.NewDecoder(input)
	for {
		var command fixedPeerWindowsStageCommandV1
		if err := decoder.Decode(&command); err != nil {
			return nil, err
		}
		var reply fixedPeerWindowsStageReplyV1
		if command.Activate {
			config, err := fixedPeerReadTestConfigV1(path)
			if err == nil {
				wanted := make(map[string]bool)
				for _, address := range fixedPeerTCPListenAddressesV1(config) {
					wanted[address] = true
				}
				for address, listener := range listeners {
					if !wanted[address] {
						_ = listener.Close()
						delete(listeners, address)
					}
				}
				for address := range wanted {
					if listeners[address] == nil {
						err = fmt.Errorf("configured role was not selected in child: %s", address)
						break
					}
				}
			}
			if err != nil {
				reply.Error = err.Error()
			}
			if err := fixedPeerWindowsWriteReplyV1(path, command.Sequence, reply); err != nil {
				return nil, err
			}
			if reply.Error != "" {
				return nil, errors.New(reply.Error)
			}
			return listeners, nil
		}
		if len(listeners) >= 128 {
			return nil, errors.New("child socket budget exceeded")
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		reply.Address = listener.Addr().String()
		listeners[reply.Address] = listener
		if err := fixedPeerWindowsWriteReplyV1(path, command.Sequence, reply); err != nil {
			return nil, err
		}
	}
}
func fixedPeerWindowsWriteReplyV1(path string, sequence int, reply fixedPeerWindowsStageReplyV1) error {
	raw, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	target := fmt.Sprintf("%s.reply-%d", path, sequence)
	if err := os.WriteFile(target+".tmp", raw, 0600); err != nil {
		return err
	}
	return os.Rename(target+".tmp", target)
}

func fixedPeerSparseSubprocessAllocatorV1(t testing.TB) func(raftcluster.NodeID) string {
	return fixedPeerWindowsAllocatorV1(t, "TestSparseCatalogBenchmarkProcessV1")
}

func TestFixedPeerWindowsStagedListenerOwnershipV1(t *testing.T) {
	configs := fixedPeerTestConfigsV1(t, fixedPeerSubprocessAllocatorV1(t))
	for _, config := range configs {
		for _, address := range fixedPeerTCPListenAddressesV1(config) {
			listener, err := net.Listen("tcp", address)
			if err == nil {
				_ = listener.Close()
				t.Fatalf("selected role was not child-owned: node=%s address=%s", config.NodeID, address)
			}
		}
	}
	config := configs[1]
	fixedPeerWindowsStagesV1.Lock()
	owner := fixedPeerWindowsStagesV1.byAddress[config.ListenAddress].process.command.Process.Pid
	fixedPeerWindowsStagesV1.Unlock()
	process := fixedPeerStartTestProcessV1(t, config)
	if process.command.Process.Pid != owner {
		t.Fatal("activation replaced socket owner process")
	}
	process.stop(t)
	for _, address := range fixedPeerTCPListenAddressesV1(config) {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("activated child leaked %s: %v", address, err)
		}
		_ = listener.Close()
	}
	restarted := fixedPeerStartTestProcessV1(t, config)
	if restarted.command.Process.Pid == owner {
		t.Fatal("restart did not create a new process")
	}
	restarted.stop(t)
}
func TestFixedPeerWindowsUnactivatedListenerCleanupV1(t *testing.T) {
	var addresses []string
	t.Run("cancel-before-config", func(t *testing.T) {
		allocate := fixedPeerSubprocessAllocatorV1(t)
		addresses = []string{allocate("unused"), allocate("unused")}
	})
	for _, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("unactivated child leaked %s: %v", address, err)
		}
		_ = listener.Close()
	}
}
func TestFixedPeerWindowsStagedConfigFailureCleanupV1(t *testing.T) {
	configs := fixedPeerTestConfigsV1(t, fixedPeerSubprocessAllocatorV1(t))
	config := configs[1]
	config.DataRoot = "relative"
	process := fixedPeerStartTestProcessV1(t, config)
	process.stopped = true
	_ = process.input.Close()
	if err := process.command.Wait(); err == nil {
		t.Fatal("invalid finalized config succeeded")
	}
	_ = process.log.Close()
	for _, address := range fixedPeerTCPListenAddressesV1(config) {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatalf("config failure leaked %s: %v", address, err)
		}
		_ = listener.Close()
	}
}

func TestFixedPeerWindowsStagedProtocolErrorCleanupV1(t *testing.T) {
	for _, mode := range []string{"decoder", "invalid-config", "reply-failure"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			input := `{"Sequence":1}` + "\n"
			switch mode {
			case "decoder":
				input += "{"
			case "invalid-config":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
				input += `{"Sequence":2,"Activate":true}` + "\n"
			case "reply-failure":
				if err := os.Mkdir(path+".reply-2.tmp", 0700); err != nil {
					t.Fatal(err)
				}
				input += `{"Sequence":2}` + "\n"
			}
			listeners, err := fixedPeerReadStagedListenersV1(path, strings.NewReader(input))
			if err == nil || listeners != nil {
				t.Fatalf("controlled %s succeeded: listeners=%v err=%v", mode, listeners, err)
			}
			raw, err := os.ReadFile(path + ".reply-1")
			if err != nil {
				t.Fatal(err)
			}
			var reply fixedPeerWindowsStageReplyV1
			if err := json.Unmarshal(raw, &reply); err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", reply.Address)
			if err != nil {
				t.Fatalf("%s retained actual socket %s after return: %v", mode, reply.Address, err)
			}
			_ = listener.Close()
		})
	}
}
