package nativewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

const cowLoadCollection = "cow_load"

// Keep the real public COW owner: a bare backend or the legacy cached-leaf
// fixture does not exercise the native semantic materialization in #5333.
type cowLoadWireOwner struct {
	owner   *treedb.DB
	manager *collections.CollectionManager
	server  *Server
	clients []*Client
	done    []<-chan error
	closed  bool
}

func openCOWLoadWireOwner(t *testing.T, profile treedb.Profile, dir string, connections int) *cowLoadWireOwner {
	return openCOWLoadWireOwnerForReplay(t, profile, dir, connections, false)
}

func openCOWLoadWireOwnerForReplay(t *testing.T, profile treedb.Profile, dir string, connections int, suppressAutoCheckpoint bool) *cowLoadWireOwner {
	t.Helper()
	opts := treedb.OptionsFor(profile, dir)
	opts.MemtableMode = "cow_btree"
	if suppressAutoCheckpoint {
		opts.FlushThreshold = 1 << 30
		opts.BackgroundCheckpointInterval = -1
	}
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	env := &cowLoadWireOwner{owner: owner}
	t.Cleanup(func() { env.close(t) })
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	env.manager = collections.NewCollectionManager(backend)
	if !env.manager.UsesNativeCommandWALApply() {
		t.Fatal("public factory did not select shared native COW owner")
	}
	env.server = NewServer(ServerOptions{Collections: env.manager, Backend: backend})
	for i := 0; i < connections; i++ {
		left, right := net.Pipe()
		done := make(chan error, 1)
		go func() { done <- env.server.ServeConn(context.Background(), right) }()
		client := NewClient(left)
		env.clients = append(env.clients, client)
		env.done = append(env.done, done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := client.Hello(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	return env
}

func (env *cowLoadWireOwner) close(t *testing.T) {
	t.Helper()
	if env.closed {
		return
	}
	env.closed = true
	for i, client := range env.clients {
		if err := client.Close(); err != nil {
			t.Errorf("client %d close: %v", i, err)
		}
		select {
		case err := <-env.done[i]:
			if err != nil {
				t.Errorf("connection %d shutdown: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("connection %d did not settle", i)
		}
	}
	if env.server != nil {
		if err := env.server.Close(); err != nil {
			t.Errorf("server close: %v", err)
		}
	}
	if env.manager != nil {
		if err := env.manager.Close(); err != nil {
			t.Errorf("manager close: %v", err)
		}
	}
	if err := env.owner.Close(); err != nil {
		t.Errorf("public owner close: %v", err)
	}
}

func cowLoadJSONDocument(t *testing.T, id []byte) []byte {
	t.Helper()
	// The pinned YCSB client JSON-encodes byte fields into base64 strings.
	fields := map[string]any{"_id": string(id)}
	for field := 0; field < 10; field++ {
		value := bytes.Repeat([]byte{byte('a' + field)}, 100)
		copy(value, id)
		fields[fmt.Sprintf("field%d", field)] = value
	}
	doc, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func assertCOWLoadReadback(ctx context.Context, client *Client, ids, expected [][]byte) error {
	docs, present, err := client.GetMany(ctx, cowLoadCollection, ids)
	if err != nil {
		return err
	}
	for i, id := range ids {
		if !present[i] || !bytes.Equal(docs[i], expected[i]) {
			return fmt.Errorf("id %q present=%v document=%q want=%q", id, present[i], docs[i], expected[i])
		}
	}
	return nil
}

// Concurrent single-document JSON requests, acknowledged value visibility,
// complete enumeration, explicit native checkpoint, checked close and a fresh
// reopen must all use the same public owner in each production profile.
func TestCOWNativewireJSONLoadCheckpointReopen(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			const workers, records = 16, 4096
			dir := t.TempDir()
			env := openCOWLoadWireOwner(t, profile, dir, workers)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if _, err := env.clients[0].CreateCollection(ctx, collections.CollectionMeta{Name: cowLoadCollection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
				t.Fatal(err)
			}
			ids, docs := make([][]byte, records), make([][]byte, records)
			for i := range ids {
				// A deterministic permutation keeps writes spread across leaves.
				ids[i] = []byte(fmt.Sprintf("user%08d", (i*2053)%records))
				docs[i] = cowLoadJSONDocument(t, ids[i])
			}
			errors := make(chan error, workers)
			var wg sync.WaitGroup
			for worker, client := range env.clients {
				wg.Add(1)
				go func(worker int, client *Client) {
					defer wg.Done()
					for i := worker; i < records; i += workers {
						if err := client.InsertBatchNoResult(ctx, cowLoadCollection, collections.DocumentFormatJSON, ids[i:i+1], docs[i:i+1], AckVisible); err != nil {
							errors <- fmt.Errorf("worker=%d insert id=%q: %w", worker, ids[i], err)
							return
						}
						if err := assertCOWLoadReadback(ctx, client, ids[i:i+1], docs[i:i+1]); err != nil {
							errors <- fmt.Errorf("worker=%d acknowledged readback: %w", worker, err)
							return
						}
					}
				}(worker, client)
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				t.Error(err)
			}
			if err := env.clients[0].Checkpoint(ctx); err != nil {
				t.Errorf("public native checkpoint: %v", err)
			}
			if t.Failed() {
				return
			}
			verify := func(client *Client) {
				t.Helper()
				for first := 0; first < records; first += 128 {
					if err := assertCOWLoadReadback(ctx, client, ids[first:first+128], docs[first:first+128]); err != nil {
						t.Fatal(err)
					}
				}
				seen := make(map[string]bool, records)
				result, err := client.OpenScan(ctx, cowLoadCollection, CursorLimits{MaxItems: 127})
				for {
					if err != nil {
						t.Fatal(err)
					}
					for _, id := range result.IDs {
						if seen[string(id)] {
							t.Fatalf("duplicate scanned id %q", id)
						}
						seen[string(id)] = true
					}
					if !result.Cursor.HasMore {
						break
					}
					result, err = client.CursorNext(ctx, result.Cursor.CursorID, CursorLimits{MaxItems: 127})
				}
				if len(seen) != records {
					t.Fatalf("scanned count=%d want=%d", len(seen), records)
				}
				for _, id := range ids {
					if !seen[string(id)] {
						t.Fatalf("scan missing id %q", id)
					}
				}
			}
			verify(env.clients[0])
			env.close(t)
			if t.Failed() {
				return
			}
			reopened := openCOWLoadWireOwner(t, profile, dir, 1)
			verify(reopened.clients[0])
		})
	}
}

// A real maintenance admission failure must cross the public wire boundary;
// an already acknowledged insert remains visible when pressure is released.
func TestCOWNativewireCheckpointErrorPropagation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	env := openCOWLoadWireOwner(t, treedb.ProfileCommandWALDurable, t.TempDir(), 1)
	client := env.clients[0]
	if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: cowLoadCollection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
		t.Fatal(err)
	}
	id := []byte("wire-acked")
	doc := cowLoadJSONDocument(t, id)
	if err := client.InsertBatchNoResult(ctx, cowLoadCollection, collections.DocumentFormatJSON, [][]byte{id}, [][]byte{doc}, AckVisible); err != nil {
		t.Fatal(err)
	}
	if err := assertCOWLoadReadback(ctx, client, [][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
	capture, err := env.owner.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	maintenance := capture.NativeMaintenanceAllocator().(*memtable.COWBudget)
	if err := capture.Close(); err != nil {
		t.Fatal(err)
	}
	stats, limits := maintenance.Stats(), maintenance.Limits()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	available := min(limits.MaxInFlightBytes-stats.ReservedBytes, limits.MaxTotalBytes-stats.TotalBytes,
		limits.MaxRetiredBytes-stats.HistoryBytes-stats.ReservedBytes-stats.DeferredBytes)
	if available <= overhead {
		t.Fatal("maintenance fixture has no admission headroom")
	}
	pressure, err := maintenance.AcquireExternal(available - overhead)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	var wireErr *WireError
	if err := client.Checkpoint(ctx); !errors.As(err, &wireErr) || wireErr.Retryable {
		t.Fatalf("checkpoint failure=%v, want a non-retryable wire error", err)
	}
	pressure.Close()
	if err := client.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint after releasing pressure: %v", err)
	}
	if err := assertCOWLoadReadback(ctx, client, [][]byte{id}, [][]byte{doc}); err != nil {
		t.Fatal(err)
	}
}

// The child exits after synced public acknowledgments without owner close. The
// parent opens the same durable profile and checks the acknowledged documents.
func TestCOWNativewireJSONDurableACKRecovery(t *testing.T) {
	const childDirEnv = "GOMAP_5333_COW_WIRE_CRASH_DIR"
	if dir := os.Getenv(childDirEnv); dir != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		env := openCOWLoadWireOwner(t, treedb.ProfileCommandWALDurable, dir, 1)
		client := env.clients[0]
		if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: cowLoadCollection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 8; i++ {
			id := []byte(fmt.Sprintf("durable-%02d", i))
			doc := cowLoadJSONDocument(t, id)
			if err := client.InsertBatchNoResult(ctx, cowLoadCollection, collections.DocumentFormatJSON, [][]byte{id}, [][]byte{doc}, AckSynced); err != nil {
				t.Fatal(err)
			}
			if err := assertCOWLoadReadback(ctx, client, [][]byte{id}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Println("all synced ACKs observed; exiting without close")
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCOWNativewireJSONDurableACKRecovery$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("all synced ACKs observed; exiting without close")) {
		t.Fatalf("unclean child exit=%v output=%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	env := openCOWLoadWireOwner(t, treedb.ProfileCommandWALDurable, dir, 1)
	for i := 0; i < 8; i++ {
		id := []byte(fmt.Sprintf("durable-%02d", i))
		if err := assertCOWLoadReadback(ctx, env.clients[0], [][]byte{id}, [][]byte{cowLoadJSONDocument(t, id)}); err != nil {
			t.Fatal(err)
		}
	}
}

// The durable profile syncs ordinary visible ACKs to its command WAL. Disable
// automatic checkpoints to prove that reopen actually replays unapplied frames.
func TestCOWNativewireJSONDurableVisibleACKWALReplay(t *testing.T) {
	const childDirEnv = "GOMAP_5333_COW_WIRE_REPLAY_DIR"
	if dir := os.Getenv(childDirEnv); dir != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		env := openCOWLoadWireOwnerForReplay(t, treedb.ProfileCommandWALDurable, dir, 1, true)
		client := env.clients[0]
		if _, err := client.CreateCollection(ctx, collections.CollectionMeta{Name: cowLoadCollection, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 8; i++ {
			id := []byte(fmt.Sprintf("wal-visible-%02d", i))
			doc := cowLoadJSONDocument(t, id)
			if err := client.InsertBatchNoResult(ctx, cowLoadCollection, collections.DocumentFormatJSON, [][]byte{id}, [][]byte{doc}, AckVisible); err != nil {
				t.Fatal(err)
			}
			if err := assertCOWLoadReadback(ctx, client, [][]byte{id}, [][]byte{doc}); err != nil {
				t.Fatal(err)
			}
		}
		fmt.Println("all durable visible ACKs observed; exiting without checkpoint or close")
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCOWNativewireJSONDurableVisibleACKWALReplay$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir)
	out, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("all durable visible ACKs observed; exiting without checkpoint or close")) {
		t.Fatalf("unclean child exit=%v output=%s", err, out)
	}
	before := workstats.Read().Replay.FramesApplied
	env := openCOWLoadWireOwnerForReplay(t, treedb.ProfileCommandWALDurable, dir, 1, true)
	after := workstats.Read().Replay.FramesApplied
	if after-before < 9 {
		t.Fatalf("expected unapplied collection and insert WAL frames: before=%d after=%d", before, after)
	}
	t.Logf("actual unapplied canonical WAL frames replayed: %d", after-before)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 8; i++ {
		id := []byte(fmt.Sprintf("wal-visible-%02d", i))
		if err := assertCOWLoadReadback(ctx, env.clients[0], [][]byte{id}, [][]byte{cowLoadJSONDocument(t, id)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := env.clients[0].Checkpoint(ctx); err != nil {
		t.Fatalf("post-replay checkpoint: %v", err)
	}
	env.close(t)
}
