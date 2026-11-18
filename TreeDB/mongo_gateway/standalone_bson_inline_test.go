package mongogateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneRelaxedBSONInlinePlacementCrashReplay(t *testing.T) {
	const databaseDirEnv = "TREEDB_MONGO_BSON_INLINE_REPLAY_DIR"
	const ready = "BSON_INLINE_REPLAY_READY"
	dir, child := os.LookupEnv(databaseDirEnv)
	if !child {
		dir = t.TempDir()
	}
	opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed}
	marshal := func(doc bson.D) bson.Raw {
		t.Helper()
		row, err := bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	medium := marshal(bson.D{{Key: "_id", Value: "medium"}, {Key: "payload", Value: strings.Repeat("m", 1000)}})
	large := marshal(bson.D{{Key: "_id", Value: "large"}, {Key: "payload", Value: strings.Repeat("L", 64<<10)}})
	// A value at the normal threshold can still exceed the available leaf
	// space when its BSON ID also occupies most of a half page.
	longKey := marshal(bson.D{{Key: "_id", Value: strings.Repeat("k", page.PageSize/2-15)}})
	if len(longKey) != page.PageSize/2 {
		t.Fatalf("bad long-key fixture size %d", len(longKey))
	}

	if child {
		owner, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		client, cancel, listener, serveErr := startStandaloneMongoClientForTest(t, owner)
		defer stopStandaloneMongoClientForTest(t, client, cancel, listener, serveErr, owner)
		database := client.Database("inline")
		journaled := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.Journaled()))
		if result, err := journaled.InsertMany(t.Context(), []bson.Raw{medium, large}); err != nil || !result.Acknowledged || len(result.InsertedIDs) != 2 {
			t.Fatalf("journaled insert result=%+v err=%v", result, err)
		}
		applied := owner.Backend.State().AppliedCommandLSN
		durable, err := strconv.ParseUint(owner.Backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if err != nil || applied == 0 || durable != applied {
			t.Fatalf("journaled prefix applied=%d durable=%d err=%v", applied, durable, err)
		}
		visible := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.W1()))
		if result, err := visible.InsertOne(t.Context(), longKey); err != nil || !result.Acknowledged {
			t.Fatalf("visible insert result=%+v err=%v", result, err)
		}
		// Force the original unapplied record to reach physical WAL without
		// publishing a new root. This is a replay fixture for a complete
		// relaxed suffix after a process crash.
		if err := owner.Backend.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		if got := owner.Backend.State().AppliedCommandLSN; got != applied {
			t.Fatalf("fixture suffix unexpectedly applied at %d, want %d", got, applied)
		}
		if next := owner.Backend.CommandWALNextLSN(); next != applied+2 {
			t.Fatalf("fixture next WAL LSN=%d, want %d", next, applied+2)
		}
		fmt.Printf("%s %d\n", ready, applied+1)
		<-t.Context().Done()
		t.Fatal("test process exited before the crash signal")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneRelaxedBSONInlinePlacementCrashReplay$", "-test.v")
	cmd.Env = append(os.Environ(), databaseDirEnv+"="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var output []string
	var wantLSN uint64
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if number, ok := strings.CutPrefix(line, ready+" "); ok {
			wantLSN, _ = strconv.ParseUint(number, 10, 64)
			break
		}
		output = append(output, line)
	}
	if wantLSN == 0 {
		cancel()
		t.Fatalf("child failed before ACK: %v\n%s\n%s", cmd.Wait(), strings.Join(output, "\n"), stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child exited gracefully")
	}

	for range 2 {
		owner, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
		})
		if got := owner.Backend.State().AppliedCommandLSN; got != wantLSN {
			t.Fatalf("applied LSN %d, want %d", got, wantLSN)
		}
		checkStandaloneBSONInlineRow(t, owner, "inline.docs", medium, false)
		checkStandaloneBSONInlineRow(t, owner, "inline.docs", large, true)
		checkStandaloneBSONInlineRow(t, owner, "inline.docs", longKey, true)
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStandaloneRelaxedBSONInlinePlacementPreservesExplicitCollectionPolicy(t *testing.T) {
	owner, err := OpenStandaloneServer(StandaloneOptions{
		Dir:     t.TempDir(),
		Profile: treedb.ProfileCommandWALRelaxed,
	})
	if err != nil {
		t.Fatal(err)
	}
	client, cancel, listener, serveErr := startStandaloneMongoClientForTest(t, owner)
	defer stopStandaloneMongoClientForTest(t, client, cancel, listener, serveErr, owner)
	if got := owner.Backend.InlineThreshold(); got != page.DefaultInlineThreshold {
		t.Fatalf("shared adaptive threshold %d, want %d", got, page.DefaultInlineThreshold)
	}
	if _, err := owner.Collections.CreateCollection(&collections.CollectionMeta{
		Name: "inline.explicit",
		Options: collections.CollectionOptions{
			DocumentFormat:        collections.DocumentFormatBSON,
			DataRootStoragePolicy: collections.RootStorageCompressed,
		},
	}); err != nil {
		t.Fatal(err)
	}
	value, err := bson.Marshal(bson.D{{Key: "_id", Value: "medium"}, {Key: "payload", Value: strings.Repeat("m", 1000)}})
	if err != nil {
		t.Fatal(err)
	}
	database := client.Database("inline")
	for _, c := range []struct {
		name        string
		wantPointer bool
	}{{"implicit", false}, {"explicit", true}} {
		journaled := database.Collection(c.name, options.Collection().SetWriteConcern(writeconcern.Journaled()))
		if result, err := journaled.InsertOne(t.Context(), bson.Raw(value)); err != nil || !result.Acknowledged {
			t.Fatalf("%s journaled insert result=%+v err=%v", c.name, result, err)
		}
		checkStandaloneBSONInlineRow(t, owner, "inline."+c.name, value, c.wantPointer)
	}
}

func checkStandaloneBSONInlineRow(t *testing.T, owner *StandaloneServer, name string, expected bson.Raw, wantPointer bool) {
	t.Helper()
	capture, err := owner.Backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := capture.Close(); err != nil {
			t.Error(err)
		}
	}()
	snapshot := capture.Snapshot()
	token, ok := snapshot.StateToken()
	if !ok {
		t.Fatal("missing replayed snapshot token")
	}
	descriptor, err := snapshot.GetAtRoot(token.SystemRootPageID, []byte("collections/root/"+name+"/primary"))
	if err != nil || len(descriptor) != 8 {
		t.Fatalf("primary root descriptor %x, err=%v", descriptor, err)
	}
	root := binary.BigEndian.Uint64(descriptor)
	reader, err := snapshot.NativeReaderAtRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	key, err := EncodePrimaryKey(expected.Lookup("_id"))
	if err != nil {
		t.Fatal(err)
	}
	var keyScratch, leafScratch [page.PageSize]byte
	entry, err := reader.GetEntryExactWithFixedScratch(key, keyScratch[:], leafScratch[:], capture.(backenddb.NativePhysicalLeafReader).NativeLeafPage)
	if err != nil || (entry.Flags&node.FlagPointer != 0) != wantPointer {
		t.Fatalf("%s BSON row pointer=%t, want %t, err=%v", name, entry.Flags&node.FlagPointer != 0, wantPointer, err)
	}
	if wantPointer {
		if _, _, err := snapshot.PinnedValueLogFile(entry.ValuePtr.FileID); err != nil {
			t.Fatalf("%s BSON row has no pinned value-log file: %v", name, err)
		}
	}
	actual, err := snapshot.GetAtRoot(root, key)
	if err != nil || !bytes.Equal(actual, expected) {
		t.Fatalf("%s BSON row readback len=%d want %d, err=%v", name, len(actual), len(expected), err)
	}
}
