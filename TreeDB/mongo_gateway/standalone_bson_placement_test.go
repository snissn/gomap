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
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneRelaxedBSONValuePlacementCrashReplay(t *testing.T) {
	const databaseDirEnv = "TREEDB_MONGO_BSON_PLACEMENT_REPLAY_DIR"
	const ready = "BSON_PLACEMENT_REPLAY_READY"
	const updatedPayload = "updated before process exit"
	dir, child := os.LookupEnv(databaseDirEnv)
	if !child {
		dir = t.TempDir()
	}
	opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed}
	makeRows := func(prefix string) []bsonPlacementRow {
		t.Helper()
		const wideIDBytes = page.PageSize/2 - 15 // _id-only BSON has 15 bytes of overhead.
		wideID := prefix + "-wide-key-"
		wideID += strings.Repeat("k", wideIDBytes-len(wideID))
		fixtures := []struct {
			kind        string
			doc         bson.D
			wantPointer bool
		}{
			{"small", bson.D{{Key: "_id", Value: prefix + "-small"}, {Key: "payload", Value: "small payload"}}, false},
			{"medium", bson.D{{Key: "_id", Value: prefix + "-medium"}, {Key: "payload", Value: strings.Repeat("m", 1000)}}, true},
			{"large", bson.D{{Key: "_id", Value: prefix + "-large"}, {Key: "payload", Value: strings.Repeat("L", 64<<10)}}, true},
			{"wide-key", bson.D{{Key: "_id", Value: wideID}}, true},
		}
		rows := make([]bsonPlacementRow, 0, len(fixtures))
		for _, fixture := range fixtures {
			raw, err := bson.Marshal(fixture.doc)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, bsonPlacementRow{kind: fixture.kind, raw: raw, wantPointer: fixture.wantPointer})
		}
		if got := len(rows[len(rows)-1].raw); got != page.PageSize/2 {
			t.Fatalf("wide-key BSON size %d, want %d", got, page.PageSize/2)
		}
		return rows
	}
	prefixRows, suffixRows := makeRows("prefix"), makeRows("suffix")
	rawRows := func(rows []bsonPlacementRow) []bson.Raw {
		raw := make([]bson.Raw, 0, len(rows))
		for _, row := range rows {
			raw = append(raw, row.raw)
		}
		return raw
	}

	if child {
		owner, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		client, cancel, listener, serveErr := startStandaloneMongoClientForTest(t, owner)
		defer stopStandaloneMongoClientForTest(t, client, cancel, listener, serveErr, owner)
		database := client.Database("placement")
		journaled := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.Journaled()))
		if result, err := journaled.InsertMany(t.Context(), rawRows(prefixRows)); err != nil || !result.Acknowledged || len(result.InsertedIDs) != len(prefixRows) {
			t.Fatalf("journaled insert result=%+v err=%v", result, err)
		}
		applied := owner.Backend.State().AppliedCommandLSN
		durable, err := strconv.ParseUint(owner.Backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if err != nil || applied == 0 || durable != applied {
			t.Fatalf("journaled prefix applied=%d durable=%d err=%v", applied, durable, err)
		}
		checkStandaloneBSONValuePlacement(t, owner, "placement.docs", collections.RootStoragePager, prefixRows)
		visible := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.W1()))
		if result, err := visible.InsertMany(t.Context(), rawRows(suffixRows)); err != nil || !result.Acknowledged || len(result.InsertedIDs) != len(suffixRows) {
			t.Fatalf("visible insert result=%+v err=%v", result, err)
		}
		// Force the original unapplied record to reach physical WAL without
		// publishing a new root. This is a replay fixture for a complete
		// relaxed suffix after a process crash.
		var synced atomic.Bool
		restore := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync && event.StablePath != nil {
				synced.Store(true)
			}
			return nil
		})
		err = owner.Backend.FlushCommandWAL(true)
		restore()
		if err != nil || !synced.Load() {
			t.Fatalf("physical suffix WAL sync observed=%t err=%v", synced.Load(), err)
		}
		if got := owner.Backend.State().AppliedCommandLSN; got != applied {
			t.Fatalf("fixture suffix unexpectedly applied at %d, want %d", got, applied)
		}
		if next := owner.Backend.CommandWALNextLSN(); next != applied+2 {
			t.Fatalf("fixture next WAL LSN=%d, want %d", next, applied+2)
		}
		// Leave this ordinary UPDATE above both the applied root and the last
		// explicit WAL sync. The child receives no further storage barrier.
		result, err := visible.UpdateOne(t.Context(), bson.D{{Key: "_id", Value: "prefix-small"}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "payload", Value: updatedPayload}}}})
		if err != nil || !result.Acknowledged || result.MatchedCount != 1 || result.ModifiedCount != 1 {
			t.Fatalf("relaxed update result=%+v err=%v", result, err)
		}
		if next := owner.Backend.CommandWALNextLSN(); next != applied+3 {
			t.Fatalf("next WAL LSN after UPDATE=%d, want %d", next, applied+3)
		}
		if got := owner.Backend.State().AppliedCommandLSN; got != applied {
			t.Fatalf("UPDATE already root-covered at %d, want %d", got, applied)
		}
		durable, err = strconv.ParseUint(owner.Backend.Stats()["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if err != nil || durable >= applied+2 {
			t.Fatalf("expected relaxed UPDATE at %d above durable WAL prefix %d: %v", applied+2, durable, err)
		}
		fmt.Printf("%s %d\n", ready, applied+2)
		<-t.Context().Done()
		t.Fatal("test process exited before the crash signal")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneRelaxedBSONValuePlacementCrashReplay$", "-test.v")
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

	updated, err := bson.Marshal(bson.D{{Key: "_id", Value: "prefix-small"}, {Key: "payload", Value: updatedPayload}})
	if err != nil {
		t.Fatal(err)
	}
	prefixRows[0].raw = updated
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
		all := append(prefixRows, suffixRows...)
		checkStandaloneBSONValuePlacement(t, owner, "placement.docs", collections.RootStoragePager, all)
		reply := serveCommand(t, owner.Server, 1, bson.D{{Key: "find", Value: "docs"}, {Key: "$db", Value: "placement"}})
		assertOK(t, reply)
		got := cursorFirstBatch(t, reply)
		if len(got) != len(all) {
			t.Fatalf("public find returned %d rows, want %d", len(got), len(all))
		}
		byID := make(map[string]bson.Raw, len(got))
		for _, doc := range got {
			byID[doc.Lookup("_id").StringValue()] = doc
		}
		for _, row := range all {
			actual := byID[row.raw.Lookup("_id").StringValue()]
			if !bytes.Equal(actual, row.raw) {
				t.Fatalf("public find %s BSON size=%d, want %d and identical contents", row.kind, len(actual), len(row.raw))
			}
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStandaloneRelaxedBSONValuePlacementPreservesExplicitCollectionPolicy(t *testing.T) {
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
		Name: "placement.explicit",
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
	database := client.Database("placement")
	for _, c := range []struct {
		name   string
		policy collections.RootStoragePolicy
	}{{"implicit", collections.RootStoragePager}, {"explicit", collections.RootStorageCompressed}} {
		journaled := database.Collection(c.name, options.Collection().SetWriteConcern(writeconcern.Journaled()))
		if result, err := journaled.InsertOne(t.Context(), bson.Raw(value)); err != nil || !result.Acknowledged {
			t.Fatalf("%s journaled insert result=%+v err=%v", c.name, result, err)
		}
		checkStandaloneBSONValuePlacement(t, owner, "placement."+c.name, c.policy, []bsonPlacementRow{
			{kind: "medium", raw: value, wantPointer: true},
		})
	}
}

type bsonPlacementRow struct {
	kind        string
	raw         bson.Raw
	wantPointer bool
}

func checkStandaloneBSONValuePlacement(t *testing.T, owner *StandaloneServer, name string, policy collections.RootStoragePolicy, rows []bsonPlacementRow) {
	t.Helper()
	collection, err := owner.Collections.OpenCollection(name)
	if err != nil {
		t.Fatal(err)
	}
	if got := collection.Meta().Options.DataRootStoragePolicy; got != policy {
		t.Fatalf("%s data root policy=%q, want %q", name, got, policy)
	}
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
		t.Fatal("missing physical snapshot token")
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
	if policy == collections.RootStoragePager {
		var externalLeaves int
		err := leafrefscan.Walk(t.Context(), root, snapshot.Pager().Get, nil, func(page.LeafLogPtr) error {
			externalLeaves++
			return nil
		})
		if err != nil || externalLeaves != 0 {
			t.Fatalf("%s Pager primary root has %d external leaves, want 0, err=%v", name, externalLeaves, err)
		}
	}
	for _, row := range rows {
		key, err := EncodePrimaryKey(row.raw.Lookup("_id"))
		if err != nil {
			t.Fatal(err)
		}
		var keyScratch, leafScratch [page.PageSize]byte
		entry, err := reader.GetEntryExactWithFixedScratch(key, keyScratch[:], leafScratch[:], capture.(backenddb.NativePhysicalLeafReader).NativeLeafPage)
		if err != nil || (entry.Flags&node.FlagPointer != 0) != row.wantPointer {
			t.Fatalf("%s %s BSON row pointer=%t, want %t, err=%v", name, row.kind, entry.Flags&node.FlagPointer != 0, row.wantPointer, err)
		}
		if row.wantPointer {
			if _, _, err := snapshot.PinnedValueLogFile(entry.ValuePtr.FileID); err != nil {
				t.Fatalf("%s %s BSON row has no pinned value-log file, err=%v", name, row.kind, err)
			}
		}
		actual, err := snapshot.GetAtRoot(root, key)
		if err != nil || !bytes.Equal(actual, row.raw) {
			t.Fatalf("%s %s BSON row readback len=%d, want %d, err=%v", name, row.kind, len(actual), len(row.raw), err)
		}
	}
}
