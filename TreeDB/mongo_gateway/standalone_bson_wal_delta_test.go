package mongogateway

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneBSONSetWALDeltaCrashReplay(t *testing.T) {
	const dirEnv = "TREEDB_MONGO_BSON_WAL_DELTA_REPLAY_DIR"
	const optEnv = "TREEDB_MONGO_BSON_WAL_DELTA_REPLAY_OPT_IN"
	const marker = "BSON_SET_WAL_DELTA_SYNCED"
	newDocument := func(value byte) bson.D {
		return bson.D{
			{Key: "_id", Value: int32(41)},
			{Key: "same", Value: bson.Binary{Data: bytes.Repeat([]byte{42}, 1024)}},
			{Key: "target", Value: bson.Binary{Data: bytes.Repeat([]byte{value}, 100)}},
			{Key: "typed", Value: int64(41)},
		}
	}
	if dir := os.Getenv(dirEnv); dir != "" {
		opts := StandaloneOptions{
			Dir: dir, Profile: treedb.ProfileCommandWALRelaxed,
			ExperimentalBSONSetWALDelta: os.Getenv(optEnv) == "1",
		}
		owner, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		client, cancel, listener, serveErr := startStandaloneMongoClientForTest(t, owner)
		defer stopStandaloneMongoClientForTest(t, client, cancel, listener, serveErr, owner)
		database := client.Database("delta")
		journaled := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.Journaled()))
		if result, err := journaled.InsertOne(t.Context(), newDocument(1)); err != nil || !result.Acknowledged {
			t.Fatalf("journaled insert result=%+v err=%v", result, err)
		}
		prefix := owner.Backend.State().AppliedCommandLSN
		visible := database.Collection("docs", options.Collection().SetWriteConcern(writeconcern.W1()))
		for _, n := range []byte{2, 3, 3, 4} {
			set := bson.D{{Key: "$set", Value: bson.D{
				{Key: "target", Value: bson.Binary{Data: bytes.Repeat([]byte{n}, 100)}},
			}}}
			result, err := visible.UpdateOne(t.Context(), bson.D{{Key: "_id", Value: int32(41)}}, set)
			if err != nil || !result.Acknowledged || result.MatchedCount != 1 {
				t.Fatalf("visible update %d result=%+v err=%v", n, result, err)
			}
		}
		var synced atomic.Bool
		remove := durabilitycut.Install(func(event durabilitycut.Event) error {
			if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyFileSync && event.StablePath != nil {
				synced.Store(true)
			}
			return nil
		})
		err = owner.Backend.FlushCommandWAL(true)
		remove()
		if err != nil || !synced.Load() || prefix == 0 ||
			owner.Backend.State().AppliedCommandLSN != prefix || owner.Backend.CommandWALNextLSN() != prefix+4 {
			t.Fatalf("physical suffix sync=%t err=%v applied=%d prefix=%d next=%d",
				synced.Load(), err, owner.Backend.State().AppliedCommandLSN, prefix, owner.Backend.CommandWALNextLSN())
		}
		fmt.Printf("%s %d\n", marker, prefix)
		os.Exit(96)
	}

	for _, opt := range []string{"0", "1"} {
		t.Run("opt_in_"+opt, func(t *testing.T) {
			dir := t.TempDir()
			parent, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			stream := commitlog.IsExt4WALFilesystem(parent)
			if err := parent.Close(); err != nil {
				t.Fatal(err)
			}
			if !stream {
				t.Skip("BSON $set WAL delta requires ext4 stream")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneBSONSetWALDeltaCrashReplay$", "-test.v")
			cmd.Env = append(os.Environ(), dirEnv+"="+dir, optEnv+"="+opt)
			output, err := cmd.CombinedOutput()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 96 || bytes.Contains(output, []byte("DATA RACE")) {
				t.Fatalf("physical crash control err=%v\n%s", err, output)
			}
			var prefix uint64
			for _, line := range strings.Split(string(output), "\n") {
				if n, _ := fmt.Sscanf(line, marker+" %d", &prefix); n == 1 {
					break
				}
			}
			if prefix == 0 {
				t.Fatalf("missing durable prefix receipt\n%s", output)
			}
			dbdir := filepath.Join(dir, "maindb")
			wal := backenddb.WALDirPath(dbdir)
			names, err := os.ReadDir(wal)
			if err != nil {
				t.Fatal(err)
			}
			var suffix []commitlog.CommandEnvelope
			for _, file := range names {
				if !commitlog.IsCommandSegmentName(file.Name()) {
					continue
				}
				frames, err := commitlog.ScanCommandFramesV2(filepath.Join(wal, file.Name()), commitlog.Options{})
				if err != nil {
					t.Fatal(err)
				}
				for _, frame := range frames {
					if frame.LSN > prefix {
						suffix = append(suffix, frame)
					}
				}
			}
			if len(suffix) != 3 {
				t.Fatalf("got %d replay frames, want 3", len(suffix))
			}
			for i, frame := range suffix {
				if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID || frame.LSN != prefix+uint64(i)+1 {
					t.Fatalf("unexpected positioned UPDATE frame %+v", frame)
				}
				if delta := commitlog.IsCollectionBSONSetDelta(frame.Payload); delta != (opt == "1") {
					t.Fatalf("frame %d delta=%t for opt=%s", frame.LSN, delta, opt)
				}
				if opt == "1" && len(frame.Payload) >= 300 || opt == "0" && len(frame.Payload) <= 1100 {
					t.Fatalf("frame %d payload size %d for opt=%s", frame.LSN, len(frame.Payload), opt)
				}
				t.Logf("opt_in=%s complete positioned frame LSN=%d payload_bytes=%d", opt, frame.LSN, len(frame.Payload))
			}
			want, err := bson.Marshal(newDocument(4))
			if err != nil {
				t.Fatal(err)
			}
			// No writer opt-in is needed to recover already-written delta frames.
			opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed}
			for range 2 {
				owner, err := OpenStandaloneServer(opts)
				if err != nil {
					t.Fatal(err)
				}
				if got := owner.Backend.State().AppliedCommandLSN; got != prefix+3 {
					t.Fatalf("replay applied=%d, want %d", got, prefix+3)
				}
				reply := serveCommand(t, owner.Server, 1, bson.D{{Key: "find", Value: "docs"}, {Key: "$db", Value: "delta"}})
				assertOK(t, reply)
				rows := cursorFirstBatch(t, reply)
				if len(rows) != 1 || !bytes.Equal(rows[0], want) {
					t.Fatalf("typed full BSON replay returned %v, want %v", rows, bson.Raw(want))
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
