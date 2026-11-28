package mongogateway

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func requireStandaloneExt4(t *testing.T, dir string) {
	t.Helper()
	f, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ext4 := commitlog.IsExt4WALFilesystem(f)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if !ext4 {
		t.Skip("requires a positively identified ext4 filesystem")
	}
}

func checkStandaloneWALStream(t *testing.T, owner *StandaloneServer, want bool) {
	t.Helper()
	format, ok, err := backenddb.LoadFormatConfig(owner.Options.Dir)
	if err != nil || !ok || format.RelaxedStandaloneWALStream != want {
		t.Fatalf("persisted stream policy=%t found=%t err=%v, want %t", format.RelaxedStandaloneWALStream, ok, err, want)
	}
	stats := owner.Backend.Stats()
	container := "fixed"
	if want {
		container = "stream"
	}
	if got := stats["treedb.command_wal.writer.container"]; got != container {
		t.Fatalf("container=%q reason=%q, want %q", got, stats["treedb.command_wal.writer.container_reason"], container)
	}
}

func TestStandaloneRelaxedExt4StreamRoleAndLegacy(t *testing.T) {
	base := t.TempDir()
	requireStandaloneExt4(t, base)
	for _, name := range []string{"relaxed_bson", "durable_bson", "relaxed_json", "clustered_bson", "legacy_bson"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(base, name)
			opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed}
			switch name {
			case "durable_bson":
				opts.Profile = treedb.ProfileCommandWALDurable
			case "relaxed_json":
				opts.DefaultCollectionOptions.DocumentFormat = collections.DocumentFormatJSON
			case "clustered_bson":
				opts.ClusterSubmitter = &mongoClusterFakeSubmitter{}
				opts.ClusterCatalogVersion = mongoClusterStaticCatalogVersion(1)
			case "legacy_bson":
				seed := treedb.OptionsFor(opts.Profile, dir)
				seed.MemtableMode = "cow_btree"
				seed.FreshSystemPagerLeaves = true
				db, err := treedb.Open(seed)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				if err := db.SetSync([]byte("legacy"), []byte("record")); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}

			wantStream := name == "relaxed_bson"
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
				checkStandaloneWALStream(t, owner, wantStream)
				if name == "legacy_bson" {
					value, err := owner.Backend.Get([]byte("legacy"))
					if err != nil || string(value) != "record" {
						t.Fatalf("legacy value=%q err=%v", value, err)
					}
				}
				if err := owner.Backend.RotateCommandWALActiveSegment(true); err != nil {
					t.Fatal(err)
				}
				checkStandaloneWALStream(t, owner, wantStream)
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDirectDurableExt4KeepsFixedCommandWAL(t *testing.T) {
	dir := t.TempDir()
	requireStandaloneExt4(t, dir)
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.SetSync([]byte("key"), []byte("durable")); err != nil {
		t.Fatal(err)
	}
	stats := db.Stats()
	lsn, err := strconv.ParseUint(stats["treedb.command_wal.durable_wal_lsn"], 10, 64)
	if stats["treedb.command_wal.writer.container"] != "fixed" || err != nil || lsn == 0 {
		t.Fatalf("direct durable container=%q LSN=%d err=%v", stats["treedb.command_wal.writer.container"], lsn, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStandaloneRelaxedExt4StreamJournalAckCrashReopen(t *testing.T) {
	const dirEnv = "TREEDB_MONGO_EXT4_STREAM_REPLAY_DIR"
	dir := os.Getenv(dirEnv)
	child := dir != ""
	if !child {
		dir = t.TempDir()
		requireStandaloneExt4(t, dir)
	}
	opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed}
	payload := bytes.Repeat([]byte{0x71, 0x33, 0xa8, 0x25}, 16<<10)
	if child {
		owner, err := OpenStandaloneServer(opts)
		if err != nil {
			t.Fatal(err)
		}
		checkStandaloneWALStream(t, owner, true)
		reply := serveCommand(t, owner.Server, 1, bson.D{
			{Key: "insert", Value: "docs"},
			{Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "synced"}, {Key: "payload", Value: bson.Binary{Data: payload}}}}},
			{Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}},
			{Key: "$db", Value: "stream"},
		})
		assertOK(t, reply)
		assertInt32(t, reply, "n", 1)
		stats := owner.Backend.Stats()
		durable, err := strconv.ParseUint(stats["treedb.command_wal.durable_wal_lsn"], 10, 64)
		if applied := owner.Backend.State().AppliedCommandLSN; err != nil || durable != 2 || applied != durable {
			t.Fatalf("expected explicit ACK to close the recoverable prefix: durable=%d applied=%d err=%v", durable, applied, err)
		}
		// Exit without a graceful close after the explicit durable ACK.
		os.Exit(0)
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestStandaloneRelaxedExt4StreamJournalAckCrashReopen$")
	cmd.Env = append(os.Environ(), dirEnv+"="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("journal child: %v\n%s", err, output)
	}
	owner, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	checkStandaloneWALStream(t, owner, true)
	if applied := owner.Backend.State().AppliedCommandLSN; applied != 2 {
		t.Fatalf("applied LSN=%d, want 2", applied)
	}
	reply := serveCommand(t, owner.Server, 2, bson.D{
		{Key: "find", Value: "docs"},
		{Key: "filter", Value: bson.D{{Key: "_id", Value: "synced"}}},
		{Key: "$db", Value: "stream"},
	})
	assertOK(t, reply)
	docs := cursorFirstBatch(t, reply)
	if len(docs) != 1 {
		t.Fatalf("reopened %d documents, want 1", len(docs))
	}
	_, got, ok := docs[0].Lookup("payload").BinaryOK()
	if !ok || !bytes.Equal(got, payload) {
		t.Fatalf("reopened payload has %d bytes, valid=%t", len(got), ok)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}
