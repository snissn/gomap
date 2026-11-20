package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
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
			var suffixSegment string
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
						if suffixSegment != "" && suffixSegment != file.Name() {
							t.Fatal("fixture suffix split across multiple segments")
						}
						suffixSegment = file.Name()
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
			if opt == "1" {
				prior, err := bson.Marshal(newDocument(3))
				if err != nil {
					t.Fatal(err)
				}
				checkBSONSetWALDeltaAlternateRecovery(t, dir, suffixSegment, prefix, want, prior)
			}
			for range 2 {
				checkBSONSetWALDeltaRecovered(t, dir, prefix+3, want)
			}
			cfg, found, err := backenddb.LoadFormatConfig(dbdir)
			if err != nil || !found {
				t.Fatalf("loading post-close format: found=%t err=%v", found, err)
			}
			marked := slices.Contains(cfg.RequiredFeatures, backenddb.RequiredFeatureBSONSetWALDelta)
			if marked != (opt == "1") {
				t.Fatalf("post-close marker=%t for opt=%s", marked, opt)
			}
			if marked {
				cfg.RequiredFeatures = slices.DeleteFunc(cfg.RequiredFeatures, func(feature string) bool {
					return feature == backenddb.RequiredFeatureBSONSetWALDelta
				})
				if err := backenddb.SaveFormatConfig(dbdir, cfg); !errors.Is(err, backenddb.ErrLegacyFormatRebuildRequired) {
					t.Fatalf("removing the persistent marker returned %v", err)
				}
			} else {
				owner, err := OpenStandaloneServer(StandaloneOptions{
					Dir: dir, Profile: treedb.ProfileCommandWALRelaxed, ExperimentalBSONSetWALDelta: true,
				})
				if err == nil {
					_ = owner.Close()
					t.Fatal("experimental writer opened an existing unmarked store")
				}
				if !errors.Is(err, backenddb.ErrLegacyFormatRebuildRequired) {
					t.Fatalf("existing unmarked store returned %v", err)
				}
			}
		})
	}
}

func checkBSONSetWALDeltaRecovered(t *testing.T, dir string, lsn uint64, want []byte) {
	t.Helper()
	// No writer opt-in is needed to recover already-written delta frames.
	owner, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got := owner.Backend.State().AppliedCommandLSN; got != lsn {
		t.Fatalf("replay applied=%d, want %d", got, lsn)
	}
	reply := serveCommand(t, owner.Server, 1, bson.D{{Key: "find", Value: "docs"}, {Key: "$db", Value: "delta"}})
	assertOK(t, reply)
	rows := cursorFirstBatch(t, reply)
	if len(rows) != 1 || !bytes.Equal(rows[0], want) {
		t.Fatalf("typed full BSON replay returned %v, want %v", rows, bson.Raw(want))
	}
}

func checkBSONSetWALDeltaAlternateRecovery(t *testing.T, source, segment string, prefix uint64, final, prior []byte) {
	t.Helper()
	copyCrashStore := func(t *testing.T) (string, string) {
		t.Helper()
		dir := t.TempDir()
		if err := os.CopyFS(dir, os.DirFS(source)); err != nil {
			t.Fatal(err)
		}
		return dir, filepath.Join(dir, "maindb")
	}
	t.Run("alternate_DATA_slot", func(t *testing.T) {
		dir, dbdir := copyCrashStore(t)
		file, err := os.OpenFile(filepath.Join(dbdir, "index.db"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		slots := [2]rootpublication.DurableRootRecordV1{}
		for i := range slots {
			var image [page.PageSize]byte
			if _, err := file.ReadAt(image[:], int64(i*page.PageSize)); err != nil {
				t.Fatal(err)
			}
			if !page.VerifyChecksumNonMutating(image[:]) {
				t.Fatalf("invalid slot %d checksum", i)
			}
			meta, err := page.DecodeDurableMetaV1(image[page.PageHeaderSize:])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := file.ReadAt(image[:], int64(meta.RootRecordPageID*page.PageSize)); err != nil {
				t.Fatal(err)
			}
			slots[i], err = rootpublication.DecodeDurableRootRecordV1(image[:], meta.RootRecordPageID, meta.RootRecordDigest)
			if err != nil {
				t.Fatal(err)
			}
		}
		newest := 0
		if slots[1].CommitSeq > slots[0].CommitSeq {
			newest = 1
		}
		if slots[newest].AppliedCommandLSN != prefix || slots[newest].CommitSeq <= slots[1-newest].CommitSeq {
			t.Fatalf("fixture lacks current and distinct preceding roots: %+v", slots)
		}
		t.Logf("original selected slot=%d applied=%d, alternate slot=%d applied=%d",
			newest, prefix, 1-newest, slots[1-newest].AppliedCommandLSN)
		if _, err := file.WriteAt(make([]byte, page.PageSize), int64(newest*page.PageSize)); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			t.Fatal(err)
		}
		checkBSONSetWALDeltaRecovered(t, dir, prefix+3, final)
	})
	t.Run("incomplete_relaxed_tail", func(t *testing.T) {
		dir, dbdir := copyCrashStore(t)
		path := filepath.Join(backenddb.WALDirPath(dbdir), segment)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		// Model a missing terminal body. Its full physical stream header
		// remains recognizable and there is no later durable barrier.
		if err := os.Truncate(path, info.Size()-32); err != nil {
			t.Fatal(err)
		}
		checkBSONSetWALDeltaRecovered(t, dir, prefix+2, prior)
	})
	for _, failure := range []string{"missing", "before", "after", "incarnation", "marker"} {
		t.Run(failure, func(t *testing.T) {
			dir, dbdir := copyCrashStore(t)
			path := filepath.Join(backenddb.WALDirPath(dbdir), segment)
			if failure == "marker" {
				file := filepath.Join(dbdir, "format.json")
				data, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				altered := bytes.Replace(data, []byte(",\n    \"standalone_bson_set_wal_delta_v1\""), nil, 1)
				if bytes.Equal(altered, data) {
					t.Fatal("fixture format is missing the required marker")
				}
				if err := os.WriteFile(file, altered, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				alterBSONSetWALDeltaFrame(t, path, prefix+1, failure)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed})
			if err == nil {
				_ = owner.Close()
				t.Fatal("opened an inconsistent complete delta")
			}
			want := "replay BSON $set WAL delta"
			if failure == "marker" {
				want = "requires the store feature marker"
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(before, after) {
				t.Fatalf("complete frame refusal changed the physical WAL: %v", err)
			}
		})
	}
}

func alterBSONSetWALDeltaFrame(t *testing.T, path string, lsn uint64, failure string) {
	t.Helper()
	frames, err := commitlog.ScanCommandFramesV2(path, commitlog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var changed bool
	temp := filepath.Join(t.TempDir(), "frame.log")
	writer, err := commitlog.NewWriter(temp)
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		if frame.LSN == lsn {
			name, delta, err := commitlog.DecodeCollectionBSONSetDelta(frame.Payload)
			if err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "missing":
				delta.ID[len(delta.ID)-1] ^= 1
			case "before":
				delta.Before[0] ^= 1
			case "after":
				delta.After[0] ^= 1
			case "incarnation":
				delta.Incarnation++
			}
			frame.Payload, err = commitlog.EncodeCollectionBSONSetDelta(string(name), delta)
			if err != nil {
				t.Fatal(err)
			}
			changed = true
		}
		if err := writer.AppendCommandV2(frame); err != nil {
			t.Fatal(err)
		}
	}
	if !changed {
		t.Fatalf("fixture did not find LSN %d", lsn)
	}
	if err := errors.Join(writer.Sync(), writer.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp, path); err != nil {
		t.Fatal(err)
	}
}
