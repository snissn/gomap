package ycsbevidence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	mongogateway "github.com/snissn/gomap/TreeDB/mongo_gateway"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// No command WAL and no graceful owner Close may rescue the checkpoint. The
// empty catalog baseline is sealed first, then only the public control can seal
// the newly accepted binary document before the child exits abruptly.
func TestExplicitCheckpointSealsCOWBeforeOwnerClose(t *testing.T) {
	requireLinuxProcessEvidence(t)
	const childEnv = "GOMAP_YCSB_CHECKPOINT_CRASH_ROOT"
	doc, err := bson.Marshal(bson.D{{Key: "_id", Value: "row"}, {Key: "n", Value: int64(7)}, {Key: "binary", Value: bson.Binary{Subtype: 0, Data: []byte{0, 255, 1, 0, 128}}}})
	if err != nil {
		t.Fatal(err)
	}
	assertOwner := func(s *mongogateway.StandaloneServer) uint64 {
		t.Helper()
		stats := s.Stats()
		seq, err := strconv.ParseUint(stats["treedb.cache.cow.txn.logical_seq"], 10, 64)
		if err != nil || !s.Backend.HasNativeSemanticBridge() || s.Backend.CommandWALEnabled() || stats["treedb.command_wal.enabled"] != "false" || stats["treedb.cache.memtable_mode"] != "cow_btree" || stats["treedb.profile.resolved"] != string(treedb.ProfileNoWALFast) {
			t.Fatalf("not actual no-WAL COW owner: seq=%d err=%v stats=%v", seq, err, stats)
		}
		return seq
	}
	if root := os.Getenv(childEnv); root != "" {
		data, evidence := filepath.Join(root, "data"), filepath.Join(root, "evidence")
		if err := os.MkdirAll(evidence, 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("YCSB_SERVER_RECEIPT_DIR", evidence)
		t.Setenv("YCSB_OWNER_CHECKPOINT", "1")
		s, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: data, Profile: treedb.ProfileNoWALFast})
		if err != nil {
			t.Fatal(err)
		}
		assertOwner(s)
		if _, err = s.Collections.CreateCollection(&collections.CollectionMeta{Name: "db.rows", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}); err != nil {
			t.Fatal(err)
		}
		c, err := s.Collections.OpenCollection("db.rows")
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Collections.CheckpointStorage(); err != nil {
			t.Fatal(err)
		}
		baseline := assertOwner(s)
		if _, err = c.InsertBatchValidatedBSON([][]byte{[]byte("row")}, [][]byte{doc}); err != nil {
			t.Fatal(err)
		}
		if got, found, err := c.GetInto([]byte("row"), nil); err != nil || !found || !bytes.Equal(got, doc) {
			t.Fatalf("live accepted row=%x found=%v err=%v", got, found, err)
		}
		if seq := assertOwner(s); seq <= baseline {
			t.Fatalf("acceptance did not advance COW sequence: %d <= %d", seq, baseline)
		}
		o, err := OpenFromEnv(string(treedb.ProfileNoWALFast), data, s.Backend, s.Stats)
		if err != nil {
			t.Fatal(err)
		}
		control, err := startActualOwnerCheckpoint(o, s)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := filepath.Join(evidence, "accepted-row.bson")
		if err = os.WriteFile(snapshot, doc, 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		receipt, err := RequestCheckpoint(ctx, evidence, strings.Repeat("a", 40), "no-wal-binary-row", snapshot)
		if err != nil || !receipt.Result.Success || !receipt.Flush.Success || !receipt.Checkpoint.Success || receipt.Owner.OwnerID != o.ownerID || receipt.Owner.Process.PID != os.Getpid() || receipt.Owner.Backend != "cow" {
			t.Fatalf("checkpoint receipt=%+v err=%v", receipt, err)
		}
		if err = control.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(evidence, "owner.checkpoint.json"))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ACTUAL_CHECKPOINT_RECEIPT %s", raw)
		if _, err = os.Stat(filepath.Join(evidence, "owner.close.json")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("owner Close unexpectedly recorded: %v", err)
		}
		t.Log("ABRUPT_EXIT_97_NO_OWNER_CLOSE command_wal=false live_binary_row=true")
		os.Exit(97)
	}
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExplicitCheckpointSealsCOWBeforeOwnerClose$", "-test.timeout=15s", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"="+root)
	output, err := cmd.CombinedOutput()
	t.Logf("actual child output:\n%s", output)
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 97 || !bytes.Contains(output, []byte("ABRUPT_EXIT_97_NO_OWNER_CLOSE")) {
		t.Fatalf("child exit=%v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "evidence", "owner.checkpoint.json"))
	var receipt CheckpointReceipt
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &receipt); err != nil || !receipt.Result.Success || !receipt.Checkpoint.Success {
		t.Fatalf("retained success receipt=%s err=%v", raw, err)
	}
	if _, err = os.Stat(filepath.Join(root, "evidence", "owner.close.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owner Close unexpectedly recorded: %v", err)
	}
	s, err := mongogateway.OpenStandaloneServer(mongogateway.StandaloneOptions{Dir: filepath.Join(root, "data"), Profile: treedb.ProfileNoWALFast})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	assertOwner(s)
	c, err := s.Collections.OpenCollection("db.rows")
	if err != nil {
		t.Fatal(err)
	}
	got, found, err := c.GetInto([]byte("row"), nil)
	if err != nil || !found || !bytes.Equal(got, doc) {
		t.Fatalf("checkpoint failed to persist exact accepted row before Close: found=%v got=%x want=%x err=%v", found, got, doc, err)
	}
}
