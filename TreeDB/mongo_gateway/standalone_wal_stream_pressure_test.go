package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	streamAcceptedPressureTrigger = 24 // Three quarters of 4 sources × 8 generations.
	streamAcceptedPressureRows    = streamAcceptedPressureTrigger + 2
	streamAcceptedPressureDB      = "stream"
	streamAcceptedPressureColl    = "docs"
)

func TestStandaloneRelaxedExt4StreamJournalAckAfterAcceptedOccupancyCrashReopen(t *testing.T) {
	const (
		dirEnv = "TREEDB_MONGO_EXT4_STREAM_ACCEPTED_PRESSURE_DIR"
		marker = "EXT4 STREAM ACCEPTED PRESSURE ACK "
	)
	dir := os.Getenv(dirEnv)
	child := dir != ""
	if !child {
		dir = t.TempDir()
		requireStandaloneExt4(t, dir)
	}
	limits := treedb.DefaultCOWMemtableLimits()
	limits.MaxSources, limits.MaxGenerations = 4, 8
	opts := StandaloneOptions{Dir: dir, Profile: treedb.ProfileCommandWALRelaxed, COWMemtableLimits: limits}
	if child {
		t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
		lsn := runStreamAcceptedPressureJournalAck(t, opts)
		fmt.Printf("%s%d\n", marker, lsn)
		// Exit immediately after the public response, before any graceful close.
		os.Exit(97)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStandaloneRelaxedExt4StreamJournalAckAfterAcceptedOccupancyCrashReopen$", "-test.v")
	cmd.Env = append(os.Environ(), dirEnv+"="+dir)
	output, err := cmd.CombinedOutput()
	var exited *exec.ExitError
	_, suffix, marked := bytes.Cut(output, []byte("\n"+marker))
	line, _, _ := bytes.Cut(suffix, []byte("\n"))
	wantLSN, parseErr := strconv.ParseUint(string(line), 10, 64)
	if ctx.Err() != nil || !errors.As(err, &exited) || exited.ExitCode() != 97 || !marked || parseErr != nil || wantLSN == 0 || bytes.Contains(output, []byte("DATA RACE")) {
		t.Fatalf("journal child failed before checked abrupt exit: %v (deadline: %v)\n%s", err, ctx.Err(), output)
	}
	t.Logf("checked abrupt exit after journaled LSN %d\n%s", wantLSN, output)

	owner, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	requireStandaloneNativeJournalOwner(t, owner)
	checkStandaloneWALStream(t, owner, true)
	if applied := owner.Backend.State().AppliedCommandLSN; applied != wantLSN || owner.Backend.CommandWALNextLSN() != wantLSN+1 {
		t.Fatalf("fresh stream owner applied LSN=%d next=%d, want %d and %d", applied, owner.Backend.CommandWALNextLSN(), wantLSN, wantLSN+1)
	}
	collection, err := owner.Collections.OpenCollection(streamAcceptedPressureDB + "." + streamAcceptedPressureColl)
	if err != nil {
		t.Fatal(err)
	}
	rows, truncated, err := collection.ScanDocuments(streamAcceptedPressureRows)
	if err != nil || truncated || len(rows) != streamAcceptedPressureRows {
		t.Fatalf("fresh typed census rows=%d truncated=%t err=%v, want %d", len(rows), truncated, err, streamAcceptedPressureRows)
	}
	want := make(map[string][]byte, streamAcceptedPressureRows)
	for n := range streamAcceptedPressureRows {
		doc := streamAcceptedPressureDocument(n)
		want[doc[0].Value.(string)], err = bson.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range rows {
		doc := bson.Raw(row.Document)
		id, ok := doc.Lookup("_id").StringValueOK()
		if !ok || !bytes.Equal(doc, want[id]) {
			t.Fatalf("fresh typed row %q has unexpected BSON (%d bytes)", id, len(doc))
		}
		delete(want, id)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
}

func streamAcceptedPressureDocument(n int) bson.D {
	return bson.D{
		{Key: "_id", Value: fmt.Sprintf("row-%03d", n)},
		{Key: "n", Value: int64(n)},
		// Larger than the pager's inline limit, so physical Apply uses the
		// original document value log. The full fixture stays below idle WAL pressure.
		{Key: "payload", Value: bson.Binary{Subtype: 0x80, Data: bytes.Repeat([]byte{byte(n), 0x71, 0x33, 0xa8}, 1024)}},
	}
}

func streamAcceptedPressureInsert(n int, journal bool) bson.D {
	return bson.D{
		{Key: "insert", Value: streamAcceptedPressureColl},
		{Key: "documents", Value: bson.A{streamAcceptedPressureDocument(n)}},
		{Key: "writeConcern", Value: bson.D{{Key: "w", Value: int32(1)}, {Key: "j", Value: journal}}},
		{Key: "$db", Value: streamAcceptedPressureDB},
	}
}

func runStreamAcceptedPressureJournalAck(t *testing.T, opts StandaloneOptions) uint64 {
	t.Helper()
	const wait = 5 * time.Second
	owner, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	requireStandaloneNativeJournalOwner(t, owner)
	checkStandaloneWALStream(t, owner, true)
	seed := serveCommand(t, owner.Server, 1, streamAcceptedPressureInsert(0, true))
	assertOK(t, seed)
	assertInt32(t, seed, "n", 1)
	initial := owner.Backend.State().AppliedCommandLSN
	before := owner.Server.StandaloneWriteConcernStats()
	if initial == 0 || before.JournalAcknowledgements != 1 || owner.Backend.CommandWALNextLSN() != initial+1 {
		t.Fatalf("fixture did not start from a complete applied seed: LSN=%d stats=%+v", initial, before)
	}
	followingLSN := initial + streamAcceptedPressureTrigger + 1

	root := filepath.Join(opts.Dir, "maindb")
	wal := backenddb.WALDirPath(root)
	followingAppend := make(chan struct{})
	followingApply, releaseFollowingApply := make(chan struct{}), make(chan struct{})
	unblockFollowing := sync.OnceFunc(func() { close(releaseFollowingApply) })
	markAppend := sync.OnceFunc(func() { close(followingAppend) })
	holdFollowing := sync.OnceFunc(func() {
		close(followingApply)
		<-releaseFollowingApply
	})
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Root != root {
			return nil
		}
		switch {
		case event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.AfterDependencyAppend &&
			event.LSN == followingLSN && filepath.Dir(event.Path) == wal:
			markAppend()
		case event.Resource == durabilitycut.ResourceMeta && event.Point == durabilitycut.BeforeAppliedLSNAdvance && event.LSN == followingLSN:
			holdFollowing()
		}
		return nil
	})
	defer restore()
	defer unblockFollowing()

	for n := 1; n <= streamAcceptedPressureTrigger; n++ {
		if n == streamAcceptedPressureTrigger {
			pressure, err := owner.Backend.ObserveNativeProducer()
			if err != nil || pressure.UnderPressure {
				t.Fatalf("fixture reached producer pressure before the occupancy watermark: %+v %v", pressure, err)
			}
		}
		reply := serveCommand(t, owner.Server, int32(n+1), streamAcceptedPressureInsert(n, false))
		assertOK(t, reply)
		assertInt32(t, reply, "n", 1)
	}
	if applied, next := owner.Backend.State().AppliedCommandLSN, owner.Backend.CommandWALNextLSN(); applied != initial || next != followingLSN {
		t.Fatalf("visible writes at the occupancy watermark changed the applied frontier: applied=%d next=%d, want %d and %d", applied, next, initial, followingLSN)
	}

	following := streamAcceptedPressureInsert(streamAcceptedPressureTrigger+1, true)
	var result commandResult
	done := make(chan struct{})
	go func() {
		result.doc, result.err = serveCommandResult(owner.Server, streamAcceptedPressureRows, following)
		close(done)
	}()
	defer func() {
		unblockFollowing()
		select {
		case <-done:
		case <-time.After(wait):
			t.Error("journaled request failed to join after releasing the applied-root hold")
		}
	}()
	assertPending := func(point string, applied uint64) {
		t.Helper()
		select {
		case <-done:
			t.Fatalf("journaled request finished while %s: reply=%s err=%v", point, bson.Raw(result.doc), result.err)
		default:
		}
		stats := owner.Server.StandaloneWriteConcernStats()
		if stats.JournalAcknowledgements != before.JournalAcknowledgements || stats.SyncFailures != before.SyncFailures || owner.Backend.State().AppliedCommandLSN != applied {
			t.Fatalf("unexpected ACK, sync failure or applied frontier while %s: before=%+v after=%+v applied=%d want=%d", point, before, stats, owner.Backend.State().AppliedCommandLSN, applied)
		}
	}
	select {
	case <-followingAppend:
	case <-done:
		t.Fatalf("journaled request returned without its WAL append: %s %v", bson.Raw(result.doc), result.err)
	case <-time.After(wait):
		t.Fatal("journaled request did not reach its own WAL append")
	}
	select {
	case <-followingApply:
	case <-done:
		t.Fatalf("journaled request returned without its applied-root boundary: %s %v", bson.Raw(result.doc), result.err)
	case <-time.After(wait):
		t.Fatal("journaled request did not reach its own applied-root boundary")
	}
	assertPending("its applied-root advance is held", initial)
	unblockFollowing()
	select {
	case <-done:
	case <-time.After(wait):
		t.Fatal("journaled request did not return after releasing its applied-root advance")
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	assertOK(t, result.doc)
	assertInt32(t, result.doc, "n", 1)
	if doc := bson.Raw(result.doc); doc.Lookup("writeConcernError").Type != 0 || doc.Lookup("writeErrors").Type != 0 {
		t.Fatalf("journaled request reported an error: %s", doc)
	}
	after := owner.Server.StandaloneWriteConcernStats()
	if after.VisibleAcknowledgements-before.VisibleAcknowledgements != streamAcceptedPressureTrigger ||
		after.JournalAcknowledgements-before.JournalAcknowledgements != 1 || after.SyncAttempts-before.SyncAttempts != 1 ||
		after.PhysicalSyncBoundaries-before.PhysicalSyncBoundaries != 1 || after.SyncFailures != before.SyncFailures {
		t.Fatalf("wrong public completion counts: before=%+v after=%+v", before, after)
	}
	durable := writeConcernBenchmarkStat(t, owner.Backend.Stats(), "treedb.command_wal.durable_wal_lsn")
	if applied, next := owner.Backend.State().AppliedCommandLSN, owner.Backend.CommandWALNextLSN(); durable != followingLSN || applied != followingLSN || next != followingLSN+1 {
		t.Fatalf("journaled suffix durable=%d applied=%d next=%d, want %d, %d, %d", durable, applied, next, followingLSN, followingLSN, followingLSN+1)
	}
	t.Logf("applied LSN stayed at %d after %d visible writes, then the journaled LSN %d waited for its applied-root advance", initial, streamAcceptedPressureTrigger, followingLSN)
	return followingLSN
}
