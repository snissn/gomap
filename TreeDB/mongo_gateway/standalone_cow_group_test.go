package mongogateway

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Use the product defaults and real outgoing driver requests. Holding the first
// real sync lets seven same-collection requests reach the existing coalescer;
// the unchanged batch planner must publish those seven together on release.
func TestStandaloneSharedCOWDefaultCoalescerDurableEightWriters(t *testing.T) {
	s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	client, cancel, ln, served := startStandaloneMongoClientForTest(t, s)
	defer stopStandaloneMongoClientForTest(t, client, cancel, ln, served, s)
	ctx, ctxCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer ctxCancel()
	c := client.Database("group").Collection("rows")
	if _, err := c.Indexes().CreateMany(ctx, []mongo.IndexModel{{Keys: bson.D{{Key: "n", Value: 1}}}, {Keys: bson.D{{Key: "email", Value: 1}}}}); err != nil {
		t.Fatal(err)
	}
	const writers = 8
	rows := make([]any, writers)
	for i := range rows {
		rows[i] = bson.D{{Key: "_id", Value: int64(i)}, {Key: "n", Value: int64(0)}, {Key: "email", Value: fmt.Sprintf("%d@example.test", i)}}
	}
	if _, err := c.InsertMany(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if !s.Backend.HasNativeSemanticBridge() {
		t.Fatal("shared native owner inactive")
	}
	before := s.Backend.Stats()
	atSync, release := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Resource == durabilitycut.ResourceCommandWAL && event.Point == durabilitycut.BeforeDependencyFileSync {
			enterOnce.Do(func() { close(atSync); <-release })
		}
		return nil
	})
	defer restore()
	errs := make(chan error, writers)
	update := func(i int) {
		result, err := c.UpdateOne(ctx, bson.D{{Key: "_id", Value: int64(i)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: int64(1)}}}})
		if err == nil && (result.MatchedCount != 1 || result.ModifiedCount != 1) {
			err = fmt.Errorf("writer%d counts=%+v", i, result)
		}
		errs <- err
	}
	go update(0)
	select {
	case <-atSync:
	case err := <-errs:
		t.Fatalf("first ACK before held sync:%v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	for i := 1; i < writers; i++ {
		go update(i)
	}
	deadline := time.Now().Add(5 * time.Second)
	queued := 0
	for time.Now().Before(deadline) {
		s.Server.updateMu.Lock()
		coalescer := s.Server.updateCoalescers["group.rows"]
		if coalescer != nil {
			queued = len(coalescer.requests)
		}
		s.Server.updateMu.Unlock()
		if queued == writers-1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if queued != writers-1 {
		t.Fatalf("queued outgoing same-collection requests=%d want%d", queued, writers-1)
	}
	select {
	case err := <-errs:
		t.Fatalf("ACK escaped held sync:%v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	for range writers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	after := s.Backend.Stats()
	stat := func(stats map[string]string, key string) uint64 {
		n, err := strconv.ParseUint(stats[key], 10, 64)
		if err != nil {
			t.Fatalf("stat%s=%q:%v", key, stats[key], err)
		}
		return n
	}
	syncs := stat(after, "treedb.command_wal.file_sync.calls_total") - stat(before, "treedb.command_wal.file_sync.calls_total")
	if syncs != 2 {
		t.Fatalf("eight outgoing requests file syncs=%d want2 (one initial + one existing coalesced batch)", syncs)
	}
	for i := range writers {
		var doc bson.M
		if err := c.FindOne(ctx, bson.D{{Key: "_id", Value: int64(i)}}).Decode(&doc); err != nil || doc["n"] != int64(1) {
			t.Fatalf("writer%d read=%v/%v", i, doc, err)
		}
	}
	t.Logf("product default durable ACK: eight outgoing samecollection requests, seven queued, file syncs=%d", syncs)
}
