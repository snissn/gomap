package mongogateway

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func TestStandaloneNativeJournaledInsertDuringGenerationDrain(t *testing.T) {
	const wait = 5 * time.Second
	limits := treedb.DefaultCOWMemtableLimits()
	limits.MaxGenerationBytes = 2 << 20
	opts := StandaloneOptions{
		Dir:                      t.TempDir(),
		Profile:                  treedb.ProfileCommandWALDurable,
		COWMemtableLimits:        limits,
		DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON},
	}
	owner, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, owner)
	defer func() {
		if client != nil {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, owner)
		}
	}()

	ctx, cancelContext := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancelContext()
	database := client.Database("app")
	const collectionName = "generation_drain"
	if err := database.CreateCollection(ctx, collectionName); err != nil {
		t.Fatal(err)
	}
	if !owner.Collections.UsesNativeCommandWALApply() {
		t.Fatal("fixture must use the original native command WAL owner")
	}
	journal := true
	coll := database.Collection(collectionName, options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
	payload := strings.Repeat("0123456789abcdef", 2048)
	doc := func(i int) bson.D {
		return bson.D{{Key: "_id", Value: fmt.Sprintf("row-%04d", i)}, {Key: "payload", Value: payload}}
	}
	filled := 0
	reachedPressure := false
	for ; filled < 512; filled++ {
		if _, err := coll.InsertOne(ctx, doc(filled)); err != nil {
			t.Fatalf("fill row %d: %v", filled, err)
		}
		pressure, err := owner.Backend.ObserveNativeProducer()
		if err != nil {
			t.Fatal(err)
		}
		if !pressure.UnderPressure {
			continue
		}
		if !pressure.Semantic.GenerationUnderPressure || pressure.Semantic.ConstructionBytes != 0 ||
			pressure.ReservedBytes >= limits.MaxInFlightBytes-limits.MaxInFlightBytes/4 ||
			pressure.Sources >= limits.MaxSources || pressure.Generations >= limits.MaxGenerations {
			t.Fatalf("fixture reached pressure outside the generation-only route: %+v", pressure)
		}
		filled++
		reachedPressure = true
		t.Logf("generation-only pressure after %d checked journaled rows: %+v", filled, pressure)
		break
	}
	if !reachedPressure {
		t.Fatal("fixture failed to reach generation pressure")
	}

	nextLSN := owner.Backend.CommandWALNextLSN()
	if nextLSN == 0 {
		t.Fatal("no canonical WAL sequence")
	}
	prefixHeld := make(chan string, 1)
	walHeld := make(chan uint64, 1)
	releasePrefix, releaseWAL := make(chan struct{}), make(chan struct{})
	unblockPrefix, unblockWAL := sync.OnceFunc(func() { close(releasePrefix) }), sync.OnceFunc(func() { close(releaseWAL) })
	var prefixClaimed atomic.Bool
	var walClaimed, walSynced atomic.Uint64
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if event.Point == durabilitycut.AfterDependencyAppend && event.Resource == durabilitycut.ResourceValueLog &&
			len(event.Paths) > 0 && strings.HasPrefix(filepath.Clean(event.Paths[0]), filepath.Clean(opts.Dir)+string(filepath.Separator)) &&
			prefixClaimed.CompareAndSwap(false, true) {
			prefixHeld <- event.Paths[0]
			<-releasePrefix
		}
		if event.Resource == durabilitycut.ResourceCommandWAL && event.LSN >= nextLSN {
			if event.Point == durabilitycut.BeforeDependencyFileSync && walClaimed.CompareAndSwap(0, event.LSN) {
				if event.StablePath == nil {
					return fmt.Errorf("successor WAL lacks stable authority")
				}
				walHeld <- event.LSN
				<-releaseWAL
			}
			if event.Point == durabilitycut.AfterDependencyFileSync && walClaimed.Load() == event.LSN {
				walSynced.Store(event.LSN)
			}
		}
		return nil
	})
	type result struct {
		err       error
		syncedLSN uint64
	}
	done := make(chan result, 1)
	joined := false
	defer func() {
		unblockWAL()
		unblockPrefix()
		if !joined {
			select {
			case got := <-done:
				if got.err != nil {
					t.Errorf("joining triggering INSERT: %v", got.err)
				}
			case <-time.After(wait):
				t.Error("triggering INSERT did not drain after releasing both dependencies")
			}
		}
		restore()
	}()
	go func() {
		_, err := coll.InsertOne(ctx, doc(filled))
		done <- result{err: err, syncedLSN: walSynced.Load()}
	}()

	select {
	case path := <-prefixHeld:
		t.Logf("holding old prefix at document value-log append: %s", filepath.Base(path))
	case got := <-done:
		joined = true
		t.Fatalf("INSERT returned before older prefix reached the physical witness: %+v", got)
	case <-time.After(wait):
		t.Fatal("older prefix did not reach the physical witness")
	}
	var lsn uint64
	select {
	case lsn = <-walHeld:
		select {
		case got := <-done:
			joined = true
			t.Fatalf("INSERT returned with its own WAL held: %+v", got)
		default:
		}
	case got := <-done:
		joined = true
		t.Fatalf("INSERT returned before reaching its own WAL: %+v", got)
	case <-time.After(2 * time.Second):
		t.Error("triggering INSERT could not reach its own WAL while the old prefix was held")
		unblockPrefix()
		select {
		case lsn = <-walHeld:
		case got := <-done:
			joined = true
			t.Fatalf("triggering INSERT returned without its WAL sync after prefix release: %+v", got)
		case <-time.After(wait):
			t.Fatal("triggering INSERT did not reach own WAL after prefix release")
		}
	}
	unblockWAL()
	select {
	case got := <-done:
		joined = true
		if got.err != nil || got.syncedLSN != lsn {
			t.Fatalf("acknowledgement did not follow own physical WAL sync: assigned=%d result=%+v", lsn, got)
		}
	case <-time.After(wait):
		t.Fatal("no acknowledgement after own WAL sync and before old prefix release")
	}
	unblockPrefix()
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, owner)
	client = nil

	reopened, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("checked fresh owner close: %v", err)
		}
	}()
	stored, err := reopened.Collections.OpenCollection("app." + collectionName)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= filled; i++ {
		id := fmt.Sprintf("row-%04d", i)
		key, _, err := prepareInsertDocument(mustDocument(t, doc(i)), collections.DocumentFormatBSON)
		if err != nil {
			t.Fatal(err)
		}
		body, err := stored.Get(key)
		if err != nil {
			t.Fatalf("fresh owner row %q: %v", id, err)
		}
		var got struct {
			ID      string `bson:"_id"`
			Payload string `bson:"payload"`
		}
		if err := bson.Unmarshal(body, &got); err != nil || got.ID != id || got.Payload != payload {
			t.Fatalf("fresh owner row %q id=%q payloadBytes=%d error=%v", id, got.ID, len(got.Payload), err)
		}
	}
}
