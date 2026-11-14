package mongogateway

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// One cold public insert stages CREATE before admission. A different, existing
// namespace then changes the real native semantic cut before CREATE is released.
// The observer only waits: it does not inject an error, alter state, or retry.
func TestStandaloneColdCreateSurvivesUnrelatedNativePublication(t *testing.T) {
	opts := StandaloneOptions{Dir: t.TempDir(), Profile: treedb.ProfileCommandWALRelaxed}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	client, cancelServe, ln, serveErr := startColdCreateBasisClient(t, s)
	open := true
	t.Cleanup(func() {
		if open {
			stopColdCreateBasisClient(t, client, cancelServe, ln, serveErr, s)
		}
	})
	if !s.Backend.HasNativeSemanticBridge() || !s.Collections.UsesNativeCommandWALApply() {
		t.Fatal("fixture did not select native shared-COW command WAL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db := client.Database("basisproof", options.Database().SetWriteConcern(writeconcern.W1()))
	advance, target := db.Collection("advance"), db.Collection("target")
	if result, err := advance.InsertOne(ctx, bson.D{{Key: "_id", Value: "seed"}, {Key: "marker", Value: "seed"}}); err != nil || result == nil || result.InsertedID != "seed" {
		t.Fatalf("seed existing namespace: result=%+v err=%v", result, err)
	}
	if _, err := s.Collections.OpenCollection("basisproof.target"); !errors.Is(err, collections.ErrCollectionNotFound) {
		t.Fatalf("target must be absent before its only InsertOne: %v", err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce, restoreOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var admissions atomic.Int32
	var beforeAppend, afterAppend, cutSwap, staged atomic.Uint64
	var rootsMu sync.Mutex
	roots := make(map[string]uint64)
	observedRoots := func() map[string]uint64 {
		rootsMu.Lock()
		defer rootsMu.Unlock()
		out := make(map[string]uint64, len(roots))
		for root, count := range roots {
			out[root] = count
		}
		return out
	}
	type cuts struct{ BeforeAppend, AfterAppend, CutSwap, Staged uint64 }
	snapshot := func() cuts { return cuts{beforeAppend.Load(), afterAppend.Load(), cutSwap.Load(), staged.Load()} }
	restoreHook := durabilitycut.Install(func(event durabilitycut.Event) error {
		rel, err := filepath.Rel(opts.Dir, event.Root)
		if err != nil || filepath.IsAbs(rel) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil
		}
		rootsMu.Lock()
		roots[event.Root]++
		rootsMu.Unlock()
		switch event.Point {
		case durabilitycut.BeforeDependencyAppend:
			if event.Resource == durabilitycut.ResourceCommandWAL {
				beforeAppend.Add(1)
			}
		case durabilitycut.AfterDependencyAppend:
			if event.Resource == durabilitycut.ResourceCommandWAL {
				afterAppend.Add(1)
			}
		case durabilitycut.BeforeCOWCutSwap:
			cutSwap.Add(1)
		case durabilitycut.AfterLogicalMVCCStaging:
			staged.Add(1)
		}
		if event.Point == durabilitycut.BeforeCOWAdmission {
			if admissions.Add(1) == 1 {
				close(entered)
				<-release
			}
		}
		return nil
	})
	restore := func() { restoreOnce.Do(restoreHook) }
	defer restore()
	type outcome struct {
		result *mongo.InsertOneResult
		err    error
	}
	done := make(chan outcome, 1)
	joined := false
	defer func() {
		unblock()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("target did not terminate after release")
			}
		}
	}()
	baselineCuts := snapshot()
	t.Logf("cut counters baseline=%+v observed_roots=%v", baselineCuts, observedRoots())
	go func() {
		result, err := target.InsertOne(ctx, bson.D{{Key: "_id", Value: "target-once"}, {Key: "marker", Value: "exact target"}})
		done <- outcome{result, err}
	}()
	select {
	case <-entered:
	case result := <-done:
		joined = true
		t.Fatalf("target returned before CREATE admission: result=%+v err=%v", result.result, result.err)
	case <-time.After(10 * time.Second):
		t.Fatal("cold target did not reach CREATE admission")
	}
	before, err := s.Backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	beforeOpen := true
	defer func() {
		if beforeOpen {
			if err := before.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	beforeLSN := s.Backend.CommandWALNextLSN()
	advanceCtx, advanceCancel := context.WithTimeout(ctx, 5*time.Second)
	advanced, advanceErr := advance.InsertOne(advanceCtx, bson.D{{Key: "_id", Value: "advance-once"}, {Key: "marker", Value: "real publication"}})
	advanceCancel()
	if advanceErr != nil || advanced == nil || advanced.InsertedID != "advance-once" {
		t.Fatalf("one existing-namespace public insert while CREATE parked: result=%+v err=%v", advanced, advanceErr)
	}
	after, err := s.Backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	moved := before.State() != nil && before.State() != after.State()
	opening, advancedSequence := before.LogicalSequence(), after.LogicalSequence()
	afterOpen := true
	defer func() {
		if afterOpen {
			if err := after.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	closeErr := before.Close()
	beforeOpen = false
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	afterLSN := s.Backend.CommandWALNextLSN()
	if !moved || advancedSequence != opening+1 || afterLSN != beforeLSN+1 || admissions.Load() != 2 {
		t.Fatalf("real cut did not move exactly once while CREATE parked: moved=%t sequence=%d->%d nextLSN=%d->%d admissions=%d", moved, opening, advancedSequence, beforeLSN, afterLSN, admissions.Load())
	}
	advanceCuts := snapshot()
	t.Logf("cut counters after_advance=%+v delta=%+v", advanceCuts, cuts{advanceCuts.BeforeAppend - baselineCuts.BeforeAppend, advanceCuts.AfterAppend - baselineCuts.AfterAppend, advanceCuts.CutSwap - baselineCuts.CutSwap, advanceCuts.Staged - baselineCuts.Staged})
	t.Logf("observed event roots after_advance=%v", observedRoots())
	unblock()
	var result outcome
	select {
	case result = <-done:
		joined = true
	case <-time.After(10 * time.Second):
		t.Fatal("target did not complete after its single release")
	}
	post, err := s.Backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	installed := post.State() != after.State()
	postSequence := post.LogicalSequence()
	closeErr = errors.Join(post.Close(), after.Close())
	afterOpen = false
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	targetCuts := snapshot()
	t.Logf("observed event roots after_target=%v", observedRoots())
	restore()
	t.Logf("cut counters after_target=%+v delta_from_advance=%+v installed=%t sequence=%d->%d", targetCuts, cuts{targetCuts.BeforeAppend - advanceCuts.BeforeAppend, targetCuts.AfterAppend - advanceCuts.AfterAppend, targetCuts.CutSwap - advanceCuts.CutSwap, targetCuts.Staged - advanceCuts.Staged}, installed, advancedSequence, postSequence)
	accepted := result.err == nil && result.result != nil && result.result.InsertedID == "target-once"
	t.Logf("CREATE result=%+v err=%v cut_moved=%t sequence=%d->%d nextLSN=%d->%d->%d admissions=%d", result.result, result.err, moved, opening, advancedSequence, beforeLSN, afterLSN, s.Backend.CommandWALNextLSN(), admissions.Load())
	if !accepted {
		var commandError mongo.CommandError
		classified := errors.As(result.err, &commandError)
		t.Logf("refused command error classified=%t Code=%d CodeName=%q Message=%q", classified, commandError.Code, commandError.Name, commandError.Message)
		if !classified || commandError.Code != 2 || commandError.Name != "BadValue" || commandError.Message != backenddb.ErrRootPublicationBasisMismatch.Error() {
			t.Errorf("refusal was not the selected exact basis mismatch: %#v", result.err)
		}
		if installed || postSequence != advancedSequence || targetCuts != advanceCuts {
			t.Errorf("refused CREATE appended/staged/installed: installed=%t sequence=%d->%d counters=%+v->%+v", installed, advancedSequence, postSequence, advanceCuts, targetCuts)
		}
		t.Errorf("cold public InsertOne must survive one unrelated native publication: result=%+v err=%v", result.result, result.err)
		if s.Backend.CommandWALNextLSN() != afterLSN || admissions.Load() != 2 {
			t.Errorf("refused CREATE unexpectedly assigned or retried: nextLSN=%d want=%d admissions=%d want=2", s.Backend.CommandWALNextLSN(), afterLSN, admissions.Load())
		}
	} else if !installed || postSequence != advancedSequence+2 || s.Backend.CommandWALNextLSN() != afterLSN+2 || admissions.Load() != 3 {
		t.Errorf("target CREATE plus document were not accepted exactly once: nextLSN=%d want=%d admissions=%d want=3", s.Backend.CommandWALNextLSN(), afterLSN+2, admissions.Load())
	}
	if accepted {
		// Native publication emits CutSwap, not AfterLogicalMVCCStaging.
		if delta := (cuts{targetCuts.BeforeAppend - advanceCuts.BeforeAppend, targetCuts.AfterAppend - advanceCuts.AfterAppend, targetCuts.CutSwap - advanceCuts.CutSwap, targetCuts.Staged - advanceCuts.Staged}); delta != (cuts{2, 2, 2, 0}) {
			t.Errorf("CREATE plus row append/install counters=%+v want={2 2 2 0}", delta)
		}
	}
	verify := func(c *mongo.Client) {
		t.Helper()
		d := c.Database("basisproof")
		for _, item := range []struct{ collection, id, marker string }{{"advance", "seed", "seed"}, {"advance", "advance-once", "real publication"}, {"target", "target-once", "exact target"}} {
			var got struct {
				Marker string `bson:"marker"`
			}
			err := d.Collection(item.collection).FindOne(ctx, bson.D{{Key: "_id", Value: item.id}}).Decode(&got)
			if item.collection == "target" && !accepted {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					t.Errorf("refused target unexpectedly visible: got=%+v err=%v", got, err)
				}
				continue
			}
			if err != nil || got.Marker != item.marker {
				t.Errorf("readback %s/%s: got=%+v err=%v", item.collection, item.id, got, err)
			}
		}
		for _, item := range []struct {
			name string
			want int64
		}{{"advance", 2}, {"target", 1}} {
			if item.name == "target" && !accepted {
				item.want = 0
			}
			count, err := func() (int64, error) {
				countCtx, countCancel := context.WithCancel(context.Background())
				defer countCancel()
				timer := time.AfterFunc(5*time.Second, countCancel)
				defer timer.Stop()
				return d.Collection(item.name).CountDocuments(countCtx, bson.D{})
			}()
			if err != nil || count != item.want {
				t.Errorf("exact census %s: count=%d want=%d err=%v", item.name, count, item.want, err)
			}
		}
	}
	verify(client)
	open = false
	if !stopColdCreateBasisClient(t, client, cancelServe, ln, serveErr, s) {
		t.Fatal("original checked shutdown failed; reopen not attempted")
	}
	reopened, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatalf("fresh reopen after checked Close: %v", err)
	}
	reopenedClient, reopenedCancel, reopenedLN, reopenedServeErr := startColdCreateBasisClient(t, reopened)
	defer stopColdCreateBasisClient(t, reopenedClient, reopenedCancel, reopenedLN, reopenedServeErr, reopened)
	verify(reopenedClient)
}

// Mirrors startStandaloneMongoClientForTest's listener lifecycle. This proof
// explicitly disables driver retries and allows the parked and advancing calls
// to use different connections. Ordinary gateway scheduling is unchanged.
func startColdCreateBasisClient(t *testing.T, s *StandaloneServer) (*mongo.Client, context.CancelFunc, net.Listener, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		_ = s.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + ln.Addr().String()).SetDirect(true).
		SetRetryWrites(false).SetRetryReads(false).SetMaxPoolSize(2).SetServerSelectionTimeout(time.Second))
	if err != nil {
		cancel()
		_ = ln.Close()
		_ = s.Close()
		t.Fatal(err)
	}
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		stopColdCreateBasisClient(t, client, cancel, ln, done, s)
		t.Fatal(err)
	}
	return client, cancel, ln, done
}

// Bounded adaptation of the maintained shutdown helper: same order and checks.
func stopColdCreateBasisClient(t *testing.T, client *mongo.Client, cancel context.CancelFunc, ln net.Listener, serveErr chan error, s *StandaloneServer) bool {
	t.Helper()
	ok := true
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := client.Disconnect(closeCtx)
	closeCancel()
	if err != nil {
		t.Errorf("Disconnect: %v", err)
		ok = false
	}
	cancel()
	_ = ln.Close()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Errorf("Serve join: %v", err)
			ok = false
		}
	case <-time.After(5 * time.Second):
		t.Error("Serve did not stop in 5s")
		ok = false
	}
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("server Close: %v", err)
			ok = false
		}
	case <-time.After(5 * time.Second):
		t.Error("server Close did not stop in 5s")
		ok = false
	}
	t.Logf("checked shutdown client/Serve/server complete=%t", ok)
	return ok
}
