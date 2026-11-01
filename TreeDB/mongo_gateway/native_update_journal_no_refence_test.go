package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Durable-profile real-driver behavioral regression; relaxed profile is a mandatory follow-up.
// Target strict updates require their own completed original-owner durable publication.
// An existing AfterDependencyAppend hold occupies the DEFAULT update worker so
// two real distinct-ID BSON-set requests are observably queued together.
func TestStandaloneNativeJournalCoalescedBSONSetPhysicalReceiptNoRefence(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) { exerciseStandaloneNativeBSONSetReceiptNoRefence(t, profile) })
	}
}

func exerciseStandaloneNativeBSONSetReceiptNoRefence(t *testing.T, profile treedb.Profile) {
	const wait = 5 * time.Second
	opts := StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	original := s
	t.Cleanup(func() {
		if err := original.Close(); err != nil {
			t.Errorf("checked original fallback Close: %v", err)
		}
	})
	requireStandaloneNativeJournalOwner(t, s)
	if s.Server.Collections != s.Collections || s.Server.standaloneWriteConcernSync != nil {
		t.Fatal("requires original native owner, general shared admission and real physical sync")
	}
	client, cancel, listener, served := startStandaloneMongoClientForTest(t, s)
	active := true
	ctx, cancelContext := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelContext()
	ids := [3]string{"occupy-worker", "batch-one", "batch-two"}
	type result struct {
		matched, modified int64
		err               error
		after, returned   uint64
	}
	results := [3]chan result{make(chan result, 1), make(chan result, 1), make(chan result, 1)}
	var started, joined [3]bool
	releases := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var releaseOnce [3]sync.Once
	release := func(i int) { releaseOnce[i].Do(func() { close(releases[i]) }) }
	restore := func() {}
	// Register before installing any blocking observer. Always release, join,
	// restore exactly once and check original transport/owner shutdown.
	defer func() {
		for i := range releases {
			release(i)
		}
		for i := range results {
			if !started[i] || joined[i] {
				continue
			}
			select {
			case got := <-results[i]:
				joined[i] = true
				if got.err != nil {
					t.Errorf("cleanup request%d: %v", i, got.err)
				}
			case <-time.After(wait):
				cancelContext()
				t.Errorf("cleanup request%d missing bounded terminal", i)
			}
		}
		restore()
		if active {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, original)
		}
		for i := range results {
			if started[i] && !joined[i] {
				select {
				case got := <-results[i]:
					t.Logf("request%d terminal after cancellation/transport Close: %v", i, got.err)
				case <-time.After(wait):
					t.Errorf("request%d still missing terminal after transport Close", i)
				}
			}
		}
	}()
	if err := client.Database("epochs").CreateCollection(ctx, "rows"); err != nil {
		t.Fatal(err)
	}
	if !s.Server.canShareStandaloneJournalBoundary("update") {
		t.Fatal("existing collection must authenticate original shared native completion owner")
	}
	if s.Server.UpdateCoalescingMaxBatch < 2 || s.Server.UpdateCoalescingMaxDelay < 0 {
		t.Fatal("product default update coalescer unavailable")
	}
	journal := true
	plain := client.Database("epochs").Collection("rows", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1}))
	strict := client.Database("epochs").Collection("rows", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
	for i, id := range ids {
		if _, err := plain.InsertOne(ctx, standaloneNativeJournalDocument(id, i+1)); err != nil {
			t.Fatalf("preload binary BSON row%d: %v", i, err)
		}
	}
	before := s.Stats()
	beforeACK := s.Server.StandaloneWriteConcernStats()
	type sample struct {
		lsn, sequence, attempts uint64
		root, path              string
	}
	type pair struct{ before, after sample }
	var observationsMu sync.Mutex
	var observations [8]pair
	observed := 0
	var sequence, explicitAfter atomic.Uint64
	var appendCount int
	var appends [3]sample
	var targetPublicationLSN, capturedPrefixLSN atomic.Uint64
	var targetHeld atomic.Bool
	predecessorAppended := make(chan sample, 1)
	targetEntered := make(chan sample, 1)
	undo := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.AfterDependencyAppend && e.LSN != 0 {
			v := sample{lsn: e.LSN, root: e.Root, path: e.Path, sequence: sequence.Add(1), attempts: s.Server.writeConcernStats.syncAttempts.Load() - beforeACK.SyncAttempts}
			observationsMu.Lock()
			i := appendCount
			if i >= len(appends) {
				observationsMu.Unlock()
				return errors.New("unexpected additional update publication append")
			}
			// A relaxed tail requires exactly one root-neutral durable-prefix
			// promotion append before capture. It is not an update publication.
			// Decode and verify its actual V2 identity after requests join below.
			if i == 2 && (profile != treedb.ProfileCommandWALRelaxed || v.attempts == 0 || v.attempts > 2 || v.lsn != appends[1].lsn+1 || v.root != appends[1].root || v.path != appends[1].path) {
				observationsMu.Unlock()
				return errors.New("unexpected update promotion append identity")
			}
			appends[i] = v
			appendCount++
			observationsMu.Unlock()
			if i == 0 {
				predecessorAppended <- v
				select {
				case <-releases[0]:
				case <-ctx.Done():
					return fmt.Errorf("bounded predecessor append hold: %w", ctx.Err())
				}
			} else if i == 1 {
				targetPublicationLSN.Store(v.lsn)
				if profile == treedb.ProfileCommandWALDurable {
					capturedPrefixLSN.Store(v.lsn)
				}
			} else {
				capturedPrefixLSN.Store(v.lsn)
			}
			return nil
		}
		if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 || (e.Point != durabilitycut.BeforeDependencyFileSync && e.Point != durabilitycut.AfterDependencyFileSync) {
			return nil
		}
		if e.StablePath == nil {
			return errors.New("characterization active-file sync missing retained handle")
		}
		// No Stats, storage calls or borrowed handle retention in the observer.
		v := sample{lsn: e.LSN, root: e.Root, path: e.Path, sequence: sequence.Add(1), attempts: s.Server.writeConcernStats.syncAttempts.Load() - beforeACK.SyncAttempts}
		observationsMu.Lock()
		if e.Point == durabilitycut.BeforeDependencyFileSync {
			if observed == len(observations) {
				observationsMu.Unlock()
				return errors.New("unexpected characterization sync observation overflow")
			}
			i := observed
			observations[i].before = v
			observed++
			observationsMu.Unlock()
			// Hold the target ordinary physical completion. Later explicit syncs
			// are observed without blocking so the baseline can finish and drain.
			if v.lsn == capturedPrefixLSN.Load() && targetHeld.CompareAndSwap(false, true) {
				targetEntered <- v
				select {
				case <-releases[1]:
				case <-ctx.Done():
					return fmt.Errorf("bounded target physical sync: %w", ctx.Err())
				}
			}
			return nil
		}
		if observed == 0 {
			observationsMu.Unlock()
			return errors.New("physical After without observed Before")
		}
		p := &observations[observed-1]
		if p.after.sequence != 0 || p.before.lsn != v.lsn || p.before.root != v.root || p.before.path != v.path {
			observationsMu.Unlock()
			return errors.New("physical Before/After identity mismatch")
		}
		p.after = v
		explicitAfter.Store(v.sequence)
		observationsMu.Unlock()
		return nil
	})
	var restoreOnce sync.Once
	restore = func() { restoreOnce.Do(undo) }
	start := func(i int) {
		started[i] = true
		go func() {
			collection := strict
			if i == 0 {
				collection = plain
			}
			set := standaloneNativeJournalDocument(ids[i], i+11)[1:]
			r, err := collection.UpdateOne(ctx, bson.D{{Key: "_id", Value: ids[i]}}, bson.D{{Key: "$set", Value: set}})
			got := result{err: err, after: explicitAfter.Load(), returned: sequence.Add(1)}
			if r != nil {
				got.matched, got.modified = r.MatchedCount, r.ModifiedCount
			}
			results[i] <- got
		}()
	}
	noACK := func(i int, boundary string) {
		t.Helper()
		select {
		case got := <-results[i]:
			joined[i] = true
			t.Fatalf("request%d returned before %s: matched=%d modified=%d err=%v", i, boundary, got.matched, got.modified, got.err)
		default:
		}
	}
	poll := func(label string, ready func() bool) {
		t.Helper()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		deadline := time.NewTimer(wait)
		defer deadline.Stop()
		for !ready() {
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatalf("bounded state observation failed: %s", label)
			}
		}
	}
	start(0)
	var first sample
	select {
	case first = <-predecessorAppended:
	case <-time.After(wait):
		t.Fatal("missing real predecessor WAL append hold")
	}
	if first.attempts != 0 {
		t.Fatalf("plain predecessor publication attempts=%d", first.attempts)
	}
	start(1)
	start(2)
	poll("two accepted journal requests queued behind occupied original update worker", func() bool {
		noACK(0, "predecessor publication hold")
		noACK(1, "target physical sync")
		noACK(2, "target physical sync")
		s.Server.updateMu.Lock()
		c := s.Server.updateCoalescers["epochs.rows"]
		queued := 0
		if c != nil {
			queued = len(c.requests)
		}
		s.Server.updateMu.Unlock()
		return queued == 2 && s.Server.writeConcernStats.journalRequests.Load()-beforeACK.JournalRequests == 2
	})
	t.Log("ACTUAL_SAME_COLLECTION_BSON_SET_QUEUE_PASS queued=2 distinct_ids=2 accepted_journal_requests=2")
	release(0)
	var batch sample
	select {
	case batch = <-targetEntered:
	case <-time.After(wait):
		t.Fatal("missing actual target physical sync")
	}
	if batch.lsn <= first.lsn || batch.lsn != capturedPrefixLSN.Load() || (profile == treedb.ProfileCommandWALRelaxed && (batch.attempts == 0 || batch.attempts > 2)) {
		t.Fatalf("profile-specific target physical identity: profile=%s first=%+v target=%+v", profile, first, batch)
	}
	observationsMu.Lock()
	actualAppends, targetAppend, promotionAppend := appendCount, appends[1], appends[2]
	observationsMu.Unlock()
	expectedAppends := 2
	if profile == treedb.ProfileCommandWALRelaxed {
		expectedAppends = 3
		if promotionAppend.lsn != targetAppend.lsn+1 || promotionAppend.lsn != batch.lsn || promotionAppend.sequence <= targetAppend.sequence || promotionAppend.sequence >= batch.sequence || promotionAppend.attempts == 0 || promotionAppend.attempts > 2 || promotionAppend.root != targetAppend.root || promotionAppend.path != targetAppend.path {
			t.Fatalf("relaxed prefix promotion chronology: target=%+v promotion=%+v sync=%+v", targetAppend, promotionAppend, batch)
		}
	} else if targetAppend.lsn != batch.lsn {
		t.Fatalf("durable capture must name actual target publication: target=%+v sync=%+v", targetAppend, batch)
	}
	if actualAppends != expectedAppends || targetAppend.lsn != targetPublicationLSN.Load() || targetAppend.lsn != first.lsn+1 || targetAppend.sequence >= batch.sequence || targetAppend.root != first.root || targetAppend.path != first.path || batch.root != targetAppend.root || batch.path != targetAppend.path {
		t.Fatalf("exactly two native publications plus profile-specific promotion: appends=%d target=%+v sync=%+v", actualAppends, targetAppend, batch)
	}
	for i := 1; i < len(ids); i++ {
		noACK(i, "target physical sync")
		readCtx, readCancel := context.WithTimeout(ctx, time.Second)
		var raw bson.Raw
		err := plain.FindOne(readCtx, bson.D{{Key: "_id", Value: ids[i]}}).Decode(&raw)
		readCancel()
		if err != nil {
			t.Fatalf("installed batch document%d: %v", i, err)
		}
		requireStandaloneNativeJournalDocument(t, raw, ids[i], i+11)
	}
	delta := func(stats map[string]string, key string) uint64 { return writeConcernBenchmarkStat(t, stats, key) - writeConcernBenchmarkStat(t, before, key) }
	if delta(s.Stats(), "treedb.command_wal.group_commit.forced_total") != 0 {
		t.Fatal("target forced before ordinary physical completion")
	}
	t.Logf("TARGET_BSON_SET_INSTALLED_ORDINARY_HELD_PASS target_lsn=%d members=2", batch.lsn)
	release(1)
	release(2)
	var acknowledgements [3]result
	for i := range results {
		select {
		case got := <-results[i]:
			joined[i] = true
			acknowledgements[i] = got
			if got.err != nil || got.matched != 1 || got.modified != 1 || (i > 0 && (got.after <= batch.sequence || got.returned <= got.after)) {
				t.Fatalf("request%d actual ACK matched=%d modified=%d err=%v after=%d returned=%d batch_before=%d", i, got.matched, got.modified, got.err, got.after, got.returned, batch.sequence)
			}
			t.Logf("ACTUAL_BSON_SET_DRIVER_ACK_PASS request=%d matched=1 modified=1 last_physical_after_sequence=%d driver_return_sequence=%d", i, got.after, got.returned)
		case <-time.After(wait):
			t.Fatalf("request%d missing bounded terminal", i)
		}
	}
	restore()
	observationsMu.Lock()
	pairs := append([]pair(nil), observations[:observed]...)
	observationsMu.Unlock()
	if len(pairs) < 2 {
		t.Fatalf("missing predecessor or target physical epoch: %d", len(pairs))
	}
	ordinaryDone := pairs[1].after
	if ordinaryDone.lsn != batch.lsn || ordinaryDone.sequence <= batch.sequence {
		t.Fatalf("target ordinary successful After identity: %+v", ordinaryDone)
	}
	for i := 1; i < len(acknowledgements); i++ {
		got := acknowledgements[i]
		if got.after < ordinaryDone.sequence || got.returned <= ordinaryDone.sequence {
			t.Fatalf("request%d ACK preceded actual target completion: after=%d returned=%d target_after=%d", i, got.after, got.returned, ordinaryDone.sequence)
		}
	}
	for i, p := range pairs {
		if p.after.sequence <= p.before.sequence || (i >= 2 && (p.before.lsn != batch.lsn || p.before.sequence <= ordinaryDone.sequence)) {
			t.Fatalf("physical epoch%d chronology=%+v", i, p)
		}
		t.Logf("BSON_SET_PHYSICAL_EPOCH_PASS epoch=%d lsn=%d before=%d successful_after=%d", i, p.before.lsn, p.before.sequence, p.after.sequence)
	}
	t.Logf("TARGET_PHYSICAL_AFTER_BEFORE_ACK_PASS target_lsn=%d target_after=%d", batch.lsn, ordinaryDone.sequence)
	observationsMu.Lock()
	finalAppends := appendCount
	appendSamples := appends
	observationsMu.Unlock()
	if finalAppends != expectedAppends {
		t.Fatalf("expected two publications plus exact profile-specific promotion, actual=%d want=%d", finalAppends, expectedAppends)
	}
	// All RPCs have joined and the observer is restored. Inspect the exact
	// active file named by Before/After; never perform storage IO in a hook.
	// The bounded fresh fixture has no rotation or maintenance in this window.
	// A decoder-verified empty durable marker immediately after the target,
	// not merely an LSN >= target, proves which physical prefix covers it.
	reader, err := commitlog.NewReaderWithOptions(batch.path, commitlog.Options{MaxSegmentSize: 1 << 20})
	if err != nil {
		t.Fatalf("open exact observed command WAL: %v", err)
	}
	readerClosed := false
	defer func() {
		if !readerClosed {
			if err := reader.Close(); err != nil {
				t.Errorf("checked command WAL reader Close: %v", err)
			}
		}
	}()
	frames := 0
	for scanned := 0; ; scanned++ {
		if scanned == 64 {
			t.Fatal("bounded fresh-fixture command WAL frame census exceeded")
		}
		frame, readErr := reader.ReadCommandFrameV2()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatalf("decode exact observed V2 command frame: %v", readErr)
		}
		if frame.LSN < first.lsn {
			continue // Collection creation and three preloads precede the hook.
		}
		if frames == finalAppends || frame.Version != commitlog.CommandFrameVersionV2 || frame.LSN != appendSamples[frames].lsn {
			t.Fatalf("exact observed frame/append census: index=%d lsn=%d kind=%d", frames, frame.LSN, frame.Kind)
		}
		if frames == 2 {
			if profile != treedb.ProfileCommandWALRelaxed || frame.LSN != targetAppend.lsn+1 || frame.LSN != batch.lsn || frame.Kind != commitlog.CommandKindDurablePrefixBarrier || frame.Scope != commitlog.CommandScopeSystem || frame.PayloadFormat != commitlog.PayloadFormatDurablePrefixBarrierV1 || frame.DurabilityClass != commitlog.CommandDurabilityDurable || len(frame.Payload) != 0 || len(frame.ExternalRefs) != 0 || len(frame.Preconditions) != 0 || len(frame.ResultAssertions) != 0 {
				t.Fatalf("actual relaxed promotion must be exact empty durable system barrier: lsn=%d kind=%d scope=%d format=%d class=%d", frame.LSN, frame.Kind, frame.Scope, frame.PayloadFormat, frame.DurabilityClass)
			}
			t.Logf("RELAXED_DURABLE_PREFIX_PROMOTION_PASS target_publication_lsn=%d captured_prefix_lsn=%d marker_kind=%d empty_payload=true", targetAppend.lsn, frame.LSN, frame.Kind)
		} else {
			class := commitlog.CommandDurabilityRelaxed
			if profile == treedb.ProfileCommandWALDurable {
				class = commitlog.CommandDurabilityDurable
			}
			if frame.Kind != commitlog.CommandKindCollectionUpdateBatchByID || frame.Scope != commitlog.CommandScopeCollection || frame.PayloadFormat != commitlog.PayloadFormatCollectionUpdateBatchByIDV1 || frame.DurabilityClass != class {
				t.Fatalf("actual update frame identity: lsn=%d kind=%d scope=%d format=%d class=%d", frame.LSN, frame.Kind, frame.Scope, frame.PayloadFormat, frame.DurabilityClass)
			}
			payload, decodeErr := commitlog.DecodeCollectionUpdateBatchByIDPayload(frame.Payload)
			wantMembers := 1
			if frames == 1 {
				wantMembers = 2
			}
			if decodeErr != nil || payload.Collection != "epochs.rows" || payload.Colocated != nil || len(payload.Documents) != wantMembers {
				t.Fatalf("actual update action census: frame=%d collection=%q members=%d err=%v", frames, payload.Collection, len(payload.Documents), decodeErr)
			}
			seen := make(map[string]bool, wantMembers)
			for _, document := range payload.Documents {
				raw := bson.Raw(document.Document)
				id, ok := raw.Lookup("_id").StringValueOK()
				matched := false
				for i, want := range ids {
					if !ok || id != want || seen[id] || (frames == 0 && i != 0) || (frames == 1 && i == 0) {
						continue
					}
					key, keyErr := encodePrimaryKey(raw.Lookup("_id"))
					if keyErr != nil || !bytes.Equal(key, document.ID) {
						t.Fatalf("actual WAL action ID must equal BSON primary key: id=%q err=%v", id, keyErr)
					}
					requireStandaloneNativeJournalDocument(t, raw, id, i+11)
					seen[id], matched = true, true
					break
				}
				if !matched {
					t.Fatalf("unexpected or duplicate actual WAL update action: frame=%d id=%q", frames, id)
				}
			}
		}
		frames++
	}
	readerClosed = true
	if err := reader.Close(); err != nil {
		t.Fatalf("checked exact command WAL reader Close: %v", err)
	}
	if frames != finalAppends {
		t.Fatalf("missing exact observed WAL frames: got=%d want=%d", frames, finalAppends)
	}
	t.Logf("BSON_SET_EXACT_WAL_ACTION_CENSUS_PASS frames=%d native_publications=2 update_actions=3 target_members=2 promotion_markers=%d", frames, frames-2)
	t.Logf("BSON_SET_SHARED_PUBLICATION_PASS publications=2 target_members=2 target_lsn=%d captured_prefix_lsn=%d ordinary_epochs=%d physical_epochs=%d", targetAppend.lsn, batch.lsn, 2, len(pairs))
	after := s.Stats()
	afterACK := s.Server.StandaloneWriteConcernStats()
	if delta(after, "treedb.cache.checkpoint.runs") != 0 || delta(after, "treedb.command_wal.group_commit.commits_total") != 2 || delta(after, "treedb.command_wal.group_commit.syncs_total") != uint64(len(pairs)) || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
		t.Fatalf("unexpected owner counters: commits=%d groups=%d checkpoints=%d ACKs=%+v->%+v", delta(after, "treedb.command_wal.group_commit.commits_total"), delta(after, "treedb.command_wal.group_commit.syncs_total"), delta(after, "treedb.cache.checkpoint.runs"), beforeACK, afterACK)
	}
	check := func() {
		t.Helper()
		cursor, err := client.Database("epochs").Collection("rows").Find(ctx, bson.D{})
		if err != nil {
			t.Fatal(err)
		}
		var docs []bson.Raw
		if err := cursor.All(ctx, &docs); err != nil || len(docs) != len(ids) {
			t.Fatalf("exact BSON census count=%d err=%v", len(docs), err)
		}
		seen := make(map[string]bool, len(ids))
		for _, raw := range docs {
			id, ok := raw.Lookup("_id").StringValueOK()
			matched := false
			for i, want := range ids {
				if id == want && ok && !seen[id] {
					requireStandaloneNativeJournalDocument(t, raw, id, i+11)
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("unexpected/duplicate exact BSON census id=%q", id)
			}
			seen[id] = true
		}
	}
	check()
	active = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, original)
	t.Log("BSON_SET_ORIGINAL_EXACT_BSON_CHECKED_CLOSE_PASS")
	s, err = OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	fresh := s
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Errorf("checked fresh fallback Close: %v", err)
		}
	})
	requireStandaloneNativeJournalOwner(t, fresh)
	client, cancel, listener, served = startStandaloneMongoClientForTest(t, fresh)
	freshActive := true
	defer func() {
		if freshActive {
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, fresh)
		}
	}()
	check()
	freshActive = false
	stopStandaloneMongoClientForTest(t, client, cancel, listener, served, fresh)
	t.Log("BSON_SET_FRESH_EXACT_BSON_READBACK_AND_CHECKED_CLOSE_PASS")
	forced := delta(after, "treedb.command_wal.group_commit.forced_total")
	if len(pairs) != 2 || forced != 0 {
		t.Errorf("BSON_SET_REDUNDANT_POSTDISPATCH_FORCE: actual_epochs=%d want2 original_force_registrations=%d want0 target_lsn=%d", len(pairs), forced, batch.lsn)
	}
}
