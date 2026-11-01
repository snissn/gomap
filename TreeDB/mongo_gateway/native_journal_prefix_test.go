package mongogateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

func checkNativeJournalPartialEffects(t *testing.T, s *StandaloneServer) {
	t.Helper()
	requireStandaloneNativeJournalOwner(t, s)
	if !writeConcernMarker(t, s.Server, "u1") || writeConcernMarker(t, s.Server, "u2") {
		t.Fatal("partial update did not preserve exact successful/failed members")
	}
	c, err := s.Collections.OpenCollection("app.users")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, index := range c.Meta().Indexes {
		names[index.Name] = true
	}
	if !names["age_1"] || names["email_1"] {
		t.Fatalf("partial DDL index state=%v", names)
	}
}

// Real native standalone dispatch retains the successful DDL/update members,
// catalog auto-create and auth raw writes across immediate process exit.
func TestStandaloneNativeJournalPartialDDLAndAuthCrashReopen(t *testing.T) {
	const dirEnv = "GOMAP_NATIVE_JOURNAL_PARTIAL_DIR"
	const profileEnv = "GOMAP_NATIVE_JOURNAL_PARTIAL_PROFILE"
	const marker = "NATIVE PARTIAL JOURNAL ACK COMPLETE"
	childDir := os.Getenv(dirEnv)
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		if childDir != "" && string(profile) != os.Getenv(profileEnv) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := childDir
			if dir == "" {
				dir = t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestStandaloneNativeJournalPartialDDLAndAuthCrashReopen$", "-test.v")
				cmd.Env = append(os.Environ(), dirEnv+"="+dir, profileEnv+"="+string(profile))
				output, err := cmd.CombinedOutput()
				t.Logf("native partial/auth crash child: %s", output)
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != 97 || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("native partial child: %v", err)
				}
			}
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: profile, AuthenticationEnabled: true, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			requireStandaloneNativeJournalOwner(t, s)
			if childDir != "" {
				// Auth's public raw owner is shared with native semantic state. Keep the
				// wire command fixture unauthenticated while exercising the same catalog.
				s.Server.AuthenticationEnabled = false
				if err := s.Server.AuthCatalog.UpsertPassword("admin", "keep", []byte("before")); err != nil {
					t.Fatal(err)
				}
				if err := s.Server.AuthCatalog.UpsertPassword("admin", "keep", []byte("after")); err != nil {
					t.Fatal(err)
				}
				if err := s.Server.AuthCatalog.UpsertPassword("app", "drop", []byte("deleted")); err != nil {
					t.Fatal(err)
				}
				if err := s.Server.AuthCatalog.DropUser("app", "drop"); err != nil {
					t.Fatal(err)
				}
				seedWriteConcernDuplicateEmails(t, s.Server, false)
				before := s.Stats()
				beforeACK := s.Server.StandaloneWriteConcernStats()
				ddl := serveCommand(t, s.Server, 2, writeConcernPartialCreateIndexesCommand())
				if mongoCommandResponseOK(ddl) || bson.Raw(ddl).Lookup("writeConcernError").Type != 0 {
					t.Fatalf("partial createIndexes response=%s", ddl)
				}
				update := serveCommand(t, s.Server, 3, writeConcernPartialUpdateCommand())
				if !mongoCommandResponseOK(update) || bson.Raw(update).Lookup("writeErrors").Type != bson.TypeArray || bson.Raw(update).Lookup("writeConcernError").Type != 0 {
					t.Fatalf("partial update response=%s", update)
				}
				after := s.Stats()
				afterACK := s.Server.StandaloneWriteConcernStats()
				if delta := writeConcernBenchmarkStat(t, after, "treedb.cache.checkpoint.runs") - writeConcernBenchmarkStat(t, before, "treedb.cache.checkpoint.runs"); delta != 0 {
					t.Fatalf("native partial commands checkpointed: %d", delta)
				}
				if delta := writeConcernBenchmarkStat(t, after, "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total"); delta != 2 {
					t.Fatalf("partial native forced groups=%d want2", delta)
				}
				if afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 {
					t.Fatalf("accepted error responses did not fence: before=%+v after=%+v", beforeACK, afterACK)
				}
			} else {
				s.Server.AuthenticationEnabled = false
			}
			checkNativeJournalPartialEffects(t, s)
			if _, err := s.Server.AuthCatalog.VerifyPassword("admin", "keep", []byte("after")); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Server.AuthCatalog.VerifyPassword("admin", "keep", []byte("before")); err == nil {
				t.Fatal("old password recovered")
			}
			if _, err := s.Server.AuthCatalog.VerifyPassword("app", "drop", []byte("deleted")); err == nil {
				t.Fatal("dropped user recovered")
			}
			if childDir != "" {
				fmt.Println(marker)
				os.Exit(97)
			}
		})
	}
}

// This retains the existing injected-callback test as a separate control and
// fails the actual captured native journal file sync after dispatch instead.
func TestStandaloneNativeJournalPartialErrorRealFenceFailureIsUncertain(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			requireStandaloneNativeJournalOwner(t, s)
			seedWriteConcernDuplicateEmails(t, s.Server, false)
			before := s.Stats()
			beforeACK := s.Server.StandaloneWriteConcernStats()
			failure := errors.New("actual native explicit journal file sync failure")
			defer func() {
				err := s.Close()
				if err != nil && !errors.Is(err, failure) && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Errorf("original poisoned-owner Close: %v", err)
				} else {
					t.Logf("checked original poisoned-owner Close: %v", err)
				}
			}()
			syncs := 0
			failAt := 1
			if profile == treedb.ProfileCommandWALDurable {
				failAt = 2
			} // ordinary durable completion precedes the explicit fence.
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
					syncs++
					if syncs == failAt {
						if e.StablePath == nil {
							t.Error("missing retained command file")
						}
						return failure
					}
				}
				return nil
			})
			response := serveCommand(t, s.Server, 2, writeConcernPartialUpdateCommand())
			restore()
			raw := bson.Raw(response)
			wc, ok := raw.Lookup("writeConcernError").DocumentOK()
			if !mongoCommandResponseOK(response) || raw.Lookup("writeErrors").Type != bson.TypeArray || !ok || wc.Lookup("codeName").StringValue() != "WriteConcernFailed" {
				t.Fatalf("lost original partial response/uncertainty: %s", response)
			}
			after := s.Stats()
			afterACK := s.Server.StandaloneWriteConcernStats()
			if syncs != failAt || afterACK.SyncFailures-beforeACK.SyncFailures != 1 || afterACK.UncertainOutcomes-beforeACK.UncertainOutcomes != 1 || afterACK.JournalAcknowledgements != beforeACK.JournalAcknowledgements || afterACK.PhysicalSyncBoundaries != beforeACK.PhysicalSyncBoundaries {
				t.Fatalf("actual failure syncs=%d want%d before=%+v after=%+v", syncs, failAt, beforeACK, afterACK)
			}
			if delta := writeConcernBenchmarkStat(t, after, "treedb.cache.checkpoint.runs") - writeConcernBenchmarkStat(t, before, "treedb.cache.checkpoint.runs"); delta != 0 {
				t.Fatalf("failure fell back to checkpoint: %d", delta)
			}
			if delta := writeConcernBenchmarkStat(t, after, "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total"); delta != 1 {
				t.Fatalf("actual explicit fence activation=%d want1", delta)
			}
			if !writeConcernMarker(t, s.Server, "u1") || writeConcernMarker(t, s.Server, "u2") {
				t.Fatal("uncertainty lost partial effects")
			}
			next := s.Backend.CommandWALNextLSN()
			if physical, err := s.Collections.SyncForStandaloneWriteConcern(); err == nil || physical {
				t.Fatalf("fence after poison=%t/%v", physical, err)
			}
			if s.Backend.CommandWALNextLSN() != next {
				t.Fatal("poison fence retried canonical command")
			}
		})
	}
}

// Exercise general original-owner shared entry with two independently enrolled
// native publications. Explicitly disable gateway coalescing: a default worker
// waits for its own publication before dequeuing its same-collection successor.
// Default queued batching and its held-receipt lifetime have separate controls.
func TestStandaloneNativeSharedBoundaryHeldSyncExactACK(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			const wait = 5 * time.Second
			opts := StandaloneOptions{Dir: t.TempDir(), Profile: profile, InsertCoalescingMaxBatchSet: true, InsertCoalescingMaxBatch: 0, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
			standalone, err := OpenStandaloneServer(opts)
			if err != nil {
				t.Fatalf("open original native owner: %v", err)
			}
			originalOwner := standalone
			t.Cleanup(func() {
				if err := originalOwner.Close(); err != nil {
					t.Errorf("checked original owner cleanup: %v", err)
				}
			})
			requireStandaloneNativeJournalOwner(t, standalone)
			if standalone.Server.Collections != standalone.Collections || standalone.Server.standaloneWriteConcernSync != nil {
				t.Fatal("control requires original native manager and real sync, without custom callback")
			}
			client, cancel, listener, served := startStandaloneMongoClientForTest(t, standalone)
			active := true
			ctx, cancelContext := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancelContext()
			var secondClient *mongo.Client
			type commandResult struct {
				id                           any
				err                          error
				publicationAfterBeforeReturn bool
				returnSequence               uint64
			}
			results := [2]chan commandResult{make(chan commandResult, 1), make(chan commandResult, 1)}
			var started, joined [2]bool
			released := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			var releaseOnce [2]sync.Once
			release := func(i int) { releaseOnce[i].Do(func() { close(released[i]) }) }
			restore := func() {}
			// Unblock every observer before joining requests or closing the server,
			// including the expected baseline failure path. This cleanup is installed
			// before any operation can enter the blocking observer.
			defer func() {
				release(0)
				release(1)
				for i := range results {
					if !started[i] || joined[i] {
						continue
					}
					select {
					case result := <-results[i]:
						joined[i] = true
						if result.err != nil {
							t.Errorf("cleanup command%d: %v", i, result.err)
						}
					case <-time.After(wait):
						cancelContext()
						t.Errorf("cleanup command%d did not join after both syncs released", i)
					}
				}
				restore()
				if secondClient != nil {
					disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), wait)
					if err := secondClient.Disconnect(disconnectCtx); err != nil {
						t.Errorf("checked second driver disconnect: %v", err)
					}
					disconnectCancel()
				}
				if active {
					stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
				}
				// If the initial join exceeded its bound, cancellation/transport Close
				// must still produce an observed terminal for every started writer.
				for i := range results {
					if !started[i] || joined[i] {
						continue
					}
					select {
					case result := <-results[i]:
						joined[i] = true
						t.Logf("cleanup command%d terminal after cancellation: %v", i, result.err)
					case <-time.After(wait):
						t.Errorf("cleanup command%d still lacks terminal after transport Close", i)
					}
				}
			}()
			if err := client.Database("app").CreateCollection(ctx, "shared_journal"); err != nil {
				t.Fatalf("precreate BSON collection: %v", err)
			}
			// A separate driver client guarantees that the second writer has its own
			// connection. The first client's pool also serves independent readback.
			secondClient, err = mongo.Connect(options.Client().ApplyURI("mongodb://" + listener.Addr().String()).SetDirect(true).SetMaxPoolSize(1).SetServerSelectionTimeout(time.Second))
			if err != nil {
				t.Fatalf("connect second official driver: %v", err)
			}
			if err := secondClient.Ping(ctx, nil); err != nil {
				t.Fatalf("ping second official driver: %v", err)
			}
			journal := true
			concern := options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal})
			writers := [2]*mongo.Collection{
				client.Database("app").Collection("shared_journal", concern),
				secondClient.Database("app").Collection("shared_journal", concern),
			}
			ids := [2]string{"predecessor", "successor"}
			before := standalone.Stats()
			beforeACK := standalone.Server.StandaloneWriteConcernStats()
			type heldSync struct {
				lsn      uint64
				root     string
				path     string
				sequence uint64
				attempts uint64
			}
			entered := [2]chan heldSync{make(chan heldSync, 1), make(chan heldSync, 1)}
			publicationBefore := make(chan heldSync, 1)
			publicationAfter := make(chan heldSync, 1)
			var firstLSN atomic.Uint64
			var laterClaimed atomic.Bool
			var eventSequence atomic.Uint64
			var publicationTarget atomic.Pointer[heldSync]
			var publicationAfterSequence atomic.Uint64
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 || (e.Point != durabilitycut.BeforeDependencyFileSync && e.Point != durabilitycut.AfterDependencyFileSync) {
					return nil
				}
				// Scalar atomic access only: no Stats, storage calls or borrowed-handle
				// retention. Per-RPC attempt accounting arrives after the native wait;
				// identify the operation with its own actual captured LSN.
				sample := heldSync{lsn: e.LSN, root: e.Root, path: e.Path, sequence: eventSequence.Add(1), attempts: standalone.Server.writeConcernStats.syncAttempts.Load()}
				if e.Point == durabilitycut.AfterDependencyFileSync {
					target := publicationTarget.Load()
					if target != nil && sample.lsn == target.lsn && sample.root == target.root && sample.path == target.path {
						if e.StablePath == nil {
							return errors.New("successor publication After sync lacks retained original file")
						}
						if !publicationAfterSequence.CompareAndSwap(0, sample.sequence) {
							return errors.New("duplicate successor publication After sync")
						}
						select {
						case publicationAfter <- sample:
						default:
							return errors.New("successor publication After observation overflow")
						}
					}
					return nil
				}
				// Observe the successor's actual own-publication Before and After.
				// Neither registration nor aggregate counts certify physical completion.
				if sample.lsn > firstLSN.Load() && firstLSN.Load() != 0 {
					if e.StablePath == nil {
						return errors.New("successor publication Before sync lacks retained original file")
					}
					if !publicationTarget.CompareAndSwap(nil, &sample) {
						return errors.New("duplicate successor publication Before sync")
					}
					select {
					case publicationBefore <- sample:
					default:
						return errors.New("successor publication Before observation overflow")
					}
				}
				if firstLSN.Load() == 0 {
					if !firstLSN.CompareAndSwap(0, e.LSN) {
						return nil
					}
					if e.StablePath == nil {
						return errors.New("first publication sync lacks retained original file")
					}
					entered[0] <- sample
					<-released[0]
				} else if e.LSN > firstLSN.Load() && laterClaimed.CompareAndSwap(false, true) {
					if e.StablePath == nil {
						return errors.New("later captured sync lacks retained original file")
					}
					entered[1] <- sample
					<-released[1]
				}
				return nil
			})
			var restoreOnce sync.Once
			restore = func() { restoreOnce.Do(undo) }
			start := func(i int) {
				t.Helper()
				started[i] = true
				go func() {
					result, err := writers[i].InsertOne(ctx, standaloneNativeJournalDocument(ids[i], i+1))
					out := commandResult{err: err}
					if i == 1 {
						// Sample immediately after the actual driver return, independently of
						// when the main test receives its buffered result.
						out.publicationAfterBeforeReturn = publicationAfterSequence.Load() != 0
						out.returnSequence = eventSequence.Add(1)
					}
					if result != nil {
						out.id = result.InsertedID
					}
					results[i] <- out
				}()
			}
			requireNoACK := func(i int, boundary string) {
				t.Helper()
				select {
				case result := <-results[i]:
					joined[i] = true
					t.Fatalf("command%d completed before %s: id=%v err=%v", i, boundary, result.id, result.err)
				default:
				}
			}
			var successorReturnSequence uint64
			joinACK := func(i int) {
				t.Helper()
				select {
				case result := <-results[i]:
					joined[i] = true
					if result.err != nil || result.id != ids[i] {
						t.Fatalf("command%d journal ACK id=%v want=%s err=%v", i, result.id, ids[i], result.err)
					}
					if i == 1 && (!result.publicationAfterBeforeReturn || result.returnSequence <= publicationAfterSequence.Load()) {
						t.Fatalf("successor driver returned before successful own-publication physical sync: observed=%t return_sequence=%d after_sequence=%d", result.publicationAfterBeforeReturn, result.returnSequence, publicationAfterSequence.Load())
					}
					if i == 1 {
						successorReturnSequence = result.returnSequence
					}
				case <-time.After(wait):
					t.Fatalf("command%d did not ACK after its own sync completed", i)
				}
			}
			read := func(id string) (bson.Raw, error) {
				readCtx, readCancel := context.WithTimeout(ctx, time.Second)
				defer readCancel()
				var raw bson.Raw
				err := client.Database("app").Collection("shared_journal").FindOne(readCtx, bson.D{{Key: "_id", Value: id}}).Decode(&raw)
				return raw, err
			}
			start(0)
			var first heldSync
			select {
			case first = <-entered[0]:
			case <-time.After(wait):
				t.Fatal("setup: first native journal publication did not reach real active-file sync")
			}
			firstHeld := standalone.Stats()
			forcedBefore := writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total")
			if got := writeConcernBenchmarkStat(t, firstHeld, "treedb.command_wal.group_commit.forced_total"); got != forcedBefore {
				t.Fatalf("setup: held sync did not select original ordinary group: before=%d held=%d", forcedBefore, got)
			}
			if raw, err := read(ids[0]); err != nil {
				t.Fatalf("setup: predecessor not publicly readable before sync: %v", err)
			} else {
				requireStandaloneNativeJournalDocument(t, raw, ids[0], 1)
			}
			requireNoACK(0, "predecessor physical sync")
			t.Logf("SETUP_PASS profile=%s first_publication_captured_lsn=%d retained_path=%s original_forced_delta=0", profile, first.lsn, first.path)
			start(1)
			// JournalRequests increments after concern validation but before the
			// boundary lock. Observe the second actual accepted wire request so an
			// unscheduled RPC cannot masquerade as the intended installation RED.
			acceptedTick := time.NewTicker(10 * time.Millisecond)
			defer acceptedTick.Stop()
			acceptedDeadline := time.NewTimer(wait)
			defer acceptedDeadline.Stop()
			for standalone.Server.writeConcernStats.journalRequests.Load() != beforeACK.JournalRequests+2 {
				requireNoACK(0, "predecessor physical sync")
				requireNoACK(1, "accepted second journal boundary")
				select {
				case <-acceptedTick.C:
				case <-acceptedDeadline.C:
					t.Fatal("setup: second real RPC did not enter accepted journal boundary before lock")
				}
			}
			acceptedTick.Stop()
			acceptedDeadline.Stop()
			t.Logf("SECOND_ACCEPTED_BOUNDARY_PASS journal_requests_delta=2 publication_sync_attempts_delta=%d first_lsn=%d", standalone.Server.writeConcernStats.syncAttempts.Load()-beforeACK.SyncAttempts, first.lsn)
			installed := false
			poll := time.NewTicker(10 * time.Millisecond)
			defer poll.Stop()
			installDeadline := time.NewTimer(2 * time.Second)
			defer installDeadline.Stop()
			// This bounded public-state poll is the causal discriminator; time is
			// not a substitute for a capture/installation synchronization event.
		pollInstall:
			for {
				requireNoACK(0, "predecessor physical sync")
				requireNoACK(1, "successor's own captured physical sync")
				raw, err := read(ids[1])
				if err == nil {
					requireStandaloneNativeJournalDocument(t, raw, ids[1], 2)
					installed = true
					break
				}
				if !errors.Is(err, mongo.ErrNoDocuments) {
					t.Fatalf("public successor installation read: %v", err)
				}
				select {
				case <-installDeadline.C:
					break pollInstall
				case <-poll.C:
				}
			}
			poll.Stop()
			installDeadline.Stop()
			requireNoACK(0, "predecessor physical sync")
			requireNoACK(1, "successor's own captured physical sync")
			held := standalone.Stats()
			if got, want := writeConcernBenchmarkStat(t, held, "treedb.command_wal.durable_wal_lsn"), writeConcernBenchmarkStat(t, firstHeld, "treedb.command_wal.durable_wal_lsn"); got != want {
				t.Fatalf("held first sync advanced durable frontier: got=%d want=%d", got, want)
			}
			if !installed {
				// Keep running after the named baseline RED so original/fresh typed
				// readback and checked shutdown are still observed on that same arm.
				t.Error("NATIVE_SHARED_BOUNDARY_SUCCESSOR_INSTALL_BLOCKED: successor did not install while predecessor own-publication physical sync was held")
			} else {
				t.Log("SUCCESSOR_INSTALLED_WITH_BOTH_ACKS_HELD")
			}
			// One leader is active. The observer is already armed, but the next epoch
			// cannot capture until this predecessor completes. Do not invert these.
			release(0)
			var later heldSync
			select {
			case later = <-entered[1]:
			case <-time.After(wait):
				t.Fatal("later-LSN native sync did not start after predecessor release")
			}
			if later.lsn <= first.lsn {
				t.Fatalf("later captured LSN=%d must exceed first=%d", later.lsn, first.lsn)
			}
			joinACK(0)
			requireNoACK(1, "later captured physical sync")
			if stable := writeConcernBenchmarkStat(t, standalone.Stats(), "treedb.command_wal.durable_wal_lsn"); stable >= later.lsn {
				t.Fatalf("later held target already certified: stable=%d later=%d", stable, later.lsn)
			}
			t.Logf("EXACT_PREFIX_ACK_PASS first_lsn=%d later_lsn=%d first_ACK=true successor_ACK=false", first.lsn, later.lsn)
			release(1)
			// The successor receipt requires the matching successful own-publication
			// After event. Aggregate counts alone cannot certify that physical work.
			var publicationStart, publicationDone heldSync
			select {
			case publicationStart = <-publicationBefore:
			case <-time.After(wait):
				t.Fatal("successor own-publication physical Before sync was not observed after second SyncAttempts")
			}
			select {
			case publicationDone = <-publicationAfter:
			case <-time.After(wait):
				t.Fatal("successor own-publication physical sync lacks successful matching After event")
			}
			if publicationStart.lsn < later.lsn || publicationStart.sequence < later.sequence || publicationStart.lsn != publicationDone.lsn || publicationStart.root != publicationDone.root || publicationStart.path != publicationDone.path || publicationStart.sequence >= publicationDone.sequence {
				t.Fatalf("successor own-publication physical sync identity/chronology mismatch: later=%+v before=%+v after=%+v", later, publicationStart, publicationDone)
			}
			joinACK(1)
			t.Logf("SUCCESSOR_PUBLICATION_PHYSICAL_SYNC_BEFORE_ACK_PASS profile=%s later_lsn=%d later_attempts_delta=%d publication_lsn=%d root=%s path=%s per_operation_receipt=true before_sequence=%d successful_after_sequence=%d driver_return_sequence=%d", profile, later.lsn, later.attempts-beforeACK.SyncAttempts, publicationStart.lsn, publicationStart.root, publicationStart.path, publicationStart.sequence, publicationDone.sequence, successorReturnSequence)
			restore()
			after := standalone.Stats()
			afterACK := standalone.Server.StandaloneWriteConcernStats()
			for _, key := range []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.group_commit.forced_total", "treedb.command_wal.sync.count_total", "treedb.command_wal.file_sync.calls_total"} {
				if writeConcernBenchmarkStat(t, after, key) < writeConcernBenchmarkStat(t, before, key) {
					t.Fatalf("counter reset: %s", key)
				}
			}
			delta := func(key string) uint64 {
				return writeConcernBenchmarkStat(t, after, key) - writeConcernBenchmarkStat(t, before, key)
			}
			if delta("treedb.cache.checkpoint.runs") != 0 || delta("treedb.command_wal.group_commit.forced_total") != 0 || delta("treedb.command_wal.sync.count_total") < 2 || delta("treedb.command_wal.file_sync.calls_total") < 2 {
				t.Fatalf("native boundary deltas checkpoints=%d forced=%d sync=%d file_sync=%d", delta("treedb.cache.checkpoint.runs"), delta("treedb.command_wal.group_commit.forced_total"), delta("treedb.command_wal.sync.count_total"), delta("treedb.command_wal.file_sync.calls_total"))
			}
			if afterACK.SyncAttempts-beforeACK.SyncAttempts != 2 || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 || afterACK.SyncFailures != beforeACK.SyncFailures || afterACK.UncertainOutcomes != beforeACK.UncertainOutcomes {
				t.Fatalf("actual journal ACK/boundary mismatch before=%+v after=%+v", beforeACK, afterACK)
			}
			t.Logf("COUNTERS_PASS profile=%s checkpoints=%d forced=%d sync=%d file_sync=%d journal_ACKs=2", profile, delta("treedb.cache.checkpoint.runs"), delta("treedb.command_wal.group_commit.forced_total"), delta("treedb.command_wal.sync.count_total"), delta("treedb.command_wal.file_sync.calls_total"))
			check := func(c *mongo.Client) {
				t.Helper()
				collection := c.Database("app").Collection("shared_journal")
				cursor, err := collection.Find(ctx, bson.D{})
				if err != nil {
					t.Fatalf("typed census find: %v", err)
				}
				var raw []bson.Raw
				if err := cursor.All(ctx, &raw); err != nil || len(raw) != 2 {
					t.Fatalf("typed census count=%d want2 err=%v", len(raw), err)
				}
				seen := make(map[string]bool, 2)
				for _, document := range raw {
					id, ok := document.Lookup("_id").StringValueOK()
					if !ok || seen[id] || (id != ids[0] && id != ids[1]) {
						t.Fatalf("typed census invalid/duplicate id=%q", id)
					}
					seen[id] = true
					generation := 1
					if id == ids[1] {
						generation = 2
					}
					requireStandaloneNativeJournalDocument(t, document, id, generation)
				}
			}
			check(client)
			if err := secondClient.Disconnect(ctx); err != nil {
				t.Fatalf("checked second driver disconnect: %v", err)
			}
			secondClient = nil
			active = false
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
			t.Log("ORIGINAL_TYPED_READBACK_AND_CHECKED_CLOSE_PASS")
			standalone, err = OpenStandaloneServer(opts)
			if err != nil {
				t.Fatalf("fresh original native owner reopen: %v", err)
			}
			freshOwner := standalone
			t.Cleanup(func() {
				if err := freshOwner.Close(); err != nil {
					t.Errorf("checked fresh owner cleanup: %v", err)
				}
			})
			requireStandaloneNativeJournalOwner(t, standalone)
			client, cancel, listener, served = startStandaloneMongoClientForTest(t, standalone)
			active = true
			check(client)
			active = false
			stopStandaloneMongoClientForTest(t, client, cancel, listener, served, standalone)
			t.Log("FRESH_TYPED_READBACK_AND_CHECKED_CLOSE_PASS (graceful reopen; not crash proof)")
		})
	}
}

// Shared native-entry safety fixtures use the original owner and transport.
// Disable only the gateway coalescer: a default same-collection worker must wait
// for its own native completion before dequeuing its successor. Independent RPCs
// here isolate shared native-group overlap, never default coalescer activation.
// The physical group collector policy and real sync callback stay unchanged.
// Product-default batching is covered separately by the exact receipt RED and
// the default held-receipt/queued-successor lifecycle control.
type nativeSharedSafetyFixture struct {
	s          *StandaloneServer
	clients    [2]*mongo.Client
	ctx        context.Context
	allowClose error
	finish     func()
}

func newNativeSharedSafetyFixture(t *testing.T, dir string, profile treedb.Profile, productDefault ...bool) *nativeSharedSafetyFixture {
	t.Helper()
	opts := StandaloneOptions{Dir: dir, Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}}
	if len(productDefault) == 0 || !productDefault[0] {
		opts.InsertCoalescingMaxBatchSet = true
	}
	s, err := OpenStandaloneServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	f := &nativeSharedSafetyFixture{s: s}
	ctx, cancelContext := context.WithTimeout(context.Background(), 60*time.Second)
	f.ctx = ctx
	// Register owner cleanup before transport setup or any blocking observer.
	var finishOnce sync.Once
	stopTransport := func() {}
	f.finish = func() {
		finishOnce.Do(func() {
			cancelContext()
			for i, client := range f.clients {
				if client == nil {
					continue
				}
				disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := client.Disconnect(disconnectCtx); err != nil {
					t.Errorf("checked driver%d Disconnect: %v", i, err)
				}
				cancel()
			}
			stopTransport()
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			select {
			case err := <-closed:
				if err != nil && (f.allowClose == nil || (!errors.Is(err, f.allowClose) && !errors.Is(err, backenddb.ErrRecoveryRequired))) {
					t.Errorf("checked original owner Close: %v", err)
				} else {
					t.Logf("CHECKED_SHARED_SAFETY_CLOSE profile=%s err=%v", profile, err)
				}
			case <-time.After(5 * time.Second):
				t.Error("original owner Close lacks bounded terminal")
			}
		})
	}
	t.Cleanup(f.finish)
	requireStandaloneNativeJournalOwner(t, s)
	if s.Server.Collections != s.Collections || s.Server.standaloneWriteConcernSync != nil {
		t.Fatal("safety fixture lost original native completion route")
	}
	client, cancelServe, listener, served := startStandaloneMongoClientForTest(t, s)
	f.clients[0] = client
	stopTransport = func() {
		cancelServe()
		_ = listener.Close() // Serve owns the listener and may already have closed it.
		select {
		case err := <-served:
			if err != nil {
				t.Errorf("checked Serve terminal: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve lacks bounded terminal")
		}
	}
	f.clients[1], err = mongo.Connect(options.Client().ApplyURI("mongodb://" + listener.Addr().String()).SetDirect(true).SetMaxPoolSize(1).SetServerSelectionTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.clients[1].Ping(ctx, nil); err != nil {
		t.Fatal(err)
	}
	return f
}

func nativeSharedSafetyWriter(f *nativeSharedSafetyFixture, i int) *mongo.Collection {
	journal := true
	return f.clients[i].Database("app").Collection("shared_journal", options.Collection().SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}))
}

func requireNativeSharedSafetyInstalled(t *testing.T, f *nativeSharedSafetyFixture, id string, generation int) {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		readCtx, cancel := context.WithTimeout(f.ctx, time.Second)
		var raw bson.Raw
		err := f.clients[0].Database("app").Collection("shared_journal").FindOne(readCtx, bson.D{{Key: "_id", Value: id}}).Decode(&raw)
		cancel()
		if err == nil {
			requireStandaloneNativeJournalDocument(t, raw, id, generation)
			return
		}
		if !errors.Is(err, mongo.ErrNoDocuments) {
			t.Fatalf("public installed read: %v", err)
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("successor %q did not install while predecessor explicit sync held", id)
		}
	}
}

func requireNativeSharedSafetyCensus(t *testing.T, f *nativeSharedSafetyFixture) {
	t.Helper()
	cursor, err := f.clients[0].Database("app").Collection("shared_journal").Find(f.ctx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	var docs []bson.Raw
	if err := cursor.All(f.ctx, &docs); err != nil || len(docs) != 2 {
		t.Fatalf("shared safety typed census count=%d want2 err=%v", len(docs), err)
	}
	seen := map[string]bool{}
	for _, raw := range docs {
		id, ok := raw.Lookup("_id").StringValueOK()
		if !ok || seen[id] || (id != "predecessor" && id != "successor") {
			t.Fatalf("shared safety invalid/duplicate id=%q", id)
		}
		seen[id] = true
		generation := 1
		if id == "successor" {
			generation = 2
		}
		requireStandaloneNativeJournalDocument(t, raw, id, generation)
	}
}

func TestStandaloneNativeSharedBoundaryHeldSyncFailurePoisons(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			f := newNativeSharedSafetyFixture(t, dir, profile)
			if err := f.clients[0].Database("app").CreateCollection(f.ctx, "shared_journal"); err != nil {
				t.Fatal(err)
			}
			seedWriteConcernDuplicateEmails(t, f.s.Server, false)
			before, beforeACK := f.s.Stats(), f.s.Server.StandaloneWriteConcernStats()
			failure := errors.New("shared-entry original explicit physical sync failed")
			f.allowClose = failure
			entered, released := make(chan uint64, 1), make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(released) }) }
			type wireResult struct {
				raw bson.Raw
				err error
			}
			partial := make(chan wireResult, 1)
			successor := make(chan error, 1)
			var started, joined [2]bool
			restore := func() {}
			t.Cleanup(func() {
				release()
				if started[0] && !joined[0] {
					select {
					case r := <-partial:
						joined[0] = true
						t.Logf("failure cleanup partial terminal err=%v response=%s", r.err, r.raw)
					case <-time.After(5 * time.Second):
						t.Error("failure cleanup partial request lacks terminal")
					}
				}
				if started[1] && !joined[1] {
					select {
					case err := <-successor:
						joined[1] = true
						t.Logf("failure cleanup successor terminal: %v", err)
					case <-time.After(5 * time.Second):
						t.Error("failure cleanup successor lacks terminal")
					}
				}
				restore()
				f.finish()
				if started[0] && !joined[0] {
					select {
					case r := <-partial:
						joined[0] = true
						t.Logf("failure partial terminal after transport Close: %v/%s", r.err, r.raw)
					case <-time.After(5 * time.Second):
						t.Error("failure partial still lacks terminal after transport Close")
					}
				}
				if started[1] && !joined[1] {
					select {
					case err := <-successor:
						joined[1] = true
						t.Logf("failure successor terminal after transport Close: %v", err)
					case <-time.After(5 * time.Second):
						t.Error("failure successor still lacks terminal after transport Close")
					}
				}
			})
			var selected atomic.Bool
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 && f.s.Server.writeConcernStats.syncAttempts.Load() == beforeACK.SyncAttempts+1 && selected.CompareAndSwap(false, true) {
					if e.StablePath == nil {
						return errors.New("failure hold lost original file")
					}
					entered <- e.LSN
					<-released
					return failure
				}
				return nil
			})
			var restoreOnce sync.Once
			restore = func() { restoreOnce.Do(undo) }
			started[0] = true
			go func() {
				raw, err := serveCommandResult(f.s.Server, 201, writeConcernPartialUpdateCommand())
				partial <- wireResult{raw: bson.Raw(raw), err: err}
			}()
			var firstLSN uint64
			select {
			case firstLSN = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("partial predecessor explicit physical hold not reached")
			}
			if got := writeConcernBenchmarkStat(t, f.s.Stats(), "treedb.command_wal.group_commit.forced_total") - writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total"); got != 1 {
				t.Fatalf("failure hold selected forced registrations=%d want1", got)
			}
			if !writeConcernMarker(t, f.s.Server, "u1") || writeConcernMarker(t, f.s.Server, "u2") {
				t.Fatal("held partial update lost exact successful/failed members")
			}
			started[1] = true
			go func() {
				_, err := nativeSharedSafetyWriter(f, 1).InsertOne(f.ctx, standaloneNativeJournalDocument("successor", 2))
				successor <- err
			}()
			requireNativeSharedSafetyInstalled(t, f, "successor", 2)
			if got := f.s.Server.writeConcernStats.journalRequests.Load() - beforeACK.JournalRequests; got != 2 {
				t.Fatalf("accepted journal requests=%d want2", got)
			}
			select {
			case r := <-partial:
				joined[0] = true
				t.Fatalf("partial completed before injected failure: %v/%s", r.err, r.raw)
			default:
			}
			select {
			case err := <-successor:
				joined[1] = true
				t.Fatalf("successor ACK before failed prefix resolved: %v", err)
			default:
			}
			release()
			select {
			case r := <-partial:
				joined[0] = true
				wc, ok := r.raw.Lookup("writeConcernError").DocumentOK()
				if r.err != nil || !mongoCommandResponseOK(r.raw) || r.raw.Lookup("writeErrors").Type != bson.TypeArray || !ok || wc.Lookup("codeName").StringValue() != "WriteConcernFailed" {
					t.Fatalf("partial wire envelope lost uncertainty: err=%v raw=%s", r.err, r.raw)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("partial waiter stranded after physical failure")
			}
			select {
			case err := <-successor:
				joined[1] = true
				var writeErr mongo.WriteException
				var commandErr mongo.CommandError
				var raw bson.Raw
				if errors.As(err, &writeErr) {
					raw = writeErr.Raw
				} else if errors.As(err, &commandErr) {
					raw = commandErr.Raw
				}
				wc, ok := raw.Lookup("writeConcernError").DocumentOK()
				if err == nil || !ok || wc.Lookup("codeName").StringValue() != "WriteConcernFailed" {
					t.Fatalf("successor lacked wire uncertainty: err=%v raw=%s", err, raw)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("successor waiter stranded after physical failure")
			}
			restore()
			if err := f.s.Backend.CheckCommandWALPublishReady(); !errors.Is(err, backenddb.ErrRecoveryRequired) && !errors.Is(err, failure) {
				t.Fatalf("assigned sync failure did not poison original owner: %v", err)
			}
			next := f.s.Backend.CommandWALNextLSN()
			if physical, err := f.s.Collections.SyncForStandaloneWriteConcern(); physical || err == nil {
				t.Fatalf("poisoned manager fence=%t/%v", physical, err)
			}
			if _, err := nativeSharedSafetyWriter(f, 1).InsertOne(f.ctx, standaloneNativeJournalDocument("after-poison", 3)); err == nil {
				t.Fatal("later public journal request cleanly ACKed after poison")
			}
			if f.s.Backend.CommandWALNextLSN() != next {
				t.Fatal("poison refusal retried/appended canonical command")
			}
			after, afterACK := f.s.Stats(), f.s.Server.StandaloneWriteConcernStats()
			if writeConcernBenchmarkStat(t, after, "treedb.cache.checkpoint.runs") != writeConcernBenchmarkStat(t, before, "treedb.cache.checkpoint.runs") || afterACK.JournalAcknowledgements != beforeACK.JournalAcknowledgements || afterACK.PhysicalSyncBoundaries != beforeACK.PhysicalSyncBoundaries || afterACK.SyncFailures-beforeACK.SyncFailures != 3 || afterACK.UncertainOutcomes-beforeACK.UncertainOutcomes != 3 {
				t.Fatalf("failure/fallback accounting first_lsn=%d beforeACK=%+v afterACK=%+v", firstLSN, beforeACK, afterACK)
			}
			t.Logf("SHARED_FAILURE_POISON_NO_RETRY_PASS profile=%s first_lsn=%d journal_ACKs=0 uncertain=3 checkpoint_delta=0 canonical_next=%d", profile, firstLSN, next)
			f.finish()
			fresh := newNativeSharedSafetyFixture(t, dir, profile)
			fresh.finish() // Unacknowledged ambiguous mutations may survive or be absent.
			t.Logf("SHARED_FAILURE_FRESH_OWNER_CHECKED_CLOSE_PASS profile=%s", profile)
		})
	}
}

func TestStandaloneNativeSharedBoundaryCloseJoinsHeldSync(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			dir := t.TempDir()
			f := newNativeSharedSafetyFixture(t, dir, profile)
			if err := f.clients[0].Database("app").CreateCollection(f.ctx, "shared_journal"); err != nil {
				t.Fatal(err)
			}
			registry := f.s.Backend.ValueLogIdentityPinRegistry()
			if registry == nil {
				t.Fatal("original registry absent")
			}
			entered, released := make(chan uint64, 1), make(chan struct{})
			var selected atomic.Bool
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(released) }) }
			results := [2]chan error{make(chan error, 1), make(chan error, 1)}
			var started, joined [2]bool
			closed := make(chan error, 1)
			closeStarted, closeJoined := false, false
			restore := func() {}
			t.Cleanup(func() {
				release()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case err := <-results[i]:
							joined[i] = true
							t.Logf("Close cleanup command%d terminal: %v", i, err)
						case <-time.After(5 * time.Second):
							t.Errorf("Close cleanup command%d lacks terminal", i)
						}
					}
				}
				if closeStarted && !closeJoined {
					select {
					case err := <-closed:
						closeJoined = true
						if err != nil {
							t.Errorf("original Close producer error: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Error("original Close producer lacks terminal")
					}
				}
				restore()
				f.finish()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case err := <-results[i]:
							joined[i] = true
							t.Logf("Close command%d terminal after transport Close: %v", i, err)
						case <-time.After(5 * time.Second):
							t.Errorf("Close command%d still lacks terminal after transport Close", i)
						}
					}
				}
				if closeStarted && !closeJoined {
					select {
					case err := <-closed:
						closeJoined = true
						if err != nil {
							t.Errorf("original Close producer error after transport Close: %v", err)
						}
					case <-time.After(5 * time.Second):
						t.Error("original Close producer still lacks terminal after transport Close")
					}
				}
			})
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync && e.LSN != 0 && selected.CompareAndSwap(false, true) {
					if e.StablePath == nil {
						return errors.New("Close hold lost original file")
					}
					entered <- e.LSN
					<-released
				}
				return nil
			})
			var restoreOnce sync.Once
			restore = func() { restoreOnce.Do(undo) }
			ids := [2]string{"predecessor", "successor"}
			start := func(i int) {
				started[i] = true
				go func() {
					result, err := nativeSharedSafetyWriter(f, i).InsertOne(f.ctx, standaloneNativeJournalDocument(ids[i], i+1))
					if err == nil && (result == nil || result.InsertedID != ids[i]) {
						err = fmt.Errorf("wrong driver insert result: %v", result)
					}
					results[i] <- err
				}()
			}
			start(0)
			var lsn uint64
			select {
			case lsn = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Close predecessor own-publication physical hold not reached")
			}
			start(1)
			requireNativeSharedSafetyInstalled(t, f, "successor", 2)
			requireNativeSharedSafetyCensus(t, f)
			for i := range results {
				select {
				case err := <-results[i]:
					joined[i] = true
					t.Fatalf("command%d ACKed before Close/held sync: %v", i, err)
				default:
				}
			}
			closeStarted = true
			go func() { closed <- f.s.Close() }()
			tick, deadline := time.NewTicker(10*time.Millisecond), time.NewTimer(5*time.Second)
			defer tick.Stop()
			defer deadline.Stop()
			for !f.s.Server.closed.Load() {
				select {
				case <-tick.C:
				case <-deadline.C:
					t.Fatal("Close did not enter existing closed admission")
				}
			}
			select {
			case err := <-closed:
				closeJoined = true
				t.Fatalf("Close overtook held original completion: %v", err)
			default:
			}
			release()
			for i := range results {
				select {
				case err := <-results[i]:
					joined[i] = true
					t.Logf("Close command%d terminal (socket Close may abort ACK): %v", i, err)
				case <-time.After(5 * time.Second):
					t.Fatalf("Close stranded command%d", i)
				}
			}
			select {
			case err := <-closed:
				closeJoined = true
				if err != nil {
					t.Fatalf("original Close producer error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close failed to join released original completion")
			}
			restore()
			if pins := registry.Stats(); pins.ActivePins != 0 || pins.ActiveIdentities != 0 {
				t.Fatalf("Close retained original dependency pins: %+v", pins)
			}
			f.finish()
			fresh := newNativeSharedSafetyFixture(t, dir, profile)
			requireNativeSharedSafetyCensus(t, fresh)
			fresh.finish()
			t.Logf("SHARED_CLOSE_JOIN_AND_FRESH_TYPED_PASS profile=%s held_lsn=%d pins=0 identities=0", profile, lsn)
		})
	}
}

func TestStandaloneNativeSharedBoundaryCrashReopen(t *testing.T) {
	const dirEnv = "GOMAP_SHARED_BOUNDARY_CRASH_DIR"
	const profileEnv = "GOMAP_SHARED_BOUNDARY_CRASH_PROFILE"
	const marker = "SHARED BOUNDARY BOTH EXPLICIT JOURNAL ACKS COMPLETE"
	childDir := os.Getenv(dirEnv)
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		if childDir != "" && os.Getenv(profileEnv) != string(profile) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := childDir
			if dir == "" {
				dir = t.TempDir()
				childCtx, cancelChild := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancelChild()
				cmd := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestStandaloneNativeSharedBoundaryCrashReopen$", "-test.v", "-test.timeout=45s")
				// CommandContext's default cancellation kills the child; CombinedOutput
				// waits/reaps it. Bound inherited-output-pipe draining as well.
				cmd.WaitDelay = 5 * time.Second
				cmd.Env = append(os.Environ(), dirEnv+"="+dir, profileEnv+"="+string(profile))
				output, err := cmd.CombinedOutput()
				contextErr := childCtx.Err()
				cancelChild()
				t.Logf("shared-boundary crash child: %s", output)
				if contextErr != nil {
					t.Fatalf("shared-boundary crash child canceled/timed out; kill/wait returned: context=%v err=%v state=%v", contextErr, err, cmd.ProcessState)
				}
				if cmd.ProcessState == nil {
					t.Fatalf("shared-boundary crash child lacks reaped process state: %v", err)
				}
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != 97 || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("shared-boundary crash child: %v", err)
				}
				fresh := newNativeSharedSafetyFixture(t, dir, profile)
				requireNativeSharedSafetyCensus(t, fresh)
				fresh.finish()
				t.Logf("SHARED_CRASH_FRESH_TYPED_CHECKED_CLOSE_PASS profile=%s", profile)
				return
			}
			f := newNativeSharedSafetyFixture(t, dir, profile)
			if err := f.clients[0].Database("app").CreateCollection(f.ctx, "shared_journal"); err != nil {
				t.Fatal(err)
			}
			before, beforeACK := f.s.Stats(), f.s.Server.StandaloneWriteConcernStats()
			type syncIdentity struct {
				lsn        uint64
				root, path string
			}
			entered, released := make(chan uint64, 1), make(chan struct{})
			var held atomic.Bool
			var heldLSN atomic.Uint64
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(released) }) }
			var publicationBefore atomic.Pointer[syncIdentity]
			var publicationAfter atomic.Bool
			type result struct {
				err   error
				after bool
			}
			results := [2]chan result{make(chan result, 1), make(chan result, 1)}
			var started, joined [2]bool
			restore := func() {}
			t.Cleanup(func() {
				release()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case r := <-results[i]:
							joined[i] = true
							if r.err != nil {
								t.Errorf("crash cleanup command%d: %v", i, r.err)
							}
						case <-time.After(5 * time.Second):
							t.Errorf("crash cleanup command%d lacks terminal", i)
						}
					}
				}
				restore()
				f.finish()
				for i := range results {
					if started[i] && !joined[i] {
						select {
						case r := <-results[i]:
							joined[i] = true
							t.Logf("crash command%d terminal after transport Close: %v", i, r.err)
						case <-time.After(5 * time.Second):
							t.Errorf("crash command%d still lacks terminal after transport Close", i)
						}
					}
				}
			})
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource != durabilitycut.ResourceCommandWAL || e.LSN == 0 {
					return nil
				}
				if e.Point == durabilitycut.AfterDependencyFileSync {
					if target := publicationBefore.Load(); target != nil && e.LSN == target.lsn && e.Root == target.root && e.Path == target.path {
						if e.StablePath == nil {
							return errors.New("crash publication After lost original file")
						}
						publicationAfter.Store(true)
					}
					return nil
				}
				if e.Point != durabilitycut.BeforeDependencyFileSync {
					return nil
				}
				if heldLSN.Load() != 0 && e.LSN > heldLSN.Load() {
					if e.StablePath == nil {
						return errors.New("crash publication Before lost original file")
					}
					target := &syncIdentity{lsn: e.LSN, root: e.Root, path: e.Path}
					if !publicationBefore.CompareAndSwap(nil, target) {
						return errors.New("duplicate crash successor publication capture")
					}
				}
				if held.CompareAndSwap(false, true) {
					heldLSN.Store(e.LSN)
					if e.StablePath == nil {
						return errors.New("crash predecessor hold lost original file")
					}
					entered <- e.LSN
					<-released
				}
				return nil
			})
			var restoreOnce sync.Once
			restore = func() { restoreOnce.Do(undo) }
			ids := [2]string{"predecessor", "successor"}
			start := func(i int) {
				started[i] = true
				go func() {
					out, err := nativeSharedSafetyWriter(f, i).InsertOne(f.ctx, standaloneNativeJournalDocument(ids[i], i+1))
					if err == nil && (out == nil || out.InsertedID != ids[i]) {
						err = fmt.Errorf("wrong insert result: %v", out)
					}
					results[i] <- result{err: err, after: publicationAfter.Load()}
				}()
			}
			start(0)
			var firstLSN uint64
			select {
			case firstLSN = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("crash predecessor own-publication physical hold not reached")
			}
			start(1)
			requireNativeSharedSafetyInstalled(t, f, "successor", 2)
			for i := range results {
				select {
				case r := <-results[i]:
					joined[i] = true
					t.Fatalf("crash command%d ACK before held predecessor release: %v", i, r.err)
				default:
				}
			}
			release()
			for i := range results {
				select {
				case r := <-results[i]:
					joined[i] = true
					if r.err != nil || (i == 1 && !r.after) {
						t.Fatalf("crash command%d ACK lacks successful own-publication sync: err=%v after=%t", i, r.err, r.after)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("crash command%d lacks ACK terminal", i)
				}
			}
			target := publicationBefore.Load()
			if target == nil || target.lsn <= firstLSN || !publicationAfter.Load() {
				t.Fatalf("crash successor publication target invalid: first=%d target=%+v after=%t", firstLSN, target, publicationAfter.Load())
			}
			after, afterACK := f.s.Stats(), f.s.Server.StandaloneWriteConcernStats()
			if writeConcernBenchmarkStat(t, after, "treedb.cache.checkpoint.runs") != writeConcernBenchmarkStat(t, before, "treedb.cache.checkpoint.runs") || writeConcernBenchmarkStat(t, after, "treedb.command_wal.group_commit.forced_total")-writeConcernBenchmarkStat(t, before, "treedb.command_wal.group_commit.forced_total") != 0 || afterACK.JournalAcknowledgements-beforeACK.JournalAcknowledgements != 2 || afterACK.PhysicalSyncBoundaries-beforeACK.PhysicalSyncBoundaries != 2 {
				t.Fatalf("crash ACK/fence contract mismatch: before=%+v after=%+v", beforeACK, afterACK)
			}
			fmt.Printf("%s profile=%s first_lsn=%d publication_successor_lsn=%d checkpoint_delta=0\n", marker, profile, firstLSN, target.lsn)
			os.Exit(97) // No Checkpoint, Close, Disconnect or cleanup rescues these ACKs.
		})
	}
}

// The auth store is deliberately unrelated to Collections. Selection must never
// invoke it or infer its ownership from the actual native collection manager.
type journalBoundaryUnrelatedAuthStore struct{}

func (journalBoundaryUnrelatedAuthStore) Get([]byte) ([]byte, error) {
	panic("scheduling query touched unrelated auth store")
}
func (journalBoundaryUnrelatedAuthStore) SetSync([]byte, []byte) error {
	panic("scheduling query touched unrelated auth store")
}

func TestStandaloneNativeJournalBoundaryEligibility(t *testing.T) {
	var missing *Server
	if missing.canShareStandaloneJournalBoundary("insert") || NewServer().canShareStandaloneJournalBoundary("insert") {
		t.Fatal("missing manager granted shared journal entry")
	}
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			server := s.Server
			before := s.Stats()
			const dictionaryHits = "treedb.process.read_path.outer_leaf.cache.dictionary_verified_hits"
			if value, ok := before[dictionaryHits]; !ok || value != s.Backend.Stats()[dictionaryHits] {
				t.Fatal("standalone original owner stats lost dictionary hit counter")
			}
			next := s.Backend.CommandWALNextLSN()
			for _, name := range []string{"create", "createIndexes", "delete", "dropIndexes", "findAndModify", "insert", "update"} {
				if got, want := server.canShareStandaloneJournalBoundary(name), profile != treedb.ProfileNoWALFast; got != want {
					t.Fatalf("%s shared=%t want=%t", name, got, want)
				}
			}
			server.AuthCatalog = &AuthCatalog{db: journalBoundaryUnrelatedAuthStore{}}
			for _, name := range []string{"createUser", "updateUser", "dropUser", "unknown"} {
				if server.canShareStandaloneJournalBoundary(name) {
					t.Fatalf("%s granted sharing without collection-owner proof", name)
				}
			}
			server.standaloneWriteConcernSync = func() (bool, error) {
				panic("capability query invoked custom sync")
			}
			if server.canShareStandaloneJournalBoundary("insert") {
				t.Fatal("custom sync override granted sharing")
			}
			server.standaloneWriteConcernSync = nil
			after := s.Stats()
			keys := []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.file_sync.calls_total"}
			if profile != treedb.ProfileNoWALFast {
				keys = append(keys, "treedb.command_wal.group_commit.forced_total")
			}
			for _, key := range keys {
				if writeConcernBenchmarkStat(t, after, key) != writeConcernBenchmarkStat(t, before, key) {
					t.Fatalf("selection changed %s", key)
				}
			}
			if s.Backend.CommandWALNextLSN() != next {
				t.Fatal("selection assigned a command")
			}
		})
	}
}

func TestStandaloneNativeJournalCustomBoundaryRemainsExclusive(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if !s.Collections.CanShareStandaloneJournalBoundary() {
				t.Fatal("fixture lacks actual original native completion owner")
			}
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			releaseSync := func() { once.Do(func() { close(release) }) }
			done := make(chan error, 1)
			s.Server.standaloneWriteConcernSync = func() (bool, error) {
				entered <- struct{}{}
				<-release
				return false, backenddb.ErrNativeSemanticUnsupported
			}
			command := mustDocument(t, bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "custom"}}}}, {Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}}, {Key: "$db", Value: "app"}})
			// Cleanup is registered before launch and joins even an assertion failure.
			joined := false
			t.Cleanup(func() {
				releaseSync()
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("custom boundary command did not join")
					}
				}
			})
			go func() {
				response, err := s.Server.commandResponse(context.Background(), "insert", command, nil, 0)
				if err == nil && bson.Raw(response).Lookup("writeConcernError").Type == 0 {
					err = errors.New("custom failure lost writeConcernError")
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("custom sync boundary not reached")
			}
			if s.Server.standaloneWriteBoundaryMu.TryRLock() {
				s.Server.standaloneWriteBoundaryMu.RUnlock()
				t.Fatal("custom native journal boundary allowed a visible reader")
			}
			releaseSync()
			select {
			case err := <-done:
				joined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("custom boundary command did not finish")
			}
		})
	}
}

func TestStandaloneNativeJournalSharedBoundaryUnlocksAfterSyncPanic(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALRelaxed, treedb.ProfileCommandWALDurable} {
		t.Run(string(profile), func(t *testing.T) {
			// Keep the injected panic in this caller, rather than the default worker.
			s, err := OpenStandaloneServer(StandaloneOptions{Dir: t.TempDir(), Profile: profile, InsertCoalescingMaxBatchSet: true, InsertCoalescingMaxBatch: 0, DefaultCollectionOptions: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatBSON}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil && !errors.Is(err, backenddb.ErrRecoveryRequired) {
					t.Errorf("original poisoned Close: %v", err)
				}
			})
			if !s.Server.canShareStandaloneJournalBoundary("insert") {
				t.Fatal("fixture did not select shared native entry")
			}
			seed := mustDocument(t, bson.D{{Key: "create", Value: "rows"}, {Key: "$db", Value: "app"}})
			if response, err := s.Server.commandResponse(context.Background(), "create", seed, nil, 0); err != nil || !mongoCommandResponseOK(response) {
				t.Fatalf("create before panic: %s/%v", response, err)
			}
			failure := errors.New("real shared-entry own-publication sync panic")
			undo := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
					panic(failure)
				}
				return nil
			})
			var restoreOnce sync.Once
			restore := func() { restoreOnce.Do(undo) }
			t.Cleanup(restore)
			command := mustDocument(t, bson.D{{Key: "insert", Value: "rows"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "panic"}}}}, {Key: "writeConcern", Value: bson.D{{Key: "j", Value: true}}}, {Key: "$db", Value: "app"}})
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, _ = s.Server.commandResponse(context.Background(), "insert", command, nil, 0)
			}()
			restore()
			if recovered != failure {
				t.Fatalf("real own-publication sync panic=%v", recovered)
			}
			if !s.Server.standaloneWriteBoundaryMu.TryLock() {
				t.Fatal("panic retained shared boundary")
			}
			s.Server.standaloneWriteBoundaryMu.Unlock()
			if s.Collections.CanShareStandaloneJournalBoundary() || s.Backend.CheckCommandWALPublishReady() == nil {
				t.Fatal("panicked real sync did not retain recovery-required admission")
			}
		})
	}
}
