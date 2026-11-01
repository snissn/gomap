package mongogateway

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"go.mongodb.org/mongo-driver/v2/bson"
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
