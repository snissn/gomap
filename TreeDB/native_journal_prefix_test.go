package treedb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
)

func requireNativeJournalStatDelta(t *testing.T, before, after map[string]string, key string, want uint64) {
	t.Helper()
	old, next := statMapUint64(t, before, key), statMapUint64(t, after, key)
	if next-old != want {
		t.Fatalf("%s before=%d after=%d delta=%d want=%d", key, old, next, next-old, want)
	}
}

func nativeJournalPrefixFixture(t *testing.T, profile Profile) (*DB, *collections.CollectionManager) {
	t.Helper()
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(profile, t.TempDir())
	opts.ValueLog.PointerThreshold = 1
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	t.Cleanup(func() {
		poison := backend.CheckCommandWALPublishReady()
		for _, close := range []func() error{manager.Close, database.Close} {
			if err := close(); err != nil {
				if poison == nil || (!errors.Is(err, poison) && !errors.Is(err, backenddb.ErrRecoveryRequired)) {
					t.Errorf("original cleanup: %v", err)
				} else {
					t.Logf("checked poisoned-owner cleanup retained: %v", err)
				}
			}
		}
	})
	return database, manager
}

func TestNativeJournalPrefixOriginalOwnerAndFallback(t *testing.T) {
	var nilPublic *DB
	if err := nilPublic.SyncNativeCommandWALPrefix(); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil public: %v", err)
	}
	var nilBackend *backenddb.DB
	if supported, err := nilBackend.SyncNativeCommandWALPrefix(); supported || !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil backend: %t/%v", supported, err)
	}
	if err := nilBackend.CheckNativeCommandWALPrefixOwner(nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil admission: %v", err)
	}
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileCommandWALDurable, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			database, manager := nativeJournalPrefixFixture(t, profile)
			backend := database.backend
			if err := backend.CheckNativeCommandWALPrefixOwner(nil); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("nil owner: %v", err)
			}
			if err := backend.CheckNativeCommandWALPrefixOwner(transactionowner.New()); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("wrong owner: %v", err)
			}
			if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: "rows"}); err != nil {
				t.Fatal(err)
			}
			c, err := manager.OpenCollection("rows")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"n":1}`)}); err != nil {
				t.Fatal(err)
			}
			before := database.Stats()
			if physical, err := manager.SyncForStandaloneWriteConcern(); err != nil || !physical {
				t.Fatalf("journal boundary: %t/%v", physical, err)
			}
			if profile == ProfileNoWALFast {
				if supported, err := backend.SyncNativeCommandWALPrefix(); supported || err != nil {
					t.Fatalf("NoWAL capability: %t/%v", supported, err)
				}
				if err := database.SyncNativeCommandWALPrefix(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
					t.Fatalf("NoWAL public: %v", err)
				}
				requireNativeJournalStatDelta(t, before, database.Stats(), "treedb.cache.checkpoint.runs", 1)
			} else {
				if err := database.SyncNativeCommandWALPrefix(); err != nil {
					t.Fatal(err)
				}
				after := database.Stats()
				requireNativeJournalStatDelta(t, before, after, "treedb.cache.checkpoint.runs", 0)
				requireNativeJournalStatDelta(t, before, after, "treedb.command_wal.group_commit.forced_total", 2)
				requireNativeJournalStatDelta(t, before, after, "treedb.command_wal.group_commit.syncs_total", 2)
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if physical, err := manager.SyncForStandaloneWriteConcern(); physical || !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed manager: %t/%v", physical, err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.SyncNativeCommandWALPrefix(); !errors.Is(err, ErrClosed) {
				t.Fatalf("closed public: %v", err)
			}
			if supported, err := backend.SyncNativeCommandWALPrefix(); supported || !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed backend: %t/%v", supported, err)
			}
		})
	}
	for _, mode := range []string{"cow_btree", "hash_sorted"} {
		t.Run("inactive_"+mode, func(t *testing.T) {
			opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
			opts.MemtableMode = mode
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err := database.SyncNativeCommandWALPrefix(); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("inactive public: %v", err)
			}
			if supported, err := database.backend.SyncNativeCommandWALPrefix(); supported || !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
				t.Fatalf("inactive backend: %t/%v", supported, err)
			}
		})
	}
}

func TestNativeJournalPrefixSyncFailurePoisonsWithoutFallback(t *testing.T) {
	for _, direct := range []bool{false, true} {
		for _, point := range []durabilitycut.Point{durabilitycut.BeforeDependencyFileSync, durabilitycut.AfterDependencyFileSync} {
			t.Run(fmt.Sprintf("direct=%t/%s", direct, point), func(t *testing.T) {
				database, manager := nativeJournalPrefixFixture(t, ProfileCommandWALRelaxed)
				if err := database.Set([]byte("installed"), []byte("original")); err != nil {
					t.Fatal(err)
				}
				before := database.Stats()
				failure := errors.New("original captured journal sync failed")
				calls := 0
				restore := durabilitycut.Install(func(e durabilitycut.Event) error {
					if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == point {
						calls++
						if e.StablePath == nil {
							t.Error("physical sync lost original file handle")
						}
						return failure
					}
					return nil
				})
				var err error
				if direct {
					err = database.SyncNativeCommandWALPrefix()
				} else {
					var physical bool
					physical, err = manager.SyncForStandaloneWriteConcern()
					if physical {
						t.Error("failed native manager reported completed boundary")
					}
				}
				restore()
				if !errors.Is(err, failure) || calls != 1 {
					t.Fatalf("actual sync failure calls=%d err=%v", calls, err)
				}
				after := database.Stats()
				requireNativeJournalStatDelta(t, before, after, "treedb.cache.checkpoint.runs", 0)
				requireNativeJournalStatDelta(t, before, after, "treedb.command_wal.group_commit.forced_total", 1)
				next := database.backend.CommandWALNextLSN()
				if got, err := database.Get([]byte("installed")); err != nil || !bytes.Equal(got, []byte("original")) {
					t.Fatalf("installed mutation lost: %q/%v", got, err)
				}
				if database.backend.CanShareNativeCommandWALPrefix() || manager.CanShareStandaloneJournalBoundary() {
					t.Fatal("poisoned owner granted shared scheduling")
				}
				if err := database.SyncNativeCommandWALPrefix(); err == nil {
					t.Fatal("poisoned direct fence succeeded")
				}
				if supported, err := database.backend.SyncNativeCommandWALPrefix(); err == nil || supported {
					t.Fatalf("poison admission became healthy capability: %t/%v", supported, err)
				}
				if physical, err := manager.SyncForStandaloneWriteConcern(); err == nil || physical {
					t.Fatalf("poison manager: %t/%v", physical, err)
				}
				if err := database.Set([]byte("after"), []byte("no")); err == nil {
					t.Fatal("write after poison succeeded")
				}
				if database.backend.CommandWALNextLSN() != next {
					t.Fatal("failure retried/reappended canonical command")
				}
				requireNativeJournalStatDelta(t, before, database.Stats(), "treedb.cache.checkpoint.runs", 0)
			})
		}
	}
}

func TestNativeJournalPrefixRetainsLifecycleReleasesGatesBeforeACK(t *testing.T) {
	database, manager := nativeJournalPrefixFixture(t, ProfileCommandWALRelaxed)
	if err := database.Set([]byte("before"), []byte("prefix")); err != nil {
		t.Fatal(err)
	}
	before := database.Stats()
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Resource == durabilitycut.ResourceCommandWAL && e.Point == durabilitycut.BeforeDependencyFileSync {
			if e.StablePath == nil {
				return errors.New("missing original captured file")
			}
			once.Do(func() { close(entered); <-release })
		}
		return nil
	})
	defer restore()
	fence := make(chan error, 1)
	go func() {
		physical, err := manager.SyncForStandaloneWriteConcern()
		if err == nil && !physical {
			err = errors.New("no completed physical boundary")
		}
		fence <- err
	}()
	select {
	case <-entered:
	case err := <-fence:
		t.Fatalf("ACK before actual file fence: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("fence did not reach retained file")
	}
	if database.lifecycleMu.TryLock() {
		database.lifecycleMu.Unlock()
		t.Fatal("fence did not retain public lifetime")
	}
	if got, want := statMapUint64(t, database.Stats(), "treedb.command_wal.durable_wal_lsn"), statMapUint64(t, before, "treedb.command_wal.durable_wal_lsn"); got != want {
		t.Fatalf("held physical fence advanced stable prefix: got=%d want=%d", got, want)
	}
	if !database.commandWALPublicOperationGate.TryLock() {
		t.Fatal("fence retained public operation gate")
	}
	database.commandWALPublicOperationGate.Unlock()
	if !database.commandWALPublicPublishMu.TryLock() {
		t.Fatal("fence retained public publish gate")
	}
	database.commandWALPublicPublishMu.Unlock()
	raw := make(chan error, 1)
	go func() {
		guard, err := database.backend.LockPrivateCOWStagingGuard(database.txnOwner)
		if err == nil {
			guard.Release()
		}
		raw <- err
	}()
	select {
	case err := <-raw:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fence retained raw/teardown gates")
	}
	closing, closed := make(chan struct{}), make(chan error, 1)
	cache, registry := database.cached, database.backend.ValueLogIdentityPinRegistry()
	go func() { close(closing); closed <- database.Close() }()
	<-closing
	select {
	case err := <-fence:
		t.Fatalf("fence overtook file sync: %v", err)
	default:
	}
	select {
	case err := <-closed:
		t.Fatalf("Close overtook held completion: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-fence:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fence failed to complete")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join retained completion")
	}
	requirePublicCOWFinalZero(t, cache)
	if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
		t.Fatalf("retained dependency pins: %+v", stats)
	}
}

// The child exits immediately after the journal fence. There is no rescuing
// Checkpoint/Close; the NoWAL arm remains the physical fallback control.
func TestNativeJournalPrefixCrashReopenDependencies(t *testing.T) {
	const dirEnv = "GOMAP_NATIVE_JPREFIX_DEPENDENCY_DIR"
	const profileEnv = "GOMAP_NATIVE_JPREFIX_DEPENDENCY_PROFILE"
	const marker = "NATIVE JOURNAL PREFIX DEPENDENCIES SYNCED"
	childDir := os.Getenv(dirEnv)
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileCommandWALDurable, ProfileNoWALFast} {
		if childDir != "" && string(profile) != os.Getenv(profileEnv) {
			continue
		}
		t.Run(string(profile), func(t *testing.T) {
			dir := childDir
			if dir == "" {
				dir = t.TempDir()
				cmd := exec.Command(os.Args[0], "-test.run=^TestNativeJournalPrefixCrashReopenDependencies$", "-test.v")
				cmd.Env = append(os.Environ(), dirEnv+"="+dir, profileEnv+"="+string(profile))
				output, err := cmd.CombinedOutput()
				t.Logf("journal-only crash child: %s", output)
				var exited *exec.ExitError
				if !errors.As(err, &exited) || exited.ExitCode() != 97 || !bytes.Contains(output, []byte(marker)) || bytes.Contains(output, []byte("DATA RACE")) {
					t.Fatalf("crash child: %v", err)
				}
			}
			t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
			opts := indexedSliceOptions(profile, dir)
			opts.ValueLog.PointerThreshold = 1
			opts.CommandWALSegmentTargetBytes = 1
			database, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			backend, err := database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager := collections.NewCollectionManager(backend)
			t.Cleanup(func() {
				if err := manager.Close(); err != nil {
					t.Error(err)
				}
				if err := database.Close(); err != nil {
					t.Error(err)
				}
			})
			if !manager.UsesNativeCommandWALApply() || !backend.HasNativeSemanticBridge() {
				t.Fatal("missing original native bridge")
			}
			const source = `{"city":"hnl","payload":"valid JSON template persistent dependency"}`
			if childDir != "" {
				for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
					name := string(format)
					if _, err := manager.CreateCollection(&collections.CollectionMeta{Name: name, Options: collections.CollectionOptions{DocumentFormat: format}, Indexes: []collections.IndexDefinition{{Name: "city", Field: "city", ValueType: collections.IndexValueString}}}); err != nil {
						t.Fatal(err)
					}
					c, err := manager.OpenCollection(name)
					if err != nil {
						t.Fatal(err)
					}
					doc := []byte(source)
					if format == collections.DocumentFormatTemplateV1 {
						doc = nativeTemplateDoc(t, source)
					}
					if format == collections.DocumentFormatTemplateV1 {
						var encoder collections.TemplateV1Encoder
						if _, err := c.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("a")}, [][]byte{doc}, &encoder); err != nil {
							t.Fatal(err)
						}
						compact, err := encoder.EncodeDocument([]string{"city", "payload"}, []any{"hnl", "valid JSON template persistent dependency"})
						if err != nil {
							t.Fatal(err)
						}
						if _, err := c.InsertBatchWithTemplateV1Encoder([][]byte{[]byte("b")}, [][]byte{compact}, &encoder); err != nil {
							t.Fatal(err)
						}
					} else if _, err := c.InsertBatch([][]byte{[]byte("a")}, [][]byte{doc}); err != nil {
						t.Fatal(err)
					}
				}
				batch := database.NewBatch()
				if err := batch.Set([]byte("raw-batch"), bytes.Repeat([]byte("raw-value"), 512)); err != nil {
					t.Fatal(err)
				}
				if err := batch.Write(); err != nil {
					t.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				before := database.Stats()
				for range 2 {
					if physical, err := manager.SyncForStandaloneWriteConcern(); err != nil || !physical {
						t.Fatalf("journal prefix %t/%v", physical, err)
					}
				}
				if profile != ProfileNoWALFast {
					requireNativeJournalStatDelta(t, before, database.Stats(), "treedb.cache.checkpoint.runs", 0)
					requireNativeJournalStatDelta(t, before, database.Stats(), "treedb.command_wal.group_commit.forced_total", 2)
				}
			}
			for _, format := range []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1} {
				c, err := manager.OpenCollection(string(format))
				if err != nil {
					t.Fatal(err)
				}
				got, err := c.Get([]byte("a"))
				if err != nil {
					t.Fatal(err)
				}
				if format == collections.DocumentFormatTemplateV1 {
					m, err := c.NewStoredDocumentJSONMaterializer()
					if err != nil {
						t.Fatal(err)
					}
					checkNativeTemplateJSON(t, m, got, source)
					compact, err := c.Get([]byte("b"))
					if err != nil {
						t.Fatal(err)
					}
					checkNativeTemplateJSON(t, m, compact, source)
					if err := m.Close(); err != nil {
						t.Fatal(err)
					}
				} else if string(got) != source {
					t.Fatalf("JSON recovered=%s", got)
				}
				ids, err := c.FindByIndexValue("city", "hnl")
				wantIDs := 1
				if format == collections.DocumentFormatTemplateV1 {
					wantIDs = 2
				}
				if err != nil || len(ids) != wantIDs || string(ids[0]) != "a" || (wantIDs == 2 && string(ids[1]) != "b") {
					t.Fatalf("recovered secondary index=%q/%v", ids, err)
				}
			}
			if got, err := database.Get([]byte("raw-batch")); err != nil || !bytes.Equal(got, bytes.Repeat([]byte("raw-value"), 512)) {
				t.Fatalf("raw persistent pointer value=%d/%v", len(got), err)
			}
			if childDir != "" {
				fmt.Println(marker)
				os.Exit(97)
			}
		})
	}
}

// The capability is a pure scheduling snapshot of the actual original binding,
// never a successful fence or a replacement admission/lifetime lease.
func TestNativeJournalBoundaryOwnerCapability(t *testing.T) {
	var missing *DB
	var missingBackend *backenddb.DB
	if missing.NativeCommandWALPrefixOwnerOpen(nil, nil) || missingBackend.CanShareNativeCommandWALPrefix() {
		t.Fatal("nil owner granted sharing")
	}
	for _, profile := range []Profile{ProfileCommandWALRelaxed, ProfileCommandWALDurable, ProfileNoWALFast} {
		t.Run(string(profile), func(t *testing.T) {
			database, manager := nativeJournalPrefixFixture(t, profile)
			backend := database.backend
			before := database.Stats()
			const dictionaryHits = "treedb.process.read_path.outer_leaf.cache.dictionary_verified_hits"
			if value, ok := before[dictionaryHits]; !ok || value != backend.Stats()[dictionaryHits] {
				t.Fatal("original public stats lost the backend dictionary hit counter")
			}
			next := backend.CommandWALNextLSN()
			want := profile != ProfileNoWALFast
			for i := 0; i < 32; i++ {
				if got := backend.CanShareNativeCommandWALPrefix(); got != want {
					t.Fatalf("backend scheduling capability=%t want %t", got, want)
				}
				if got := manager.CanShareStandaloneJournalBoundary(); got != want {
					t.Fatalf("manager scheduling capability=%t want %t", got, want)
				}
			}
			if database.NativeCommandWALPrefixOwnerOpen(nil, database.txnOwner) ||
				database.NativeCommandWALPrefixOwnerOpen(backend, nil) ||
				database.NativeCommandWALPrefixOwnerOpen(new(backenddb.DB), database.txnOwner) ||
				database.NativeCommandWALPrefixOwnerOpen(backend, transactionowner.New()) {
				t.Fatal("foreign backend/private owner granted sharing")
			}
			database.lifecycleMu.Lock()
			refused := !backend.CanShareNativeCommandWALPrefix()
			database.lifecycleMu.Unlock()
			if !refused {
				t.Fatal("contended public lifecycle granted sharing")
			}
			after := database.Stats()
			keys := []string{"treedb.cache.checkpoint.runs", "treedb.command_wal.file_sync.calls_total"}
			if want {
				keys = append(keys, "treedb.command_wal.group_commit.forced_total", "treedb.command_wal.group_commit.syncs_total")
			}
			for _, key := range keys {
				requireNativeJournalStatDelta(t, before, after, key, 0)
			}
			if backend.CommandWALNextLSN() != next {
				t.Fatal("capability query appended a command")
			}
			if err := manager.Close(); err != nil {
				t.Fatal(err)
			}
			if manager.CanShareStandaloneJournalBoundary() {
				t.Fatal("closed manager granted sharing")
			}
			stop := make(chan struct{})
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				for {
					select {
					case <-stop:
						return
					default:
						_ = backend.CanShareNativeCommandWALPrefix()
						runtime.Gosched()
					}
				}
			}()
			err := database.Close()
			close(stop)
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("capability/Close query did not join")
			}
			if err != nil {
				t.Fatal(err)
			}
			if backend.CanShareNativeCommandWALPrefix() || database.NativeCommandWALPrefixOwnerOpen(backend, database.txnOwner) {
				t.Fatal("closed original owner granted sharing")
			}
		})
	}
}

func TestNativeJournalBoundaryCapabilityRefusesPendingClose(t *testing.T) {
	database, manager := nativeJournalPrefixFixture(t, ProfileCommandWALRelaxed)
	backend := database.backend
	if !backend.CanShareNativeCommandWALPrefix() {
		t.Fatal("fixture lacks original eligible owner")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	// A real pending Close writer must make the query refuse without waiting
	// behind it. Keep one preexisting reader until the refusal is observed.
	database.lifecycleMu.RLock()
	var once sync.Once
	release := func() { once.Do(database.lifecycleMu.RUnlock) }
	closed := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case err := <-closed:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("pending original Close did not join")
			}
		}
	})
	go func() { closed <- database.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for backend.CanShareNativeCommandWALPrefix() {
		if time.Now().After(deadline) {
			t.Fatal("query did not refuse pending lifecycle writer")
		}
		runtime.Gosched()
	}
	select {
	case err := <-closed:
		joined = true
		t.Fatalf("Close bypassed retained public reader: %v", err)
	default:
	}
	release()
	select {
	case err := <-closed:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original Close did not finish after lifecycle release")
	}
}
