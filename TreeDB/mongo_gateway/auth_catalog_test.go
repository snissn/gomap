package mongogateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/mongo_gateway/wire"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/crypto/pbkdf2"
)

type failingAuthCatalogPointerBatch struct {
	batchpkg.Interface
	writeErr error
	ptr      page.ValuePtr
	closed   bool
}

func (b *failingAuthCatalogPointerBatch) SetPointer(_ []byte, ptr page.ValuePtr) error {
	b.ptr = ptr
	return nil
}

func (b *failingAuthCatalogPointerBatch) WriteSync() error { return b.writeErr }

func (b *failingAuthCatalogPointerBatch) Close() error {
	b.closed = true
	return nil
}

type failingAuthCatalogLargeValueStore struct {
	ptr      page.ValuePtr
	batch    *failingAuthCatalogPointerBatch
	released []page.ValuePtr
}

func (s *failingAuthCatalogLargeValueStore) Get([]byte) ([]byte, error) {
	return nil, treedb.ErrKeyNotFound
}
func (s *failingAuthCatalogLargeValueStore) SetSync([]byte, []byte) error {
	return batchpkg.ErrValueTooLarge
}
func (s *failingAuthCatalogLargeValueStore) AppendValueLogValues([][]byte) ([]page.ValuePtr, error) {
	return []page.ValuePtr{s.ptr}, nil
}
func (s *failingAuthCatalogLargeValueStore) ReleaseValueLogValues(ptrs []page.ValuePtr) {
	s.released = append(s.released, ptrs...)
}
func (s *failingAuthCatalogLargeValueStore) NewBatch() batchpkg.Interface { return s.batch }

func TestSetAuthCatalogValueSyncReleasesAbandonedPersistentValueLogPointer(t *testing.T) {
	writeErr := errors.New("injected early publication failure")
	ptr := page.ValuePtr{FileID: page.ValueLogFileID(7), Offset: 11, Length: 17}
	batch := &failingAuthCatalogPointerBatch{writeErr: writeErr}
	store := &failingAuthCatalogLargeValueStore{ptr: ptr, batch: batch}

	err := setAuthCatalogValueSync(store, []byte("catalog"), []byte("large authorization record"))
	if !errors.Is(err, writeErr) {
		t.Fatalf("setAuthCatalogValueSync err=%v want %v", err, writeErr)
	}
	if batch.ptr != ptr || !batch.closed {
		t.Fatalf("pointer publication ptr=%+v closed=%v want ptr=%+v closed=true", batch.ptr, batch.closed, ptr)
	}
	if !reflect.DeepEqual(store.released, []page.ValuePtr{ptr}) {
		t.Fatalf("released pointers=%v want exactly [%v]", store.released, ptr)
	}
}

func TestAuthCatalogForcedPersistentValueLogReopenAndCorruptionFailClosed(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true

	db, cleanup, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("app", "reader", []byte("reader password")); err != nil {
		t.Fatal(err)
	}
	wantRoles := []AuthRoleGrant{{Role: AuthRoleRead, Database: "app", Collection: "items"}}
	if err := catalog.SetUserRoles("app", "reader", wantRoles); err != nil {
		t.Fatal(err)
	}
	snapshot := db.AcquireSnapshot()
	entry, err := snapshot.GetEntry(authAuthorizationCatalogKey())
	snapshot.Close()
	if err != nil || entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("authorization catalog entry pointer=%+v err=%v", entry.ValuePtr, err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}

	db, cleanup, err = treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("app", "reader", []byte("reader password")); err != nil {
		t.Fatalf("forced-pointer verifier after reopen: %v", err)
	}
	if roles, err := catalog.UserRoles("app", "reader"); err != nil || !reflect.DeepEqual(roles, wantRoles) {
		t.Fatalf("forced-pointer roles after reopen=%v err=%v", roles, err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}

	var segments []string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !info.IsDir() && filepath.Base(filepath.Dir(path)) == "value_vlog" && strings.HasPrefix(info.Name(), "value-l") && strings.HasSuffix(info.Name(), ".log") {
			segments = append(segments, path)
		}
		return nil
	})
	if err != nil || len(segments) == 0 {
		t.Fatalf("persistent ValueLog segments=%v err=%v", segments, err)
	}
	for _, segment := range segments {
		if err := os.Truncate(segment, 0); err != nil {
			t.Fatal(err)
		}
	}
	db, cleanup, err = treedb.OpenBackend(opts)
	if err != nil {
		return // Refusing to open corrupted persistent ValueLog state is fail closed.
	}
	defer cleanup()
	if catalog, err = NewAuthCatalog(db); err == nil {
		t.Fatal("corrupted persistent ValueLog authorization state was accepted")
	}
}

func TestAuthCatalogPersistedEmptyAuthorizationRecordsFailClosed(t *testing.T) {
	for name, raw := range map[string][]byte{
		"legacy_missing_users": []byte(`{"version":1}`),
		"missing_users":        []byte(`{"version":2}`),
		"null_users":           []byte(`{"version":2,"users":null}`),
		"empty_users":          []byte(`{"version":2,"users":[]}`),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.SetSync(authAuthorizationCatalogKey(), raw); err != nil {
				t.Fatal(err)
			}
			if catalog, err := NewAuthCatalog(db); err == nil || catalog != nil {
				t.Fatalf("persisted empty authorization record accepted: catalog=%v err=%v", catalog, err)
			}
			if verifier, err := getAuthCatalogValue(db, authCatalogKey("admin", "bootstrap")); !errors.Is(err, treedb.ErrKeyNotFound) || len(verifier) != 0 {
				t.Fatalf("failed constructor created bootstrap verifier=%q err=%v", verifier, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if catalog, err := NewAuthCatalog(db); err == nil || catalog != nil {
				t.Fatalf("reopened persisted empty authorization record accepted: catalog=%v err=%v", catalog, err)
			}
			if verifier, err := getAuthCatalogValue(db, authCatalogKey("admin", "bootstrap")); !errors.Is(err, treedb.ErrKeyNotFound) || len(verifier) != 0 {
				t.Fatalf("reopened failed constructor created bootstrap verifier=%q err=%v", verifier, err)
			}
		})
	}
}

func TestAuthCatalogLegacyAndMismatchedIncarnationsFailClosed(t *testing.T) {
	for name, mutate := range map[string]func(*AuthCatalog, *treedb.DB) error{
		"legacy_verifier": func(catalog *AuthCatalog, db *treedb.DB) error {
			raw, err := db.Get(authCatalogKey("admin", "root"))
			if err != nil {
				return err
			}
			var record AuthUserRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			record.Version = 1
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			return db.SetSync(authCatalogKey("admin", "root"), raw)
		},
		"missing_verifier_incarnation": func(catalog *AuthCatalog, db *treedb.DB) error {
			raw, err := db.Get(authCatalogKey("admin", "root"))
			if err != nil {
				return err
			}
			var record AuthUserRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			record.Incarnation = 0
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			return db.SetSync(authCatalogKey("admin", "root"), raw)
		},
		"legacy_authorization": func(catalog *AuthCatalog, db *treedb.DB) error {
			raw, err := db.Get(authAuthorizationCatalogKey())
			if err != nil {
				return err
			}
			var record authAuthorizationRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			record.Version = 1
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			return db.SetSync(authAuthorizationCatalogKey(), raw)
		},
		"missing_authorization_incarnation": func(catalog *AuthCatalog, db *treedb.DB) error {
			raw, err := db.Get(authAuthorizationCatalogKey())
			if err != nil {
				return err
			}
			var record authAuthorizationRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			for i := range record.Users {
				record.Users[i].Incarnation = 0
			}
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			return db.SetSync(authAuthorizationCatalogKey(), raw)
		},
		"mismatched_incarnation": func(catalog *AuthCatalog, db *treedb.DB) error {
			raw, err := db.Get(authAuthorizationCatalogKey())
			if err != nil {
				return err
			}
			var record authAuthorizationRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			record.Users[0].Incarnation++
			if record.Users[0].Incarnation == 0 {
				record.Users[0].Incarnation = 1
			}
			raw, err = json.Marshal(record)
			if err != nil {
				return err
			}
			return db.SetSync(authAuthorizationCatalogKey(), raw)
		},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			db, err := treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := NewAuthCatalog(db)
			if err != nil {
				t.Fatal(err)
			}
			if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
				t.Fatal(err)
			}
			if err := mutate(catalog, db); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db, err = treedb.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			reopened, err := NewAuthCatalog(db)
			if name == "legacy_verifier" || name == "missing_verifier_incarnation" {
				if err != nil {
					t.Fatalf("unrelated catalog construction failed: %v", err)
				}
				if _, verifyErr := reopened.VerifyPassword("admin", "root", []byte("root password")); verifyErr == nil {
					t.Fatal("legacy verifier authenticated")
				}
				if upsertErr := reopened.UpsertPassword("admin", "root", []byte("replacement password")); upsertErr == nil {
					t.Fatal("trusted upsert silently replaced a legacy verifier")
				}
				return
			}
			if name == "mismatched_incarnation" {
				if err != nil {
					t.Fatalf("valid but mismatched records should open for offline repair: %v", err)
				}
				identity, verifyErr := reopened.VerifyPassword("admin", "root", []byte("root password"))
				if verifyErr != nil {
					t.Fatal(verifyErr)
				}
				if _, rolesErr := reopened.effectiveRolesForUser(identity); rolesErr == nil {
					t.Fatal("mismatched verifier and grant incarnations authorized")
				}
				if upsertErr := reopened.UpsertPassword("admin", "candidate", []byte("candidate password")); !errors.Is(upsertErr, errNoUsableServerAdministrator) {
					t.Fatalf("mismatched catalog bootstrap err=%v want offline repair requirement", upsertErr)
				}
				return
			}
			if err == nil || reopened != nil {
				t.Fatalf("legacy catalog opened: catalog=%v err=%v", reopened, err)
			}
		})
	}
}

func TestAuthCatalogMissingAuthorizationRecordIsBootstrapState(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "bootstrap", []byte("bootstrap password")); err != nil {
		t.Fatal(err)
	}
	roles, err := catalog.UserRoles("admin", "bootstrap")
	if err != nil || !reflect.DeepEqual(roles, []AuthRoleGrant{{Role: AuthRoleServerAdmin}}) {
		t.Fatalf("missing-key bootstrap roles=%v err=%v", roles, err)
	}
}

func TestAuthCatalogUserIncarnationPersistsAcrossReopenAndRotation(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "root", []byte("root password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "worker", []byte("worker password")); err != nil {
		t.Fatal(err)
	}
	before, err := catalog.VerifyPassword("admin", "worker", []byte("worker password"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	afterReopen, err := catalog.VerifyPassword("admin", "worker", []byte("worker password"))
	if err != nil {
		t.Fatal(err)
	}
	if afterReopen != before {
		t.Fatalf("reopen changed identity: before=%+v after=%+v", before, afterReopen)
	}
	if err := catalog.UpsertPassword("admin", "worker", []byte("rotated password")); err != nil {
		t.Fatal(err)
	}
	afterRotation, err := catalog.VerifyPassword("admin", "worker", []byte("rotated password"))
	if err != nil {
		t.Fatal(err)
	}
	if afterRotation != before {
		t.Fatalf("password rotation changed identity: before=%+v after=%+v", before, afterRotation)
	}
	if err := catalog.DropUser("admin", "worker"); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "worker", []byte("recreated password")); err != nil {
		t.Fatal(err)
	}
	afterRecreate, err := catalog.VerifyPassword("admin", "worker", []byte("recreated password"))
	if err != nil {
		t.Fatal(err)
	}
	if afterRecreate == before {
		t.Fatalf("account recreation reused identity: before=%+v after=%+v", before, afterRecreate)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	afterRecreateReopen, err := catalog.VerifyPassword("admin", "worker", []byte("recreated password"))
	if err != nil {
		t.Fatal(err)
	}
	if afterRecreateReopen != afterRecreate {
		t.Fatalf("reopen changed recreated identity: before=%+v after=%+v", afterRecreate, afterRecreateReopen)
	}
}

func TestAuthCatalogBootstrapRefusesUnusableNonemptyAdministratorCatalog(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	record := authAuthorizationRecord{Version: authAuthorizationVersion, Users: []authRoleAssignment{{
		Username:    "missing-verifier",
		AuthDB:      "admin",
		Incarnation: 1,
		Roles:       []AuthRoleGrant{{Role: AuthRoleServerAdmin}},
	}}}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync(authAuthorizationCatalogKey(), raw); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	err = catalog.UpsertPassword("admin", "candidate", []byte("candidate password"))
	if !errors.Is(err, errNoUsableServerAdministrator) {
		t.Fatalf("UpsertPassword err=%v want offline repair requirement", err)
	}
	if catalog.userExists("admin", "candidate") {
		t.Fatal("recovery attempt created a credential in a nonempty unusable administrator catalog")
	}
	if roles, err := catalog.UserRoles("admin", "candidate"); err != nil || len(roles) != 0 {
		t.Fatalf("recovery attempt roles=%v err=%v", roles, err)
	}
}

func addBackupAuthAdministrator(t *testing.T, catalog *AuthCatalog) {
	t.Helper()
	if err := catalog.UpsertPassword("admin", "backup-admin", []byte("backup administrator password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetUserRoles("admin", "backup-admin", []AuthRoleGrant{{Role: AuthRoleServerAdmin}}); err != nil {
		t.Fatal(err)
	}
}

func TestAuthCatalogDurableVerifierRotationAndDisable(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	addBackupAuthAdministrator(t, catalog)
	raw, err := db.Get(authCatalogKey("admin", "alice"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("correct horse battery staple")) {
		t.Fatal("catalog stored plaintext password")
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("wrong")); err == nil {
		t.Fatal("wrong password accepted")
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("rotated password")); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("correct horse battery staple")); err == nil {
		t.Fatal("old password survived rotation")
	}
	if err := catalog.SetEnabled("admin", "alice", false); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("rotated password")); err == nil {
		t.Fatal("disabled user authenticated")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("rotated password")); err == nil {
		t.Fatal("disabled user authenticated after reopen")
	}
}

func TestAuthCatalogRejectsCorruptOrOversizedRecords(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	if err := db.SetSync(authCatalogKey("admin", "bad"), []byte(`{"version":1,"username":"bad"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "bad", []byte("password")); err == nil {
		t.Fatal("corrupt record authenticated")
	}
	if err := catalog.UpsertPassword("admin", "huge", make([]byte, maxAuthPasswordBytes+1)); err == nil {
		t.Fatal("oversized password accepted")
	}
}

func TestAuthCatalogRejectsInvalidUTF8IdentitiesBeforePersistence(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	bad := string([]byte{0xff})
	if err := catalog.UpsertPassword("admin", bad, []byte("password")); err == nil {
		t.Fatal("invalid UTF-8 username persisted")
	}
	if err := catalog.UpsertPassword(bad, "alice", []byte("password")); err == nil {
		t.Fatal("invalid UTF-8 auth DB persisted")
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	payload := append([]byte("n,,n="), []byte(bad)...)
	payload = append(payload, []byte(",r=nonce")...)
	response := serveCommand(t, server, 77, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: payload}}, {Key: "$db", Value: "admin"}})
	assertCommandError(t, response, "AuthenticationFailed")
}

func TestHelloAdvertisesOnlySCRAMSHA256WhenRequested(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	requested := serveCommand(t, server, 78, bson.D{{Key: "hello", Value: 1}, {Key: "saslSupportedMechs", Value: "admin.alice"}, {Key: "$db", Value: "admin"}})
	mechs := bson.Raw(requested).Lookup("saslSupportedMechs").Array()
	values, err := mechs.Values()
	if err != nil || len(values) != 1 {
		t.Fatalf("mechanisms=%s err=%v", bson.Raw(requested), err)
	}
	if got, ok := values[0].StringValueOK(); !ok || got != "SCRAM-SHA-256" {
		t.Fatalf("mechanisms=%s", bson.Raw(requested))
	}
	plain := serveCommand(t, server, 79, bson.D{{Key: "hello", Value: 1}, {Key: "$db", Value: "admin"}})
	if bson.Raw(plain).Lookup("saslSupportedMechs").Type != 0 {
		t.Fatalf("unrequested mechanisms advertised: %s", bson.Raw(plain))
	}
	unsupported := serveCommand(t, server, 80, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-1"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=alice,r=nonce")}}, {Key: "$db", Value: "admin"}})
	assertCommandError(t, unsupported, "AuthenticationFailed")
}

func TestAuthCatalogSASLprepAndAtomicDisableRotation(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	// SASLprep maps non-ASCII space, which is what the official driver applies
	// before producing its SCRAM proof.
	if err := catalog.UpsertPassword("admin", "alice", []byte("p\u00e4ss\u00a0word")); err != nil {
		t.Fatal(err)
	}
	addBackupAuthAdministrator(t, catalog)
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("p\u00e4ss word")); err != nil {
		t.Fatalf("SASLprep equivalent rejected: %v", err)
	}
	if err := catalog.UpsertPassword("admin", "bad", []byte("bad\x00password")); err == nil {
		t.Fatal("prohibited password accepted")
	}

	entered, release := make(chan struct{}), make(chan struct{})
	catalog.beforeSetEnabledWrite = func() { close(entered); <-release }
	setDone := make(chan error, 1)
	go func() { setDone <- catalog.SetEnabled("admin", "alice", false) }()
	<-entered
	rotateDone := make(chan error, 1)
	go func() { rotateDone <- catalog.UpsertPassword("admin", "alice", []byte("fresh password")) }()
	select {
	case err := <-rotateDone:
		t.Fatalf("rotation bypassed SetEnabled atomic section: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-setDone; err != nil {
		t.Fatal(err)
	}
	if err := <-rotateDone; err != nil {
		t.Fatal(err)
	}
	catalog.beforeSetEnabledWrite = nil
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("fresh password")); err != nil {
		t.Fatalf("rotation lost or left disabled: %v", err)
	}
	if err := catalog.SetEnabled("admin", "alice", false); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetEnabled("admin", "alice", true); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.VerifyPassword("admin", "alice", []byte("fresh password")); err != nil {
		t.Fatalf("re-enable lost fresh verifier: %v", err)
	}
}

func TestAuthCatalogBackendScopedMutationLockPreventsLostRotation(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	setter, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	rotator, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := setter.UpsertPassword("admin", "alice", []byte("old password")); err != nil {
		t.Fatal(err)
	}
	addBackupAuthAdministrator(t, setter)
	entered, release := make(chan struct{}), make(chan struct{})
	setter.beforeSetEnabledWrite = func() { close(entered); <-release }
	setDone := make(chan error, 1)
	go func() { setDone <- setter.SetEnabled("admin", "alice", false) }()
	<-entered
	rotateDone := make(chan error, 1)
	go func() { rotateDone <- rotator.UpsertPassword("admin", "alice", []byte("fresh password")) }()
	select {
	case err := <-rotateDone:
		t.Fatalf("second catalog rotation bypassed backend mutation lock: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-setDone; err != nil {
		t.Fatal(err)
	}
	if err := <-rotateDone; err != nil {
		t.Fatal(err)
	}
	if _, err := rotator.VerifyPassword("admin", "alice", []byte("fresh password")); err != nil {
		t.Fatalf("fresh verifier was lost or disabled: %v", err)
	}
}

func TestAuthenticationEnabledWithoutCatalogFailsClosed(t *testing.T) {
	server := NewServer()
	server.AuthenticationEnabled = true
	denied, err := server.commandResponse(context.Background(), "find", mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, denied, "Unauthorized")
	start, err := server.commandResponse(context.Background(), "saslStart", mustDocument(t, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=alice,r=nonce")}}, {Key: "$db", Value: "admin"}}), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, start, "AuthenticationFailed")
}

func TestSASLStartRejectsMandatoryExtensionAndCountsFailures(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	for _, payload := range []string{"n,,n=alice,r=nonce,m=unsupported", "n,,n=alice,r=nonce"} {
		mechanism := "SCRAM-SHA-256"
		if payload == "n,,n=alice,r=nonce" {
			mechanism = "SCRAM-SHA-1"
		}
		response, err := server.commandResponse(context.Background(), "saslStart", mustDocument(t, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: mechanism}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(payload)}}, {Key: "$db", Value: "admin"}}), nil, 53)
		if err != nil {
			t.Fatal(err)
		}
		assertCommandError(t, response, "AuthenticationFailed")
	}
	if got := server.authFailures.Load(); got != 2 {
		t.Fatalf("auth failures=%d want 2", got)
	}
}

func TestSyntheticSCRAMVerifierIsServerAndUserScoped(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	a1, err := catalog.syntheticSCRAMRecord("admin", "missing")
	if err != nil {
		t.Fatal(err)
	}
	a2, err := catalog.syntheticSCRAMRecord("admin", "other")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a1.Salt, a2.Salt) {
		t.Fatal("synthetic verifier salt is not user scoped")
	}
	if a1.Iterations != defaultSCRAMIterations || len(a1.StoredKey) != sha256.Size || len(a1.ServerKey) != sha256.Size {
		t.Fatalf("bad synthetic record: %+v", a1)
	}
}

func TestAuthCatalogSyntheticVerifierSecretSurvivesReopenAndFailsClosedOnCorruption(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	first, err := catalog.syntheticSCRAMRecord("admin", "missing")
	if err != nil {
		t.Fatal(err)
	}
	other, err := catalog.syntheticSCRAMRecord("admin", "other")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Salt, other.Salt) {
		t.Fatal("synthetic salts do not differentiate identities")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err = NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := catalog.syntheticSCRAMRecord("admin", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Salt, reopened.Salt) || !bytes.Equal(first.StoredKey, reopened.StoredKey) || !bytes.Equal(first.ServerKey, reopened.ServerKey) {
		t.Fatal("synthetic verifier changed across catalog reopen")
	}
	if err := db.SetSync(authCatalogSyntheticSecretKey(), []byte(`{"version":1,"secret":"bad"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAuthCatalog(db); err == nil {
		t.Fatal("corrupt synthetic secret opened fail-open")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAuthCatalogSyntheticSecretRejectsEveryPresentMalformedValue(t *testing.T) {
	for _, raw := range [][]byte{nil, []byte("null"), []byte(`{"version":3,"secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`), []byte(`{"version":1,"secret":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}`), []byte(`{"version":2,"secret":"AQI="}`)} {
		db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SetSync(authCatalogSyntheticSecretKey(), raw); err != nil {
			t.Fatal(err)
		}
		if _, err := NewAuthCatalog(db); err == nil {
			t.Fatalf("accepted malformed synthetic secret %q", raw)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAuthCatalogConcurrentConstructorsConvergeOnSyntheticSecret(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const constructors = 24
	catalogs := make(chan *AuthCatalog, constructors)
	errs := make(chan error, constructors)
	for range constructors {
		go func() {
			c, err := NewAuthCatalog(db)
			if err != nil {
				errs <- err
				return
			}
			catalogs <- c
		}()
	}
	var first []byte
	for range constructors {
		select {
		case err := <-errs:
			t.Fatal(err)
		case c := <-catalogs:
			r, err := c.syntheticSCRAMRecord("admin", "same")
			if err != nil {
				t.Fatal(err)
			}
			if first == nil {
				first = r.Salt
			} else if !bytes.Equal(first, r.Salt) {
				t.Fatal("concurrent catalogs diverged")
			}
		}
	}
}

func TestSCRAMPayloadWrongTypesFailClosedOverWire(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	for _, payload := range []any{"not-binary", bson.D{{Key: "x", Value: 1}}, nil} {
		response := serveCommand(t, server, 100, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: payload}, {Key: "$db", Value: "admin"}})
		assertCommandError(t, response, "AuthenticationFailed")
		if got := serveCommand(t, server, 101, bson.D{{Key: "hello", Value: 1}, {Key: "$db", Value: "admin"}}); bson.Raw(got).Lookup("ok").Double() != 1 {
			t.Fatalf("connection became unusable: %s", bson.Raw(got))
		}
	}
	response := serveCommand(t, server, 102, bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: 1}, {Key: "payload", Value: "not-binary"}, {Key: "$db", Value: "admin"}})
	assertCommandError(t, response, "AuthenticationFailed")
}

func TestSCRAMClientFinalNonceMustMatchExactly(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	password := []byte("correct password")
	if err := catalog.UpsertPassword("admin", "alice", password); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	bare := "n=alice,r=clientnonce"
	startRaw := mustDocument(t, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,," + bare)}}, {Key: "$db", Value: "admin"}})
	started, err := server.commandResponse(context.Background(), "saslStart", startRaw, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	start := bson.Raw(started)
	id := start.Lookup("conversationId").Int32()
	_, payload := start.Lookup("payload").Binary()
	serverFirst := string(payload)
	fields, ok := scramFields(serverFirst)
	if !ok {
		t.Fatal(serverFirst)
	}
	salt, err := base64.StdEncoding.DecodeString(fields["s"])
	if err != nil {
		t.Fatal(err)
	}
	var iterations int
	if _, err := fmt.Sscanf(fields["i"], "%d", &iterations); err != nil {
		t.Fatal(err)
	}
	salted := pbkdf2.Key(password, salt, iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	// This proof is otherwise valid for the altered final message. A prefix
	// check would accept it; SCRAM requires the server nonce verbatim.
	withoutProof := "c=biws,r=" + fields["r"] + "suffix"
	signature := hmacSHA256(stored[:], []byte(bare+","+serverFirst+","+withoutProof))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	continueRaw := mustDocument(t, bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: id}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))}}, {Key: "$db", Value: "admin"}})
	response, err := server.commandResponse(context.Background(), "saslContinue", continueRaw, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, response, "AuthenticationFailed")
}

func TestSCRAMConversationCapExpiryAndReplayFailClosed(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	owner := int64(44)
	start := func(nonce string) bson.Raw {
		raw := mustDocument(t, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=missing,r=" + nonce)}}, {Key: "$db", Value: "admin"}})
		response, err := server.commandResponse(context.Background(), "saslStart", raw, nil, owner)
		if err != nil {
			t.Fatal(err)
		}
		return bson.Raw(response)
	}
	for i := 0; i < maxSCRAMConversationsPerConnection; i++ {
		if got := start(fmt.Sprintf("n%d", i)); got.Lookup("ok").Double() != 1 {
			t.Fatalf("start %d: %s", i, got)
		}
	}
	assertCommandError(t, start("overflow"), "AuthenticationFailed")
	value, ok := server.authConnections.Load(owner)
	if !ok {
		t.Fatal("missing auth connection state")
	}
	state := value.(*authConnectionState)
	state.mu.Lock()
	for id, c := range state.conversations {
		c.issuedAt = time.Now().Add(-maxSCRAMConversationAge - time.Second)
		state.conversations[id] = c
	}
	state.mu.Unlock()
	first := start("after-expiry")
	id := first.Lookup("conversationId").Int32()
	bad := mustDocument(t, bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: id}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("c=biws,r=wrong,p=" + base64.StdEncoding.EncodeToString(make([]byte, 32)))}}, {Key: "$db", Value: "admin"}})
	response, err := server.commandResponse(context.Background(), "saslContinue", bad, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, response, "AuthenticationFailed")
	replayed, err := server.commandResponse(context.Background(), "saslContinue", bad, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, replayed, "AuthenticationFailed")
}

func TestSCRAMSHA256EstablishesConnectionIdentityAndGatesCommands(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	password := []byte("correct horse battery staple")
	if err := catalog.UpsertPassword("admin", "alice", password); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	owner := int64(99)
	denied, err := server.commandResponse(context.Background(), "find", mustDocument(t, bson.D{{Key: "find", Value: "users"}, {Key: "$db", Value: "app"}}), nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if bson.Raw(denied).Lookup("code").Int32() != 13 {
		t.Fatalf("unauthenticated find=%s", bson.Raw(denied))
	}
	clientFirstBare := "n=alice,r=clientnonce"
	startRaw, _ := marshalDocument(bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,," + clientFirstBare)}}, {Key: "$db", Value: "admin"}})
	start, err := server.commandResponse(context.Background(), "saslStart", startRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	startDoc := bson.Raw(start)
	if startDoc.Lookup("ok").Double() != 1 {
		t.Fatalf("start=%s", startDoc)
	}
	id := startDoc.Lookup("conversationId").Int32()
	_, serverFirstBytes := startDoc.Lookup("payload").Binary()
	serverFirst := string(serverFirstBytes)
	parts, ok := scramFields(serverFirst)
	if !ok {
		t.Fatal(serverFirst)
	}
	salt, err := base64.StdEncoding.DecodeString(parts["s"])
	if err != nil {
		t.Fatal(err)
	}
	var count int
	_, err = fmt.Sscanf(parts["i"], "%d", &count)
	if err != nil {
		t.Fatal(err)
	}
	salted := pbkdf2.Key(password, salt, count, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + parts["r"]
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	signature := hmacSHA256(stored[:], []byte(authMessage))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	continueRaw, _ := marshalDocument(bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: id}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))}}, {Key: "$db", Value: "admin"}})
	continued, err := server.commandResponse(context.Background(), "saslContinue", continueRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if got := bson.Raw(continued); got.Lookup("done").Boolean() != true || got.Lookup("ok").Double() != 1 {
		t.Fatalf("continue=%s", got)
	}
	_, serverSignature, ok := bson.Raw(continued).Lookup("payload").BinaryOK()
	if !ok || string(serverSignature) != "v="+base64.StdEncoding.EncodeToString(hmacSHA256(hmacSHA256(salted, []byte("Server Key")), []byte(authMessage))) {
		t.Fatalf("server signature=%q", serverSignature)
	}
	status, err := server.commandResponse(context.Background(), "connectionStatus", mustDocument(t, bson.D{{Key: "connectionStatus", Value: 1}, {Key: "$db", Value: "admin"}}), nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bson.Raw(status).String(), "alice") {
		t.Fatalf("connection status missed identity: %s", bson.Raw(status))
	}
}

func TestSCRAMCompletionDoesNotAuthenticateReusedOwner(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	password := []byte("correct horse battery staple")
	if err := catalog.UpsertPassword("admin", "alice", password); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	entered, release := make(chan struct{}), make(chan struct{})
	server.beforeSCRAMIdentityStore = func() { close(entered); <-release }
	done := make(chan struct{})
	go func() { authenticateOwner(t, server, 92, password); close(done) }()
	<-entered
	server.ReleaseOwner(92)
	newState := server.authState(92)
	close(release)
	<-done
	if newState.user.Load() != nil || server.authenticated(92) {
		t.Fatal("old SCRAM completion authenticated the replacement owner state")
	}
}

func TestReleaseOwnerClearsAuthenticationAndCursorsBeforeOwnerReuse(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	password := []byte("correct horse battery staple")
	if err := catalog.UpsertPassword("admin", "alice", password); err != nil {
		t.Fatal(err)
	}
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbuffered", true: "buffered"}[buffered], func(t *testing.T) {
			server := NewServer()
			server.AuthenticationEnabled, server.AuthCatalog = true, catalog
			owner := int64(91)
			authenticateOwner(t, server, owner, password)
			cursorID, err := server.openRetainedCursor("app.items", []wire.Document{mustDocument(t, bson.D{{Key: "_id", Value: "cursor"}})}, compiledProjection{}, owner)
			if err != nil || cursorID == 0 {
				t.Fatalf("open retained cursor id=%d err=%v", cursorID, err)
			}
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() { defer wg.Done(); server.ReleaseOwner(owner) }()
			}
			wg.Wait()
			if server.authenticated(owner) {
				t.Fatal("released owner retained authentication identity")
			}
			server.cursorMu.Lock()
			_, cursorStillPresent := server.cursors[cursorID]
			server.cursorMu.Unlock()
			if cursorStillPresent {
				t.Fatal("released owner retained cursor")
			}
			response := serveOwnedCommand(t, server, owner, buffered, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}})
			assertCommandError(t, response, "Unauthorized")
		})
	}
}

func TestServeOneWithOwnerRejectsZeroBeforeAuthenticationOrCursorDispatch(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, err := NewAuthCatalog(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "alice", []byte("correct horse battery staple")); err != nil {
		t.Fatal(err)
	}
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "unbuffered", true: "buffered"}[buffered], func(t *testing.T) {
			server := NewServer()
			server.AuthenticationEnabled, server.AuthCatalog = true, catalog
			for _, command := range []bson.D{
				{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=alice,r=zero-owner-nonce")}}, {Key: "$db", Value: "admin"}},
				{{Key: "find", Value: "items"}, {Key: "batchSize", Value: 1}, {Key: "$db", Value: "app"}},
			} {
				err, readBytes := serveOwnedCommandError(t, server, 0, buffered, command)
				if !errors.Is(err, errInvalidCursorOwner) {
					t.Fatalf("owner-zero command error=%v want %v", err, errInvalidCursorOwner)
				}
				if readBytes != 0 {
					t.Fatalf("owner-zero command consumed %d bytes before rejection", readBytes)
				}
			}
			if _, ok := server.authConnections.Load(0); ok || server.authenticated(0) {
				t.Fatal("owner zero retained authentication state")
			}
			server.cursorMu.Lock()
			cursorCount := len(server.cursors)
			server.cursorMu.Unlock()
			if cursorCount != 0 {
				t.Fatalf("owner zero retained %d cursor(s)", cursorCount)
			}
			server.ReleaseOwner(0)
			if err, _ := serveOwnedCommandError(t, server, 0, buffered, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}); !errors.Is(err, errInvalidCursorOwner) {
				t.Fatalf("reused owner zero error=%v want %v", err, errInvalidCursorOwner)
			}
		})
	}
}

func authenticateOwner(t *testing.T, server *Server, owner int64, password []byte) {
	authenticateUser(t, server, owner, "alice", password)
}

func authenticateUser(t *testing.T, server *Server, owner int64, username string, password []byte) {
	t.Helper()
	clientFirstBare := "n=" + username + ",r=owner-reuse-nonce"
	startRaw := mustDocument(t, bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,," + clientFirstBare)}}, {Key: "$db", Value: "admin"}})
	start, err := server.commandResponse(context.Background(), "saslStart", startRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	startDoc := bson.Raw(start)
	conversationID, ok := startDoc.Lookup("conversationId").Int32OK()
	if !ok || startDoc.Lookup("ok").Double() != 1 {
		t.Fatalf("saslStart=%s", startDoc)
	}
	_, payload, ok := startDoc.Lookup("payload").BinaryOK()
	if !ok {
		t.Fatalf("saslStart payload=%s", startDoc)
	}
	serverFirst := string(payload)
	parts, ok := scramFields(serverFirst)
	if !ok {
		t.Fatal(serverFirst)
	}
	salt, err := base64.StdEncoding.DecodeString(parts["s"])
	if err != nil {
		t.Fatal(err)
	}
	var iterations int
	if _, err := fmt.Sscanf(parts["i"], "%d", &iterations); err != nil {
		t.Fatal(err)
	}
	salted := pbkdf2.Key(password, salt, iterations, sha256.Size, sha256.New)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + parts["r"]
	signature := hmacSHA256(stored[:], []byte(clientFirstBare+","+serverFirst+","+withoutProof))
	proof := make([]byte, len(clientKey))
	for i := range proof {
		proof[i] = clientKey[i] ^ signature[i]
	}
	continueRaw := mustDocument(t, bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: conversationID}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))}}, {Key: "$db", Value: "admin"}})
	continued, err := server.commandResponse(context.Background(), "saslContinue", continueRaw, nil, owner)
	if err != nil {
		t.Fatal(err)
	}
	if bson.Raw(continued).Lookup("ok").Double() != 1 {
		t.Fatalf("saslContinue=%s", bson.Raw(continued))
	}
}

func serveOwnedCommand(t *testing.T, server *Server, owner int64, buffered bool, command bson.D) wire.Document {
	t.Helper()
	raw := mustDocument(t, command)
	requestID := int32(931)
	request, err := wire.AppendMsgMessage(nil, requestID, 0, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(request)}
	if buffered {
		err = server.ServeOneWithOwnerBuffered(rw, owner, &ServeBuffers{})
	} else {
		err = server.ServeOneWithOwner(rw, owner)
	}
	if err != nil {
		t.Fatal(err)
	}
	response, err := readMsgResponseResult(rw.w.Bytes(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func serveOwnedCommandError(t *testing.T, server *Server, owner int64, buffered bool, command bson.D) (error, int) {
	t.Helper()
	raw := mustDocument(t, command)
	request, err := wire.AppendMsgMessage(nil, 932, 0, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(request)
	rw := &readWriter{r: reader}
	if buffered {
		return server.ServeOneWithOwnerBuffered(rw, owner, &ServeBuffers{}), len(request) - reader.Len()
	}
	return server.ServeOneWithOwner(rw, owner), len(request) - reader.Len()
}

// Exercise the OP_MSG fast find path, cursor commands, write admission, and
// legacy OP_QUERY through the wire server rather than commandResponse alone.
func TestAuthenticationAdmissionCoversWireAndCursorCommandPaths(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	for requestID, doc := range map[int32]bson.D{
		1: {{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}},
		2: {{Key: "getMore", Value: int64(42)}, {Key: "collection", Value: "items"}, {Key: "$db", Value: "app"}},
		3: {{Key: "killCursors", Value: "items"}, {Key: "cursors", Value: bson.A{int64(42)}}, {Key: "$db", Value: "app"}},
		4: {{Key: "insert", Value: "items"}, {Key: "documents", Value: bson.A{bson.D{{Key: "_id", Value: "x"}}}}, {Key: "$db", Value: "app"}},
	} {
		assertCommandError(t, serveCommand(t, server, requestID, doc), "Unauthorized")
	}
	query, err := wire.AppendQueryMessage(nil, 9, 0, 0, "admin.$cmd", 0, -1, mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}}), nil)
	if err != nil {
		t.Fatal(err)
	}
	rw := &readWriter{r: bytes.NewReader(query)}
	if err := server.ServeOneWithOwner(rw, 71); err != nil {
		t.Fatal(err)
	}
	_, body, err := wire.ReadMessage(bytes.NewReader(rw.w.Bytes()), 0)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := wire.ParseReply(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Documents) != 1 {
		t.Fatalf("reply documents=%d", len(reply.Documents))
	}
	assertCommandError(t, reply.Documents[0], "Unauthorized")
	findRaw := mustDocument(t, bson.D{{Key: "find", Value: "items"}, {Key: "$db", Value: "app"}})
	fastResponse, err := server.findMsgResponse(context.Background(), findRaw, 11, 10, 72)
	if err != nil {
		t.Fatal(err)
	}
	fastDoc, err := readMsgResponseResult(fastResponse, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, fastDoc, "Unauthorized")
}

func TestSCRAMUnknownUserKeepsChallengeShapeThenFailsGenerically(t *testing.T) {
	db, err := treedb.Open(treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	catalog, _ := NewAuthCatalog(db)
	if err := catalog.UpsertPassword("admin", "alice", []byte("password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpsertPassword("admin", "disabled", []byte("password")); err != nil {
		t.Fatal(err)
	}
	if err := catalog.SetEnabled("admin", "disabled", false); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync(authCatalogKey("admin", "corrupt"), []byte(`{bad`)); err != nil {
		t.Fatal(err)
	}
	server := NewServer()
	server.AuthenticationEnabled, server.AuthCatalog = true, catalog
	var unknownConversationID int32
	for owner, username := range map[int64]string{1: "alice", 2: "unknown", 3: "disabled", 4: "corrupt"} {
		raw, _ := marshalDocument(bson.D{{Key: "saslStart", Value: 1}, {Key: "mechanism", Value: "SCRAM-SHA-256"}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("n,,n=" + username + ",r=nonce")}}, {Key: "$db", Value: "admin"}})
		response, err := server.commandResponse(context.Background(), "saslStart", raw, nil, owner)
		if err != nil {
			t.Fatal(err)
		}
		if got := bson.Raw(response); got.Lookup("ok").Double() != 1 || got.Lookup("done").Boolean() {
			t.Fatalf("%s saslStart shape=%s", username, got)
		}
		if username == "unknown" {
			unknownConversationID = bson.Raw(response).Lookup("conversationId").Int32()
		}
	}
	invalid, _ := marshalDocument(bson.D{{Key: "saslContinue", Value: 1}, {Key: "conversationId", Value: unknownConversationID}, {Key: "payload", Value: bson.Binary{Subtype: 0, Data: []byte("c=biws,r=nonce,p=AAAA")}}, {Key: "$db", Value: "admin"}})
	response, err := server.commandResponse(context.Background(), "saslContinue", invalid, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertCommandError(t, response, "AuthenticationFailed")
}
