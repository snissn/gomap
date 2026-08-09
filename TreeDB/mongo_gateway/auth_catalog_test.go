package mongogateway

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

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
