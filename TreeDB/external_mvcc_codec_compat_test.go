package treedb

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
)

func TestExternalMVCCCodecIsOptInAndDoesNotReplaceEntryRevision(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Existing raw APIs continue to accept and store their caller-provided bytes
	// verbatim, even when those bytes begin with the opt-in namespace marker.
	rawKey := mvcckey.AppendNamespaceLower(nil)
	if err := db.Set(rawKey, []byte("raw")); err != nil {
		t.Fatalf("Set raw key: %v", err)
	}
	got, revision, err := db.GetVersioned(rawKey)
	if err != nil {
		t.Fatalf("GetVersioned raw key: %v", err)
	}
	if !bytes.Equal(got, []byte("raw")) || revision == LegacyEntryRevision {
		t.Fatalf("GetVersioned=(%q,%d), want raw with native EntryRevision", got, revision)
	}

	physical, err := mvcckey.Encode(rawKey, 7)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if bytes.Equal(physical, rawKey) {
		t.Fatal("MVCC encoding unexpectedly reused the raw logical key")
	}
	if got, err := db.Get(physical); err != nil || got != nil {
		t.Fatalf("Get opt-in physical key=(%x,%v), want absent,nil", got, err)
	}
}
