package mongogateway

import (
	"bytes"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestStandaloneSharedCOWRejectsMissingCatalogIdentityAtOpen(t *testing.T) {
	dir := t.TempDir()
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	backend, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	if _, err = manager.CreateCollection(&collections.CollectionMeta{Name: "db.old"}); err != nil {
		t.Fatal(err)
	}
	c, err := manager.OpenCollection("db.old")
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"n":1}`)
	if _, err = c.Insert([]byte("a"), document); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	// Current legacy producers already write identities. Model an older
	// pre-alpha catalog by removing just those records through its system root.
	missingIdentity := memtable.NewBTree()
	missingIdentity.Delete([]byte("collections/native/v1/next-incarnation"))
	missingIdentity.Delete([]byte("collections/native/v1/incarnation/db.old"))
	_, _, err = backend.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return missingIdentity.NewIterator(nil, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = cleanup(); err != nil {
		t.Fatal(err)
	}
	backend, cleanup, err = treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(backend)
	c, err = manager.OpenCollection("db.old")
	if err != nil {
		t.Fatalf("legacy fixture did not persist: %v", err)
	}
	if got, err := c.Get([]byte("a")); err != nil || !bytes.Equal(got, document) {
		t.Fatalf("legacy fixture did not persist document: got=%q err=%v", got, err)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}

	server, err := OpenStandaloneServer(StandaloneOptions{Dir: dir, Profile: treedb.ProfileNoWALFast})
	if err == nil {
		if server != nil {
			_ = server.Close()
		}
		t.Fatal("legacy catalog accepted by the shared COW opener")
	}
	if server != nil || !strings.Contains(err.Error(), "shared COW catalog") {
		t.Fatalf("missing catalog preflight error: server=%v err=%v", server, err)
	}
	// Refusal releases the original owner's lock and preserves readable data.
	backend, cleanup, err = treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	manager = collections.NewCollectionManager(backend)
	defer manager.Close()
	c, err = manager.OpenCollection("db.old")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Get([]byte("a"))
	if err != nil || !bytes.Equal(got, document) {
		t.Fatalf("legacy data changed on refused open: got=%q err=%v", got, err)
	}
}
