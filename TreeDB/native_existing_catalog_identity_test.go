package treedb

import (
	"github.com/snissn/gomap/TreeDB/collections"
	"testing"
)

func TestNativeExistingCollectionCatalogIdentity(t *testing.T) {
	database, c := nativeCommandWALGroupFixture(t, false)
	backend := database.backend
	manager := collections.NewCollectionManager(backend)
	defer manager.Close()
	other, err := manager.OpenCollection("rows")
	if err != nil {
		t.Fatal(err)
	}
	if !c.SameCachedCatalog(other) || !other.SameCachedCatalog(c) {
		t.Fatal("same native incarnation/schema refused existing coalescer")
	}
	if c.SameCachedCatalog(nil) || (*collections.Collection)(nil).SameCachedCatalog(c) {
		t.Fatal("nil identity accepted")
	}
	second, foreign := nativeCommandWALGroupFixture(t, false)
	_ = second
	if c.SameCachedCatalog(foreign) {
		t.Fatal("foreign backend same collection name accepted")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if c.SameCachedCatalog(other) {
		t.Fatal("closed native facade accepted")
	}
}
