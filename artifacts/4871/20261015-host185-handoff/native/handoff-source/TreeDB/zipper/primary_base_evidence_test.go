package zipper

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestPrimaryBaseEvidenceExpiresWithOriginalConstruction(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 256*page.PageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err = p.Alloc(3); err != nil {
		t.Fatal(err)
	}
	image, err := p.GetForWrite(2)
	if err != nil {
		t.Fatal(err)
	}
	b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	b.SetPageID(2)
	b.FinishNoNode()
	a, err := primaryarena.OpenCapsule(filepath.Join(dir, "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.SetMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5); err != nil {
		t.Fatal(err)
	}
	if err = p.AttachPrimaryBankPager(a.Pager()); err != nil {
		t.Fatal(err)
	}
	z := New(p, &MockAllocator{p: p})
	z.SetPrimaryArena(a)
	z.SetPrimaryConstructorV6(rootpublication.NewPrimaryConstructionTransactionV6)
	var construction primaryarena.ReadRootConstructionV6
	var abortErr error
	z.SetPrimaryConstructionOutputV6(&construction, &abortErr)
	ops := batch.New(nil, page.PageSize)
	defer ops.Close()
	if err = ops.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("key"), Value: []byte("value"), Revision: 1}}); err != nil {
		t.Fatal(err)
	}
	result, err := z.ApplyWithOptions(2, ops, ApplyOptions{CollectOldPointerRefs: true})
	if err != nil {
		t.Fatal(err)
	}
	if construction == nil {
		t.Fatal("missing original construction")
	}
	defer construction.Abort()
	if !z.PreservesMaterializedBaseV6(&result, 2, result.RootID) {
		t.Fatal("actual sealed apply lost base evidence")
	}
	if z.PreservesMaterializedBaseV6(&result, 3, result.RootID) || z.PreservesMaterializedBaseV6(&result, 2, result.RootID+1) {
		t.Fatal("receipt accepted different apply roots")
	}
	other := New(nil, nil)
	if other.PreservesMaterializedBaseV6(&result, 2, result.RootID) {
		t.Fatal("receipt accepted another index")
	}
	empty := ApplyResult{RootID: result.RootID, OldPointerRefsCollected: true}
	if z.PreservesMaterializedBaseV6(&empty, 2, result.RootID) {
		t.Fatal("caller-assigned result forged evidence")
	}
	original := construction
	large := make([]batch.Entry, node.PrimaryDirectoryMaxEntries+2)
	for i := range large {
		large[i] = batch.Entry{Type: batch.OpPut, Key: []byte(fmt.Sprintf("materialize/%03d", i)), Value: []byte("new"), Revision: 2}
	}
	if err = ops.SetOps(large); err != nil {
		t.Fatal(err)
	}
	materialized, err := z.ApplyWithOptions(result.RootID, ops, ApplyOptions{CollectOldPointerRefs: true})
	if err != nil {
		t.Fatal(err)
	}
	if z.PreservesMaterializedBaseV6(&materialized, result.RootID, materialized.RootID) {
		t.Fatal("materialized base received unchanged-base evidence")
	}
	if err = construction.Abort(); err != nil {
		t.Fatal(err)
	}
	if err = original.Abort(); err != nil {
		t.Fatal(err)
	}
	if z.PreservesMaterializedBaseV6(&result, 2, result.RootID) {
		t.Fatal("receipt survived original construction disposal")
	}
}
