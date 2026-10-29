package collections

import (
	"errors"
	"fmt"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A real mutation microblock abandons blockmax and recursively scans exactly.
// Once ordinary projection storage exists, deny a second allocation: eventual
// deferred Close is insufficient, because recursion must borrow it immediately.
func TestNativeProjectionSingleTermFallbackReturnsLoanBeforeRecursion(t *testing.T) {
	owner, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	const count = 32
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("d%03d", i))
		docs[i] = []byte(`{"content":"refund policy","tenant":"one"}`)
	}
	if _, err := c.InsertBatch(ids, docs); err != nil {
		t.Fatal(err)
	}
	if _, err := c.RewriteTextIndex("content", TextIndexRewriteOptions{TargetPostingsPerBlock: 8}); err != nil {
		t.Fatal(err)
	}
	opts := TextSearchOptions{IndexName: "content", Query: "refund", TopK: 5, CandidateLimit: count, MaxPostingsScanned: count * 8}
	sealed, err := c.searchText(opts, textSearchResultScoreOnly)
	if err != nil || len(sealed.Results) != opts.TopK || sealed.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("sealed blockmax fixture results=%d stats=%+v err=%v", len(sealed.Results), sealed.Stats, err)
	}
	matched, modified, err := c.Update([]byte("d005"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"content":"refund refund refund policy","tenant":"one"}`), true, nil
	})
	if err != nil || !matched || !modified {
		t.Fatalf("mutation matched=%v modified=%v err=%v", matched, modified, err)
	}
	if err := owner.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	lease, err := read.capture.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var charged uint64
	denyProjection := false
	projectionCharges, refusedProjection := 0, 0
	projectionSize := memtable.COWAllocationCharge(tree.OwnedPointerProjectionAllocationSize())
	denied := errors.New("second projection allocation before fallback reuse")
	catalog, err := newNativeTextCatalog(read, func(n uint64) error {
		if n == projectionSize {
			if denyProjection {
				refusedProjection++
				return denied
			}
			projectionCharges++
		}
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := catalog.nativeText
	// Prime the actual physical posting root, so its exclusively lent ordinary
	// layout can serve both the abandoned blockmax and recursive exact cursors.
	root := collectionTextV2PostingBlocksRootName(c.name, "content")
	prefix := encodeTextV2PostingBlockTermPrefix("refund")
	prime, err := scope.iterator(root, prefix, textSearchPrefixEnd(prefix), true, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !prime.Valid() || !prime.(*nativeTextIterator).cursor.borrowedProjection || projectionCharges != 1 {
		_ = prime.Close()
		t.Fatal("fixture did not prime one borrowed physical projection")
	}
	if err := prime.Close(); err != nil {
		t.Fatal(err)
	}
	if scope.projection.InUse() || read.visits != 0 {
		t.Fatal("priming cursor retained its loan or visit")
	}
	denyProjection = true
	exactOpts := opts
	exactOpts.textV2DisableBlockMax = true
	exact, err := c.searchNativeTextAtCatalog(exactOpts, textSearchResultScoreOnly, read, TextSearchResponse{}, catalog)
	if err != nil || len(exact.Results) != opts.TopK || exact.Stats.TextBlockMaxFallbacks != 0 {
		t.Fatalf("same-cut exhaustive control results=%d stats=%+v err=%v", len(exact.Results), exact.Stats, err)
	}
	got, err := c.searchNativeTextAtCatalog(opts, textSearchResultScoreOnly, read, TextSearchResponse{}, catalog)
	if err != nil {
		t.Fatalf("successful native fallback required another projection: %v (refusals=%d)", err, refusedProjection)
	}
	assertTextSearchParity2627(t, got, exact)
	if got.Stats.TextBlockMaxFallbacks != 1 || got.Stats.Truncated || got.Stats.FailClosed != 0 || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.TextPostingsScanned < exact.Stats.TextPostingsScanned {
		t.Fatalf("actual fallback work/stats=%+v exhaustive=%+v", got.Stats, exact.Stats)
	}
	if refusedProjection != 0 || projectionCharges != 1 || scope.projection.InUse() || read.visits != 0 {
		t.Fatalf("fallback failed immediate loan reuse: charges=%d refusals=%d in-use=%v visits=%d", projectionCharges, refusedProjection, scope.projection.InUse(), read.visits)
	}
}
