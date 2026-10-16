package valuelog

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"path/filepath"
	"testing"
)

func TestRetirementParentPoolV6ActualCollisionExactUnlinkAndBorrower(t *testing.T) {
	root := t.TempDir()
	byBucket := map[uint8][]*os.File{}
	var parents []*os.File
	var identities []rootpublication.StableIdentity
	paths := map[*os.File]string{}
	// 65 actual physical directories force a bucket with at least three entries.
	for i := 0; i < 65; i++ {
		path := filepath.Join(root, fmt.Sprintf("parent-%02d", i))
		if e := os.Mkdir(path, 0700); e != nil {
			t.Fatal(e)
		}
		f, e := rootpublication.OpenStableParentForRetention(path)
		if e != nil {
			t.Fatal(e)
		}
		identity, e := rootpublication.StableIdentityFromFile(f)
		if e != nil {
			t.Fatal(e)
		}
		paths[f] = path
		bucket := retirementParentBucket(identity)
		byBucket[bucket] = append(byBucket[bucket], f)
		if len(byBucket[bucket]) == 3 {
			parents = byBucket[bucket]
			for _, p := range parents {
				id, e := rootpublication.StableIdentityFromFile(p)
				if e != nil {
					t.Fatal(e)
				}
				identities = append(identities, id)
			}
			break
		}
	}
	for _, list := range byBucket {
		for _, f := range list {
			selected := false
			for _, p := range parents {
				selected = selected || p == f
			}
			if !selected {
				if e := f.Close(); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	if len(parents) != 3 {
		t.Fatal("collision fixture incomplete")
	}
	pool := &retirementParentPool{}
	nodes := make([]*retirementParentHandle, 3)
	for i, p := range parents {
		nodes[i], _ = pool.acquire(p, identities[i])
	}
	middle := nodes[1]
	if middle.prev != nodes[2] || middle.next != nodes[0] {
		t.Fatal("physical collision not intrusive middle")
	}
	if token, ready := middle.dropWithWorkV6(true, parentPoolWorkV6(3)); ready || token != nil || pool.count != 3 || middle.owners != 1 {
		t.Fatal("refused unlink mutated custody")
	}
	w := parentPoolWorkV6(4)
	token, ready := middle.dropWithWorkV6(true, w)
	if !ready || token != parents[1] || w.Records != 4 || pool.count != 2 || nodes[2].next != nodes[0] || nodes[0].prev != nodes[2] {
		t.Fatal("exact neighbor unlink")
	}
	if e := token.Close(); e != nil {
		t.Fatal(e)
	}
	pool.mu.Lock()
	nodes[0].borrowers++
	pool.mu.Unlock()
	if token, ready := nodes[0].dropWithWorkV6(true, parentPoolWorkV6(2)); !ready || token != nil || pool.count != 2 {
		t.Fatal("borrower did not retain exact node")
	}
	// A distinct descriptor to the same real directory must share the node,
	// including while its only old edge is a borrower.
	alias, e := rootpublication.OpenStableParentForRetention(paths[parents[0]])
	if e != nil {
		t.Fatal(e)
	}
	aliasID, e := rootpublication.StableIdentityFromFile(alias)
	if e != nil {
		t.Fatal(e)
	}
	same, temporary := pool.acquire(alias, aliasID)
	if same != nodes[0] || temporary != alias || pool.count != 2 {
		t.Fatal("borrowed physical identity did not remain indexed")
	}
	if e := temporary.Close(); e != nil {
		t.Fatal(e)
	}
	if token, ready := nodes[0].dropWithWorkV6(true, parentPoolWorkV6(2)); !ready || token != nil {
		t.Fatal("reacquired owner dropped borrower")
	}
	if token, ready := nodes[0].dropWithWorkV6(false, parentPoolWorkV6(3)); !ready || token != parents[0] {
		t.Fatal("last borrower lost close token")
	} else if e := token.Close(); e != nil {
		t.Fatal(e)
	}
	if token, ready := nodes[2].dropWithWorkV6(true, parentPoolWorkV6(2)); !ready || token != parents[2] {
		t.Fatal("last head unlink")
	} else if e := token.Close(); e != nil {
		t.Fatal(e)
	}
	if pool.count != 0 || pool.heads != [retirementParentBuckets]*retirementParentHandle{} {
		t.Fatal("fixed directory retained departed nodes")
	}
}

func parentPoolWorkV6(limit uint64) *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
}
