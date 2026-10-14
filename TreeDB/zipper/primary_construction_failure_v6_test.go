package zipper

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// Fault injection wraps the SAME real constructor, leaving its actual root and
// component custody unchanged until the test explicitly removes the fault.
type failingPrimaryConstructorV6 struct {
	primaryarena.ReadRootConstructionV6
	sealErr, abortErr error
	abortCalls        int
}

func (c *failingPrimaryConstructorV6) SealPrimaryConstructionV6([]byte, [1]*primaryarena.Group, *iterator.OrdinalScanWork) (bool, error) {
	return false, c.sealErr
}
func (c *failingPrimaryConstructorV6) Abort() error {
	c.abortCalls++
	if c.abortErr != nil {
		return c.abortErr
	}
	return c.ReadRootConstructionV6.Abort()
}

func TestPrimaryConstructionV6SiblingFailureDisposal(t *testing.T) {
	for _, kind := range []string{"apply", "copy", "build"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			p, err := pager.Open(filepath.Join(dir, "index.db"), 256*page.PageSize)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.Alloc(3); err != nil {
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
			if err := a.SetMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5); err != nil {
				t.Fatal(err)
			}
			if err := p.AttachPrimaryBankPager(a.Pager()); err != nil {
				t.Fatal(err)
			}
			z := New(p, &MockAllocator{p: p})
			z.SetPrimaryArena(a)
			z.SetPrimaryConstructorV6(rootpublication.NewPrimaryConstructionTransactionV6)
			id, err := z.BuildPrimaryRoot(2, 1)
			if err != nil {
				t.Fatal(err)
			}
			w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
			originalBase, ready, err := a.ReadRootConstructionOwnerV6(id, w)
			if !ready || err != nil {
				t.Fatal(ready, err)
			}
			defer originalBase.Abort()
			sealErr, abortErr := errors.New("original seal failure"), errors.New("original Abort failure")
			var original *failingPrimaryConstructorV6
			z.SetPrimaryConstructorV6(func(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
				c, ready, err := rootpublication.NewPrimaryConstructionTransactionV6(a, w)
				if ready && err == nil {
					original = &failingPrimaryConstructorV6{ReadRootConstructionV6: c, sealErr: sealErr, abortErr: abortErr}
					return original, true, nil
				}
				return c, ready, err
			})
			switch kind {
			case "apply":
				ops := batch.New(nil, page.PageSize)
				defer ops.Close()
				if err := ops.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("new"), Value: []byte("value"), Revision: 2}}); err != nil {
					t.Fatal(err)
				}
				_, _, _, err = z.Apply(id, ops)
			case "copy":
				_, err = z.CopyPrimaryRoot(id)
			case "build":
				_, err = z.BuildPrimaryRoot(2, 1)
			}
			if !errors.Is(err, sealErr) || !errors.Is(err, abortErr) {
				t.Fatalf("original disposal failure lost: %v", err)
			}
			if original == nil || original.abortCalls != 1 {
				t.Fatalf("original constructor disposed %v times", original)
			}
			txn, ok := original.ReadRootConstructionV6.(*rootpublication.DurableRootTransaction)
			if !ok || txn.Owner() != rootpublication.ResourceOwnerBuilder || txn.PrimaryConstructionReferenceV6() == (primaryarena.Ref{}) {
				t.Fatal("failed original custody lost")
			}
			original.abortErr = nil
			if err := original.Abort(); err != nil {
				t.Fatal(err)
			}
			if txn.Owner() != rootpublication.ResourceOwnerReleased {
				t.Fatal("same original did not dispose after fault removal")
			}
		})
	}
}
