package rootpublication

import (
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestNativePrimaryDirectorySealUsesSameConstructorAtQ32V6(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
		if err != nil {
			t.Fatal(err)
		}
		original, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if !ready || err != nil {
			t.Fatal(ready, err)
		}
		txn := original.(*DurableRootTransaction)
		ref := txn.PrimaryConstructionReferenceV6()
		image := make([]byte, page.PageSize)
		if err := node.EncodePrimaryDirectory(image, ref.PageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("actual DATA"))}, nil); err != nil {
			t.Fatal(err)
		}
		before := a.Counters()
		if ready, err := txn.SealPrimaryConstructionV6(image, [1]*primaryarena.Group{}, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || err != nil || a.Counters() != before {
			t.Fatal("original whole-geometry refusal changed constructor", ready, err)
		}
		var work primaryarena.DirectorySealWorkV6
		turns := 0
		for ; turns < 100; turns++ {
			w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
			ready, err = txn.SealPrimaryConstructionWithWorkV6(image, [1]*primaryarena.Group{}, &work, &w)
			if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
				t.Fatal(ready, err)
			}
			t.Logf("cancel=%t turn=%d work=%d/%d ready=%t", cancel, turns, w.Records, w.Bytes, ready)
			if txn.PrimaryConstructionReferenceV6() != ref || txn.Owner() != ResourceOwnerBuilder {
				t.Fatal("retained seal replaced original custody")
			}
			if cancel || ready {
				break
			}
		}
		if turns == 100 || (!cancel && (!ready || txn.PrimaryConstructionCertificateV6() == nil)) {
			t.Fatal("retained seal did not finish")
		}
		// Producer detaches every borrowed validation alias before its SAME
		// original constructor's metadata can be physically refunded.
		work = primaryarena.DirectorySealWorkV6{}
		var release PrimaryConstructionReleaseWork
		if ready, err := release.Bind(txn, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
			t.Fatal(ready, err)
		}
		txn = nil
		original = nil
		for turns = 0; turns < 100; turns++ {
			ready, err = release.ContinueWork(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if ready {
				break
			}
		}
		if !ready {
			t.Fatal("same constructor cancellation did not physically finish")
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
