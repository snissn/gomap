package valuelog

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestManagerSharedRetirementParentAccounting(t *testing.T) {
	for _, registry := range []bool{false, true} {
		name := "nil"
		if registry {
			name = "registry"
		}
		t.Run(name, func(t *testing.T) {
			m, files := sharedParentFixture(t, registry)
			configured := filepath.Dir(files[0].Path)
			ordinary, err := rootpublication.OpenStableParent(configured)
			if err != nil {
				t.Fatal(err)
			}
			if ordinary.Name() != configured {
				_ = ordinary.Close()
				t.Fatal("ordinary stable parent changed its diagnostic name")
			}
			if err := ordinary.Close(); err != nil {
				t.Fatal(err)
			}
			snapshot := m.CurrentSetNoRefresh()
			defer func() {
				if err := m.Release(snapshot); err != nil {
					t.Error(err)
				}
			}()
			before, known := snapshot.RetentionSizes()
			if !known {
				t.Fatal("snapshot omitted capacity authority")
			}
			var paths uint64
			for _, file := range files {
				paths += 2*uint64(len(file.Path)+len(file.stableNamespace)) + 128
			}
			if before.PathEnvelope < paths+uint64(len(files))*retirementParentRetentionEnvelope {
				t.Fatal("capture omitted future pooled parent reservation")
			}
			wrappers := uint64(unsafe.Sizeof(retirementParentPool{})) + uint64(unsafe.Sizeof(retirementParentHandle{})) + uint64(unsafe.Sizeof(os.File{}))
			// Leave 1 KiB for fixed directory and local unlink overhead and 512 bytes for os.File
			// internal state/fixed diagnostic names, above the compiled wrapper sizes.
			t.Logf("retirement parent envelope=%d File=%d pool=%d entry=%d os.File=%d physical-key=%d", retirementParentRetentionEnvelope, unsafe.Sizeof(File{}), unsafe.Sizeof(retirementParentPool{}), unsafe.Sizeof(retirementParentHandle{}), unsafe.Sizeof(os.File{}), unsafe.Sizeof(rootpublication.StableIdentity{}))
			if wrappers+1536 > retirementParentRetentionEnvelope {
				t.Fatal("compiled shared wrappers exceed future parent envelope")
			}
			for _, file := range files {
				if err := m.MarkZombie(file.ID); err != nil {
					t.Fatal(err)
				}
			}
			after, _ := snapshot.RetentionSizes()
			if after != before {
				t.Fatal("retirement changed frozen capture reservation")
			}
			parent, release := sharedParentBorrow(t, files[0])
			defer func() {
				if err := release(); err != nil {
					t.Error(err)
				}
			}()
			if parent.Name() != "stable-retirement-parent" {
				t.Fatal("pooled handle retained an unbounded path name")
			}
			want, err := rootpublication.StableIdentityFromFile(parent)
			if err != nil {
				t.Fatal(err)
			}
			if !rootpublication.SamePhysicalIdentity(want, files[0].registeredParentIdentity) {
				t.Fatal("fixed diagnostic name changed physical authority")
			}
			if m.retirementParents.count != 2 {
				t.Fatal("pool retained extra physical parent entries")
			}
		})
	}
}
