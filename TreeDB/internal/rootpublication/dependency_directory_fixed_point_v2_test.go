package rootpublication

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

// A real prefix-compressed, pinned directory makes reconstructed key aliases
// observable while the nested point probe checks the physical owner.
func TestOwnedDependencyDirectoryFixedPointScope(t *testing.T) {
	for _, mode := range []string{"aliases-and-refusal", "unverified-crc", "verify-on-read-crc", "tombstone", "pointer"} {
		t.Run(mode, func(t *testing.T) {
			p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if err = p.GrowTo(3); err != nil {
				t.Fatal(err)
			}
			physical := DependencyManifestEntryV1{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "one", DiagnosticPath: "columns/one", Identity: StableIdentity{Platform: "unix", ObjectID: [16]byte{1}, Generation: 1}, Generation: 1, Frontier: DurableFrontier{Bytes: 4096}, Reachability: []ReachabilityField{ReachabilityColumnManifest}}
			obligation := StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "docs", Generation: 1, PartID: 2, FileID: 1, Offset: 100, Length: 20, Checksum: 123, Reachability: ReachabilityColumnManifest, Digest: [32]byte{7}}
			physicalKey := DependencyPhysicalKeyV2(physical)
			physicalValue, err := EncodeDependencyPhysicalV2(physical)
			if err != nil {
				t.Fatal(err)
			}
			logicalKey := DependencyLogicalKeyV2(obligation)
			logicalValue, err := EncodeDependencyLogicalV2(physicalKey, obligation)
			if err != nil {
				t.Fatal(err)
			}
			image := make([]byte, page.PageSize)
			builder := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
			builder.SetPageID(2)
			flags, ptr := byte(node.FlagInline), page.ValuePtr{}
			switch mode {
			case "tombstone":
				flags, physicalValue = node.FlagTombstone, nil
			case "pointer":
				flags, physicalValue, ptr = node.FlagPointer, nil, page.ValuePtr{FileID: 1, Offset: 100, Length: 20}
			}
			if err = builder.AddLeafEntry(physicalKey, physicalValue, flags, ptr); err != nil {
				t.Fatal(err)
			}
			if err = builder.AddLeafEntry(logicalKey, logicalValue, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatal(err)
			}
			builder.FinishNoNode()
			builder.ReleaseScratch()
			builder = nil
			if err = p.Write(2, image); err != nil {
				t.Fatal(err)
			}
			var owner retainedalloc.Owner
			owner.Initialize(0)
			budget := &registryBudget{cap: 1 << 20}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			before, budgetBefore := owner.Bytes(), budget.bytes
			release := new(importedDirectoryLifetimeRelease)
			directory, err := NewOwnedTypedDependencyDirectoryV6(p, DependencyDirectoryRefV2{RootPageID: 2, PhysicalCount: 1, LogicalCount: 1}, p.PageCount(), &owner, release)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if directory != nil {
					if _, err := directory.ReleaseOutcome(); err != nil {
						t.Error(err)
					}
				}
			}()
			retained, budgetRetained, refs := owner.Bytes(), budget.bytes, directory.refs.Load()
			lookup := func() (bool, error) {
				var found bool
				err := withOwnedDirectoryCodecV2(&owner, func(s *ownedDirectoryCodecV2) error {
					var err error
					found, err = s.lookupLogical(directory, obligation, func(key []byte, got StableLogicalObligation) error {
						if !bytes.Equal(key, physicalKey) || got != obligation {
							t.Fatalf("logical owner changed: owner=%x got=%+v", key, got)
						}
						return nil
					})
					return err
				})
				if owner.Bytes() != retained || budget.bytes != budgetRetained || directory.refs.Load() != refs {
					t.Fatal("point scope changed original retained ownership")
				}
				return found, err
			}
			switch mode {
			case "aliases-and-refusal":
				budget.cap = budget.bytes
				called := false
				err = withOwnedDirectoryCodecV2(&owner, func(*ownedDirectoryCodecV2) error { called = true; return nil })
				if !errors.Is(err, retainedalloc.ErrCapacity) || called || owner.Bytes() != retained || budget.bytes != budgetRetained || directory.refs.Load() != refs {
					t.Fatalf("scratch refusal changed original custody: err=%v called=%t", err, called)
				}
				budget.cap = 1 << 20
				err = withOwnedDirectoryCodecV2(&owner, func(s *ownedDirectoryCodecV2) error {
					if owner.Bytes() <= retained || budget.bytes <= budgetRetained {
						t.Fatal("point scratch not admitted while borrowed")
					}
					found, err := directory.lookupBorrowedRecordV2(&s.points[0], logicalKey, func(key, value []byte) error {
						expectedKey, expectedValue := bytes.Clone(key), bytes.Clone(value)
						_, got, err := decodeBorrowedLogicalV2(key, value)
						if err != nil {
							return err
						}
						found, err := directory.lookupBorrowedRecordV2(&s.points[1], physicalKey, func(_, _ []byte) error { return nil })
						if err != nil {
							return err
						}
						if !found || !bytes.Equal(key, expectedKey) || !bytes.Equal(value, expectedValue) || got != obligation {
							t.Fatal("nested physical probe overwrote borrowed logical record")
						}
						return nil
					})
					if err != nil {
						return err
					}
					if !found {
						t.Fatal("logical point absent")
					}
					missing := obligation
					missing.PartID++
					found, err = s.lookupLogical(directory, missing, func([]byte, StableLogicalObligation) error { t.Fatal("missing record callback"); return nil })
					if found || err != nil {
						t.Fatalf("exact absence: found=%t err=%v", found, err)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if found, err := lookup(); err != nil || !found {
					t.Fatalf("logical lookup: found=%t err=%v", found, err)
				}
			case "unverified-crc", "verify-on-read-crc":
				if mode == "verify-on-read-crc" {
					if found, err := lookup(); err != nil || !found {
						t.Fatalf("initial verified lookup: found=%t err=%v", found, err)
					}
					p.SetVerifyOnRead(true)
				}
				mapped, err := p.Get(2)
				if err != nil {
					t.Fatal(err)
				}
				mapped[len(mapped)-1] ^= 1 // Invalidate CRC without resetting the verified-page cache.
				mapped = nil
				if found, err := lookup(); err == nil || found {
					t.Fatalf("corrupt page accepted: found=%t err=%v", found, err)
				}
			case "tombstone", "pointer":
				if found, err := lookup(); !errors.Is(err, ErrDependencyManifestFormat) {
					t.Fatalf("non-inline owner: found=%t err=%v", found, err)
				}
			}
			if owner.Bytes() != retained || budget.bytes != budgetRetained {
				t.Fatal("scope scratch survived its last alias")
			}
			consumed, err := directory.ReleaseOutcome()
			directory = nil
			if err != nil || !consumed || release.calls != 1 || owner.Bytes() != before || budget.bytes != budgetBefore {
				t.Fatalf("last root refund: consumed=%t err=%v releases=%d owner=%d budget=%d", consumed, err, release.calls, owner.Bytes(), budget.bytes)
			}
		})
	}
}
