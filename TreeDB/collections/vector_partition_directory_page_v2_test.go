package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func testDirectoryLeafV2(t testing.TB, owner string, ordinal uint64) VectorPartitionDirectoryPageV2 {
	t.Helper()
	r := VectorPartitionDirectoryRecordV2{Owner: owner, DomainID: 7, MembershipKind: "home", Member: &VectorPartitionSourceRowIdentityV2{SourceOwner: "source-owner", ShardID: "source-a", SnapshotRevision: 3, SnapshotDigest: sha256.Sum256([]byte("snapshot")), Ordinal: ordinal, DocumentRevision: 19}}
	key, err := r.keyV2("metadata")
	if err != nil {
		t.Fatal(err)
	}
	return VectorPartitionDirectoryPageV2{Version: 2, Kind: "metadata", Generation: 2, Count: 1, First: key, Last: key, Records: []VectorPartitionDirectoryRecordV2{r}}
}

func TestVectorPartitionDirectoryPageV2OwnerSelectionAndRefusals(t *testing.T) {
	pages := make(map[ColumnAssetRef][]byte)
	next := uint64(1)
	persist := func(p VectorPartitionDirectoryPageV2) VectorPartitionDirectoryChildV2 {
		raw, err := EncodeVectorPartitionDirectoryPageV2(p)
		if err != nil {
			t.Fatal(err)
		}
		a := testVectorPartitionManifestV1().Assets[0]
		a.ID = fmt.Sprintf("page-%d", next)
		a.PartitionID = 0
		a.MembershipDigest = ""
		a.GraphVariant = ""
		a.Ref.Generation = 2
		a.Ref.PartID = next
		a.Ref.Offset = int64(next * MaxVectorPartitionDirectoryPageBytesV2)
		next++
		a.Bytes = uint64(len(raw))
		a.Ref.Length = int64(len(raw))
		sum := sha256.Sum256(raw)
		a.Checksum = hex.EncodeToString(sum[:])
		pages[a.Ref] = raw
		return VectorPartitionDirectoryChildV2{First: p.First, Last: p.Last, Count: p.Count, Asset: a}
	}
	a := persist(testDirectoryLeafV2(t, "owner-a", 0))
	b := persist(testDirectoryLeafV2(t, "owner-b", 0))
	rootPage := VectorPartitionDirectoryPageV2{Version: 2, Kind: "metadata", Generation: 2, Level: 1, Count: 2, First: a.First, Last: b.Last, Children: []VectorPartitionDirectoryChildV2{a, b}}
	root := persist(rootPage)
	for _, mutate := range []func(*VectorPartitionAssetV1){
		func(a *VectorPartitionAssetV1) { a.Ref.Length++ },
		func(a *VectorPartitionAssetV1) {
			a.Bytes = MaxVectorPartitionDirectoryPageBytesV2 + 1
			a.Ref.Length = int64(a.Bytes)
		},
	} {
		bad := root.Asset
		mutate(&bad)
		if err := walkVectorPartitionDirectoryV2(t.Context(), bad, "metadata", "", func(VectorPartitionAssetV1) ([]byte, error) {
			t.Fatal("invalid root reached asset reader")
			return nil, nil
		}, nil, nil); err == nil {
			t.Fatal("invalid root reference admitted")
		}
	}
	reads, records := 0, 0
	read := func(asset VectorPartitionAssetV1) ([]byte, error) {
		reads++
		if asset.Ref == b.Asset.Ref {
			t.Fatal("remote owner page read")
		}
		raw, ok := pages[asset.Ref]
		if !ok {
			return nil, errors.New("missing page")
		}
		return raw, nil
	}
	if err := walkVectorPartitionDirectoryV2(t.Context(), root.Asset, "metadata", vectorPartitionDirectoryOwnerPrefixV2("owner-a"), read, nil, func(r VectorPartitionDirectoryRecordV2) error {
		records++
		if r.Owner != "owner-a" {
			t.Fatal(r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if reads != 2 || records != 1 {
		t.Fatalf("reads=%d records=%d", reads, records)
	}
	stop := errors.New("callback stopped")
	if err := walkVectorPartitionDirectoryV2(t.Context(), root.Asset, "metadata", vectorPartitionDirectoryOwnerPrefixV2("owner-a"), read, nil, func(VectorPartitionDirectoryRecordV2) error { return stop }); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	for _, name := range []string{"checksum", "missing", "range", "count", "generation", "level", "mixed", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			page := testDirectoryLeafV2(t, "owner-a", 0)
			switch name {
			case "range":
				page.Last += "x"
			case "count":
				page.Count++
			case "generation":
				page.Generation++
			case "level":
				page.Level = 1
			case "mixed":
				page.Records[0].Asset = &a.Asset
			case "duplicate":
				page.Records = append(page.Records, page.Records[0])
				page.Count++
			}
			raw, err := EncodeVectorPartitionDirectoryPageV2(page)
			if err != nil {
				return
			}
			ref := a.Asset
			if name == "missing" {
				raw = nil
			}
			if name == "checksum" {
				raw[len(raw)-1] ^= 1
			} else {
				sum := sha256.Sum256(raw)
				ref.Checksum = hex.EncodeToString(sum[:])
				ref.Bytes = uint64(len(raw))
				ref.Ref.Length = int64(len(raw))
			}
			if err := walkVectorPartitionDirectoryV2(t.Context(), ref, "metadata", "", func(VectorPartitionAssetV1) ([]byte, error) { return raw, nil }, nil, nil); err == nil {
				t.Fatal("malformed page admitted")
			}
		})
	}
}

func TestVectorPartitionDirectoryPageV2StreamingWriterTails(t *testing.T) {
	for _, tc := range []struct {
		count int
		owner string
	}{{1, "owner-a"}, {127, "owner-a"}, {128, "owner-a"}, {129, "owner-a"}, {257, "owner-a"}, {16385, "owner-a"}, {257, strings.Repeat("界\"", 250)}} {
		count := tc.count
		t.Run(fmt.Sprintf("%d-ownerbytes%d", count, len(tc.owner)), func(t *testing.T) {
			pages := make(map[ColumnAssetRef][]byte)
			emit := func(p VectorPartitionDirectoryPageV2) (VectorPartitionAssetV1, error) {
				raw, err := EncodeVectorPartitionDirectoryPageV2(p)
				if err != nil {
					return VectorPartitionAssetV1{}, err
				}
				a := testVectorPartitionManifestV1().Assets[0]
				a.ID = fmt.Sprintf("page-%d", len(pages)+1)
				a.PartitionID = 0
				a.MembershipDigest = ""
				a.GraphVariant = ""
				a.Ref.Generation = 2
				a.Ref.PartID = uint64(len(pages) + 1)
				a.Ref.Offset = int64(len(pages) * MaxVectorPartitionDirectoryPageBytesV2)
				a.Bytes = uint64(len(raw))
				a.Ref.Length = int64(len(raw))
				sum := sha256.Sum256(raw)
				a.Checksum = hex.EncodeToString(sum[:])
				pages[a.Ref] = raw
				return a, nil
			}
			root, err := writeVectorPartitionDirectoryV2(t.Context(), "metadata", 2, func(visit func(VectorPartitionDirectoryRecordV2) error) error {
				record := testDirectoryLeafV2(t, tc.owner, 0).Records[0]
				for i := 0; i < count; i++ {
					record.Member.Ordinal = uint64(i)
					if err := visit(record); err != nil {
						return err
					}
				}
				return nil
			}, emit)
			if err != nil {
				t.Fatal(err)
			}
			if count >= 128 && tc.owner == "owner-a" && len(pages) > (count+127)/128+3 {
				t.Fatalf("small records used too many durable pages: %d records, %d pages", count, len(pages))
			}
			seen := 0
			visited := 0
			err = walkVectorPartitionDirectoryV2(t.Context(), root, "metadata", "", func(a VectorPartitionAssetV1) ([]byte, error) { return pages[a.Ref], nil }, func(VectorPartitionAssetV1) error { visited++; return nil }, func(r VectorPartitionDirectoryRecordV2) error {
				if r.Member.Ordinal != uint64(seen) {
					t.Fatalf("ordinal=%d want%d", r.Member.Ordinal, seen)
				}
				seen++
				return nil
			})
			if err != nil || seen != count || visited != len(pages) {
				t.Fatalf("walk err=%v records=%d pages=%d/%d", err, seen, visited, len(pages))
			}
		})
	}
}
