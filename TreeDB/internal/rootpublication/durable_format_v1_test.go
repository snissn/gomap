package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestDependencyManifestV1DeterministicMultiPageRoundTrip(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {

			entries := make([]DependencyManifestEntryV1, 0, 48)
			for i := 47; i >= 0; i-- {
				logicalDigest := sha256.Sum256([]byte(fmt.Sprintf("logical-%d", i)))
				frontier := NewRIDFrontier([]uint64{uint64(i + 1), uint64(i + 101)})
				frontier.Bytes = uint64((i + 1) * 4096)
				entries = append(entries, DependencyManifestEntryV1{
					Kind: ResourceValueLog, LogicalLane: "main", ResourceID: fmt.Sprintf("segment-%03d", i),
					DiagnosticPath: fmt.Sprintf("value_vlog/%03d-%s.vlog", i, string(make([]byte, 96))),
					Identity:       StableIdentity{Platform: "unix", VolumeID: 9, ObjectID: [16]byte{byte(i + 1)}, Generation: uint64(i + 1)},
					Generation:     uint64(i + 1), Digest: sha256.Sum256([]byte(fmt.Sprintf("segment-%d", i))), Frontier: frontier,
					Reachability: []ReachabilityField{ReachabilityValueLogPointer},
					LogicalObligations: []StableLogicalObligation{{
						Class: "fixture", Kind: "value-log-ref", Namespace: "main", Generation: uint64(i + 1),
						FileID: uint64(i + 1), Offset: int64(i * 4096), Length: 4096, Checksum: uint32(i + 1),
						Reachability: ReachabilityValueLogPointer, Digest: logicalDigest,
					}},
					Namespace: &DependencyManifestNamespaceV1{
						ParentIdentity: StableIdentity{Platform: "unix", VolumeID: 9, ObjectID: [16]byte{99}, Generation: 7},
						Operation:      NamespaceCreate, NewName: fmt.Sprintf("%03d.vlog", i), DiagnosticPath: "value_vlog",
					},
				})
			}
			manifest, err := dependencyManifestRepresentationForTest(entries, cached)
			if err != nil {
				t.Fatal(err)
			}
			wantEntries := manifest.Entries()
			// Both constructor input and Entries results may be reused or mutated;
			// normalized cache values must remain independent at every nested field.
			for _, borrowed := range [][]DependencyManifestEntryV1{entries, manifest.Entries()} {
				borrowed[0].ResourceID = "changed"
				borrowed[0].Frontier.exactRIDs.values[0] = 999
				borrowed[0].Reachability[0] = ReachabilityColumnManifest
				borrowed[0].LogicalObligations[0].PartID++
				borrowed[0].Namespace.NewName = "changed"
			}
			if !reflect.DeepEqual(manifest.Entries(), wantEntries) {
				t.Fatal("manifest aliases constructor input or returned Entries")
			}
			if _, err := NewDependencyManifestV1([]DependencyManifestEntryV1{wantEntries[0], wantEntries[0]}); !errors.Is(err, ErrDependencyManifestFormat) {
				t.Fatalf("duplicate entry error=%v", err)
			}
			invalid := wantEntries[1]
			invalid.Generation = 0
			if got, work, err := NewDependencyManifestV1WithWork([]DependencyManifestEntryV1{wantEntries[0], invalid}); !errors.Is(err, ErrDependencyManifestFormat) || got != nil || work.EntriesVisited != 2 || work.EntriesEncoded != 1 || work.BytesEncoded == 0 {
				t.Fatalf("normalization failure lost its completed prefix: manifest=%v work=%+v err=%v", got, work, err)
			}
			if manifest.PageCount() < 2 {
				t.Fatalf("page count=%d want multi-page fixture", manifest.PageCount())
			}
			reference, err := manifest.Reference(100)
			if err != nil {
				t.Fatal(err)
			}
			if allocs := testing.AllocsPerRun(10, func() {
				if got, err := manifest.Reference(100); err != nil || got != reference {
					t.Fatalf("reference=%+v, %v want %+v", got, err, reference)
				}
			}); allocs != 0 {
				t.Fatalf("reference-only preparation allocated %g times", allocs)
			}
			store := freelist.NewMemoryPageStoreV1()
			ref, err := manifest.Materialize(100, store)
			if err != nil {
				t.Fatal(err)
			}
			golden := sha256.New()
			for id := ref.FirstPageID; id < ref.FirstPageID+uint64(ref.PageCount); id++ {
				golden.Write(store.Pages[id])
			}
			if got := fmt.Sprintf("%x", golden.Sum(nil)); got != "d1ab7d92812ab54b49b84ee220bce2cd03b76b1e0e354ba3d7969d621d11bafb" {
				t.Fatalf("canonical V1 pages changed from baseline: %s", got)
			}
			if ref != reference {
				t.Fatalf("materialized reference=%+v want %+v", ref, reference)
			}
			lastFirst := ^uint64(0) - uint64(manifest.PageCount()-1)
			if got, err := manifest.Materialize(lastFirst, freelist.NewMemoryPageStoreV1()); err != nil {
				t.Fatal(err)
			} else if want, err := manifest.Reference(lastFirst); err != nil || got != want {
				t.Fatalf("last legal page interval: materialized=%+v reference=%+v, %v", got, want, err)
			}
			for _, first := range []uint64{0, 1, lastFirst + 1} {
				if got, err := manifest.Reference(first); !errors.Is(err, ErrDependencyManifestFormat) || got != (DependencyManifestRefV1{}) {
					t.Fatalf("invalid first page %d: reference=%+v, %v", first, got, err)
				}
				if got, err := manifest.Materialize(first, store); !errors.Is(err, ErrDependencyManifestFormat) || got != (DependencyManifestRefV1{}) {
					t.Fatalf("invalid first page %d: materialized=%+v, %v", first, got, err)
				}
			}
			writeFailure := errors.New("manifest sink failed")
			writes := 0
			failedRef, err := manifest.Materialize(100, dependencyManifestSinkFunc(func(id uint64, image []byte) error {
				writes++
				if id != 100+uint64(writes-1) || string(image) != string(store.Pages[id]) {
					t.Fatalf("failed-sink prefix page %d differs from successful encoding", id)
				}
				if writes == 2 {
					return writeFailure
				}
				return nil
			}))
			if !errors.Is(err, writeFailure) || writes != 2 || failedRef != (DependencyManifestRefV1{}) {
				t.Fatalf("failed materialization: ref=%+v writes=%d err=%v", failedRef, writes, err)
			}
			loaded, err := LoadDependencyManifestV1(store, ref)
			if err != nil {
				t.Fatal(err)
			}
			if got, want := loaded.Entries(), manifest.Entries(); !reflect.DeepEqual(got, want) {
				t.Fatalf("loaded manifest differs\ngot=%v\nwant=%v", got, want)
			}

			second := freelist.NewMemoryPageStoreV1()
			secondRef, err := loaded.Materialize(100, second)
			if err != nil {
				t.Fatal(err)
			}
			if ref != secondRef {
				t.Fatalf("reference changed across deterministic encode: %+v != %+v", ref, secondRef)
			}
			for id, first := range store.Pages {
				if got := second.Pages[id]; string(got) != string(first) {
					t.Fatalf("page %d encoding changed", id)
				}
			}

			delete(store.Pages, ref.FirstPageID+uint64(ref.PageCount)-1)
			if _, err := LoadDependencyManifestV1(store, ref); !errors.Is(err, ErrDependencyManifestFormat) {
				t.Fatalf("truncated manifest error=%v want %v", err, ErrDependencyManifestFormat)
			}

		})
	}
}

type dependencyManifestSinkFunc func(uint64, []byte) error

func (sink dependencyManifestSinkFunc) WritePage(id uint64, image []byte) error {
	return sink(id, image)
}

func TestDependencyManifestV1ReferenceEmptyAndNil(t *testing.T) {
	empty, err := NewDependencyManifestV1(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, first := range []uint64{2, ^uint64(0)} {
		ref, err := empty.Reference(first)
		if err != nil || ref.FirstPageID != first || ref.PageCount != 1 || ref.EntryCount != 0 || ref.ByteLength != 16 || ref.Digest != sha256.Sum256([]byte{'D', 'P', 'M', 'A', 'N', 'V', '1', 0, 1, 0, 16, 0, 0, 0, 0, 0}) {
			t.Fatalf("empty-entry manifest reference=%+v, %v", ref, err)
		}
		if got, err := empty.Materialize(first, freelist.NewMemoryPageStoreV1()); err != nil || got != ref {
			t.Fatalf("empty-entry materialized=%+v, %v want %+v", got, err, ref)
		}
	}
	for _, manifest := range []*DependencyManifestV1{nil, {}} {
		if ref, err := manifest.Reference(2); !errors.Is(err, ErrDependencyManifestFormat) || ref != (DependencyManifestRefV1{}) {
			t.Fatalf("uninitialized reference=%+v, %v", ref, err)
		}
		if ref, err := manifest.Materialize(2, freelist.NewMemoryPageStoreV1()); !errors.Is(err, ErrDependencyManifestFormat) || ref != (DependencyManifestRefV1{}) {
			t.Fatalf("uninitialized materialized=%+v, %v", ref, err)
		}
	}
	if ref, err := empty.Materialize(2, nil); !errors.Is(err, ErrDependencyManifestFormat) || ref != (DependencyManifestRefV1{}) {
		t.Fatalf("nil sink: ref=%+v, %v", ref, err)
	}
}

func BenchmarkDependencyManifestV1EncodeEntries(b *testing.B) {
	for _, count := range []int{8, 4096} {
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			entries := make([]DependencyManifestEntryV1, count)
			for i := range entries {
				generation := uint64(i + 1)
				entries[i] = DependencyManifestEntryV1{
					Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: fmt.Sprintf("part-%08d", i),
					DiagnosticPath: fmt.Sprintf("columns/part-%08d.bin", i),
					Identity:       StableIdentity{Platform: "benchmark", ObjectID: [16]byte{byte(i), byte(i >> 8), byte(i >> 16), 1}, Generation: generation},
					Generation:     generation, Frontier: DurableFrontier{Bytes: 4096},
					Reachability: []ReachabilityField{ReachabilityColumnManifest},
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := NewDependencyManifestV1(entries); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDurableRootRecordV1RoundTripBindsMetaFreelistAndManifest(t *testing.T) {
	manifest := DependencyManifestRefV1{FirstPageID: 30, ByteLength: 777, EntryCount: 4, PageCount: 2, Digest: [32]byte{7}}
	freelistRef := freelist.GenerationRefV1{HeaderPageID: 28, GenerationID: 12, CommitSeq: 12, HighWater: 33, Digest: [32]byte{8}}
	want := DurableRootRecordV1{
		CommitSeq: 12, DurableSeq: 11, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 34,
		MaxEntryRevision: 55, AppliedCommandLSN: 89, LastCommitHeight: 144,
		Freelist: freelistRef, FreelistFreeCount: 5, FreelistRetiredCount: 6, Manifest: manifest,
		ParentRecordPageID: 20, ParentCommitSeq: 11, ParentRecordDigest: [32]byte{9}, MetaProjectionDigest: [32]byte{10},
	}
	image, digest, err := want.EncodePage(33)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDurableRootRecordV1(image, 33, digest)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip=%+v want %+v", got, want)
	}
	image[128] ^= 1
	page.UpdateChecksum(image)
	if _, err := DecodeDurableRootRecordV1(image, 33, digest); !errors.Is(err, ErrDurableRootRecordDigest) {
		t.Fatalf("record corruption error=%v want %v", err, ErrDurableRootRecordDigest)
	}
}

// Read the actual persisted body, not the private encoding representation.
func dependencyManifestPayloadForTest(t testing.TB, manifest *DependencyManifestV1) []byte {
	t.Helper()
	store := freelist.NewMemoryPageStoreV1()
	ref, err := manifest.Materialize(100, store)
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	for i := uint32(0); i < ref.PageCount; i++ {
		image := store.Pages[100+uint64(i)]
		size := binary.LittleEndian.Uint32(image[88:92])
		payload = append(payload, image[dependencyManifestPageHeaderV1:dependencyManifestPageHeaderV1+int(size)]...)
	}
	return payload
}

func TestDependencyManifestV1PageBoundaryAndSinkOwnership(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%t", cached), func(t *testing.T) {

			// Place the next record length on either side of, and across, a page boundary.
			for _, remaining := range []int{0, 1, 2, 3, 4} {
				t.Run(fmt.Sprint(remaining), func(t *testing.T) {
					entry := DependencyManifestEntryV1{Kind: ResourceColumnAsset, LogicalLane: "columns", ResourceID: "a", DiagnosticPath: "a",
						Identity: StableIdentity{Platform: "fixture", ObjectID: [16]byte{1}, Generation: 1}, Generation: 1,
						Frontier: DurableFrontier{Bytes: 1}, Reachability: []ReachabilityField{ReachabilityColumnManifest}}
					single, err := NewDependencyManifestV1([]DependencyManifestEntryV1{entry})
					if err != nil {
						t.Fatal(err)
					}
					entry.DiagnosticPath = strings.Repeat("x", 1+dependencyManifestPayloadV1-remaining-len(dependencyManifestPayloadForTest(t, single)))
					second := entry
					second.ResourceID = "b"
					second.DiagnosticPath = "tail"
					manifest, err := dependencyManifestRepresentationForTest([]DependencyManifestEntryV1{entry, second}, cached)
					if err != nil {
						t.Fatal(err)
					}
					// Verify the intended fixture boundary through persisted bytes.
					payload := dependencyManifestPayloadForTest(t, manifest)
					if got := 16 + 4 + int(binary.LittleEndian.Uint32(payload[16:20])); got != dependencyManifestPayloadV1-remaining {
						t.Fatalf("next length begins at %d", got)
					}
					store := freelist.NewMemoryPageStoreV1()
					ref, err := manifest.Materialize(100, store)
					if err != nil {
						t.Fatal(err)
					}
					var retained [][]byte
					_, err = manifest.Materialize(100, dependencyManifestSinkFunc(func(id uint64, image []byte) error {
						if !bytes.Equal(image, store.Pages[id]) {
							t.Fatalf("page %d changed", id)
						}
						image[0] ^= 0xff
						retained = append(retained, image)
						return nil
					}))
					if err != nil {
						t.Fatal(err)
					}
					for i, image := range retained {
						image[0] ^= 0xff
						if !bytes.Equal(image, store.Pages[100+uint64(i)]) {
							t.Fatal("sink pages alias")
						}
					}
					if got := dependencyManifestPayloadForTest(t, manifest); !bytes.Equal(got, payload) {
						t.Fatal("sink mutated manifest")
					}
					loaded, err := LoadDependencyManifestV1(store, ref)
					if err != nil || !reflect.DeepEqual(loaded.Entries(), manifest.Entries()) {
						t.Fatalf("boundary load: %v", err)
					}
					if remaining == 0 {
						// Reorder otherwise valid records and recompute outer integrity. Load
						// must still reject a noncanonical stream, not merely trust its digest.
						split := 20 + int(binary.LittleEndian.Uint32(payload[16:20]))
						corrupt := append(bytes.Clone(payload[:16]), payload[split:]...)
						corrupt = append(corrupt, payload[16:split]...)
						ref.Digest = sha256.Sum256(corrupt)
						offset := 0
						for i := uint32(0); i < ref.PageCount; i++ {
							image := store.Pages[100+uint64(i)]
							size := int(binary.LittleEndian.Uint32(image[88:92]))
							copy(image[56:88], ref.Digest[:])
							copy(image[dependencyManifestPageHeaderV1:], corrupt[offset:offset+size])
							page.UpdateChecksum(image)
							offset += size
						}
						if _, err := LoadDependencyManifestV1(store, ref); !errors.Is(err, ErrDependencyManifestFormat) {
							t.Fatalf("noncanonical valid-digest payload: %v", err)
						}
					}
				})
			}

		})
	}
}

// Use the same normalized immutable entry representation as the resource cache.
func dependencyManifestRepresentationForTest(entries []DependencyManifestEntryV1, cached bool) (*DependencyManifestV1, error) {
	if !cached {
		return NewDependencyManifestV1(entries)
	}
	encoded := make([]*dependencyManifestEncodedEntryV1, len(entries))
	for i := range entries {
		entry, err := normalizeDependencyManifestEntryV1(entries[i])
		if err != nil {
			return nil, err
		}
		encoded[i] = &dependencyManifestEncodedEntryV1{entry: entry, encoded: encodeDependencyManifestEntryV1(entry)}
	}
	return newDependencyManifestV1FromEncoded(encoded, true)
}
