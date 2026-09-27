package collections

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	source "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// This fixture models already admitted remote semantic metadata; it does not
// claim to prepare remote storage or activate distributed queries. Remote
// source/graph assets are deliberately absent from the local namespace.
func pagedScalingSourceMapV2(tb testing.TB, remote int) sourcepartition.ResolvedSourceShardMapV2 {
	tb.Helper()
	shards := []sourcepartition.SourceShardV2{{ShardID: "source-a", GroupID: "group-a", Start: 0, End: ^uint64(0) - uint64(remote)}}
	for i := 0; i < remote; i++ {
		token := ^uint64(0) - uint64(remote) + 1 + uint64(i)
		shards = append(shards, sourcepartition.SourceShardV2{ShardID: fmt.Sprintf("remote-%06d", i), GroupID: "group-z", Start: token, End: token})
	}
	m, err := sourcepartition.CanonicalSourceShardMapV2(sourcepartition.SourceShardMapV2{Format: sourcepartition.SourceShardMapFormatV2, Collection: sourcepartition.CollectionRefV2{Database: "default", Catalog: "default", Collection: "minima"}, Epoch: 7, TokenAlgorithm: sourcepartition.DocumentIDTokenAlgorithmV2, Shards: shards})
	if err != nil {
		tb.Fatal(err)
	}
	resolved, err := sourcepartition.ValidateSourceShardMapV2(m)
	if err != nil {
		tb.Fatal(err)
	}
	return resolved
}

type pagedScalingFixtureV2 struct {
	db         *backenddb.DB
	collection *Collection
	ownership  sourcepartition.ResolvedSourceShardMapV2
	prepared   VectorPartitionPreparedInputV2
	snapshot   VectorPartitionSourceSnapshotV2
	rows       int
}

func newPagedScalingFixtureV2(tb testing.TB, rows, remote int) *pagedScalingFixtureV2 {
	tb.Helper()
	_, d, c := openSourceImportDirectoryCollectionV2(tb)
	_, input := sourceImportFixtureV2(tb, c, uint64(rows))
	ownership := pagedScalingSourceMapV2(tb, remote)
	digest, err := hex.DecodeString(ownership.Digest())
	if err != nil {
		tb.Fatal(err)
	}
	copy(input.Snapshot.SourceMapDigest[:], digest)
	input.Snapshot.RowsPerChunk = 256
	var progress VectorPartitionSourceImportProgressV2
	for start := 0; start < rows; start += 256 {
		end := min(start+256, rows)
		names := make([]string, end-start)
		input.DocumentRevisions = make([]uint64, end-start)
		for i := range names {
			names[i] = fmt.Sprintf("row-%06d", start+i)
			input.DocumentRevisions[i] = uint64(1000 + start + i)
		}
		ids, retained, columns := sourceImportRowsV2(names...)
		for i := range names {
			columns[0].Float32Vectors[i] = []float32{1, float32(start+i) * 0.01, float32((start+i)%7) * 0.02, 0, 0, 0, 0, 0}
		}
		input.ChunkIndex = uint64(start / 256)
		progress, err = c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
		if err != nil {
			tb.Fatal(err)
		}
	}
	if !progress.Complete {
		tb.Fatal("source import incomplete")
	}
	if err = d.Checkpoint(); err != nil {
		tb.Fatal(err)
	}
	semantic := source.NewOwnerSnapshotSetHashV2("group-a")
	source.WriteOwnerSnapshotIdentityV2(semantic, progress.Snapshot)
	owners := []source.SourceOwnerCommitmentV2{{GroupID: "group-a", ShardCount: 1, SnapshotSetDigest: hex.EncodeToString(semantic.Sum(nil))}}
	if remote != 0 {
		digest := sha256.Sum256([]byte(fmt.Sprintf("admitted-remote-owner-fixture/%d", remote)))
		owners = append(owners, source.SourceOwnerCommitmentV2{GroupID: "group-z", ShardCount: uint64(remote), SnapshotSetDigest: hex.EncodeToString(digest[:])})
	}
	snapshots, err := source.SourceOwnerSetDigestV2(owners)
	if err != nil {
		tb.Fatal(err)
	}
	f := &pagedScalingFixtureV2{db: d, collection: c, ownership: ownership, snapshot: progress.Snapshot, rows: rows}
	acc, err := source.NewANNOwnerAccumulatorV2("group-b")
	if err != nil {
		tb.Fatal(err)
	}
	if err = f.walkANN(context.Background(), func(_ string, r source.ANNRecordV2) error {
		if r.Domain != nil {
			return acc.BeginDomain(*r.Domain)
		}
		return acc.AddMember(*r.Member)
	}); err != nil {
		tb.Fatal(err)
	}
	ann, err := acc.Commitment()
	if err != nil {
		tb.Fatal(err)
	}
	placement, err := source.ANNOwnerSetDigestV2([]source.ANNOwnerCommitmentV2{ann})
	if err != nil {
		tb.Fatal(err)
	}
	f.prepared = VectorPartitionPreparedInputV2{Generation: 3, Collection: c.name, IndexName: input.IndexName, IndexDefinitionDigest: VectorIndexDefinitionDigestV1(c.Meta().VectorIndexes[0]), SourceMapEpoch: ownership.Epoch(), SourceMapDigest: ownership.Digest(), SnapshotSetDigest: snapshots, GraphProfileDigest: VectorPartitionGraphProfileDigestV2(), PlacementDigest: placement, Owners: owners, ANNOwners: []source.ANNOwnerCommitmentV2{ann}, LocalSourceOwners: []string{"group-a"}, LocalANNOwners: []string{"group-b"}}
	return f
}
func (f *pagedScalingFixtureV2) walkSource(_ context.Context, visit func(string, VectorPartitionSourceSnapshotV2) error) error {
	return visit("group-a", f.snapshot)
}
func (f *pagedScalingFixtureV2) walkANN(ctx context.Context, visit func(string, source.ANNRecordV2) error) error {
	for start := 0; start < f.rows; start += 32 {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+32, f.rows)
		domain := source.ANNDomainV2{DomainID: uint32(start / 32), LogicalPackID: fmt.Sprintf("domain-%06d", start/32), MembershipCount: uint64(end - start)}
		if err := visit("group-b", source.ANNRecordV2{Domain: &domain}); err != nil {
			return err
		}
		for i := start; i < end; i++ {
			m := source.ANNMemberV2{Kind: "home", Source: source.ANNSourceRowIdentityV2{SourceOwner: "group-a", ShardID: f.snapshot.ShardID, SnapshotRevision: f.snapshot.SnapshotRevision, SnapshotDigest: f.snapshot.Digest, Ordinal: uint64(i), DocumentRevision: uint64(1000 + i)}}
			if err := visit("group-b", source.ANNRecordV2{Member: &m}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *pagedScalingFixtureV2) build(tb testing.TB) VectorPartitionManifestV1 {
	tb.Helper()
	m, err := f.collection.BuildAndStageVectorPartitionProjectionV2(context.Background(), f.prepared, f.ownership, f.walkSource, f.walkANN)
	if err != nil {
		tb.Fatal(err)
	}
	return m
}
func (f *pagedScalingFixtureV2) checkManifest(tb testing.TB, m VectorPartitionManifestV1) (uint64, uint64) {
	tb.Helper()
	r := m.PagedRootV2
	if r == nil || r.LocalSourceShardCount != 1 || r.LocalSourceRowCount != uint64(f.rows) || r.LocalDomainCount != uint64((f.rows+31)/32) || r.LocalMembershipCount != uint64(f.rows) || len(r.SourceOwners) != 1 || r.SourceOwners[0] != "group-a" || len(r.ANNOwners) != 1 || r.ANNOwners[0] != "group-b" {
		tb.Fatalf("unexpected local projection: %+v", r)
	}
	var assets, bytes uint64
	err := walkVectorPartitionManifestAssetsV2(context.Background(), f.db.ColumnAssetRootDir(), f.collection.Meta().Options.ColumnStore.AssetManager.Namespace, m, func(a VectorPartitionAssetV1) error { assets++; bytes += a.Bytes; return nil })
	if err != nil {
		tb.Fatal(err)
	}
	return assets, bytes
}
func (f *pagedScalingFixtureV2) open(tb testing.TB) *VectorPartitionPagedSourceSessionV2 {
	tb.Helper()
	s, err := f.collection.OpenVectorPartitionPagedSourceSessionV2(context.Background(), f.prepared, f.ownership)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}
func (f *pagedScalingFixtureV2) checkDomain(tb testing.TB, d *VectorPartitionPreparedDomainV2) {
	tb.Helper()
	got, err := d.SearchLocalV2(context.Background(), []float32{1, 0, 0, 0, 0, 0, 0, 0}, 1, 64)
	f.checkResults(tb, got, err)
}

func (f *pagedScalingFixtureV2) checkResults(tb testing.TB, got []VectorPartitionPreparedResultV2, err error) {
	tb.Helper()
	if err != nil || len(got) != 1 || string(got[0].DocumentID) != "row-000000" || got[0].Source.Ordinal != 0 || got[0].Source.DocumentRevision != 1000 || got[0].Source.SnapshotDigest != f.snapshot.Digest || got[0].Source.SnapshotRevision != f.snapshot.SnapshotRevision || got[0].Source.ShardID != f.snapshot.ShardID || got[0].Source.SourceOwner != "group-a" || got[0].MembershipKind != "home" {
		tb.Fatalf("local result/provenance: %+v %v", got, err)
	}
}

// Run each exact case in its own process. Build cases use fresh DBs per
// iteration; schema7 destructive retirement remains unsupported. Setup counters
// cover fixture import/build outside the stage timer, excluding retained
// session/domain preparation in warm cases. Heap observations and process RSS
// include that preparation. Map-admission cases time full global map construction.
func BenchmarkVectorPartitionPagedProjectionV2(b *testing.B) {
	for _, remote := range []int{0, 1024, 8192} {
		b.Run(fmt.Sprintf("map_admission/remote=%d", remote), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				m := pagedScalingSourceMapV2(b, remote)
				runtime.KeepAlive(m)
			}
		})
	}
	for _, size := range []struct{ rows, remote int }{{32, 0}, {32, 1024}, {32, 8192}, {256, 0}, {256, 1024}, {256, 8192}, {1024, 0}, {1024, 1024}, {1024, 8192}} {
		for _, stage := range []string{"build_stage", "cold_open", "domain_open", "search"} {
			b.Run(fmt.Sprintf("%s/local=%d/remote=%d", stage, size.rows, size.remote), func(b *testing.B) {
				b.StopTimer()
				var setupWall time.Duration
				var setupBytes, setupAllocs, setupCount uint64
				setup := func(build bool) *pagedScalingFixtureV2 {
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					started := time.Now()
					f := newPagedScalingFixtureV2(b, size.rows, size.remote)
					if build {
						f.checkManifest(b, f.build(b))
					}
					setupWall += time.Since(started)
					runtime.ReadMemStats(&after)
					setupBytes += after.TotalAlloc - before.TotalAlloc
					setupAllocs += after.Mallocs - before.Mallocs
					setupCount++
					return f
				}
				var f *pagedScalingFixtureV2
				var session *VectorPartitionPagedSourceSessionV2
				var searchDomain *VectorPartitionPreparedDomainV2
				if stage != "build_stage" {
					f = setup(true)
					defer f.db.Close()
					if stage == "domain_open" || stage == "search" {
						session = f.open(b)
						defer session.Close()
						if stage == "search" {
							var err error
							searchDomain, err = session.OpenDomainV2(context.Background(), "group-b", 0)
							if err != nil {
								b.Fatal(err)
							}
							defer searchDomain.Close()
							// Warm the retained graph scratch once, outside measured query work.
							f.checkDomain(b, searchDomain)
						}
					}
				}
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				var outputAssets, outputBytes uint64
				b.ReportAllocs()
				b.ResetTimer()
				if stage == "search" {
					query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
					var got []VectorPartitionPreparedResultV2
					var err error
					b.StartTimer()
					for i := 0; i < b.N; i++ {
						got, err = searchDomain.SearchLocalV2(context.Background(), query, 1, 64)
						if err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					f.checkResults(b, got, err)
					if pins := vectorPartitionReaderPinCountV1(f.db.Dir(), f.prepared.Collection, f.prepared.IndexName, f.prepared.Generation); pins != 2 {
						b.Fatalf("search retained %d generation pins, want session+domain", pins)
					}
				}
				for i := 0; stage != "search" && i < b.N; i++ {
					if stage == "build_stage" {
						f = setup(false)
						b.StartTimer()
						m := f.build(b)
						b.StopTimer()
						outputAssets, outputBytes = f.checkManifest(b, m)
						s := f.open(b)
						domain, err := s.OpenDomainV2(context.Background(), "group-b", 0)
						if err != nil {
							b.Fatal(err)
						}
						f.checkDomain(b, domain)
						if err = domain.Close(); err != nil {
							b.Fatal(err)
						}
						if err = s.Close(); err != nil {
							b.Fatal(err)
						}
						if pins := vectorPartitionReaderPinCountV1(f.db.Dir(), f.prepared.Collection, f.prepared.IndexName, f.prepared.Generation); pins != 0 {
							b.Fatalf("build validation retained %d generation pins", pins)
						}
						if err = f.db.Close(); err != nil {
							b.Fatal(err)
						}
						f = nil
					} else {
						b.StartTimer()
						s := session
						if stage == "cold_open" {
							s = f.open(b)
						}
						domain, err := s.OpenDomainV2(context.Background(), "group-b", 0)
						if err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						f.checkDomain(b, domain)
						if err = domain.Close(); err != nil {
							b.Fatal(err)
						}
						if stage == "cold_open" {
							if err = s.Close(); err != nil {
								b.Fatal(err)
							}
						}
						wantPins := uint64(0)
						if stage == "domain_open" {
							wantPins = 1 // the reused session owns exactly one generation pin
						}
						if pins := vectorPartitionReaderPinCountV1(f.db.Dir(), f.prepared.Collection, f.prepared.IndexName, f.prepared.Generation); pins != wantPins {
							b.Fatalf("open/close retained %d generation pins, want %d", pins, wantPins)
						}
					}
				}
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				b.ReportMetric(float64(setupWall.Nanoseconds())/float64(setupCount), "setup_ns/fixture")
				b.ReportMetric(float64(setupBytes)/float64(setupCount), "setup_bytes/fixture")
				b.ReportMetric(float64(setupAllocs)/float64(setupCount), "setup_allocs/fixture")
				b.ReportMetric(float64(before.HeapAlloc), "heap_before_bytes")
				b.ReportMetric(float64(before.HeapObjects), "heap_before_objects")
				b.ReportMetric(float64(after.HeapAlloc), "heap_after_bytes")
				b.ReportMetric(float64(after.HeapObjects), "heap_after_objects")
				if stage == "build_stage" {
					b.ReportMetric(float64(outputAssets), "output_assets")
					b.ReportMetric(float64(outputBytes), "output_bytes")
				}
				runtime.KeepAlive(f)
				runtime.KeepAlive(session)
				runtime.KeepAlive(searchDomain)
			})
		}
	}
}
