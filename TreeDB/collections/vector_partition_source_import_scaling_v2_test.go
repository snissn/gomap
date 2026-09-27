package collections

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// This fixture also compiles against the V1 predecessor for a same-harness
// control. Only explicit directory cases request the new required feature.
func openSourceImportScalingCollectionV2(b *testing.B, directory bool) (string, *backenddb.DB, *Collection) {
	b.Helper()
	dir := b.TempDir()
	features := []string{backenddb.RequiredFeatureCommandWALV2}
	if directory {
		features = append(features, "dependency_directory_v2")
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: features, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		b.Fatal(err)
	}
	d := openTypedMinimaDB(b, dir)
	meta := typedMinimaCollectionMeta()
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		d.Close()
		b.Fatal(err)
	}
	c, err := NewCollectionManager(d).OpenCollection("minima")
	if err != nil {
		d.Close()
		b.Fatal(err)
	}
	return dir, d, c
}

// writeSourceImportIntervalProfileV2 is an opt-in diagnostic. Calibration N=1
// does not emit profiles. The before/after delta includes profile-writing work;
// production attribution must filter stacks beneath Import/Checkpoint explicitly.
func writeSourceImportIntervalProfileV2(b *testing.B, boundary string) {
	b.Helper()
	prefix := os.Getenv("TREEDB_SOURCE_IMPORT_INTERVAL_PROFILE")
	if prefix == "" || b.N != 10 {
		return
	}
	if runtime.MemProfileRate != 1 {
		b.Fatal("interval allocation diagnostic requires -test.memprofilerate=1")
	}
	// MemProfile may lag by two GC cycles. Both flushes and profile writes
	// occur outside the benchmark timer; sampling remains enabled throughout.
	runtime.GC()
	runtime.GC()
	f, err := os.Create(prefix + "." + boundary + ".pprof")
	if err != nil {
		b.Fatal(err)
	}
	writeErr := pprof.Lookup("allocs").WriteTo(f, 0)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		b.Fatalf("interval profile: write=%v close=%v", writeErr, closeErr)
	}
}

// The measured operation includes both public import and its checkpoint. Heap
// counters are post-GC whole-process observations, not per-operation allocation
// counters. Run each named case in a separate process for comparable peak RSS.
func BenchmarkVectorPartitionSourceImportCheckpointV2(b *testing.B) {
	for _, directory := range []bool{false, true} {
		format := "v1"
		if directory {
			format = "dpm2"
		}
		b.Run("format="+format, func(b *testing.B) {
			for _, reopen := range []bool{false, true} {
				b.Run(fmt.Sprintf("reopen=%t", reopen), func(b *testing.B) {
					for _, prior := range []int{32, 1024} {
						b.Run(fmt.Sprintf("prior=%d", prior), func(b *testing.B) {
							dir, d, c := openSourceImportScalingCollectionV2(b, directory)
							defer func() { d.Close() }()
							ownership, input := sourceImportFixtureV2(b, c, 2*uint64(prior+b.N))
							input.DocumentRevisions = []uint64{1, 2}
							var progress VectorPartitionSourceImportProgressV2
							importAt := func(index int) {
								input.ChunkIndex = uint64(index)
								ids, retained, columns := sourceImportRowsV2(fmt.Sprintf("row-%020d", 2*index), fmt.Sprintf("row-%020d", 2*index+1))
								var err error
								progress, err = c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
								if err != nil {
									b.Fatal(err)
								}
							}
							for i := 0; i < prior; i++ {
								importAt(i)
							}
							if err := d.Checkpoint(); err != nil {
								b.Fatal(err)
							}
							if reopen {
								if err := d.Close(); err != nil {
									b.Fatal(err)
								}
								d = openTypedMinimaDB(b, dir)
								var err error
								c, err = NewCollectionManager(d).OpenCollection("minima")
								if err != nil {
									b.Fatal(err)
								}
							}
							before := d.Stats()
							runtime.GC()
							var retainedBefore runtime.MemStats
							runtime.ReadMemStats(&retainedBefore)
							b.ReportAllocs()
							writeSourceImportIntervalProfileV2(b, "before")
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								importAt(prior + i)
								if err := d.Checkpoint(); err != nil {
									b.Fatal(err)
								}
							}
							b.StopTimer()
							writeSourceImportIntervalProfileV2(b, "after")
							after := d.Stats()
							value := func(stats map[string]string, key string) uint64 {
								v, err := strconv.ParseUint(stats[key], 10, 64)
								if err != nil {
									b.Fatalf("missing metric %s: %v", key, err)
								}
								return v
							}
							if directory {
								if after["treedb.durable_root.format_version"] != "2" {
									b.Fatal("directory fixture did not persist DPM2")
								}
								for _, counter := range []struct{ key, unit string }{
									{"changed_record_bytes", "directory_changed_bytes/op"}, {"pages_written", "directory_pages/op"},
								} {
									key := "treedb.durable_root.directory_build." + counter.key
									delta := value(after, key) - value(before, key)
									if delta == 0 {
										b.Fatalf("directory performed no %s", counter.key)
									}
									b.ReportMetric(float64(delta)/float64(b.N), counter.unit)
								}
							} else {
								key := "treedb.durable_root.manifest_build.bytes_encoded"
								b.ReportMetric(float64(value(after, key)-value(before, key))/float64(b.N), "manifest_encoded_bytes/op")
							}
							runtime.GC()
							var retainedAfter runtime.MemStats
							runtime.ReadMemStats(&retainedAfter)
							b.ReportMetric(float64(retainedBefore.HeapAlloc), "heap_before_bytes")
							b.ReportMetric(float64(retainedBefore.HeapObjects), "heap_before_objects")
							b.ReportMetric(float64(retainedAfter.HeapAlloc), "heap_after_bytes")
							b.ReportMetric(float64(retainedAfter.HeapObjects), "heap_after_objects")
							if !progress.Complete {
								b.Fatal("public import did not complete")
							}
							reader, err := c.OpenVectorPartitionSourceSnapshotV2(progress.Snapshot, input.IndexName)
							if err != nil {
								b.Fatal(err)
							}
							chunk, err := reader.ReadChunk(uint64(prior + b.N - 1))
							if err != nil {
								reader.Close()
								b.Fatal(err)
							}
							want := fmt.Sprintf("row-%020d", 2*(prior+b.N-1))
							if len(chunk.Chunk.Rows) != 2 || string(chunk.Chunk.Rows[0].DocumentID) != want {
								b.Fatalf("public import output differs: %+v", chunk)
							}
							if err := reader.Close(); err != nil {
								b.Fatal(err)
							}
							runtime.KeepAlive(c)
							runtime.KeepAlive(d)
						})
					}
				})
			}
		})
	}
}
