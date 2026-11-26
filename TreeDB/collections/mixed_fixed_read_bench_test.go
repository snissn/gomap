package collections_test

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	fixedReadLoadBurstOps          = 256
	fixedReadLoadLatencySampleMask = 15 // sample 1 in 16 reads
	fixedReadLoadMaxSamplesPerG    = 16384
)

type fixedReadLoadCase struct {
	name          string
	targetReadsPS int
}

func benchmarkCollectionMixedFixedReadLoadPrimary(b *testing.B, targetReadsPS int) {
	if targetReadsPS <= 0 {
		b.Fatalf("targetReadsPS=%d must be positive", targetReadsPS)
	}
	indexes := collectionShapeIndexes(2)
	collectionName := fmt.Sprintf("bench_mixed_fixed_read_%d", targetReadsPS)
	backend, seedCollection := openBenchmarkCollection(b, collectionName, indexes...)
	seedDocs := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_SEED_DOCS", defaultCollectionMixedSeedDocs)
	if seedDocs <= 0 {
		seedDocs = defaultCollectionMixedSeedDocs
	}
	ids := seedBenchmarkCollection(b, seedCollection, 0, seedDocs, true)
	if err := seedCollection.Flush(); err != nil {
		b.Fatalf("flush fixed-read seed collection: %v", err)
	}
	benchmarkSyncBoundary(b, backend)

	manager := collections.NewCollectionManager(backend)
	readerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open fixed-read reader collection: %v", err)
	}
	writerCollection, err := manager.OpenCollection(collectionName)
	if err != nil {
		b.Fatalf("open fixed-read writer collection: %v", err)
	}

	writeBatchSize := benchmarkIntEnv(b, "TREEDB_COLLECTION_MIXED_WRITE_BATCH_SIZE", defaultCollectionMixedWriteBatchSize)
	if writeBatchSize <= 0 {
		writeBatchSize = defaultCollectionMixedWriteBatchSize
	}
	if maxBatch := benchmarkBatchSize(b); writeBatchSize > maxBatch {
		writeBatchSize = maxBatch
	}
	documentFormat := benchmarkCollectionDocumentFormat(b)

	readers := runtime.GOMAXPROCS(0)
	if readers <= 0 {
		readers = 1
	}
	perReaderRate := float64(targetReadsPS) / float64(readers)
	burstNs := float64(fixedReadLoadBurstOps) * 1e9 / perReaderRate
	if burstNs < 1 {
		burstNs = 1
	}

	var stop atomic.Bool
	var achievedReads atomic.Uint64
	readerErrCh := make(chan error, 1)
	samplesByReader := make([][]int64, readers)
	for i := range samplesByReader {
		samplesByReader[i] = make([]int64, 0, 1024)
	}
	var readerWG sync.WaitGroup
	var readyWG sync.WaitGroup
	readyWG.Add(readers)
	startGate := make(chan struct{})

	spacing := len(ids) / readers
	if spacing <= 0 {
		spacing = 1
	}

	for r := 0; r < readers; r++ {
		readerWG.Add(1)
		go func(readerID int) {
			defer readerWG.Done()
			idx := (readerID * spacing) % len(ids)
			samples := samplesByReader[readerID]
			readyWG.Done()
			<-startGate

			origin := time.Now()
			var bursts uint64
			var localReads uint64
			for !stop.Load() {
				for j := 0; j < fixedReadLoadBurstOps; j++ {
					sample := (localReads & fixedReadLoadLatencySampleMask) == 0
					var t0 time.Time
					if sample {
						t0 = time.Now()
					}
					if _, err := readerCollection.Get(ids[idx]); err != nil {
						select {
						case readerErrCh <- fmt.Errorf("fixed-read reader %d: %w", readerID, err):
						default:
						}
						stop.Store(true)
						samplesByReader[readerID] = samples
						return
					}
					if sample && len(samples) < fixedReadLoadMaxSamplesPerG {
						samples = append(samples, time.Since(t0).Nanoseconds())
					}
					localReads++
					idx += readers
					if idx >= len(ids) {
						idx %= len(ids)
					}
				}
				bursts++
				if stop.Load() {
					break
				}
				targetElapsed := time.Duration(float64(bursts) * burstNs)
				if wait := targetElapsed - time.Since(origin); wait > 0 {
					if wait >= 250*time.Microsecond {
						time.Sleep(wait)
					} else {
						for !stop.Load() && time.Since(origin) < targetElapsed {
							runtime.Gosched()
						}
					}
				}
			}
			achievedReads.Add(localReads)
			samplesByReader[readerID] = samples
		}(r)
	}

	readyWG.Wait()

	var templateEncoder collections.TemplateV1Encoder
	docIDs := make([][]byte, writeBatchSize)
	docs := make([][]byte, writeBatchSize)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	close(startGate)

	writtenDocs := 0
	batches := 0
	startDocNum := 1_000_000
	if seedDocs >= startDocNum {
		startDocNum = seedDocs + 1
	}
	for next := startDocNum; writtenDocs < b.N; next += writeBatchSize {
		count := writeBatchSize
		if remaining := b.N - writtenDocs; remaining < count {
			count = remaining
		}
		for i := 0; i < count; i++ {
			docNum := next + i
			docIDs[i] = benchmarkDocumentID(docNum)
			if documentFormat == collections.DocumentFormatTemplateV1 {
				doc, err := templateEncoder.EncodeDocument(
					[]string{"name", "email", "city", "pad"},
					[]any{
						fmt.Sprintf("user-%09d", docNum),
						fmt.Sprintf("user-%09d@example.com", docNum),
						fmt.Sprintf("city-%02d", docNum%collectionBenchCities),
						collectionBenchIndexedPad,
					},
				)
				if err != nil {
					stop.Store(true)
					readerWG.Wait()
					b.Fatalf("encode fixed-read doc: %v", err)
				}
				docs[i] = doc
			} else if documentFormat == collections.DocumentFormatBSON {
				docs[i] = benchmarkBSONDocument(b, docNum, true)
			} else {
				docs[i] = benchmarkIndexedDocument(docNum)
			}
		}
		if _, err := writerCollection.InsertBatch(docIDs[:count], docs[:count]); err != nil {
			stop.Store(true)
			readerWG.Wait()
			b.Fatalf("fixed-read writer InsertBatch: %v", err)
		}
		writtenDocs += count
		batches++
	}

	b.StopTimer()
	elapsed := time.Since(start)
	stop.Store(true)
	readerWG.Wait()

	select {
	case err := <-readerErrCh:
		b.Fatal(err)
	default:
	}

	var allSamples []int64
	totalSamples := 0
	for _, s := range samplesByReader {
		totalSamples += len(s)
	}
	if totalSamples > 0 {
		allSamples = make([]int64, 0, totalSamples)
		for _, s := range samplesByReader {
			allSamples = append(allSamples, s...)
		}
		slices.Sort(allSamples)
	}

	b.ReportMetric(float64(seedDocs), "seed_docs")
	b.ReportMetric(float64(writeBatchSize), "writer_docs/batch")
	b.ReportMetric(float64(targetReadsPS), "target_reads/sec")
	if elapsed > 0 {
		b.ReportMetric(float64(writtenDocs)/elapsed.Seconds(), "writer_docs/sec")
		b.ReportMetric(float64(achievedReads.Load())/elapsed.Seconds(), "achieved_reads/sec")
	}
	if len(allSamples) > 0 {
		p50 := allSamples[len(allSamples)/2]
		p99Idx := (len(allSamples)*99 + 99) / 100
		if p99Idx >= len(allSamples) {
			p99Idx = len(allSamples) - 1
		}
		p99 := allSamples[p99Idx]
		b.ReportMetric(float64(p50), "read_p50_ns")
		b.ReportMetric(float64(p99), "read_p99_ns")
	}
	_ = batches
}

// BenchmarkCollectionMixedFixedReadLoadPrimary measures steady-state writer
// throughput (ns/writer_doc and writer_docs/sec) and reader p50/p99 latency
// under fixed target primary-read rates so faster readers cannot confound
// writer throughput comparisons.
func BenchmarkCollectionMixedFixedReadLoadPrimary(b *testing.B) {
	cases := []fixedReadLoadCase{
		{name: "rate_500k", targetReadsPS: 500_000},
		{name: "rate_1500k", targetReadsPS: 1_500_000},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCollectionMixedFixedReadLoadPrimary(b, tc.targetReadsPS)
		})
	}
}
