package mvcc

import (
	"bytes"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"strconv"
	"testing"
)

func TestPruneQuantumPublicProducerFixedQTerminalDiagnostic(t *testing.T) {
	for _, profile := range []treedb.Profile{treedb.ProfileCommandWALDurable, treedb.ProfileNoWALFast} {
		for _, extra := range []int{512, 4096} {
			t.Run(string(profile)+"/extra="+strconv.Itoa(extra), func(t *testing.T) {
				options := treedb.OptionsFor(profile, t.TempDir())
				options.IndexOuterLeavesInValueLog = true
				options.ValueLog.PointerThreshold = 1
				db, err := treedb.Open(options)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = db.Close() }()
				store := New(db)
				const history = 1024
				payload := bytes.Repeat([]byte("old-pointer"), 64)
				batch := db.NewBatchWithSize(history)
				for ts := uint64(1); ts <= history; ts++ {
					key, err := mvcckey.Encode([]byte("gone"), ts)
					if err != nil {
						t.Fatal(err)
					}
					record := append([]byte{recordValueV1}, payload...)
					if ts == history {
						record = []byte{recordTombstoneV1}
					}
					if err := batch.Set(key, record); err != nil {
						t.Fatal(err)
					}
				}
				if err := batch.Write(); err != nil {
					t.Fatal(err)
				}
				if err := batch.Close(); err != nil {
					t.Fatal(err)
				}
				if err := store.AdvanceDiscardFloor(history, CommitDurable); err != nil {
					t.Fatal(err)
				}
				if err := db.Checkpoint(); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = treedb.Open(options)
				if err != nil {
					t.Fatal(err)
				}
				// Native raw posting ingress can publish an older physical key after
				// the floor persisted. It must not resurrect behind later delete ACK.
				oldKey, err := mvcckey.Encode([]byte("gone"), history-1)
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Set(oldKey, append([]byte{recordValueV1}, payload...)); err != nil {
					t.Fatal(err)
				}
				oldReader := db.AcquireSnapshot()
				if oldReader == nil {
					t.Fatal("capture queued old put")
				}
				defer oldReader.Close()
				queue, err := strconv.ParseUint(db.Stats()["treedb.cache.queue_len"], 10, 64)
				if err != nil || queue == 0 {
					t.Fatalf("queued put fixture: queue=%d err=%v", queue, err)
				}
				// Pairwise ascending physical keys followed by an order break
				// produce at least 256 run transitions; long logical keys exercise
				// ordinary key-arena directories. These are real public writes.
				logical := append([]byte("future/"), bytes.Repeat([]byte("k"), 900)...)
				var sourceReaders []treedb.Snapshot
				defer func() {
					for _, reader := range sourceReaders {
						_ = reader.Close()
					}
				}()
				for i := 0; i < extra; i++ {
					ts := uint64(history + (i/2)*2 + 2 - i%2)
					key, err := mvcckey.Encode(logical, ts)
					if err != nil {
						t.Fatal(err)
					}
					if err := db.Set(key, []byte{recordValueV1, 'v'}); err != nil {
						t.Fatal(err)
					}
					if (i+1)%512 == 0 && i+1 < extra {
						reader := db.AcquireSnapshot()
						if reader == nil {
							t.Fatal("capture additional queued source")
						}
						sourceReaders = append(sourceReaders, reader)
					}
				}
				old, err := oldReader.Get(oldKey)
				if err != nil || !bytes.Equal(old, append([]byte{recordValueV1}, payload...)) {
					t.Fatalf("retained old pointer: %v", err)
				}
				if err := oldReader.Close(); err != nil {
					t.Fatal(err)
				}
				for _, reader := range sourceReaders {
					if err := reader.Close(); err != nil {
						t.Fatal(err)
					}
				}
				queue, err = strconv.ParseUint(db.Stats()["treedb.cache.queue_len"], 10, 64)
				if err != nil || queue == 0 {
					t.Fatalf("lost queued fixture: queue=%d err=%v", queue, err)
				}
				store = New(db) // floor remains cold; no GetAt/DiscardFloor warmup.
				t.Logf("fixture profile=%s history=%d extra=%d queued=%d Q=32/1048576", profile, history, extra, queue)
				var cursor *PruneCursor
				var deleted uint64
				interrupted := false

				var total PruneStats
				phases := make(map[string]PruneStats)
				calls := make(map[string]uint64)
				firstACK, lastACK := -1, -1
				var lastStats PruneStats
				var lastRecord iterator.OrdinalRecordCursor
				var recordChanged, recordSame, recordResets, pointerChanges uint64
				var runSame, maxSame uint64
				var lastFingerprint pruneTerminalDiagnosticFingerprint
				var haveFingerprint bool
				printDiagnostic := func() {
					t.Logf("DIAGNOSTIC total=%+v firstACK=%d lastACK=%d deleted=%d interrupted=%v recordChanged=%d recordSame=%d recordResets=%d pointerChanges=%d maxIdenticalStateRun=%d", total, firstACK, lastACK, deleted, interrupted, recordChanged, recordSame, recordResets, pointerChanges, maxSame)
					for _, phase := range []string{"capture-floor", "anchor-reproof", "seek", "winner-metadata", "winner-value", "candidate-decision", "prepare", "sync", "delete"} {
						t.Logf("DIAGNOSTIC phase=%s calls=%d totals=%+v", phase, calls[phase], phases[phase])
					}
					t.Logf("DIAGNOSTIC lastStats=%+v", lastStats)
					if cursor != nil {
						t.Logf("DIAGNOSTIC terminal action=%d prepDone=%v floorSynced=%v loadingFloor=%v floorChecked=%v orderedDone=%v anchorReproof=%v haveAnchor=%v metadataRead=%v validated=%v candidateTS=%d candidatePtr=%+v anchorTS=%d anchorPtr=%+v record=%+v ordinal=%+v", cursor.action, cursor.preparationDone, cursor.floorSynced, cursor.loadingFloor, cursor.floorChecked, cursor.orderedDone, cursor.anchorReproof, cursor.haveAnchor, cursor.metadataRead, cursor.validated, cursor.candidate.timestamp, cursor.candidate.ptr, cursor.anchor.timestamp, cursor.anchor.ptr, cursor.record, cursor.ordinal)
					}
				}
				for quantum := 0; quantum < (history+extra)*16; quantum++ {
					phase := pruneTerminalDiagnosticPhase(cursor)
					stats, err := store.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: cursor})

					lastStats = stats
					pruneTerminalDiagnosticAdd(&total, stats)
					aggregate := phases[phase]
					pruneTerminalDiagnosticAdd(&aggregate, stats)
					phases[phase], calls[phase] = aggregate, calls[phase]+1
					if stats.Pruned != 0 {
						if firstACK < 0 {
							firstACK = quantum
						}
						lastACK = quantum
					}
					if stats.Cursor != nil {
						nextRecord := stats.Cursor.record
						if nextRecord == lastRecord {
							recordSame++
						} else {
							recordChanged++
						}
						if lastRecord.Active && !nextRecord.Active {
							recordResets++
						}
						if lastRecord.Pointer != nextRecord.Pointer {
							pointerChanges++
						}
						lastRecord = nextRecord
						fingerprint := pruneTerminalDiagnosticFingerprint{record: nextRecord, phase: pruneTerminalDiagnosticPhase(stats.Cursor), candidateTS: stats.Cursor.candidate.timestamp, anchorTS: stats.Cursor.anchor.timestamp, bound: string(stats.Cursor.bound)}
						if haveFingerprint && fingerprint == lastFingerprint {
							runSame++
						} else {
							runSame = 1
						}
						if runSame > maxSame {
							maxSame = runSame
						}
						lastFingerprint, haveFingerprint = fingerprint, true
					}
					if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 {
						t.Fatalf("unadmitted physical/setup/publication/cleanup quantum %d: %+v", quantum, stats)
					}
					if err != nil {
						t.Fatalf("fixed-Q public first-delete gate quantum=%d deleted=%d: %v; stats=%+v", quantum, deleted, err, stats)
					}
					deleted += stats.Pruned
					if stats.Pruned != 0 && !interrupted {
						if err := db.Close(); err != nil {
							t.Fatal(err)
						}
						db, err = treedb.Open(options)
						if err != nil {
							t.Fatal(err)
						}
						store, cursor, interrupted = New(db), nil, true
						continue
					}
					if stats.Complete {
						if !interrupted || deleted != history {
							t.Fatalf("ACK/reclaim incomplete interrupted=%v deletes=%d want=%d", interrupted, deleted, history)
						}
						requireResult(t, store, []byte("gone"), history+1, Absent, 0, nil)
						requireResult(t, store, logical, uint64(history+extra+1), Present, uint64(history+extra), []byte("v"))
						printDiagnostic()
						return
					}
					if stats.Cursor == nil {
						t.Fatal("missing continuation before ACK+cleanup")
					}
					cursor = stats.Cursor
				}
				printDiagnostic()
				t.Fatal("fixed-Q progress did not reach complete physical anchor retirement")
			})
		}
	}
}

// This test-only copy changes observation only; the original public gate remains unchanged.
type pruneTerminalDiagnosticFingerprint struct {
	record                iterator.OrdinalRecordCursor
	phase, bound          string
	candidateTS, anchorTS uint64
}

func pruneTerminalDiagnosticPhase(c *PruneCursor) string {
	if c == nil || c.loadingFloor {
		return "capture-floor"
	}
	if c.action != 0 {
		if !c.preparationDone {
			return "prepare"
		}
		if !c.floorSynced {
			return "sync"
		}
		return "delete"
	}
	if c.anchorReproof {
		return "anchor-reproof"
	}
	if !c.orderedDone {
		return "seek"
	}
	if !c.metadataRead {
		return "winner-metadata"
	}
	if !c.validated {
		return "winner-value"
	}
	return "candidate-decision"
}
func pruneTerminalDiagnosticAdd(dst *PruneStats, src PruneStats) {
	dst.Visited += src.Visited
	dst.Skipped += src.Skipped
	dst.Retained += src.Retained
	dst.Pruned += src.Pruned
	dst.WorkRecords += src.WorkRecords
	dst.WorkBytes += src.WorkBytes
	dst.SetupRecords += src.SetupRecords
	dst.CleanupRecords += src.CleanupRecords
	dst.Batches += src.Batches
}
