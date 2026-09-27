package raftfsm

// Bounded document-substrate evidence, identical on base and candidate. This
// does not exercise vector assets, ANN, or public replica replacement. Run each
// leaf in a fresh process with -benchtime=1x; retain JSON and process receipts.
import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

const documentEvidenceSeed uint64 = 0x475348115eed

// SplitMix64 with a fixed seed and alphabet: deterministic printable payloads,
// not a repeated compressible byte. Row identity participates in the seed.
func documentEvidenceRow(row int) ([]byte, []byte) {
	id := []byte(fmt.Sprintf("doc-%09d", row))
	x := documentEvidenceSeed + uint64(row)
	payload := make([]byte, 1024)
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	for i := range payload {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		payload[i] = alphabet[(z^(z>>31))&63]
	}
	return id, []byte(fmt.Sprintf(`{"_id":"%s","payload":"%s"}`, id, payload))
}

func documentEvidenceBatch(b *testing.B, first, count int) []byte {
	b.Helper()
	ids, docs := make([][]byte, count), make([][]byte, count)
	for i := range ids {
		ids[i], docs[i] = documentEvidenceRow(first + i)
	}
	return deterministicInsertBatchEntry(b, "users", fmt.Sprintf("evidence-insert-%d", first), iwire.DocumentFormatJSON, ids, docs)
}

func documentEvidenceApply(b *testing.B, f *FSM, index uint64, raw []byte) {
	b.Helper()
	result, err := f.ApplyCommittedEntryV1(committedCommand(2, index, raw))
	if err != nil || result.Status != raftentry.ApplyStatusApplied {
		b.Fatalf("populate index=%d result=%+v error=%v", index, result, err)
	}
}

func documentEvidenceCheck(b *testing.B, f *FSM, rows int, digest raftapply.LogicalDigestV1, applied raftentry.ApplyEntryID) int {
	b.Helper()
	got, err := f.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
	if err != nil || got != digest {
		b.Fatalf("logical digest got=%s want=%s error=%v", got.Hex(), digest.Hex(), err)
	}
	if actual, ok := f.LastApplied(); !ok || actual != applied {
		b.Fatalf("applied got=%+v known=%v want=%+v", actual, ok, applied)
	}
	coll, err := collections.NewCollectionManager(f.db).OpenCollection("users")
	if err != nil {
		b.Fatal(err)
	}
	actualRows := 0
	truncated, err := coll.ScanDocumentIDsFunc(rows+1, func([]byte) (bool, error) {
		actualRows++
		return true, nil
	})
	if err != nil || truncated || actualRows != rows {
		b.Fatalf("row count got=%d want=%d truncated=%v error=%v", actualRows, rows, truncated, err)
	}
	for _, row := range []int{0, rows / 2, rows - 1} {
		id, want := documentEvidenceRow(row)
		got, err := coll.Get(id)
		if err != nil || !bytes.Equal(got, want) {
			b.Fatalf("row %d mismatch: %v", row, err)
		}
	}
	return actualRows
}

func documentEvidenceLog(b *testing.B, value any) {
	b.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("DOCUMENT_SNAPSHOT_EVIDENCE %s", raw)
}

func documentEvidenceDiskBytes(b *testing.B, root string) int64 {
	b.Helper()
	var total int64
	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return total
}

type documentEvidenceReader struct {
	io.Reader
	bytes int64
}

func (r *documentEvidenceReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytes += int64(n)
	return n, err
}

func BenchmarkDocumentSnapshotGrowthV1(b *testing.B) {
	for _, rows := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			requireRaftSnapshotInstallSupportedV1(b)
			if b.N != 1 {
				b.Fatal("bounded evidence requires -benchtime=1x")
			}
			b.StopTimer()
			root := b.TempDir()
			sourceDir, targetDir := filepath.Join(root, "source"), filepath.Join(root, "target")
			sourceDB := openRaftSnapshotFSMTestDBWithOptions(b, sourceDir, true, true)
			defer sourceDB.Close()
			source := openRaftSnapshotFSMForTestWithOptions(b, sourceDB, sourceDir, true, true)
			defer source.Close()
			documentEvidenceApply(b, source, 1, deterministicCreateCollectionEntry(b, "users", "evidence-create"))
			index := uint64(2)
			for first := 0; first < rows; first += 128 {
				documentEvidenceApply(b, source, index, documentEvidenceBatch(b, first, min(128, rows-first)))
				index++
			}
			digest, err := source.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
			if err != nil {
				b.Fatal(err)
			}
			applied, ok := source.LastApplied()
			if !ok {
				b.Fatal("missing source applied boundary")
			}
			sourceRows := documentEvidenceCheck(b, source, rows, digest, applied)
			if err := sourceDB.Checkpoint(); err != nil {
				b.Fatal(err)
			}
			sourceBytes := documentEvidenceDiskBytes(b, sourceDir)
			targetDB := openRaftSnapshotFSMTestDBWithOptions(b, targetDir, true, true)
			defer targetDB.Close()
			target := openRaftSnapshotFSMForTestWithOptions(b, targetDB, targetDir, true, true)
			defer target.Close()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			start := time.Now()
			snapshot, err := source.ExportRaftSnapshotV1()
			if err != nil {
				b.Fatal(err)
			}
			defer snapshot.Release()
			reader, err := snapshot.OpenArchive()
			if err != nil {
				b.Fatal(err)
			}
			counted := &documentEvidenceReader{Reader: reader}
			installErr := target.InstallRaftSnapshotV1(counted)
			closeErr := reader.Close()
			elapsed := time.Since(start)
			b.StopTimer()
			runtime.ReadMemStats(&after)
			if installErr != nil || closeErr != nil {
				b.Fatalf("streamed install: %v / close: %v", installErr, closeErr)
			}
			if snapshot.Manifest.LogicalDigestV1 != digest.Hex() || snapshot.Manifest.LastIncludedIndex != applied.Index || snapshot.Manifest.LastIncludedTerm != applied.Term {
				b.Fatal("manifest does not bind source boundary")
			}
			installedRows := documentEvidenceCheck(b, target, rows, digest, applied)
			if err := snapshot.Release(); err != nil {
				b.Fatal(err)
			}
			if err := target.Close(); err != nil {
				b.Fatal(err)
			}
			reopenedDB := openRaftSnapshotFSMTestDBWithOptions(b, targetDir, true, true)
			defer reopenedDB.Close()
			reopened := openRaftSnapshotFSMForTestWithOptions(b, reopenedDB, targetDir, true, true)
			defer reopened.Close()
			reopenedRows := documentEvidenceCheck(b, reopened, rows, digest, applied)
			documentEvidenceLog(b, map[string]any{"kind": "document-export-install-v1", "rows": rows, "actual_source_rows": sourceRows, "actual_installed_rows": installedRows, "actual_reopened_rows": reopenedRows, "payload_bytes_per_row": 1024, "seed": documentEvidenceSeed, "source_tree_file_bytes_before_export": sourceBytes, "archive_bytes": counted.bytes, "elapsed_ns": elapsed.Nanoseconds(), "phase_total_alloc_bytes": after.TotalAlloc - before.TotalAlloc, "phase_mallocs": after.Mallocs - before.Mallocs, "manifest": snapshot.Manifest, "reopened_verified": true, "population": "direct committed FSM entries; existing helper DisableSync=true; no consensus timing"})
			b.ReportMetric(float64(counted.bytes), "archive-bytes")
		})
	}
}

// This timestamp includes native scheduling/configuration work as well as FSM
// capture. It is not a measurement of pure capture CPU or exact lock hold time.
type documentEvidenceSnapshotStore struct {
	hraft.SnapshotStore
	createAt atomic.Int64
}

func (s *documentEvidenceSnapshotStore) Create(v hraft.SnapshotVersion, index, term uint64, config hraft.Configuration, configIndex uint64, transport hraft.Transport) (hraft.SnapshotSink, error) {
	s.createAt.Store(time.Now().UnixNano())
	return s.SnapshotStore.Create(v, index, term, config, configIndex, transport)
}

func BenchmarkDocumentSnapshotForegroundV1(b *testing.B) {
	for _, concurrent := range []bool{false, true} {
		b.Run(fmt.Sprintf("snapshot=%t", concurrent), func(b *testing.B) {
			if b.N != 1 {
				b.Fatal("bounded evidence requires -benchtime=1x")
			}
			b.StopTimer()
			root := b.TempDir()
			dir := filepath.Join(root, "data")
			db := openRaftSnapshotFSMTestDBWithOptions(b, dir, true, true)
			defer db.Close()
			_, transport := hraft.NewInmemTransport("node-a")
			defer transport.Close()
			cfg := raftcluster.Config{Dir: dir, ClusterDir: filepath.Join(root, "raft"), NodeID: "node-a", GroupID: "default", DisableSideStores: true, Peers: []raftcluster.Peer{{ID: "node-a", Address: "node-a"}}}
			fsm, err := Open(Options{DB: db, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
			if err != nil {
				b.Fatal(err)
			}
			defer fsm.Close()
			files, err := hraft.NewFileSnapshotStore(filepath.Join(root, "snapshots"), 1, io.Discard)
			if err != nil {
				b.Fatal(err)
			}
			store := &documentEvidenceSnapshotStore{SnapshotStore: files}
			rc := hraft.DefaultConfig()
			rc.HeartbeatTimeout, rc.ElectionTimeout, rc.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
			rc.SnapshotInterval, rc.SnapshotThreshold, rc.LogOutput = time.Hour, 1<<60, io.Discard
			provider, err := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{Cluster: cfg, Applier: fsm, Transport: transport, RaftConfig: rc, Bootstrap: true, SnapshotStore: store})
			if err != nil {
				b.Fatal(err)
			}
			defer provider.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			for {
				status, err := provider.RuntimeStatusV1(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if status.State == "Leader" {
					break
				}
				select {
				case <-ctx.Done():
					b.Fatal(ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			commit := func(raw []byte) error {
				_, err := provider.CommitCommandEntryV1(ctx, raftcluster.CommitCommandEntryV1Request{NodeID: cfg.NodeID, GroupID: cfg.GroupID, EntryBytes: raw, CurrentCatalogVersion: testCatalogVersionStart, HasCurrentCatalogVersion: true, SyncLocalCommandWAL: true})
				return err
			}
			if err := commit(deterministicCreateCollectionEntry(b, "users", "evidence-create")); err != nil {
				b.Fatal(err)
			}
			for first := 0; first < 100000; first += 128 {
				if err := commit(documentEvidenceBatch(b, first, min(128, 100000-first))); err != nil {
					b.Fatal(err)
				}
			}
			populationDigest, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
			if err != nil {
				b.Fatal(err)
			}
			populationApplied, ok := fsm.LastApplied()
			if !ok {
				b.Fatal("missing population applied boundary")
			}
			initialRows := documentEvidenceCheck(b, fsm, 100000, populationDigest, populationApplied)
			const writes = 512
			commands := make([][]byte, writes)
			for i := range commands {
				commands[i] = documentEvidenceBatch(b, 100000+i, 1)
			}
			type sample struct {
				StartNS, EndNS int64
				Error          string
			}
			samples := make([]sample, writes)
			var snap raftcluster.HashicorpRaftSnapshotResultV1
			var snapshotErr error
			var snapshotStart, snapshotEnd time.Time
			done := make(chan struct{})
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			start := time.Now()
			completed := 0
			for i := range commands {
				if concurrent && i == 8 {
					started := make(chan struct{})
					go func() {
						defer close(done)
						snapshotStart = time.Now()
						close(started)
						snap, snapshotErr = provider.Snapshot(ctx)
						snapshotEnd = time.Now()
					}()
					<-started
				}
				samples[i].StartNS = time.Since(start).Nanoseconds()
				if err := commit(commands[i]); err != nil {
					samples[i].Error = err.Error()
				} else {
					completed++
				}
				samples[i].EndNS = time.Since(start).Nanoseconds()
			}
			foregroundElapsed := time.Since(start)
			if concurrent {
				<-done
			}
			elapsed := time.Since(start)
			b.StopTimer()
			runtime.ReadMemStats(&after)
			overlap, maxLatency := 0, int64(0)
			for _, s := range samples {
				maxLatency = max(maxLatency, s.EndNS-s.StartNS)
				if concurrent && s.StartNS < snapshotEnd.Sub(start).Nanoseconds() && s.EndNS > snapshotStart.Sub(start).Nanoseconds() {
					overlap++
				}
			}
			var requestToCreate int64
			if concurrent && store.createAt.Load() != 0 {
				requestToCreate = store.createAt.Load() - snapshotStart.UnixNano()
			}
			documentEvidenceLog(b, map[string]any{"kind": "document-snapshot-foreground-v1", "initial_rows": 100000, "actual_initial_rows": initialRows, "seed": documentEvidenceSeed, "snapshot": concurrent, "attempted": writes, "completed": completed, "errors": writes - completed, "overlapping_requests": overlap, "foreground_elapsed_ns": foregroundElapsed.Nanoseconds(), "phase_elapsed_ns": elapsed.Nanoseconds(), "max_request_latency_ns": maxLatency, "request_to_native_sink_create_ns": requestToCreate, "snapshot_elapsed_ns": snapshotEnd.Sub(snapshotStart).Nanoseconds(), "snapshot_start_ns": func() int64 {
				if !concurrent {
					return 0
				}
				return snapshotStart.Sub(start).Nanoseconds()
			}(), "snapshot_end_ns": func() int64 {
				if !concurrent {
					return 0
				}
				return snapshotEnd.Sub(start).Nanoseconds()
			}(), "snapshot_error": fmt.Sprint(snapshotErr), "archive_bytes": snap.SizeBytes, "phase_total_alloc_bytes": after.TotalAlloc - before.TotalAlloc, "phase_mallocs": after.Mallocs - before.Mallocs, "samples": samples})
			if completed != writes || snapshotErr != nil {
				b.Fatalf("foreground/native snapshot failed: %d/%d %v", completed, writes, snapshotErr)
			}
			if concurrent && (overlap == 0 || requestToCreate <= 0 || snap.SizeBytes <= 0) {
				b.Fatal("missing measured snapshot overlap/capture boundary; no interference conclusion")
			}
			coll, err := collections.NewCollectionManager(fsm.db).OpenCollection("users")
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < writes; i++ {
				id, want := documentEvidenceRow(100000 + i)
				got, err := coll.Get(id)
				if err != nil || !bytes.Equal(got, want) {
					b.Fatalf("acknowledged tail row %d absent: %v", i, err)
				}
			}
			digest, err := fsm.LogicalDigestV1(raftapply.LogicalDigestOptionsV1{})
			if err != nil {
				b.Fatal(err)
			}
			applied, ok := fsm.LastApplied()
			if !ok {
				b.Fatal("missing foreground applied boundary")
			}
			if concurrent && (snap.Manifest.LastIncludedIndex != snap.LastIncludedIndex || snap.Manifest.LastIncludedTerm != snap.LastIncludedTerm || snap.Manifest.LogicalDigestV1 == "" || snap.LastIncludedIndex > applied.Index) {
				b.Fatal("invalid native snapshot boundary")
			}
			if err := provider.Close(); err != nil {
				b.Fatal(err)
			}
			if err := fsm.Close(); err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			reopenedDB := openRaftSnapshotFSMTestDBWithOptions(b, dir, true, true)
			defer reopenedDB.Close()
			reopened, err := Open(Options{DB: reopenedDB, Cluster: cfg, StoreOptions: raftapply.DurableApplyStoreOptions{AllowInitialIndexGap: true}})
			if err != nil {
				b.Fatal(err)
			}
			defer reopened.Close()
			reopenedRows := documentEvidenceCheck(b, reopened, 100000+writes, digest, applied)
			documentEvidenceLog(b, map[string]any{"kind": "document-snapshot-foreground-verification-v1", "snapshot": concurrent, "acknowledged_rows_verified": writes, "actual_initial_rows": initialRows, "actual_reopened_rows": reopenedRows, "reopened_digest": digest.Hex(), "reopened_applied": applied, "native_snapshot_manifest": snap.Manifest})
			b.ReportMetric(float64(foregroundElapsed.Nanoseconds())/writes, "foreground-ns/write")
			b.ReportMetric(float64(maxLatency), "max-write-ns")
		})
	}
}
