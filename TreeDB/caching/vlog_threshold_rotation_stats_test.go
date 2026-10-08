package caching

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafLogThresholdStatsTrackActualPhysicalWorkerRotations(t *testing.T) {
	db, captured, _ := openCachingLeafPageLogLaneTestDB(t)
	defer db.Close()
	// A small test cap exercises the observer's meaning, not default economics.
	db.valueLogMaxSegmentBytes = 2 * page.PageSize
	provider := captured.leafLog.(backenddb.LeafPageLogLaneProvider)
	var pointers []page.LeafLogPtr
	var payloads [][]byte
	pagePayload := func(worker, ordinal int) []byte {
		payload := make([]byte, page.PageSize)
		_, _ = rand.New(rand.NewSource(int64(worker*100 + ordinal + 1))).Read(payload)
		return payload
	}
	for worker := range 4 {
		log, ok := provider.LeafPageLogLane(worker)
		if !ok {
			t.Fatalf("missing physical worker %d", worker)
		}
		payload := pagePayload(worker, 0)
		ptr, err := log.AppendLeafPage(payload)
		if err != nil {
			t.Fatal(err)
		}
		pointers = append(pointers, ptr)
		payloads = append(payloads, payload)
		l := db.leafLogAppendLanesSnapshot()[worker]
		if got := l.vlogRotateThresholdTotal.Load(); got != 0 {
			t.Fatalf("creation counted as threshold: %d", got)
		}
		for i := range 8 {
			payload = pagePayload(worker, i+1)
			ptr, err = log.AppendLeafPage(payload)
			if err != nil {
				t.Fatal(err)
			}
			pointers = append(pointers, ptr)
			payloads = append(payloads, payload)
		}
		threshold := l.vlogRotateThresholdTotal.Load()
		if threshold < 2 {
			t.Fatalf("worker%d natural rotations=%d want >=2", worker, threshold)
		}
		l.vlogMu.Lock()
		err = db.rotateValueLogMuHeld(l)
		l.vlogMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if got := l.vlogRotateThresholdTotal.Load(); got != threshold {
			t.Fatalf("explicit rotation counted as threshold: %d -> %d", threshold, got)
		}
		stats := db.Stats()
		prefix := fmt.Sprintf("treedb.cache.leaf_log_lanes.lane.%02d", worker)
		if got := requireStatUint64(t, stats, prefix+".threshold_rotations_total"); got != threshold {
			t.Fatalf("threshold stat=%d want%d", got, threshold)
		}
		fileID, err := valuelog.EncodeFileID(uint32(l.id), uint32(l.vlogSeq))
		if err != nil {
			t.Fatal(err)
		}
		if requireStatUint64(t, stats, prefix+".current_sequence") != uint64(l.vlogSeq) || requireStatUint64(t, stats, prefix+".current_file_id") != uint64(fileID) {
			t.Fatal("physical current identity mismatch")
		}
		if got := requireStatUint64(t, stats, prefix+".maintenance_handoffs_total"); got != 0 {
			t.Fatalf("ordinary rotation fabricated handoff: %d", got)
		}
	}
	for i, ptr := range pointers {
		got, err := db.ReadValueLogRecord(ptr.ValuePtr())
		if err != nil || !bytes.Equal(got, payloads[i]) {
			t.Fatalf("old physical pointer%d unreadable: %v", i, err)
		}
	}
}

func TestThresholdRotationStatsCountInstalledRotationOnly(t *testing.T) {
	for _, installed := range []bool{false, true} {
		t.Run(fmt.Sprintf("installed=%t", installed), func(t *testing.T) {
			dir := t.TempDir()
			writer := &rotateFailValueWriter{size: 128, installed: installed}
			l := &lane{id: 0, vlog: writer, vlogSeq: 59, vlogPath: filepath.Join(dir, valueLogName(0, 59))}
			db := &DB{dir: dir, valueLogMaxSegmentBytes: 64}
			if err := db.rotateValueLogForMaxSegmentMuHeld(l, writer); !errors.Is(err, errRotateFailed) {
				t.Fatalf("rotation error=%v", err)
			}
			want := uint64(0)
			if installed {
				want = 1
			}
			if got := l.vlogRotateThresholdTotal.Load(); got != want {
				t.Fatalf("threshold=%d want%d for installed=%t", got, want, installed)
			}
			if got := l.vlogSeq; got != 59+int(want) {
				t.Fatalf("current sequence=%d want%d", got, 59+int(want))
			}
		})
	}
}
