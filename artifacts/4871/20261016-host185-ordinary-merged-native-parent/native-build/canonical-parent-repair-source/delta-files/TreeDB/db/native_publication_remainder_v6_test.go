package db

import (
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

// These are arithmetic/control tests. They construct no DB, candidate,
// allocator, owner, channel, timer, builder, page or publication.
func TestNativePublicationRemainderAndFloorV6(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux single-attempt diagnostic pricing")
	}
	g := freelist.NativePublicationBoundV6{Prefix: 1, ClaimedIDs: 49, MetadataPages: 3, PublicationRecords: 14 + 49, PublicationBytes: 4096, PageRecords: 22, PageBytes: 32768, MaterializationRecords: 10, MaterializationBytes: 4096}
	p, ready, err := nativePublicationRemainderForGeometryV6(g, 1)
	if !ready || err != nil || p.records != 190+g.ClaimedIDs || p.bytes == 0 {
		t.Fatal(p, ready, err)
	}
	t.Logf("POST-ACTIVATION ONLY records=%d bytes=%d P=1 M=3 S=1 N-upper=49; synthetic arithmetic operands, not actual33 observation or runtime allowance", p.records, p.bytes)
	for _, outcome := range []string{"success", "refusal", "error"} {
		t.Run(outcome, func(t *testing.T) {
			w := iterator.OrdinalScanWork{RecordLimit: p.records + 100, ByteLimit: p.bytes + 1<<20}
			records, bytes := w.RecordLimit, w.ByteLimit
			func() {
				f, ok := p.withhold(&w)
				if !ok {
					t.Fatal("withhold refused", w)
				}
				defer f.restore()
				if w.RecordLimit != records-p.records || w.ByteLimit != bytes-p.bytes {
					t.Fatal("wrong remaining floor", w)
				}
				if outcome == "success" {
					if !w.Reserve(2, 16) {
						t.Fatal("original activation work refused")
					}
					f.restore()
				}
			}()
			if w.RecordLimit != records || w.ByteLimit != bytes {
				t.Fatal("limits escaped invocation", w)
			}
		})
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if f, ok := p.withhold(&w); ok || f.work != nil || w.RecordLimit != 32 || w.ByteLimit != 1<<20 {
		t.Fatal("Q32 floor refusal changed original limits", w)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20, Records: 33}
	if _, ok := p.withhold(&w); ok || w.RecordLimit != 32 {
		t.Fatal("invalid consumption wrapped remaining work")
	}
	if _, _, ok := checkedNativeAuthorityAdmissionV6(^uint64(0), 1, 1, 1); ok {
		t.Fatal("authority record overflow accepted")
	}
	if _, _, ok := checkedNativeAuthorityAdmissionV6(1, 1, ^uint64(0)/3, 1); ok {
		t.Fatal("authority byte overflow accepted")
	}
	g.PublicationBytes = ^uint64(0)
	if _, ok, err := nativePublicationRemainderForGeometryV6(g, 1); ok || err == nil {
		t.Fatal("composite byte overflow accepted")
	}
}

func TestNativePostFenceCapabilitySkipsSnapshotScanV6(t *testing.T) {
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
	previous := minPinnedSnapshotCommitSeqAfterScanForTesting
	calls := 0
	minPinnedSnapshotCommitSeqAfterScanForTesting = func() { calls++ }
	minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	defer func() {
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Lock()
		minPinnedSnapshotCommitSeqAfterScanForTesting = previous
		minPinnedSnapshotCommitSeqAfterScanForTestingMu.Unlock()
	}()
	// Zero DB is sufficient for this pure capability branch: no index, snapshot,
	// physical file, allocator, current-root install or publication exists.
	d := &DB{}
	current := durableRootRuntimeV1{slotCommit: [2]uint64{2, 3}}
	native, err := d.postFenceReuseCapabilityV6(current, &nativeAuthorityScopeV6{})
	if err != nil || native != (freelist.ReuseCapability{}) || calls != 0 {
		t.Fatal("native computed live reuse capability", native, calls, err)
	}
	ordinary, err := d.postFenceReuseCapabilityV6(current, nil)
	if err != nil || ordinary == (freelist.ReuseCapability{}) || calls != 1 {
		t.Fatal("ordinary capability scan changed", ordinary, calls, err)
	}
	if current.slotCommit != [2]uint64{2, 3} {
		t.Fatal("capability branch mutated original slots")
	}
}
