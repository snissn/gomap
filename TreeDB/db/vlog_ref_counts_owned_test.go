package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type refCountsBudget struct{ cap, bytes uint64 }
type refCountsLease struct {
	b     *refCountsBudget
	bytes uint64
}

func (b *refCountsBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	if n > b.cap-b.bytes {
		return nil, retainedalloc.ErrCapacity
	}
	b.bytes += n
	return &refCountsLease{b: b, bytes: n}, nil
}
func (l *refCountsLease) Resize(n uint64) error {
	if n > l.bytes && n-l.bytes > l.b.cap-l.b.bytes {
		return retainedalloc.ErrCapacity
	}
	l.b.bytes = l.b.bytes - l.bytes + n
	l.bytes = n
	return nil
}
func (l *refCountsLease) Close() { l.b.bytes -= l.bytes; l.bytes = 0 }

func TestValueLogRefCountOwnedCollisionDeletionGrowthAndTransfer(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	records := valueLogRefCounts{metadata: &owner}
	defer records.close()
	if err := records.set(0, 1); err == nil {
		t.Fatal("zero sentinel admitted")
	}
	id, err := valuelog.EncodeFileID(0, 0)
	if err != nil || id == 0 || !page.IsValueLogFileID(id) {
		t.Fatalf("actual segment-zero domain id=%d error=%v", id, err)
	}
	var ids [5]uint32
	n := 0
	for candidate := uint32(1); n < len(ids); candidate++ {
		marked := page.ValueLogFileID(candidate)
		if refCountHash(marked)&7 == refCountHash(id)&7 {
			ids[n] = marked
			n++
		}
	}
	for i := 0; i < 4; i++ {
		if err := records.set(ids[i], uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	if len(records.slots) != 8 || records.used != 4 {
		t.Fatalf("actual capacity=%d used=%d", len(records.slots), records.used)
	}
	for i := 0; i < 4; i++ {
		if records.get(ids[i]) != uint64(i+1) {
			t.Fatal("collision lost count")
		}
	}
	if err := records.set(ids[1], 0); err != nil {
		t.Fatal(err)
	}
	if records.get(ids[2]) != 3 || records.get(ids[1]) != 0 || records.tombstones != 1 {
		t.Fatal("tombstone broke probe chain")
	}
	if err := records.set(ids[4], math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if records.get(ids[4]) != math.MaxUint64 || records.tombstones != 0 {
		t.Fatal("tombstone/value sentinel conflated")
	}
	old := records.slots
	if err := records.set(id, 5); err != nil {
		t.Fatal(err)
	}
	if len(records.slots) != 16 || records.used != 5 {
		t.Fatal("geometric growth not installed")
	}
	for _, s := range old {
		if s != (valueLogRefCountSlot{}) {
			t.Fatal("old backing not scrubbed before refund")
		}
	}
	want := retainedalloc.AllocationCharge(uint64(cap(records.slots)) * uint64(unsafe.Sizeof(valueLogRefCountSlot{})))
	if owner.Bytes() != want {
		t.Fatalf("actual containing backing charge=%d want=%d", owner.Bytes(), want)
	}
	original := &records.slots[0]
	tracker := valueLogRefTracker{metadata: &owner}
	tracker.replaceOwned(&records, 9, valueLogRefIncarnation{}, true)
	if records.slots != nil || records.charge != 0 || &tracker.counts.slots[0] != original || owner.Bytes() != want {
		t.Fatal("transfer cloned or refunded live backing")
	}
	records.close()
	if tracker.counts.get(id) != 5 || owner.Bytes() != want {
		t.Fatal("donor disposal affected live tracker")
	}
	tracker.counts.close()
	if owner.Bytes() != 0 {
		t.Fatal("last payload disposal did not refund")
	}
}
func TestValueLogRefCountOwnedGrowthRefusalPreservesOriginal(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &refCountsBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	records := valueLogRefCounts{metadata: &owner}
	defer records.close()
	for i := uint32(1); i <= 4; i++ {
		if err := records.set(i, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	original := &records.slots[0]
	before := records.toMap()
	bytes := owner.Bytes()
	// Fits a replacement only after an illicit refund of still-live old backing.
	replacement := retainedalloc.AllocationCharge(16 * uint64(unsafe.Sizeof(valueLogRefCountSlot{})))
	budget.cap = bytes + replacement - 1
	if err := records.set(5, 5); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("growth refusal=%v", err)
	}
	if &records.slots[0] != original || !reflect.DeepEqual(records.toMap(), before) || owner.Bytes() != bytes || budget.bytes != bytes {
		t.Fatal("refused growth mutated/refunded original payload")
	}
	budget.cap = bytes + replacement
	if err := records.set(5, 5); err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != bytes-recordsChargeForSlots(8)+replacement {
		t.Fatal("growth overlap was not released after copy")
	}
}
func recordsChargeForSlots(n uint64) uint64 {
	return retainedalloc.AllocationCharge(n * uint64(unsafe.Sizeof(valueLogRefCountSlot{})))
}
func TestValueLogRefCountFullProbeAndOptionalDeltaRefusal(t *testing.T) {
	records := valueLogRefCounts{slots: make([]valueLogRefCountSlot, 8), used: 8}
	for i := range records.slots {
		records.slots[i] = valueLogRefCountSlot{fileID: uint32(i + 1), count: 1}
	}
	if at, found := records.find(99); at != -1 || found {
		t.Fatal("full probe failed to terminate")
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &refCountsBudget{cap: 1 << 20}
	e, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	tracker := valueLogRefTracker{metadata: &owner}
	counts, err := valueLogRefCountsFromMap(&owner, map[uint32]uint64{1: 1, 2: 1, 3: 1, 4: 1})
	if err != nil {
		t.Fatal(err)
	}
	tracker.replaceOwned(&counts, 10, valueLogRefIncarnation{indexID: 7}, true)
	budget.cap = owner.Bytes()
	delta := newValueLogRefDelta()
	defer releaseValueLogRefDelta(delta)
	delta.add(5, 1)
	if err := tracker.applyDelta(11, 7, delta); err != nil {
		t.Fatalf("optional post-acceptance cache reservation rejected accepted operation: %v", err)
	}
	if tracker.valid || tracker.counts.slots != nil {
		t.Fatal("partial optional cache remained eligible")
	}
}
func TestValueLogRefCountOriginalCollectorTransfersAndPreservesDiagnostics(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ptrs := appendPointersInNewSegment(t, d.dir, 0, 17, 1700, 1, func(int) []byte { return []byte("owned-count-control") })
	b := d.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("real-key"), ptrs[0]); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	records, seq, pair, err := d.scanValueLogRefCountsOwned(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer records.close()
	if records.used != 1 || records.metadata != d.valueLogRefTracker.metadata || records.get(ptrs[0].FileID) != 1 {
		t.Fatal("original collector missed admitted real pointer")
	}
	original := &records.slots[0]
	d.valueLogRefTracker.replaceOwned(&records, seq, pair, true)
	if &d.valueLogRefTracker.counts.slots[0] != original || records.slots != nil {
		t.Fatal("production transfer cloned collector")
	}
	// Use an actual captured snapshot and pointer-only collector, withholding the
	// exact set entry only in its private copied view to exercise dangling refs.
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("snapshot")
	}
	defer snap.Close()
	copiedState := *snap.state
	copiedState.ValueLogSet = &valuelog.Set{}
	originalState := snap.state
	snap.state = &copiedState
	defer func() { snap.state = originalState }()
	diag, err := d.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{Collectors: maintenanceReachabilityValueLogRefCounts})
	if err != nil || diag.valueLogRefCounts[ptrs[0].FileID] != 1 {
		t.Fatalf("legacy diagnostic projection changed: counts=%v err=%v", diag.valueLogRefCounts, err)
	}
	before := d.valueLogRefTracker.metadata.Bytes()
	retained, err := d.maintenanceReachabilityScan(context.Background(), snap, maintenanceReachabilityScanOptions{Collectors: maintenanceReachabilityValueLogRefCounts, CountMetadata: d.valueLogRefTracker.metadata})
	if !errors.Is(err, valuelog.ErrFileNotFound) || retained.countRecords.slots != nil || d.valueLogRefTracker.metadata.Bytes() != before {
		t.Fatalf("dangling retained evidence not refused before ownership: err=%v", err)
	}
}

// Serialization owns an exact clone and exact canonical buffer capacity;
// malformed decoded records must release the original admitted backing.
func TestValueLogRefCountOwnedCanonicalSerializationAndRefusal(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	pair, err := captureValueLogRefIncarnation(d.idx.Load())
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	records := valueLogRefCounts{metadata: &owner}
	defer records.close()
	for _, id := range []uint32{page.ValueLogFileID(8), page.ValueLogFileID(2), page.ValueLogFileID(5)} {
		if err := records.set(id, uint64(id)); err != nil {
			t.Fatal(err)
		}
	}
	originalBytes := owner.Bytes()
	clone, err := records.clone()
	if err != nil {
		t.Fatal(err)
	}
	if &clone.slots[0] == &records.slots[0] || owner.Bytes() != originalBytes+clone.charge {
		t.Fatal("persistence clone not independently admitted")
	}
	blob, charge, err := encodeValueLogRefCountsOwned(17, pair, &clone)
	if err != nil {
		t.Fatal(err)
	}
	if owner.Bytes() != originalBytes+clone.charge+charge {
		t.Fatal("canonical buffer charge lost or sort scratch retained")
	}
	clone.close()
	canonical, err := encodeValueLogRefCounts(17, pair, map[uint32]uint64{page.ValueLogFileID(5): uint64(page.ValueLogFileID(5)), page.ValueLogFileID(8): uint64(page.ValueLogFileID(8)), page.ValueLogFileID(2): uint64(page.ValueLogFileID(2))})
	if err != nil || !bytes.Equal(blob, canonical) {
		t.Fatal("owned count order changed canonical format", err)
	}
	decoded, err := decodeValueLogRefCountsOwned(blob, &owner)
	if err != nil || decoded.records.used != 3 {
		t.Fatal("owned decode", err)
	}
	decoded.records.close()
	if owner.Bytes() != originalBytes+charge {
		t.Fatal("decoded records retained after disposal")
	}
	// CRC-valid zero ID exercises a post-allocation decode failure.
	bad := bytes.Clone(blob)
	offset := len(valueLogRefCountsMagic) + 4 + 8 + valueLogRefIncarnationDiskSize + 4
	binary.LittleEndian.PutUint32(bad[offset:offset+4], 0)
	binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc.Checksum(bad[:len(bad)-4]))
	if _, err := decodeValueLogRefCountsOwned(bad, &owner); !errors.Is(err, errValueLogRefCorrupt) {
		t.Fatal("malformed ID accepted", err)
	}
	if owner.Bytes() != originalBytes+charge {
		t.Fatal("failed decode refunded unrelated backing or retained scratch")
	}
	clear(blob)
	blob = nil
	owner.RemovePending(charge)
	budget := &refCountsBudget{cap: 0}
	var denied retainedalloc.Owner
	denied.Initialize(0)
	e, err := denied.Enroll(budget)
	// Enrollment itself has real fixed control capacity.
	if err == nil {
		e.Close()
		t.Fatal("zero budget admitted controls")
	}
	budget.cap = 1 << 20
	e, err = denied.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	before := denied.Bytes()
	budget.cap = before
	if _, err := decodeValueLogRefCountsOwned(canonical, &denied); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatal("decoder allocated before refusal", err)
	}
	if denied.Bytes() != before || budget.bytes != before {
		t.Fatal("decoder refusal changed original charge")
	}
}
