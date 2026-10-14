package valuelog

import (
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestTerminalSetRetirementV5PinnedZombiesAndWorkerJoin(t *testing.T) {
	dir := t.TempDir()
	id1, path1 := writeIdentityPinTestSegment(t, dir)
	id2, e := EncodeFileID(1, 2)
	if e != nil {
		t.Fatal(e)
	}
	path2 := SegmentPath(dir, id2)
	writer, e := NewWriter(path2, id2)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = writer.Append(0, nil, 2, []byte("second persistent value")); e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	manager, e := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if e != nil {
		t.Fatal(e)
	}
	defer manager.Close()
	token1, token2 := stableManagerToken(t, manager, id1), stableManagerToken(t, manager, id2)
	defer token1.Release()
	defer token2.Release()
	set1 := manager.CurrentSubsetNoRefresh(map[uint32]struct{}{id1: {}})
	set2 := manager.CurrentSubsetNoRefresh(map[uint32]struct{}{id2: {}})
	file1, file2 := set1.Files[id1], set2.Files[id2]
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	manager.retryWaitHook = func() { once.Do(func() { close(entered); <-resume }) }
	if e = manager.MarkZombie(id1); e != nil {
		t.Fatal(e)
	}
	if e = manager.Release(set1); e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual pin-conflicted retry did not enter")
	}
	if e = manager.MarkZombie(id2); e != nil {
		t.Fatal(e)
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	joined := make(chan error, 1)
	go func() {
		ready, e := manager.BeginTerminalRetirementV5(work)
		if !ready && e == nil {
			e = ErrTerminalRetirementV5
		}
		joined <- e
	}()
	select {
	case e := <-joined:
		close(resume)
		t.Fatalf("worker not joined %v", e)
	case <-time.After(50 * time.Millisecond):
	}
	manager.mu.RLock()
	closing := manager.closing
	manager.mu.RUnlock()
	if !closing {
		t.Fatal("terminal admission did not close deletion gate")
	}
	close(resume)
	select {
	case e = <-joined:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal worker join deadlocked")
	}
	if file1.retryDeletePending.Load() {
		t.Fatal("worker survived terminal admission")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	cursor, ready, e := manager.PrepareTerminalSetRetirementV5(set2, work)
	if e != nil || !ready {
		t.Fatalf("prepare %v %v", ready, e)
	}
	// Exactly two actual operands reach Set refs zero while File remains retained.
	work = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	done, progress, e := cursor.Step(work)
	if e != nil || done || !progress || set2.RefCount.Load() != 0 || file2.RefCount.Load() != 1 {
		t.Fatalf("decisive Set/File cut %v %v %v", done, progress, e)
	}
	if file2.retryDeletePending.Load() {
		t.Fatal("typed release started hidden retry")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	done, progress, e = cursor.Step(work)
	if e != nil || !done || !progress || file2.RefCount.Load() != 0 || file2.retryDeletePending.Load() {
		t.Fatalf("last File ref %v %v %v", done, progress, e)
	}
	for _, path := range []string{path1, path2} {
		if _, e = os.Stat(path); e != nil {
			t.Fatalf("pinned persistent path removed %v", e)
		}
	}
	if file2.closed.Load() {
		t.Fatal("manager physical owner lost before shutdown")
	}
	if e = manager.Close(); e != nil {
		t.Fatal(e)
	}
	// Stable tokens own duplicated handles independently; ordinary Close must not
	// unlink either pinned path. Physical handle/callback assertions live in the
	// common publication test rather than accessing token internals here.
	for _, path := range []string{path1, path2} {
		if _, e = os.Stat(path); e != nil {
			t.Fatal(e)
		}
	}
}

// This is the HEAD192e Set layout/constructor preimage. It is used only for the
// affected allocation guardrail, not as another production capture path.
type terminalSetPreimageV5 struct {
	retentionShape      SetRetentionSizes
	retentionKnown      bool
	Files               map[uint32]*File
	RefCount            atomic.Int64
	disableReadChecksum bool
}

func terminalSetPreimageCaptureV5(m *Manager, ids map[uint32]struct{}, subset bool) *terminalSetPreimageV5 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	hint := len(m.files)
	if subset {
		hint = len(ids)
	}
	files := make(map[uint32]*File, hint)
	shape := SetRetentionSizes{MapHint: hint, Wrapper: uint64(unsafe.Sizeof(terminalSetPreimageV5{})), FileWrapper: uint64(unsafe.Sizeof(File{})), MapEnvelope: 512 * (uint64(hint) + 1)}
	add := func(id uint32, f *File) {
		if f == nil || f.IsZombie.Load() {
			return
		}
		files[id] = f
		shape.addFile(f)
		f.RefCount.Add(1)
	}
	if subset {
		for id := range ids {
			add(id, m.files[id])
		}
	} else {
		for id, f := range m.files {
			add(id, f)
		}
	}
	set := &terminalSetPreimageV5{Files: files, disableReadChecksum: m.disableReadChecksum, retentionKnown: true, retentionShape: shape}
	set.RefCount.Store(1)
	return set
}

var terminalSetAllocationSinkV5 any

func TestTerminalSetOrdinaryAllocationGuardrailV5(t *testing.T) {
	for _, n := range []int{0, 1, 2, 8, 17, 18, 64} {
		m := &Manager{files: make(map[uint32]*File, n)}
		ids := make(map[uint32]struct{}, n)
		for i := 0; i < n; i++ {
			id := uint32(i + 1)
			m.files[id] = &File{ID: id}
			ids[id] = struct{}{}
		}
		for _, subset := range []bool{false, true} {
			name := fmt.Sprintf("files=%d/subset=%v", n, subset)
			t.Run(name, func(t *testing.T) {
				oldCapture := func() {
					set := terminalSetPreimageCaptureV5(m, ids, subset)
					terminalSetAllocationSinkV5 = set
					for _, f := range set.Files {
						f.RefCount.Add(-1)
					}
				}
				newCapture := func() {
					var set *Set
					if subset {
						set = m.CurrentSubsetNoRefresh(ids)
					} else {
						set = m.CurrentSetNoRefresh()
					}
					terminalSetAllocationSinkV5 = set
					if cap(set.retirementFiles) != retirementSetCapacityV5(n) || len(set.retirementFiles) != n {
						t.Fatal("tail capacity/provenance")
					}
					shape, ok := set.RetentionSizes()
					if !ok || shape.Wrapper != uint64(unsafe.Sizeof(Set{})) || shape.MapEnvelope != 512*(uint64(n)+1)+uint64(cap(set.retirementFiles))*uint64(unsafe.Sizeof((*File)(nil))) {
						t.Fatal("retained tail not accounted")
					}
					for _, f := range set.Files {
						f.RefCount.Add(-1)
					}
				}
				oldAlloc := testing.AllocsPerRun(100, oldCapture)
				newAlloc := testing.AllocsPerRun(100, newCapture)
				allowance := float64(0)
				if n > 17 {
					allowance = 1
				}
				if newAlloc > oldAlloc+allowance {
					t.Fatalf("extra tail allocation old=%g new=%g", oldAlloc, newAlloc)
				}
				allocated := func(f func()) uint64 {
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					for i := 0; i < 200; i++ {
						f()
					}
					runtime.ReadMemStats(&after)
					return (after.TotalAlloc - before.TotalAlloc) / 200
				}
				oldBytes, newBytes := allocated(oldCapture), allocated(newCapture)
				maxAdded := uint64(unsafe.Sizeof(Set{})) - uint64(unsafe.Sizeof(terminalSetPreimageV5{})) + uint64(retirementSetCapacityV5(n))*uint64(unsafe.Sizeof((*File)(nil))) + 64
				if newBytes > oldBytes+maxAdded {
					t.Fatalf("retained metadata allocation exceeded envelope old=%d new=%d added limit%d", oldBytes, newBytes, maxAdded)
				}
				t.Logf("ordinary Set preimage %.0f allocs/%d B; current %.0f allocs/%d B; owned tail cap%d/header%d", oldAlloc, oldBytes, newAlloc, newBytes, retirementSetCapacityV5(n), unsafe.Sizeof(Set{}))
			})
		}
	}
	terminalSetAllocationSinkV5 = nil
}
