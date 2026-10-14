package valuelog

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"os"
	"runtime"
	"testing"
	"unsafe"
)

func TestOwnedMappingV6LargeTailHeldReaderFailureAndTerminal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	withMappedSealedBudget(t, 8)
	withMappedSealedBytesBudget(t, 1<<30)
	dir := t.TempDir()
	id, _ := EncodeFileID(1, 1)
	path := SegmentPath(dir, id)
	writer, e := NewWriter(path, id)
	if e != nil {
		t.Fatal(e)
	}
	value := bytes.Repeat([]byte("real owned mapping"), 180000)
	ptr, e := writer.Append(0, nil, 1, value)
	if e != nil {
		t.Fatal(e)
	}
	if e = writer.Close(); e != nil {
		t.Fatal(e)
	}
	m, e := NewManagerWithStableResourcePinRegistry(dir, rootpublication.NewIdentityPinRegistry())
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if e = m.Close(); e != nil {
			t.Error(e)
		}
	}()
	set := m.CurrentSubsetNoRefresh(map[uint32]struct{}{id: {}})
	set.RefCount.Add(1)
	f := set.Files[id]
	if !f.remapToFileSize() || !f.currentMappingV6.constructor {
		t.Fatal("actual producer did not map")
	}
	view, e := f.ReadUnsafe(ptr, true)
	if e != nil || !bytes.Equal(view, value) {
		t.Fatal("held reader", e)
	}
	if e = m.MarkZombie(id); e != nil {
		t.Fatal(e)
	}
	first := liveCursorV6(t, m, set)
	finishLiveV6(t, first)
	if f.currentMappingV6.offset != 0 || !bytes.Equal(view, value) || set.RefCount.Load() != 1 {
		t.Fatal("nonlast reader mapping changed")
	}
	c := liveCursorV6(t, m, set)
	var records, charged uint64
	for turn := 0; turn < 128; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 16, ByteLimit: 300 << 10}
		done, _, e := c.Step(w)
		records += w.Records
		charged += w.Bytes
		if e != nil || done {
			t.Fatalf("premature physical finish %v %v", done, e)
		}
		if c.filePhase == 6 && len(c.mappings) > 0 && c.mappings[0].offset > 0 {
			break
		}
	}
	if c.filePhase != 6 || len(c.mappings) != 1 || c.mappings[0].offset == 0 || c.mappings[0].remainingV6() <= 1<<20 {
		t.Fatal("actual large tail not retained")
	}
	before := c.mappings[0].offset
	fault := errors.New("physical munmap failure")
	original := unmapOwnedPhysicalV6
	unmapOwnedPhysicalV6 = func(_ unsafe.Pointer, _ uintptr) error { return fault }
	w := &iterator.OrdinalScanWork{RecordLimit: 16, ByteLimit: 300 << 10}
	_, _, e = c.Step(w)
	records += w.Records
	charged += w.Bytes
	unmapOwnedPhysicalV6 = original
	if !errors.Is(e, fault) || c.mappings[0].offset != before || f.ownedRetirementV6.Load() != c {
		t.Fatalf("failed physical ownership %v", e)
	}
	if ready, e := m.BeginTerminalRetirementV5(liveWorkV6(32)); !ready || e != nil {
		t.Fatal(e)
	}
	for turn := 0; turn < 128; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 16, ByteLimit: 300 << 10}
		done, p, e := c.Step(w)
		records += w.Records
		charged += w.Bytes
		if e != nil {
			t.Fatal(e)
		}
		if done {
			break
		}
		if !p {
			t.Fatalf("stalled terminal exact tail %d", c.filePhase)
		}
	}
	if !c.done || set.RefCount.Load() != 0 || f.RefCount.Load() != 0 || m.ownedSetRetirementsV6 != 0 {
		t.Fatal("terminal did not finish same mapping owner")
	}
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("physical zombie not removed", e)
	}
	t.Logf("actual >1MiB mapping retained through failure and terminal continuation %dR/%dB", records, charged)
}
func TestOwnedMappingV6LegacyAndFakeRefusedBeforeEdge(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap unsupported")
	}
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "heap", true: "legacy-constructor"}[legacy], func(t *testing.T) {
			m, set, f, _ := liveRetirementFixtureV6(t)
			var data []byte
			var e error
			if legacy {
				data, e = mmapReadOnly(f.File, 2<<20)
				if e != nil {
					t.Fatal(e)
				}
			} else {
				data = make([]byte, 2<<20)
				data[0] = 73
			}
			f.mmapData.Store(data)
			if _, ready, e := m.PrepareLiveSetRetirementV6(set, liveWorkV6(32)); ready || !errors.Is(e, errOwnedMappingV6) || set.RefCount.Load() != 1 || f.RefCount.Load() != 1 || m.ownedSetRetirementsV6 != 0 {
				t.Fatalf("ad-hoc mapping consumed %v %v", ready, e)
			}
			fake := ownedMmapV6{data: data, extent: uintptr(len(data))}
			if _, e = unmapOwnedRangeV6(&fake, uintptr(os.Getpagesize())); !errors.Is(e, errOwnedMappingV6) {
				t.Fatal("bare pointer gained authority", e)
			}
			if ready, e := m.BeginTerminalRetirementV5(liveWorkV6(32)); !ready || e != nil {
				t.Fatal(e)
			}
			c, ready, e := m.PrepareTerminalSetRetirementV5(set, liveWorkV6(32))
			if !ready || e != nil {
				t.Fatal(e)
			}
			finishLiveV6(t, c)
			if e = m.Close(); e != nil {
				t.Fatal(e)
			}
			if !legacy && data[0] != 73 {
				t.Fatal("heap mapping was unmapped")
			}
		})
	}
}
