package caching

import (
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestVlogCompressionSelector_EntersHoldAndProbes(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)

	mode, _, probe := s.choose(false, 128)
	if mode != vlogWriteBlock || probe {
		t.Fatalf("initial choose: mode=%v probe=%t", mode, probe)
	}
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 128, 128, 200, true)
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 128, 128, 200, true)

	mode, _, probe = s.choose(false, 64)
	if mode != vlogWriteOff || probe {
		t.Fatalf("expected hold bypass (off, no-probe), got mode=%v probe=%t", mode, probe)
	}

	// Consume hold bytes until probe boundary is reached.
	for i := 0; i < 8; i++ {
		mode, _, probe = s.choose(false, 64)
		if probe {
			break
		}
	}
	if !probe {
		t.Fatalf("expected periodic probe during hold")
	}
	if mode != vlogWriteBlock {
		t.Fatalf("expected block probe mode, got %v", mode)
	}
}

func TestVlogCompressionSelector_DictSelectionByPolicy(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 0

	// Establish an off baseline first.
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Dict clearly beats block on ratio while keeping throughput.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 650, 1024, false)

	mode, _, _ := s.choose(true, 1024)
	if mode != vlogWriteDict {
		t.Fatalf("expected dict mode when dict beats block materially, got %v", mode)
	}
}

func TestVlogCompressionSelector_DwellPreventsFlap(t *testing.T) {
	s := newVlogCompressionSelectorWithSeed(vlogAutoBalanced, 0, 0, valuelog.BlockCodecSnappy)
	s.dwellBytes = 4096

	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Dict looks better, but current mode should hold until dwell budget is spent.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 900, 1400, false)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 1024, 600, 1024, false)

	mode, _, _ := s.choose(true, 512)
	if mode != vlogWriteBlock {
		t.Fatalf("expected dwell to keep block mode, got %v", mode)
	}
	mode, _, _ = s.choose(true, 4096)
	if mode != vlogWriteDict {
		t.Fatalf("expected mode switch after dwell, got %v", mode)
	}
}

func TestVlogCompressionSelector_BlockCodecSelection(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoThroughput, 0, 0)
	s.dwellBytes = 0
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 1024, 1024, 1024, false)
	// Snappy compresses slightly better but is slower.
	s.observe(vlogWriteBlock, valuelog.BlockCodecSnappy, 1024, 700, 1700, false)
	// LZ4 is much faster with close-enough ratio.
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 1024, 740, 900, false)

	mode, codec, _ := s.choose(false, 1024)
	if mode != vlogWriteBlock {
		t.Fatalf("expected block mode, got %v", mode)
	}
	if codec != valuelog.BlockCodecLZ4 {
		t.Fatalf("expected lz4 selection, got %v", codec)
	}
}

func TestVlogCompressionSelector_SnapshotCounters(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)
	s.observe(vlogWriteOff, valuelog.BlockCodecSnappy, 256, 256, 256, false)
	s.observe(vlogWriteBlock, valuelog.BlockCodecLZ4, 512, 400, 600, true)
	s.observe(vlogWriteDict, valuelog.BlockCodecSnappy, 512, 280, 512, false)
	snap := s.snapshot()
	if snap.bytesByCandidate[vlogAutoCandidateOff] == 0 {
		t.Fatalf("expected off bytes > 0")
	}
	if snap.framesByCandidate[vlogAutoCandidateBlockLZ4] == 0 {
		t.Fatalf("expected lz4 frame count > 0")
	}
}

func TestVlogCompressionSelector_ConcurrentSmoke(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 4<<20, 512<<10)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				mode, codec, probe := s.choose(i%2 == 0, 512+(i%256))
				raw := 2048
				stored := 2048
				switch mode {
				case vlogWriteBlock:
					stored = 1500
				case vlogWriteDict:
					stored = 1200
				}
				s.observe(mode, codec, raw, stored, 1800, probe)
			}
		}(g)
	}
	wg.Wait()
}
