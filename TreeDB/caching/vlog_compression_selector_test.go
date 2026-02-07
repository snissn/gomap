package caching

import (
	"sync"
	"testing"
)

func TestVlogCompressionSelector_EntersHoldAndProbes(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 1024, 256)

	mode, probe := s.choose(false, 128)
	if mode != vlogWriteBlock || probe {
		t.Fatalf("initial choose: mode=%v probe=%t", mode, probe)
	}
	s.observe(vlogWriteBlock, 128, 128, true)
	s.observe(vlogWriteBlock, 128, 128, true)

	mode, probe = s.choose(false, 64)
	if mode != vlogWriteOff || probe {
		t.Fatalf("expected hold bypass (off, no-probe), got mode=%v probe=%t", mode, probe)
	}

	// Consume hold bytes until probe boundary is reached.
	for i := 0; i < 8; i++ {
		mode, probe = s.choose(false, 64)
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

	s.observe(vlogWriteBlock, 1024, 900, false)
	s.observe(vlogWriteDict, 1024, 650, false)

	mode, _ := s.choose(true, 1024)
	if mode != vlogWriteDict {
		t.Fatalf("expected dict mode when dict beats block materially, got %v", mode)
	}
}

func TestVlogCompressionSelector_DwellPreventsFlap(t *testing.T) {
	s := newVlogCompressionSelector(vlogAutoBalanced, 0, 0)
	s.dwellBytes = 4096

	// Dict looks better, but current mode should hold until dwell budget is spent.
	s.observe(vlogWriteBlock, 1024, 900, false)
	s.observe(vlogWriteDict, 1024, 600, false)

	mode, _ := s.choose(true, 512)
	if mode != vlogWriteBlock {
		t.Fatalf("expected dwell to keep block mode, got %v", mode)
	}
	mode, _ = s.choose(true, 4096)
	if mode != vlogWriteDict {
		t.Fatalf("expected mode switch after dwell, got %v", mode)
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
				mode, probe := s.choose(i%2 == 0, 512+(i%256))
				raw := 2048
				stored := 2048
				switch mode {
				case vlogWriteBlock:
					stored = 1500
				case vlogWriteDict:
					stored = 1200
				}
				s.observe(mode, raw, stored, probe)
			}
		}(g)
	}
	wg.Wait()
}
