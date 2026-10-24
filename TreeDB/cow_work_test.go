package treedb

import (
	"math"
	"sync"
	"testing"
)

func TestOrderedWorkEstimateConcurrentClose(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	started := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				default:
					_ = db.EstimateMVCCWriteWork(16, 64)
				}
			}
		})
	}
	for range 4 {
		<-started
	}
	err = db.Close()
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := db.EstimateMVCCWriteWork(16, 64); got != math.MaxUint64 {
		t.Fatalf("closed estimate=%d", got)
	}
}

func TestOrderedImmutableWorkEstimatorOutlivesClose(t *testing.T) {
	opts := OptionsFor(ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_ordered"
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	estimator := db.NewMVCCWriteWorkEstimator()
	sizes := [][2]int{{0, 0}, {1, 0}, {16, 64}, {128, 4096}, {-1, 0}, {math.MaxInt, math.MaxInt}}
	expected := make([]uint64, len(sizes))
	for i, size := range sizes {
		expected[i] = db.EstimateMVCCWriteWork(size[0], size[1])
		if got := estimator.Estimate(size[0], size[1]); got != expected[i] {
			t.Fatalf("before close sizes%v=%d want%d", size, got, expected[i])
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 1000 {
				for i, size := range sizes {
					if got := estimator.Estimate(size[0], size[1]); got != expected[i] {
						t.Errorf("after close sizes%v=%d want%d", size, got, expected[i])
						return
					}
				}
			}
		})
	}
	wg.Wait()
	if got := (MVCCWriteWorkEstimator{}).Estimate(16, 64); got != math.MaxUint64 {
		t.Fatalf("zero estimator=%d", got)
	}
	if got := db.MVCCWriteWorkLimit(); got != 0 {
		t.Fatalf("closed work limit=%d", got)
	}
}
