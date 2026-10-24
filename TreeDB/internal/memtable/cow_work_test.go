package memtable

import (
	"fmt"
	"testing"
)

func TestCOWLogicalWorkDominatesCanonicalQualifiedCases(t *testing.T) {
	limits := DefaultCOWLimits()
	for _, oldCount := range []int{0, 1, 31, 64, 2080} {
		for _, keys := range []int{1, 16, 192} {
			t.Run(fmt.Sprintf("history%d/keys%d", oldCount, keys), func(t *testing.T) {
				budget, err := NewCOWBudget(limits)
				if err != nil {
					t.Fatal(err)
				}
				writer, err := NewCOWOrderedWriter(budget)
				if err != nil {
					t.Fatal(err)
				}
				var old *COWRoot
				if oldCount > 0 {
					var seed []COWMutation
					for i := range keys {
						for version := 1; version <= oldCount; version++ {
							seed = append(seed, cowOrderedStorageMutation(t, fmt.Sprintf("k%04d", i), uint64(version)))
						}
					}
					// The largest Cartesian case needlessly fills the generation. A smaller
					// basis still checks the deep numeric history without weakening limits.
					if len(seed) > 10000 {
						seed = seed[:oldCount]
					}
					old = cowTestPublish(t, writer, seed)
				}
				defer func() {
					if old != nil {
						cowTestRelease(old)
					}
					cowTestClose(writer)
					budget.Close()
				}()
				mutations := make([]COWMutation, keys)
				work := COWOrderedCommandWork()
				for i := range mutations {
					mutations[i] = cowOrderedStorageMutation(t, fmt.Sprintf("k%04d", i), uint64(oldCount+1))
					work += COWOrderedMutationWork(uint64(len(mutations[i].Key)), uint64(len(mutations[i].Value)), limits.MaxGenerationBytes)
				}
				charge, err := writer.Estimate(mutations, COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				if charge.Total() > work {
					t.Fatalf("canonical charge %+v exceeds composable work%d", charge, work)
				}
				prepared, err := writer.Prepare(mutations, COWPrepareOptions{})
				if err != nil {
					t.Fatal(err)
				}
				next := prepared.Publish()
				cowTestRelease(next)
			})
		}
	}
}

func TestCOWOrderedImmutableWorkMatchesCanonicalModel(t *testing.T) {
	limits := DefaultCOWLimits()
	estimator := NewCOWOrderedWorkEstimator(limits.MaxGenerationBytes)
	for _, sizes := range [][2]uint64{{0, 0}, {1, 1}, {19, 25}, {128, 4096}, {65535, 65536}} {
		got := estimator.Estimate(sizes[0], sizes[1])
		want := COWOrderedMutationWork(sizes[0], sizes[1], limits.MaxGenerationBytes)
		if got != want {
			t.Fatalf("sizes%v work%d want%d", sizes, got, want)
		}
	}
}
