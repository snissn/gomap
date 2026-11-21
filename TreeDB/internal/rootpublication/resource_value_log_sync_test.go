package rootpublication

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestStableResourceParallelValueLogSync(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("parallel publication sync is enabled on Linux")
	}

	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail=%t", fail), func(t *testing.T) {
			dir := t.TempDir()
			var owner retainedalloc.Owner
			owner.Initialize(0)
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			builder, err := NewStableResourceSetBuilderWithMetadata(&owner, ReachabilityValueLogPointer)
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Abandon()

			var tokens [maxParallelValueLogs]*StableResourceToken
			for i := range tokens {
				id := fmt.Sprint(i + 1)
				name := "values-" + id
				file := writeStableResourceFixture(t, dir, name, "data")
				identity, err := StableIdentityFromFile(file)
				if err != nil {
					t.Fatal(err)
				}
				if err := registry.Observe(identity); err != nil {
					t.Fatal(err)
				}
				defer registry.Unobserve(identity)
				token, err := NewStableProducerResourceTokenForDomain(StableProducerValueLog, StableResourceSpec{
					MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceValueLog, LogicalLane: name,
					ResourceID: id, Generation: uint64(i + 1), DiagnosticPath: name, File: file,
					Reachability: ReachabilityValueLogPointer, Frontier: DurableFrontier{Bytes: 4},
				}, "authoritative")
				if err != nil {
					t.Fatal(err)
				}
				if err := builder.Add(token); err != nil {
					token.Release()
					t.Fatal(err)
				}
				tokens[i] = token
			}
			set, err := builder.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			defer set.Release()

			if fail {
				// Invalidate a test-owned descriptor to exercise a real fsync
				// failure while the other lanes still have work.
				original := tokens[0].pinned
				closed, err := duplicateStableFile(original)
				if err != nil {
					t.Fatal(err)
				}
				if err := closed.Close(); err != nil {
					t.Fatal(err)
				}
				tokens[0].pinned = closed
				defer func() { tokens[0].pinned = original }()
			}
			set.mu.Lock()
			used, err := set.syncIndependentValueLogsLocked()
			set.mu.Unlock()
			if !used || (fail && !errors.Is(err, os.ErrClosed)) || (!fail && err != nil) {
				t.Fatalf("parallel used=%v error=%v", used, err)
			}
			for i, token := range tokens {
				var want uint64 = 1
				if i == 0 && fail {
					want = 0
				}
				if got := token.metrics.physicalFileSyncs.Load(); got != want {
					t.Fatalf("log %d completed %d file syncs before return, want %d", i, got, want)
				}
			}
		})
	}
}
