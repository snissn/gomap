package rootpublication

import (
	"errors"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// A public release callback can inspect its originating Set. It must see an
// expired view rather than waiting for the callback itself to finish.
func TestSelectedResourceReleaseCallbackCanInspectOriginalSetV6(t *testing.T) {
	for _, imported := range []bool{false, true} {
		name := "selected_import"
		if imported {
			name = "same_owner_clone"
		}
		t.Run(name, func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			var selected *StableResourceSet
			var callbackErr error
			timedOut := false
			calls := 0
			inspected := make(chan error, 1)
			callback := func() {
				calls++
				go func() {
					if selected.Len() != 0 {
						inspected <- errors.New("released Set still exposes entries")
						return
					}
					if _, err := selected.Descriptors(); !errors.Is(err, ErrResourceOwnership) {
						inspected <- errors.New("released Set still exposes descriptors")
						return
					}
					if err := selected.ReleaseWithError(); !errors.Is(err, ErrResourceOwnership) {
						inspected <- errors.New("concurrent duplicate release lost its in-progress result")
						return
					}

					inspected <- nil
				}()
				select {
				case callbackErr = <-inspected:
				case <-time.After(time.Second):
					timedOut = true
					callbackErr = errors.New("public Set inspection blocked until its release callback returned")
				}
			}
			token := stableTokenFixture(t, t.TempDir(), "callback", 1, 4, ReachabilityIndexFile, "index.db", func(spec *StableResourceSpec) {
				spec.OnRelease = callback
			})
			source := freezeAppendMutationResources(t, token)
			var err error
			selected, err = ImportStableResourceSetMetadata(&owner, source)
			if err != nil {
				t.Fatal(err)
			}
			if err = source.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if imported {
				original := selected
				selected, err = ImportStableResourceSetMetadata(&owner, original)
				if err != nil {
					t.Fatal(err)
				}
				if err = original.ReleaseWithError(); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 0 {
				t.Fatal("import lost original callback custody")
			}

			if !selected.selected || selected.metadata == nil {
				t.Fatal("fixture missed selected release graph")
			}
			if err = selected.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if callbackErr != nil {
				// Let the blocked public inspection finish after the old implementation
				// releases its mutex; RED remains an ordinary assertion, without a leak.
				if timedOut {
					<-inspected
				}
				t.Fatal(callbackErr)
			}
			if err = selected.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("callback calls=%d, want1", calls)
			}
			if owner.Bytes() != 0 {
				t.Fatalf("retained metadata=%d", owner.Bytes())
			}
		})
	}
}
