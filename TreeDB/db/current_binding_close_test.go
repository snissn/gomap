package db

import (
	"errors"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCurrentBindingRawBridgeConcurrentClose(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	const readers = 8
	ready := make(chan struct{}, readers)
	errorsFound := make(chan error, readers)
	var joined sync.WaitGroup
	joined.Add(readers)
	for n := 0; n < readers; n++ {
		go func() {
			defer joined.Done()
			// Invalid identity exercises refusal without namespace I/O. The
			// backend must still capture its installed Manager safely while
			// Close revokes it; Manager's own Close admission cannot protect
			// the backend pointer load.
			err := d.ValidateCurrentWritableValueLogBinding("", 0, rootpublication.StableIdentity{}, nil)
			ready <- struct{}{}
			for errors.Is(err, rootpublication.ErrResourceConflict) {
				err = d.ValidateCurrentWritableValueLogBinding("", 0, rootpublication.StableIdentity{}, nil)
			}
			if !errors.Is(err, ErrClosed) {
				errorsFound <- err
			}
		}()
	}
	for n := 0; n < readers; n++ {
		<-ready
	}
	if err := d.Close(); err != nil {
		t.Error(err)
	}
	joined.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("raw bridge during Close: %v", err)
	}
	if err := d.ValidateCurrentWritableValueLogBinding("", 0, rootpublication.StableIdentity{}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("raw bridge after Close: %v", err)
	}
}
