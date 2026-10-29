package collections

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type nativeTextCountedSnapshot struct {
	backenddb.NativeSemanticCapture
	calls int
}

func (c *nativeTextCountedSnapshot) Snapshot() *backenddb.Snapshot {
	c.calls++
	return c.NativeSemanticCapture.Snapshot()
}

// The iterator already owns one immutable collection cut. Walking its logical
// sources must not repeatedly bind physical root readers or recapture that cut.
func TestNativeTextIteratorBindsCapturedDomainOnce(t *testing.T) {
	_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
	want := []string{"a", "bb", "ccc", "dddd"}
	for _, id := range want {
		if _, err := c.Insert([]byte(id), []byte(fmt.Sprintf(`{"content":"policy","tenant":%q}`, id))); err != nil {
			t.Fatal(err)
		}
	}
	read, err := c.nativeJSON.Capture(c.name)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	counted := &nativeTextCountedSnapshot{NativeSemanticCapture: read.capture}
	read.capture = counted
	lease, err := counted.NativeAllocator().AdmitNativeBytes(0)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var charged uint64
	scope := nativeTextCatalog{read: read, admit: func(n uint64) error {
		if n > ^uint64(0)-charged {
			return memtable.ErrCOWCapacity
		}
		if err := lease.Resize(charged + n); err != nil {
			return err
		}
		charged += n
		return nil
	}}
	it, err := scope.iterator(c.name+"/primary", nil, nil, false, 0, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var got []string
	for it.Valid() {
		got = append(got, string(it.UnsafeKey()))
		it.Next()
	}
	if err := it.Error(); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("iteration got=%q want=%q err=%v", got, want, err)
	}
	// Backward and aliased seeks retain the same root and immutable rows.
	it.Seek([]byte(want[1]))
	if !it.Valid() || string(it.UnsafeKey()) != want[1] {
		t.Fatalf("backward seek key=%q err=%v", it.UnsafeKey(), it.Error())
	}
	it.Seek(it.UnsafeKey())
	if !it.Valid() || string(it.UnsafeKey()) != want[1] {
		t.Fatalf("aliased seek key=%q err=%v", it.UnsafeKey(), it.Error())
	}
	if counted.calls != 1 {
		t.Errorf("iterator rebound captured domain %d times; want one", counted.calls)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	before := counted.calls
	it.Next()
	if it.Valid() || !errors.Is(it.Error(), backenddb.ErrClosed) || counted.calls != before {
		t.Fatalf("movement after close valid=%v err=%v snapshot calls=%d/%d", it.Valid(), it.Error(), before, counted.calls)
	}
}
