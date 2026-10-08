package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/residentcredit"
	"testing"
)

func TestNativePublicationResidentConstructorFailureKeepsActualCut(t *testing.T) {
	previous := testDBOpenHook
	injected := errors.New("injected after installed DB constructors")
	var held *residentcredit.Scope
	testDBOpenHook = func(d *DB) error {
		if previous != nil {
			if err := previous(d); err != nil {
				return err
			}
		}
		if d.nativePublicationResident == nil {
			t.Fatal("constructor did not install resident owner")
		}
		scope, err := d.NewNativePublicationResidentScopeV1()
		if err != nil {
			return err
		}
		held = scope.(*residentcredit.Scope)
		if err = held.ReserveStableMetadata(4096); err != nil {
			return err
		}
		return injected
	}
	defer func() { testDBOpenHook = previous }()
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, DisableSideStores: true})
	if !errors.Is(err, injected) || d != nil || held == nil {
		t.Fatalf("failed constructor d=%v err=%v", d, err)
	}
	got := held.OwnerStats()
	if !got.Closed || got.Refs != 1 || got.Control == 0 || got.Live != got.Control+held.Stats().Bytes {
		t.Fatalf("failure retired actual cut: %+v", got)
	}
	births := got.Births
	held.ReleaseStableMetadata()
	got = held.OwnerStats()
	if got.Live != 0 || got.Refs != 0 || got.Control != 0 || got.Births != births {
		t.Fatalf("failed constructor last edge: %+v", got)
	}
}

func TestNativePublicationResidentAllInstalledConstructorPaths(t *testing.T) {
	dir := t.TempDir()
	writable, err := Open(Options{Dir: dir, DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	if !writable.HasNativePublicationResidentOwnerV1() {
		t.Fatal("writable constructor has no owner")
	}
	if err = writable.SetSync([]byte("key"), []byte("persistent")); err != nil {
		t.Fatal(err)
	}
	if err = writable.Close(); err != nil {
		t.Fatal(err)
	}
	for _, noLock := range []bool{false, true} {
		opts := Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true, DisableSideStores: true}
		var d *DB
		if noLock {
			d, err = openReadOnlyNoLock(opts)
		} else {
			d, err = Open(opts)
		}
		if err != nil {
			t.Fatal(err)
		}
		if !d.HasNativePublicationResidentOwnerV1() {
			t.Fatal("read-only constructor has no owner")
		}
		scope, err := d.NewNativePublicationResidentScopeV1()
		if err != nil {
			t.Fatal(err)
		}
		held := scope.(*residentcredit.Scope)
		if err = d.Close(); err != nil {
			t.Fatal(err)
		}
		if got := held.OwnerStats(); !got.Closed || got.Refs != 1 || got.Control == 0 {
			t.Fatal(got)
		}
		held.ReleaseStableMetadata()
		if got := held.OwnerStats(); got.Live != 0 || got.Refs != 0 {
			t.Fatal(got)
		}
	}
	// A fabricated DB cannot acquire constructor custody through interface shape.
	empty := &DB{}
	owner, err := newNativePublicationResidentOwnerV1()
	if err != nil {
		t.Fatal(err)
	}
	defer owner.CloseStableMetadataResidentOwnerV1()
	if installed, err := empty.InstallNativePublicationResidentOwnerV1(owner, nativePublicationResidentLimitV1); installed || err == nil || empty.HasNativePublicationResidentOwnerV1() {
		t.Fatal("unowned DB retrofitted by structural facet")
	}
}
