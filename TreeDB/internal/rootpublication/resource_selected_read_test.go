package rootpublication

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOwnedSelectedResourceReadSparseExactFile(t *testing.T) {
	for _, n := range []int{1, 128, 256} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			var owner retainedalloc.Owner
			owner.Initialize(0)
			budget := &registryBudget{cap: 64 << 20}
			enrollment, err := owner.Enroll(budget)
			if err != nil {
				t.Fatal(err)
			}
			defer enrollment.Close()
			registry := NewIdentityPinRegistry()
			defer registry.Close()
			source, selector := selectedSelectorFixture(t, &owner, registry, n, nil)
			before := owner.Bytes()
			pins := registry.Stats().ActivePins
			var work selectedResourceReadWork
			var path string
			err = source.withSelectedResourceFile(selector, func(file *os.File) error {
				// This locks the same source. The file callback must be off-lock.
				if source.MetadataOwner() != &owner {
					t.Fatal("source ownership missing in callback")
				}
				// The duplicated descriptor uses a diagnostic suffix; only the
				// fixture changes the old pathname. Production never reopens it.
				path = strings.TrimSuffix(file.Name(), "#stable-pin")
				var data [7]byte
				_, err := file.ReadAt(data[:], 0)
				if string(data[:]) != "payload" {
					t.Fatalf("wrong original file: %q", data)
				}
				return err
			}, &work)
			if err != nil || work.EntriesInspected != 1 || work.LogicalIndexNodes == 0 || work.LogicalIndexNodes > 20 {
				t.Fatalf("non-sparse selected read: %+v err=%v", work, err)
			}
			if owner.Bytes() != before || budget.bytes != before || registry.Stats().ActivePins != pins {
				t.Fatal("read admitted metadata")
			}
			if err = os.Rename(path, path+".original"); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, []byte("future!"), 0o600); err != nil {
				t.Fatal(err)
			}
			budget.closed = true
			var data [7]byte
			read := func(file *os.File) error {
				_, err := file.ReadAt(data[:], 0)
				return err
			}
			if allocations := testing.AllocsPerRun(100, func() {
				if err := source.WithSelectedResourceFile(selector, read); err != nil {
					t.Fatal(err)
				}
			}); allocations != 0 {
				t.Fatalf("selected read allocated: %g", allocations)
			}
			if string(data[:]) != "payload" || owner.Bytes() != before || budget.bytes != before {
				t.Fatal("read recaptured replacement path or admission owner")
			}
			if err = source.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			source = nil
			enrollment.Close()
			if owner.Bytes() != 0 || budget.bytes != 0 || registry.Stats().ActivePins != 0 {
				t.Fatalf("last read scope leaked: owner=%d budget=%d", owner.Bytes(), budget.bytes)
			}
		})
	}
}

func TestOwnedSelectedResourceReadIndependentScopeSurvivesAncestorRelease(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	source, selector := selectedSelectorFixture(t, &owner, registry, 1, nil)
	selected, err := CloneStableResourceForSelector(source, selector)
	if err != nil {
		t.Fatal(err)
	}
	// The selected set and enrollment constitute the actual read scope. Only
	// the independent ancestor is released concurrently; selected.Release is
	// excluded until the callback returns. No callback creates a new pin.
	ancestorReleased := make(chan error, 1)
	err = selected.WithSelectedResourceFile(selector, func(file *os.File) error {
		go func() { ancestorReleased <- source.ReleaseWithError() }()
		if err := <-ancestorReleased; err != nil {
			return err
		}
		source = nil
		budget.closed = true
		if selected.MetadataOwner() != &owner || owner.Bytes() == 0 {
			t.Fatal("containing read scope ended before callback")
		}
		var data [7]byte
		_, err := file.ReadAt(data[:], 0)
		if string(data[:]) != "payload" {
			t.Fatal("ancestor release lost exact selected file")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = selected.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	selected = nil
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 || registry.Stats().ActivePins != 0 {
		t.Fatal("independent read scope leaked")
	}
}

func TestOwnedSelectedResourceReadRefusesWrongClosure(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	source, selector := selectedSelectorFixture(t, &owner, registry, 1, nil)
	callbackCalls := 0
	callback := func(*os.File) error { callbackCalls++; return nil }
	baseline := owner.Bytes()
	for _, field := range []string{"kind", "lane", "id", "generation", "obligation-key", "obligation-digest"} {
		wrong := selector
		switch field {
		case "kind":
			wrong.Kind = ResourceValueLog
		case "lane":
			wrong.LogicalLane += "wrong"
		case "id":
			wrong.ResourceID += "wrong"
		case "generation":
			wrong.PhysicalGeneration++
		case "obligation-key":
			wrong.Obligation.FileID++
		case "obligation-digest":
			wrong.Obligation.Digest[0]++
		}
		if err := source.WithSelectedResourceFile(wrong, callback); err == nil {
			t.Fatalf("accepted mismatched %s", field)
		}
	}
	if err = source.WithSelectedResourceFile(selector, nil); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil callback: %v", err)
	}
	if err = (*StableResourceSet)(nil).WithSelectedResourceFile(selector, callback); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nil source: %v", err)
	}
	if callbackCalls != 0 || owner.Bytes() != baseline {
		t.Fatal("failed lookup executed callback or changed custody")
	}
	callbackErr := errors.New("original read failed")
	if err = source.WithSelectedResourceFile(selector, func(*os.File) error { return callbackErr }); err != callbackErr || owner.Bytes() != baseline {
		t.Fatalf("read error changed: %v", err)
	}
	genericBuilder := NewStableResourceSetBuilder()
	generic, err := genericBuilder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err = generic.WithSelectedResourceFile(selector, callback); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("generic inventory accepted: %v", err)
	}
	if err = generic.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = source.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	if err = source.WithSelectedResourceFile(selector, callback); !errors.Is(err, ErrResourceOwnership) || callbackCalls != 0 {
		t.Fatalf("released inventory read: %v", err)
	}
	source = nil
	enrollment.Close()
	if owner.Bytes() != 0 || budget.bytes != 0 {
		t.Fatal("refusal scope leaked")
	}
}
