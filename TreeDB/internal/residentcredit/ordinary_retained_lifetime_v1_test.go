package residentcredit

import (
	"math"
	"testing"
)

func TestOriginalOrdinaryScopeSurvivesOwnerCloseUntilLastEdge(t *testing.T) {
	owner, err := NewOrdinary(128 << 20)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := owner.NewOrdinaryScope()
	if err != nil {
		t.Fatal(err)
	}
	owner.Close()
	if _, err := owner.NewOrdinaryScope(); err == nil {
		t.Fatal("closed owner created a new scope")
	}
	if err := scope.RetainStableMetadata(); err == nil {
		t.Fatal("generic retain revived closed owner")
	}
	before := owner.Stats()
	if err := scope.RetainOriginalLifetime(); err != nil {
		t.Fatal(err)
	}
	if err := scope.ReserveOriginalLifetime(64); err != nil {
		t.Fatal(err)
	}
	scope.ReleaseStableMetadata()
	if got := owner.Stats(); got.Live != before.Live+64 || got.Births != before.Births+64 {
		t.Fatalf("original backing lost: %+v", got)
	}
	scope.ReleaseStableMetadata()
	if got := owner.Stats(); got.Live != 0 || got.Refs != 0 {
		t.Fatalf("last edge retained backing: %+v", got)
	}
	if scope.RetainOriginalLifetime() == nil || scope.ReserveOriginalLifetime(1) == nil {
		t.Fatal("dead scope revived")
	}
}

func TestOriginalLifetimeRejectsStrictAndOverflowWithoutEffects(t *testing.T) {
	owner, _ := NewOrdinary(4096)
	ordinary, _ := owner.NewOrdinaryScope()
	strict, err := owner.NewScope()
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Stats()
	if strict.RetainOriginalLifetime() == nil || strict.ReserveOriginalLifetime(1) == nil {
		t.Fatal("strict original lifetime accepted")
	}
	if ordinary.ReserveOriginalLifetime(before.Limit-before.Live+1) == nil {
		t.Fatal("ordinary growth bypassed live strict envelope")
	}
	if got := owner.Stats(); got != before {
		t.Fatalf("refusal mutated owner: %+v -> %+v", before, got)
	}
	owner.mu.Lock()
	ordinary.retained = math.MaxUint64
	owner.mu.Unlock()
	if ordinary.RetainOriginalLifetime() == nil {
		t.Fatal("retention overflow accepted")
	}
	owner.mu.Lock()
	ordinary.retained = 1
	oldBirths := owner.births
	owner.births = math.MaxUint64
	owner.mu.Unlock()
	if ordinary.ReserveOriginalLifetime(1) == nil {
		t.Fatal("birth overflow accepted")
	}
	owner.mu.Lock()
	owner.births = oldBirths
	owner.mu.Unlock()
	strict.ReleaseStableMetadata()
	ordinary.ReleaseStableMetadata()
	owner.Close()
}
