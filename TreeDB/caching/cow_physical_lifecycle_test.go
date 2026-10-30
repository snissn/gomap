package caching

import (
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func cleanupTestPhysicalLifecycle(t *testing.T, c *cowCache) {
	t.Helper()
	t.Cleanup(func() {
		c.writerMu.Lock()
		token := cowPhysicalLifecycleToken{owner: c, epoch: c.physicalLifecycle.live}
		var cut *cowReadCut
		var err error
		if token.epoch != 0 {
			cut, err = c.finishPhysicalLifecycleLocked(token)
		}
		c.writerMu.Unlock()
		if err != nil {
			t.Errorf("finish test owner: %v", err)
		}
		if cut != nil {
			if err := c.releaseCut(cut); err != nil {
				t.Errorf("release test owner: %v", err)
			}
		}
	})
}

func testPhysicalLifecycleEdge(t *testing.T, c *cowCache, cut *cowReadCut) func() error {
	t.Helper()
	owned := cut
	release := func() error {
		if owned == nil {
			return nil
		}
		cut := owned
		owned = nil
		return c.releaseCut(cut)
	}
	t.Cleanup(func() {
		if err := release(); err != nil {
			t.Errorf("release test edge: %v", err)
		}
	})
	return release
}

func TestCOWPhysicalLifecycleJoinRetainsOriginalCut(t *testing.T) {
	_, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	c.writerMu.Lock()
	opening := c.cut
	id, err := c.registerPhysicalLifecycleLocked(opening)
	if err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}
	if _, err := c.registerPhysicalLifecycleLocked(opening); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		c.writerMu.Unlock()
		t.Fatalf("second owner admitted: %v", err)
	}
	joinedID, joined, outcome := c.joinPhysicalLifecycleLocked(opening)
	releaseJoined := testPhysicalLifecycleEdge(t, c, joined)
	if outcome != cowPhysicalLifecycleJoined || joinedID != (cowPhysicalLifecycleObservation{owner: id.owner, epoch: id.epoch}) || joined != opening {
		c.writerMu.Unlock()
		t.Fatal("join lost original physical cut identity")
	}
	if _, err := c.finishPhysicalLifecycleLocked(cowPhysicalLifecycleToken{owner: c, epoch: id.epoch + 1}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		c.writerMu.Unlock()
		t.Fatalf("foreign completion withdrew owner: %v", err)
	}
	retired, err := c.finishPhysicalLifecycleLocked(id)
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	_, _, after := c.joinPhysicalLifecycleLocked(opening)
	c.writerMu.Unlock()
	if err != nil || retired != opening || after != cowPhysicalLifecycleNoRegistration {
		t.Fatalf("retirement cut=%p after=%d err=%v", retired, after, err)
	}
	if err := releaseRetired(); err != nil {
		t.Fatal(err)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 1 {
		t.Fatal("close withdrew the independently joined old view")
	}
	if err := releaseJoined(); err != nil {
		t.Fatal(err)
	}
	if c.activeCuts.Load() != 0 {
		t.Fatal("last joined edge failed to retire")
	}
}

func TestCOWPhysicalLifecycleRegistrationOrdersJoin(t *testing.T) {
	_, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	type result struct {
		id      cowPhysicalLifecycleObservation
		cut     *cowReadCut
		outcome cowPhysicalLifecycleJoin
	}
	joined := make(chan result, 1)
	opening := c.retainCut()
	if opening == nil {
		t.Fatal("retain opening")
	}
	releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
	c.writerMu.Lock()
	go func() {
		c.writerMu.Lock()
		id, cut, outcome := c.joinPhysicalLifecycleLocked(opening)
		c.writerMu.Unlock()
		joined <- result{id: id, cut: cut, outcome: outcome}
	}()
	id, err := c.registerPhysicalLifecycleLocked(opening)
	c.writerMu.Unlock()
	got := <-joined
	releaseJoined := testPhysicalLifecycleEdge(t, c, got.cut)
	if err != nil {
		t.Fatal(err)
	}
	if got.outcome != cowPhysicalLifecycleJoined || got.id != (cowPhysicalLifecycleObservation{owner: id.owner, epoch: id.epoch}) || got.cut != opening {
		t.Fatalf("ordered join: %+v", got)
	}
	c.writerMu.Lock()
	retired, err := c.finishPhysicalLifecycleLocked(id)
	c.writerMu.Unlock()
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil {
		t.Fatal(err)
	}
	if err := releaseRetired(); err != nil {
		t.Fatal(err)
	}
	if err := releaseJoined(); err != nil {
		t.Fatal(err)
	}
	if err := releaseOpening(); err != nil {
		t.Fatal(err)
	}
}

func TestCOWPhysicalLifecycleRejectsStaleOwner(t *testing.T) {
	_, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	c.writerMu.Lock()
	opening := c.cut
	first, err := c.registerPhysicalLifecycleLocked(opening)
	if err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}

	retired, err := c.finishPhysicalLifecycleLocked(first)
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}
	second, err := c.registerPhysicalLifecycleLocked(opening)
	if err != nil {
		c.writerMu.Unlock()
		t.Fatal(err)
	}
	_, staleErr := c.finishPhysicalLifecycleLocked(first)
	joinedID, joined, outcome := c.joinPhysicalLifecycleLocked(opening)
	releaseJoined := testPhysicalLifecycleEdge(t, c, joined)
	last, finishErr := c.finishPhysicalLifecycleLocked(second)
	releaseLast := testPhysicalLifecycleEdge(t, c, last)
	c.writerMu.Unlock()
	if !errors.Is(staleErr, backenddb.ErrNativeSemanticUnsupported) || outcome != cowPhysicalLifecycleJoined || joinedID != (cowPhysicalLifecycleObservation{owner: second.owner, epoch: second.epoch}) || second == first || finishErr != nil {
		t.Fatalf("stale=%v second=%+v first=%+v join=%+v outcome=%d finish=%v", staleErr, second, first, joinedID, outcome, finishErr)
	}
	for _, release := range []func() error{releaseRetired, releaseJoined, releaseLast} {
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCOWPhysicalLifecycleRejectsForeignOwnerToken(t *testing.T) {
	_, a := cowCutFixture(t)
	_, b := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, a)
	cleanupTestPhysicalLifecycle(t, b)
	a.writerMu.Lock()
	aToken, aErr := a.registerPhysicalLifecycleLocked(a.cut)
	a.writerMu.Unlock()
	if aErr != nil {
		t.Fatal(aErr)
	}
	b.writerMu.Lock()
	bToken, bErr := b.registerPhysicalLifecycleLocked(b.cut)
	if bErr != nil {
		b.writerMu.Unlock()
		t.Fatal(bErr)
	}
	foreignCut, foreignErr := b.finishPhysicalLifecycleLocked(aToken)
	testPhysicalLifecycleEdge(t, b, foreignCut)
	bCut, finishErr := b.finishPhysicalLifecycleLocked(bToken)
	releaseB := testPhysicalLifecycleEdge(t, b, bCut)
	b.writerMu.Unlock()
	a.writerMu.Lock()
	aCut, aFinishErr := a.finishPhysicalLifecycleLocked(aToken)
	releaseA := testPhysicalLifecycleEdge(t, a, aCut)
	a.writerMu.Unlock()
	if !errors.Is(foreignErr, backenddb.ErrNativeSemanticUnsupported) || finishErr != nil || aFinishErr != nil || aToken.epoch != bToken.epoch {
		t.Fatalf("foreign=%v finish=%v aFinish=%v epochs=%d,%d", foreignErr, finishErr, aFinishErr, aToken.epoch, bToken.epoch)
	}
	if err := releaseA(); err != nil {
		t.Fatal(err)
	}
	if err := releaseB(); err != nil {
		t.Fatal(err)
	}
}

// A retained old view remains valid after Close, but cannot start a new join.
func TestCOWPhysicalLifecycleClosedRejectsJoin(t *testing.T) {
	_, c := cowCutFixture(t)
	old := c.retainCut()
	if old == nil {
		t.Fatal("retain opening")
	}
	release := testPhysicalLifecycleEdge(t, c, old)
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	c.writerMu.Lock()
	_, joined, outcome := c.joinPhysicalLifecycleLocked(old)
	c.writerMu.Unlock()
	if joined != nil || outcome != cowPhysicalLifecycleUnavailable {
		t.Fatalf("closed join outcome=%v cut=%p", outcome, joined)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
}

// The owner remains responsible for an outstanding registration on close.
// This tests the primitive, not integration with accepted handoff failures.
func TestCOWPhysicalLifecycleLiveOwnerAtClose(t *testing.T) {
	_, c := cowCutFixture(t)
	cleanupTestPhysicalLifecycle(t, c)
	opening := c.retainCut()
	if opening == nil {
		t.Fatal("retain opening")
	}
	releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
	c.writerMu.Lock()
	token, err := c.registerPhysicalLifecycleLocked(opening)
	_, joined, outcome := c.joinPhysicalLifecycleLocked(opening)
	c.writerMu.Unlock()
	releaseJoined := testPhysicalLifecycleEdge(t, c, joined)
	if err != nil || outcome != cowPhysicalLifecycleJoined {
		t.Fatalf("register=%v join=%v", err, outcome)
	}
	if err := c.close(); err != nil {
		t.Fatal(err)
	}
	c.writerMu.Lock()
	_, post, postOutcome := c.joinPhysicalLifecycleLocked(opening)
	retired, err := c.finishPhysicalLifecycleLocked(token)
	c.writerMu.Unlock()
	releaseRetired := testPhysicalLifecycleEdge(t, c, retired)
	if err != nil || post != nil || postOutcome != cowPhysicalLifecycleUnavailable {
		t.Fatalf("finish=%v post=%p outcome=%v", err, post, postOutcome)
	}
	for _, release := range []func() error{releaseRetired, releaseJoined, releaseOpening} {
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
	if c.activeCuts.Load() != 0 {
		t.Fatal("remaining retained cut")
	}
}
