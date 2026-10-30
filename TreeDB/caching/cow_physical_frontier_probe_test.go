package caching

import (
	"context"
	"errors"
	"testing"
)

// The registration is diagnostic only: it is not transferred to the real
// handoff and is not an inventory of preexisting frozen/maintenance owners.
func TestCOWPhysicalFrontierProbeAtCheckpointCapture(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		name := "joined"
		if occupied {
			name = "occupied_slot"
		}
		t.Run(name, func(t *testing.T) {
			cache, _ := nativeBridgeFixture(t, false)
			c := cache.cow
			cleanupTestPhysicalLifecycle(t, c)
			if err := cache.Set([]byte("frontier-key"), []byte("frontier-value")); err != nil {
				t.Fatal(err)
			}
			probe := &cowFrontierRegistrationProbe{}
			cache.testCOWFrontierRegistration = probe
			t.Cleanup(func() { cache.testCOWFrontierRegistration = nil; cache.testAfterCheckpointFrontierCapture = nil })
			owned := testPhysicalCutCapture(t, cache, c)
			if occupied {
				opening := c.retainCut()
				if opening == nil {
					t.Fatal("retain existing owner cut")
				}
				releaseOpening := testPhysicalLifecycleEdge(t, c, opening)
				c.writerMu.Lock()
				_, err := c.registerPhysicalLifecycleLocked(opening)
				c.writerMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
				if err = releaseOpening(); err != nil {
					t.Fatal(err)
				}
			}
			var joined cowPhysicalLifecycleJoin
			var sameCut bool
			cache.testAfterCheckpointFrontierCapture = func() {
				opening := c.retainCut()
				if opening == nil {
					return
				}
				c.writerMu.Lock()
				if !occupied {
					joined = (*owned).joinLocked(c, opening)
					sameCut = (*owned).cut == opening && c.physicalLifecycle.cut == opening
				}
				c.writerMu.Unlock()
				if err := c.releaseCut(opening); err != nil {
					t.Errorf("release opening: %v", err)
				}
			}
			err := cache.Checkpoint()
			cache.testAfterCheckpointFrontierCapture = nil
			cache.testCOWFrontierRegistration = nil
			if err != nil {
				t.Fatalf("original checkpoint: %v", err)
			}
			if !probe.attempted {
				t.Fatal("successful checkpoint did not attempt frontier registration")
			}
			if occupied {
				if probe.err == nil || probe.token.epoch != 0 {
					t.Fatalf("occupied registration err=%v token=%+v", probe.err, probe.token)
				}
			} else if probe.err != nil || probe.token.epoch == 0 || joined != cowPhysicalLifecycleJoined || !sameCut {
				t.Fatalf("registration err=%v token=%+v join=%v exact=%v", probe.err, probe.token, joined, sameCut)
			}
			if err := finishTestPhysicalCutCapture(owned); err != nil {
				t.Fatalf("finish joined capture: %v", err)
			}
			c.writerMu.Lock()
			var retired *cowReadCut
			if !occupied {
				retired, err = c.finishPhysicalLifecycleLocked(probe.token)
			}
			c.writerMu.Unlock()
			if err != nil {
				t.Fatalf("finish registration: %v", err)
			}
			if retired != nil {
				if err := c.releaseCut(retired); err != nil {
					t.Fatalf("release registered cut: %v", err)
				}
			}
		})
	}
}

func TestCOWPhysicalFrontierProbeCancellationRetiresRegistration(t *testing.T) {
	cache, _ := nativeBridgeFixture(t, false)
	c := cache.cow
	cleanupTestPhysicalLifecycle(t, c)
	if err := cache.Set([]byte("cancel-key"), []byte("cancel-value")); err != nil {
		t.Fatal(err)
	}
	probe := &cowFrontierRegistrationProbe{}
	cache.testCOWFrontierRegistration = probe
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.testAfterCheckpointFrontierCapture = cancel
	t.Cleanup(func() {
		cache.testCOWFrontierRegistration = nil
		cache.testAfterCheckpointFrontierCapture = nil
	})
	err := cache.checkpointCOWContext(ctx, false)
	cache.testCOWFrontierRegistration = nil
	cache.testAfterCheckpointFrontierCapture = nil
	if !errors.Is(err, context.Canceled) || !probe.attempted || probe.err != nil || probe.token.epoch == 0 {
		t.Fatalf("checkpoint err=%v attempted=%v registration=%v token=%+v", err, probe.attempted, probe.err, probe.token)
	}
	c.writerMu.Lock()
	retired, finishErr := c.finishPhysicalLifecycleLocked(probe.token)
	c.writerMu.Unlock()
	if finishErr != nil {
		t.Fatal(finishErr)
	}
	if err := c.releaseCut(retired); err != nil {
		t.Fatalf("release canceled frontier registration: %v", err)
	}
}
