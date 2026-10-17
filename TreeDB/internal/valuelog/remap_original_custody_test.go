package valuelog

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// Queue the real asynchronous remap behind Manager.mu, then close the original
// File independently. No worker hook or replacement physical handle is used.
func TestOriginalFileCloseJoinsQueuedRemapBeforePhysicalRetirement(t *testing.T) {
	previous := enableCurrentWritableMmap
	enableCurrentWritableMmap = true
	defer func() { enableCurrentWritableMmap = previous }()
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "physical_failure"
		}
		t.Run(name, func(t *testing.T) {
			m, f, registry := originalTerminalFixture(t)
			if err := m.PromoteCurrentWritable(f.ID); err != nil {
				t.Fatal(err)
			}
			original := f.File
			before := registry.MetadataOwner().Bytes()
			if fail {
				if err := original.Close(); err != nil {
					t.Fatal(err)
				}
			}
			m.mu.Lock()
			var unlock sync.Once
			release := func() { unlock.Do(m.mu.Unlock) }
			defer release()
			f.maybeScheduleRemap()
			if !f.remapRequested.Load() {
				t.Fatal("actual remap did not queue")
			}
			done := make(chan error, 1)
			go func() { done <- f.Close() }()
			select {
			case err := <-done:
				release()
				// Join the original asynchronous worker even when testing the old bug.
				limit := time.Now().Add(time.Second)
				for f.remapRequested.Load() && time.Now().Before(limit) {
					time.Sleep(time.Millisecond)
				}
				_ = m.Close()
				t.Fatalf("physical Close returned while original remap was queued: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			f.closeMu.Lock()
			live := f.File == original && f.manager == m && f.producerCharge != 0
			f.closeMu.Unlock()
			if !live || registry.MetadataOwner().Bytes() != before {
				t.Error("queued worker lost original aliases/accounting")
			}
			release()
			var result error
			select {
			case result = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("Close/remap lock-order deadlock")
			}
			if fail {
				if !errors.Is(result, os.ErrClosed) {
					t.Fatalf("original physical failure lost: %v", result)
				}
			} else if result != nil {
				t.Fatal(result)
			}
			if f.remapRequested.Load() {
				t.Fatal("Close returned before original worker exited")
			}
			f.maybeScheduleRemap()
			if f.remapRequested.Load() {
				t.Fatal("closed File admitted another worker")
			}
			terminal := m.Close()
			if fail {
				if !errors.Is(terminal, os.ErrClosed) || f.File != original || f.producerCharge == 0 {
					t.Fatal("failed close discarded original physical debt")
				}
				if f.Close() != result {
					t.Fatal("repeated Close changed original first result")
				}
			} else if terminal != nil || f.producerCharge != 0 || f.File != nil || f.manager != nil {
				t.Fatalf("actual terminal retirement incomplete: %v", terminal)
			}
		})
	}
}
