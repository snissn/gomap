package db

import "testing"

func TestRegisterCloseHookAfterRunCloseHooksIsIgnored(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()

	ran := 0
	unregister := d.RegisterCloseHook(func() error {
		ran++
		return nil
	})
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("RunCloseHooks: %v", err)
	}
	if ran != 1 {
		t.Fatalf("close hook ran %d times, want 1", ran)
	}

	lateRan := false
	lateUnregister := d.RegisterCloseHook(func() error {
		lateRan = true
		return nil
	})
	lateUnregister()
	unregister()
	if err := d.RunCloseHooks(); err != nil {
		t.Fatalf("second RunCloseHooks: %v", err)
	}
	if lateRan {
		t.Fatalf("late close hook ran after hook drain started")
	}
	if ran != 1 {
		t.Fatalf("close hook ran %d times after second drain, want 1", ran)
	}
}
