package stableio

import "errors"

var ErrDuplicateCustodyUsed = errors.New("stableio: duplicate custody is nil, copied, or already used")

// DuplicateCustody is a caller-owned, single-use record for one duplication.
// Allocate and account for it before calling DuplicateFileWithCustody. It must
// not be copied after use begins, or accessed concurrently. On failure the
// returned error is this record, which retains both the original operation and
// cleanup errors. Retain it while resolving any cleanup debt.
//
// A failed raw close is ambiguous: the operating system may already have
// consumed the numeric descriptor. The original raw operand is recorded here
// for custody, but is deliberately not exposed for use or another close. This
// record does not assert that the underlying object is still open or pinned.
// It does not account for syscall, error, name, or wrapping allocations.
type DuplicateCustody struct {
	self         *DuplicateCustody
	used         bool
	ambiguous    bool
	raw          uintptr
	operationErr error
	controlErr   error
	cleanupErr   error
}

func (c *DuplicateCustody) begin() bool {
	if c == nil || c.used || c.self != nil {
		return false
	}
	c.self, c.used = c, true
	return true
}

func (c *DuplicateCustody) valid() bool { return c != nil && c.self == c && c.used }

// Ambiguous reports whether the one attempted raw close failed. It does not
// establish whether the OS still owns the raw descriptor.
func (c *DuplicateCustody) Ambiguous() bool { return c.valid() && c.ambiguous }

// OperationError and ControlError distinguish failure inside the control
// callback from failure of the control operation itself.
func (c *DuplicateCustody) OperationError() error {
	if !c.valid() {
		return ErrDuplicateCustodyUsed
	}
	return c.operationErr
}
func (c *DuplicateCustody) ControlError() error {
	if !c.valid() {
		return ErrDuplicateCustodyUsed
	}
	return c.controlErr
}
func (c *DuplicateCustody) CleanupError() error {
	if !c.valid() {
		return ErrDuplicateCustodyUsed
	}
	return c.cleanupErr
}

func (c *DuplicateCustody) Error() string {
	return "stableio: duplicate file failed; inspect custody for causes"
}
func (c *DuplicateCustody) Is(target error) bool {
	if !c.valid() {
		return target == ErrDuplicateCustodyUsed
	}
	return errors.Is(c.operationErr, target) || errors.Is(c.controlErr, target) || errors.Is(c.cleanupErr, target)
}
