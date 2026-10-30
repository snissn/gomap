package retainedalloc

// RetainForOwner obtains a separately admitted edge to the same owner and
// governing budget. It serializes with closure of this exact edge; the new
// edge survives an immediately concurrent Close of the original. Paired
// enrollments are deliberately unsupported. This operation can fail capacity
// admission and must not be called under an unrelated producer or source lock.
// It does not reserve another producer's future growth or prove budget isolation.
func (e *Enrollment) RetainForOwner(owner *Owner) (*Enrollment, error) {
	if e == nil || owner == nil {
		return nil, ErrClosed
	}
	e.closeWork.mu.Lock()
	defer e.closeWork.mu.Unlock()
	if e.closed.Load() || e.owner != owner || e.binding == nil || e.companion != nil {
		return nil, ErrClosed
	}
	return owner.Enroll(e.binding.budget)
}
