package rootpublication

// EnableNativeUnboundedDiagnosticV6 selects synchronous original callbacks on
// the SAME native constructor. It grants no bounded-work or latency contract.
// Selection is immutable before visibility; native ownership is preserved.
func (t *DurableRootTransaction) EnableNativeUnboundedDiagnosticV6() error {
	if t == nil {
		return ErrDurableRootOwnership
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.constructionV6
	if c == nil || !c.nativeWorkOwned || c.cleanupPhase != 0 || t.phase != durableRootConstructingV6 {
		return ErrDurableRootOwnership
	}
	c.unboundedDiagnostic = true
	return nil
}
func (t *DurableRootTransaction) NativeUnboundedDiagnosticV6() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.constructionV6 != nil && t.constructionV6.nativeWorkOwned && t.constructionV6.unboundedDiagnostic
}

// RunNativeUnboundedDiagnosticV6 drives the original Prepare/Publish/Report/
// physical Finish engine in this caller. No native work is scheduler-owned and
// no successful return precedes actual terminal accounting.
func (c *Coordinator) RunNativeUnboundedDiagnosticV6(candidate *PreparedRootCandidate) error {
	if c == nil || candidate == nil || !candidate.DurableRoot().NativeUnboundedDiagnosticV6() {
		return ErrDurableRootOwnership
	}
	progress, completion := c.dispatchPublicationCallPolicyV6(c.StartupSynchronous(), candidate)
	// Callback and candidate aliases must end before their containing refund.
	candidate = nil
	completePrimaryTerminalsV6(&completion.head)
	c.mu.Lock()
	defer c.mu.Unlock()
	if completion.active {
		if completion.success {
			c.completePublicationLockedV6(completion)
		}
		if c.activeDispatch == 0 {
			panic("publication call join underflow")
		}
		c.activeDispatch--
		c.notifyLocked()
	}
	if !progress {
		return ErrPublisherProtocol
	}
	if err := c.terminalErrorLocked(); err != nil {
		return err
	}
	if !completion.success {
		return ErrRecoveryRequired
	}
	return nil
}
