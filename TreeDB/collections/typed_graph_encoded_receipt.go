package collections

// Primary may be pointerized before acknowledgement; flush covers the later
// typed assets and remaining native tables. These scalar bounds belong to the
// existing receipt, never to another copy of the document payload.
type typedGraphEncodedCost struct{ primary, flush int64 }

func (c typedGraphEncodedCost) total() (int64, error) {
	var n int64
	for _, term := range [...]int64{c.primary, c.flush} {
		if err := addTypedGraphEncodedBytes(&n, term); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// beginTypedGraphEncodedAttempt is called before physical preparation, not
// after an appender returns. A failed append may leave unreachable bytes. The
// first attempt consumes its reservation; every retry reserves another full
// phase. All receipts in a flush advance together or none do. No backend calls
// occur under the accounting lock. The caller separately proves input coverage.
func beginTypedGraphEncodedAttempt(receipts []*typedGraphPublicationReceipt, primary bool) error {
	if len(receipts) == 0 {
		return nil
	}
	if receipts[0] == nil || receipts[0].coord == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	coord := receipts[0].coord
	coord.typedPublicationDebtMu.Lock()
	defer coord.typedPublicationDebtMu.Unlock()
	state := coord.typedPublication.Load()
	if state == nil {
		return ErrVectorIndexSnapshotMismatch
	}
	var extra int64
	var seen map[*typedGraphPublicationReceipt]struct{}
	if len(receipts) > 1 {
		seen = make(map[*typedGraphPublicationReceipt]struct{}, len(receipts))
	}
	for _, r := range receipts {
		if r == nil || r.coord != coord || r.consumed {
			return ErrVectorIndexSnapshotMismatch
		}
		// Same single-receipt fast path as validateTypedGraphReceiptInput.
		if seen != nil {
			if _, duplicate := seen[r]; duplicate {
				return ErrVectorIndexSnapshotMismatch
			}
			seen[r] = struct{}{}
		}
		cost, attempted := r.encoded.flush, r.flushAttempted
		if primary {
			cost, attempted = r.encoded.primary, r.primaryAttempted
		}
		if attempted {
			if err := addTypedGraphEncodedBytes(&extra, cost); err != nil {
				return err
			}
		}
	}
	if extra > 0 && (state.limits.EncodedOutputBytes <= 0 || extra > state.limits.EncodedOutputBytes-coord.typedPublicationEncodedBytes) {
		return errTypedGraphOverlayFoldNeeded
	}
	coord.typedPublicationEncodedBytes += extra
	for _, r := range receipts {
		if primary {
			r.primaryAttempted = true
		} else {
			r.flushAttempted = true
		}
	}
	return nil
}
