package collections

// columnVectorGraphRawDotSearchCandidate is the scalar_u8-only traversal
// candidate shape used while ordering by raw centered dot products. Public
// results and generic search seams still use columnVectorGraphSearchCandidate
// with float64 scores.
type columnVectorGraphRawDotSearchCandidate struct {
	ordinal int
	dot     int64
}

type columnVectorGraphRawDotSearchScratch struct {
	frontier []columnVectorGraphRawDotSearchCandidate
	top      []columnVectorGraphRawDotSearchCandidate
}

func (s *columnVectorGraphNativeSearchScratch) prepareRawDotCandidateQueues(rowCount, degree, topK, efSearch int) {
	raw := s.rawDot
	if raw == nil {
		raw = &columnVectorGraphRawDotSearchScratch{}
		s.rawDot = raw
	}
	frontierCap := columnVectorGraphNativeSearchFrontierCapacity(rowCount, degree, topK, efSearch)
	raw.frontier = resizeColumnVectorGraphRawDotCandidateScratch(raw.frontier, frontierCap)
	topCandidateCap := efSearch
	if topCandidateCap > rowCount {
		topCandidateCap = rowCount
	}
	if topCandidateCap < topK {
		topCandidateCap = topK
	}
	raw.top = resizeColumnVectorGraphRawDotCandidateScratch(raw.top, topCandidateCap)
}

func resizeColumnVectorGraphRawDotCandidateScratch(dst []columnVectorGraphRawDotSearchCandidate, target int) []columnVectorGraphRawDotSearchCandidate {
	if cap(dst) < target || columnVectorGraphNativeScratchCapOversized(cap(dst), target) {
		return make([]columnVectorGraphRawDotSearchCandidate, 0, target)
	}
	return dst[:0]
}

func (s *columnVectorGraphNativeSearchScratch) pushRawDotFrontier(candidate columnVectorGraphRawDotSearchCandidate) {
	raw := s.rawDot
	raw.frontier = append(raw.frontier, columnVectorGraphRawDotSearchCandidate{})
	s.rawDotFrontierSiftUp(len(raw.frontier)-1, candidate)
}

func (s *columnVectorGraphNativeSearchScratch) popRawDotFrontier() (columnVectorGraphRawDotSearchCandidate, bool) {
	raw := s.rawDot
	if raw == nil || len(raw.frontier) == 0 {
		return columnVectorGraphRawDotSearchCandidate{}, false
	}
	lastIdx := len(raw.frontier) - 1
	best := raw.frontier[0]
	last := raw.frontier[lastIdx]
	raw.frontier = raw.frontier[:lastIdx]
	if len(raw.frontier) > 0 {
		s.rawDotFrontierSiftDown(0, last)
	}
	return best, true
}

func (s *columnVectorGraphNativeSearchScratch) rawDotFrontierSiftUp(idx int, candidate columnVectorGraphRawDotSearchCandidate) {
	frontier := s.rawDot.frontier
	for idx > 0 {
		parent := (idx - 1) / columnVectorGraphNativeFrontierHeapFanout
		parentCandidate := frontier[parent]
		if !columnVectorGraphRawDotSearchCandidateBetter(candidate, parentCandidate) {
			break
		}
		frontier[idx] = parentCandidate
		idx = parent
	}
	frontier[idx] = candidate
}

func (s *columnVectorGraphNativeSearchScratch) rawDotFrontierSiftDown(idx int, candidate columnVectorGraphRawDotSearchCandidate) {
	frontier := s.rawDot.frontier
	n := len(frontier)
	for {
		firstChild := idx*columnVectorGraphNativeFrontierHeapFanout + 1
		if firstChild >= n {
			break
		}
		child := firstChild
		childCandidate := frontier[firstChild]
		if next := firstChild + 1; next < n && columnVectorGraphRawDotSearchCandidateBetter(frontier[next], childCandidate) {
			child = next
			childCandidate = frontier[next]
		}
		if next := firstChild + 2; next < n && columnVectorGraphRawDotSearchCandidateBetter(frontier[next], childCandidate) {
			child = next
			childCandidate = frontier[next]
		}
		if next := firstChild + 3; next < n && columnVectorGraphRawDotSearchCandidateBetter(frontier[next], childCandidate) {
			child = next
			childCandidate = frontier[next]
		}
		if !columnVectorGraphRawDotSearchCandidateBetter(childCandidate, candidate) {
			break
		}
		frontier[idx] = childCandidate
		idx = child
	}
	frontier[idx] = candidate
}

func (s *columnVectorGraphNativeSearchScratch) insertRawDotTop(limit int, candidate columnVectorGraphRawDotSearchCandidate) bool {
	if limit <= 0 {
		return false
	}
	raw := s.rawDot
	top := raw.top
	pos := len(top)
	for pos > 0 && columnVectorGraphRawDotSearchCandidateBetter(candidate, top[pos-1]) {
		pos--
	}
	if pos >= limit {
		return false
	}
	if len(top) < limit {
		top = append(top, columnVectorGraphRawDotSearchCandidate{})
		raw.top = top
	}
	copy(top[pos+1:], top[pos:len(top)-1])
	top[pos] = candidate
	return true
}

func columnVectorGraphRawDotSearchCandidateBetter(left, right columnVectorGraphRawDotSearchCandidate) bool {
	if left.dot == right.dot {
		return left.ordinal < right.ordinal
	}
	return left.dot > right.dot
}

func columnVectorGraphLayer0RawDotSearchShouldStop(candidate columnVectorGraphRawDotSearchCandidate, top []columnVectorGraphRawDotSearchCandidate, efSearch int) bool {
	if efSearch <= 0 || len(top) < efSearch {
		return false
	}
	return candidate.dot < top[len(top)-1].dot
}

func (s *columnVectorGraphNativeSearchScratch) promoteRawDotTopToFloat(limit int) {
	raw := s.rawDot
	if raw == nil {
		s.top = s.top[:0]
		return
	}
	if limit < 0 {
		limit = 0
	}
	if len(raw.top) > limit {
		raw.top = raw.top[:limit]
	}
	s.top = ensureColumnVectorGraphNativeCandidateScratch(s.top, len(raw.top))
	for _, candidate := range raw.top {
		s.top = append(s.top, columnVectorGraphSearchCandidate{
			ordinal: candidate.ordinal,
			score:   scalarU8QuantizedCosineScoreFromDot(candidate.dot),
		})
	}
}

func (s *columnVectorGraphNativeSearchScratch) promoteRawDotTopOrdinalsOnly(limit int) {
	raw := s.rawDot
	if raw == nil {
		s.top = s.top[:0]
		return
	}
	if limit < 0 {
		limit = 0
	}
	if len(raw.top) > limit {
		raw.top = raw.top[:limit]
	}
	s.top = ensureColumnVectorGraphNativeCandidateScratch(s.top, len(raw.top))
	for _, candidate := range raw.top {
		s.top = append(s.top, columnVectorGraphSearchCandidate{ordinal: candidate.ordinal})
	}
}
