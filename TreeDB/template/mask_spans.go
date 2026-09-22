package template

func buildMaskSpans(mask []byte, total int) (varSpans []span, constSpans []span) {
	if total <= 0 || len(mask) == 0 {
		return nil, nil
	}
	kind := -1
	start := 0
	for i := 0; i < total; i++ {
		isVar := 0
		if mask[i/8]&(1<<uint(i%8)) != 0 {
			isVar = 1
		}
		if kind == -1 {
			kind = isVar
			start = i
			continue
		}
		if isVar == kind {
			continue
		}
		if start < i {
			if kind == 1 {
				varSpans = append(varSpans, span{start: start, end: i})
			} else {
				constSpans = append(constSpans, span{start: start, end: i})
			}
		}
		kind = isVar
		start = i
	}
	if start < total {
		if kind == 1 {
			varSpans = append(varSpans, span{start: start, end: total})
		} else {
			constSpans = append(constSpans, span{start: start, end: total})
		}
	}
	return varSpans, constSpans
}
