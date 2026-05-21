package colgranule

import (
	"errors"
	"fmt"
	"math"
)

const maxSortKeyColumns = 8

type Int64RangePredicate struct {
	Column string
	Low    int64
	High   int64
}

func (p Int64RangePredicate) Empty() bool {
	return p.Low > p.High
}

type PredicatePlan struct {
	Filter        Int64RangePredicate
	SortKeyRanges []Int64RangePredicate
}

type PredicateDiagnostics struct {
	Considered      int
	SkippedByMark   int
	SkippedByMinMax int
	Decoded         int
	Matched         int
}

type SortKeyColumn struct {
	Name   string
	Values []int64
}

type SortKeyBound struct {
	Values    []int64
	Exclusive bool
	Unbounded bool
}

type SortKeyPrefixSummary struct {
	Columns        []string
	Lower          SortKeyBound
	UpperExclusive SortKeyBound
}

type SortKeyMark struct {
	Rows         int
	Columns      []string
	ColumnValues [][]int64
	Prefixes     []SortKeyPrefixSummary
}

type sortKeyRangePlan struct {
	prefixLen int
	lower     [maxSortKeyColumns]int64
	upper     [maxSortKeyColumns]int64
	hasUpper  bool
	empty     bool
}

func BuildSortKeyMark(columns []SortKeyColumn) (SortKeyMark, error) {
	if len(columns) == 0 {
		return SortKeyMark{}, errors.New("colgranule: empty sort key")
	}
	if len(columns) > maxSortKeyColumns {
		return SortKeyMark{}, fmt.Errorf("colgranule: sort key columns=%d exceeds cap %d", len(columns), maxSortKeyColumns)
	}
	rows := len(columns[0].Values)
	if rows == 0 {
		return SortKeyMark{}, errors.New("colgranule: empty sort-key mark")
	}
	mark := SortKeyMark{
		Rows:         rows,
		Columns:      make([]string, len(columns)),
		ColumnValues: make([][]int64, len(columns)),
		Prefixes:     make([]SortKeyPrefixSummary, len(columns)),
	}
	lower := make([]int64, len(columns))
	last := make([]int64, len(columns))
	for i, c := range columns {
		if c.Name == "" {
			return SortKeyMark{}, fmt.Errorf("colgranule: empty sort key column name at %d", i)
		}
		for j := 0; j < i; j++ {
			if columns[j].Name == c.Name {
				return SortKeyMark{}, fmt.Errorf("colgranule: duplicate sort key column %q", c.Name)
			}
		}
		if len(c.Values) != rows {
			return SortKeyMark{}, fmt.Errorf("colgranule: sort key column %s rows=%d want=%d", c.Name, len(c.Values), rows)
		}
		mark.Columns[i] = c.Name
		mark.ColumnValues[i] = append([]int64(nil), c.Values...)
		lower[i] = c.Values[0]
		last[i] = c.Values[rows-1]
	}
	for row := 1; row < rows; row++ {
		if compareSortKeyRows(columns, row-1, row) > 0 {
			return SortKeyMark{}, fmt.Errorf("colgranule: sort key rows out of order at row %d", row)
		}
	}
	for prefixLen := 1; prefixLen <= len(columns); prefixLen++ {
		prefixColumns := append([]string(nil), mark.Columns[:prefixLen]...)
		prefixLower := append([]int64(nil), lower[:prefixLen]...)
		upper, hasUpper := exclusiveUpperBound(last[:prefixLen])
		mark.Prefixes[prefixLen-1] = SortKeyPrefixSummary{
			Columns: prefixColumns,
			Lower: SortKeyBound{
				Values: prefixLower,
			},
			UpperExclusive: SortKeyBound{
				Values:    upper,
				Exclusive: true,
				Unbounded: !hasUpper,
			},
		}
	}
	return mark, nil
}

func (m SortKeyMark) MayContainRanges(ranges []Int64RangePredicate) (bool, bool, error) {
	plan, err := compileSortKeyRangePlan(m.Columns, ranges)
	if err != nil {
		return false, false, err
	}
	return m.mayContainCompiled(plan)
}

func (r *GranuleReader) CountInt64RangeWithDiagnostics(granules []EncodedGranule, marks []SortKeyMark, plan PredicatePlan) (int, PredicateDiagnostics, error) {
	var diagnostics PredicateDiagnostics
	if plan.Filter.Empty() {
		return 0, diagnostics, nil
	}
	if plan.Filter.Column == "" {
		return 0, diagnostics, errors.New("colgranule: empty filter column")
	}
	if len(marks) != 0 && len(marks) != len(granules) {
		return 0, diagnostics, fmt.Errorf("colgranule: marks=%d granules=%d", len(marks), len(granules))
	}
	var sortPlan sortKeyRangePlan
	var err error
	if len(marks) != 0 {
		if !containsString(marks[0].Columns, plan.Filter.Column) {
			return 0, diagnostics, fmt.Errorf("colgranule: filter column %q not present in sort key mark", plan.Filter.Column)
		}
		sortPlan, err = compileSortKeyRangePlan(marks[0].Columns, plan.SortKeyRanges)
		if err != nil {
			return 0, diagnostics, err
		}
	}
	total := 0
	for i, g := range granules {
		diagnostics.Considered++
		if len(marks) != 0 {
			if !sameStringSlice(marks[0].Columns, marks[i].Columns) {
				return total, diagnostics, errors.New("colgranule: inconsistent sort key mark columns")
			}
			if marks[i].Rows != g.Rows {
				return total, diagnostics, fmt.Errorf("colgranule: mark rows=%d granule rows=%d at granule %d", marks[i].Rows, g.Rows, i)
			}
			mayContain, constrained, err := marks[i].mayContainCompiled(sortPlan)
			if err != nil {
				return total, diagnostics, err
			}
			if constrained && !mayContain {
				diagnostics.SkippedByMark++
				continue
			}
		}
		if g.HasMinMax && (plan.Filter.High < g.Min || plan.Filter.Low > g.Max) {
			diagnostics.SkippedByMinMax++
			continue
		}
		values, err := r.DecodeInt64(g)
		if err != nil {
			return total, diagnostics, err
		}
		diagnostics.Decoded++
		count := 0
		if len(marks) != 0 && len(plan.SortKeyRanges) != 0 {
			count, err = marks[i].countMatchingRows(values, plan)
			if err != nil {
				return total, diagnostics, err
			}
		} else {
			for _, v := range values {
				if v >= plan.Filter.Low && v <= plan.Filter.High {
					count++
				}
			}
		}
		diagnostics.Matched += count
		total += count
	}
	return total, diagnostics, nil
}

func compileSortKeyRangePlan(columns []string, ranges []Int64RangePredicate) (sortKeyRangePlan, error) {
	var plan sortKeyRangePlan
	if len(ranges) == 0 || len(columns) == 0 {
		return plan, nil
	}
	if len(columns) > maxSortKeyColumns {
		return plan, fmt.Errorf("colgranule: sort key columns=%d exceeds cap %d", len(columns), maxSortKeyColumns)
	}
	if err := validateSortKeyColumnNames(columns); err != nil {
		return plan, err
	}
	var upperInclusive [maxSortKeyColumns]int64
	for _, column := range columns {
		predicate, ok := findRangePredicate(ranges, column)
		if !ok {
			break
		}
		if predicate.Empty() {
			plan.empty = true
			return plan, nil
		}
		plan.lower[plan.prefixLen] = predicate.Low
		upperInclusive[plan.prefixLen] = predicate.High
		plan.prefixLen++
	}
	if plan.prefixLen == 0 {
		return plan, nil
	}
	plan.hasUpper = exclusiveUpperBoundArray(upperInclusive[:plan.prefixLen], &plan.upper)
	return plan, nil
}

func (m SortKeyMark) mayContainCompiled(plan sortKeyRangePlan) (bool, bool, error) {
	if plan.empty {
		return false, true, nil
	}
	if plan.prefixLen == 0 {
		return true, false, nil
	}
	if plan.prefixLen > len(m.Prefixes) {
		return false, true, errors.New("colgranule: missing sort key prefix summary")
	}
	prefix := m.Prefixes[plan.prefixLen-1]
	if plan.hasUpper && compareInt64Tuple(plan.upper[:plan.prefixLen], prefix.Lower.Values) <= 0 {
		return false, true, nil
	}
	if !prefix.UpperExclusive.Unbounded && compareInt64Tuple(prefix.UpperExclusive.Values, plan.lower[:plan.prefixLen]) <= 0 {
		return false, true, nil
	}
	return true, true, nil
}

func (m SortKeyMark) countMatchingRows(values []int64, plan PredicatePlan) (int, error) {
	if len(values) != m.Rows {
		return 0, fmt.Errorf("colgranule: decoded rows=%d mark rows=%d", len(values), m.Rows)
	}
	if len(m.ColumnValues) != len(m.Columns) {
		return 0, errors.New("colgranule: sort key mark row values missing")
	}
	count := 0
	for row, v := range values {
		if v < plan.Filter.Low || v > plan.Filter.High {
			continue
		}
		matches, err := m.rowMatchesRanges(row, plan.SortKeyRanges)
		if err != nil {
			return 0, err
		}
		if matches {
			count++
		}
	}
	return count, nil
}

func (m SortKeyMark) rowMatchesRanges(row int, ranges []Int64RangePredicate) (bool, error) {
	for _, predicate := range ranges {
		if predicate.Empty() {
			return false, nil
		}
		columnIndex := indexString(m.Columns, predicate.Column)
		if columnIndex < 0 {
			return false, fmt.Errorf("colgranule: sort key range column %q not present in mark", predicate.Column)
		}
		values := m.ColumnValues[columnIndex]
		if row < 0 || row >= len(values) {
			return false, fmt.Errorf("colgranule: sort key column %q rows=%d want row %d", predicate.Column, len(values), row)
		}
		value := values[row]
		if value < predicate.Low || value > predicate.High {
			return false, nil
		}
	}
	return true, nil
}

func findRangePredicate(ranges []Int64RangePredicate, column string) (Int64RangePredicate, bool) {
	for _, p := range ranges {
		if p.Column == column {
			return p, true
		}
	}
	return Int64RangePredicate{}, false
}

func validateSortKeyColumnNames(columns []string) error {
	for i, column := range columns {
		if column == "" {
			return fmt.Errorf("colgranule: empty sort key column name at %d", i)
		}
		for j := 0; j < i; j++ {
			if columns[j] == column {
				return fmt.Errorf("colgranule: duplicate sort key column %q", column)
			}
		}
	}
	return nil
}

func compareSortKeyRows(columns []SortKeyColumn, left int, right int) int {
	for _, column := range columns {
		lv := column.Values[left]
		rv := column.Values[right]
		if lv < rv {
			return -1
		}
		if lv > rv {
			return 1
		}
	}
	return 0
}

func containsString(values []string, want string) bool {
	return indexString(values, want) >= 0
}

func indexString(values []string, want string) int {
	for i, value := range values {
		if value == want {
			return i
		}
	}
	return -1
}

func exclusiveUpperBound(values []int64) ([]int64, bool) {
	out := append([]int64(nil), values...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] == math.MaxInt64 {
			continue
		}
		out[i]++
		for j := i + 1; j < len(out); j++ {
			out[j] = math.MinInt64
		}
		return out, true
	}
	return nil, false
}

func exclusiveUpperBoundArray(values []int64, out *[maxSortKeyColumns]int64) bool {
	for i, v := range values {
		out[i] = v
	}
	for i := len(values) - 1; i >= 0; i-- {
		if out[i] == math.MaxInt64 {
			continue
		}
		out[i]++
		for j := i + 1; j < len(values); j++ {
			out[j] = math.MinInt64
		}
		return true
	}
	return false
}

func compareInt64Tuple(a []int64, b []int64) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func sameStringSlice(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
