package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	colorTreeDBPrimary       = "#2563eb" // rich blue (TreeDB / TreeDB template-v1)
	colorTreeDBTemplateV1Raw = "#0891b2" // cyan/teal (TreeDB template-v1 raw)
	colorTreeDBBSON          = "#7c3aed" // vivid purple (TreeDB BSON)
	colorTreeDBJSON          = "#d97706" // amber/orange (TreeDB JSON)
	colorSQLiteNative        = "#059669" // emerald green (SQLite native-columns)
	colorSQLiteJSON          = "#dc2626" // crimson red (SQLite JSON)
	colorMongoDB             = "#15803d" // forest green (MongoDB)
)

var fallbackChartPalette = []string{
	"#2563eb", // rich blue
	"#d97706", // amber/orange
	"#7c3aed", // vivid purple
	"#059669", // emerald green
	"#dc2626", // crimson red
	"#0891b2", // cyan/teal
	"#db2777", // rose/magenta
	"#4f46e5", // indigo
	"#ca8a04", // ochre
	"#0d9488", // teal
	"#ea580c", // burnt orange
	"#65a30d", // olive green
	"#9333ea", // violet
	"#475569", // slate
}

// knownSeriesColor maps known engine+format series keys (whether "\x00"-delimited
// or display labels) to deterministic, high-contrast, colorblind-friendly colors.
func knownSeriesColor(key string) (string, bool) {
	norm := normalizeColorKey(key)
	if norm == "" {
		return "", false
	}
	// Do not match composite "A vs B" comparison series as a single engine color
	// unless no other logic handles them; let per-chart color assignment keep pairs distinct.
	if strings.Contains(norm, " vs ") {
		return "", false
	}
	switch {
	case strings.Contains(norm, "treedb") && strings.Contains(norm, "template v1 raw"):
		return colorTreeDBTemplateV1Raw, true
	case strings.Contains(norm, "treedb") && strings.Contains(norm, "template v1"):
		return colorTreeDBPrimary, true
	case strings.Contains(norm, "treedb") && strings.Contains(norm, "bson"):
		return colorTreeDBBSON, true
	case strings.Contains(norm, "treedb") && strings.Contains(norm, "json"):
		return colorTreeDBJSON, true
	case strings.Contains(norm, "sqlite") && (strings.Contains(norm, "native columns") || strings.Contains(norm, "native")):
		return colorSQLiteNative, true
	case strings.Contains(norm, "sqlite") && strings.Contains(norm, "json"):
		return colorSQLiteJSON, true
	case norm == "mongodb" || strings.HasPrefix(norm, "mongodb "):
		return colorMongoDB, true
	case norm == "treedb":
		return colorTreeDBPrimary, true
	default:
		return "", false
	}
}

func normalizeColorKey(key string) string {
	replacer := strings.NewReplacer("\x00", " ", "_", " ", "-", " ", "/", " ")
	fields := strings.Fields(strings.ToLower(replacer.Replace(key)))
	return strings.Join(fields, " ")
}

func collectionSeriesColor(key string) string {
	if color, ok := knownSeriesColor(key); ok {
		return color
	}
	hash := fnv32a(key)
	return fallbackChartPalette[int(hash)%len(fallbackChartPalette)]
}

func fnv32a(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// assignDistinctSeriesColors ensures every series in a chart gets a unique color,
// preferring known deterministic engine+format colors and falling back to unused
// high-contrast palette entries to guarantee zero collisions within a chart.
func assignDistinctSeriesColors(series []chartSeries) []chartSeries {
	if len(series) == 0 {
		return series
	}
	out := make([]chartSeries, len(series))
	copy(out, series)
	used := make(map[string]bool, len(out))
	locked := make([]bool, len(out))

	// First pass: lock known deterministic colors (or already-distinct preset colors
	// for known series).
	for i, s := range out {
		if c, ok := knownSeriesColor(s.Name); ok && !used[c] {
			out[i].Color = c
			used[c] = true
			locked[i] = true
		} else if s.Color != "" && !used[s.Color] {
			// Check if this preset color collides with a known color needed by a later series.
			reservedByLater := false
			for j := i + 1; j < len(out); j++ {
				if kc, ok := knownSeriesColor(out[j].Name); ok && kc == s.Color {
					reservedByLater = true
					break
				}
			}
			if !reservedByLater {
				used[s.Color] = true
				locked[i] = true
			}
		}
	}

	// Second pass: assign unused palette colors to any unlocked/colliding series.
	for i, s := range out {
		if locked[i] {
			continue
		}
		start := int(fnv32a(s.Name)) % len(fallbackChartPalette)
		picked := ""
		for offset := 0; offset < len(fallbackChartPalette); offset++ {
			cand := fallbackChartPalette[(start+offset)%len(fallbackChartPalette)]
			if !used[cand] {
				picked = cand
				break
			}
		}
		if picked == "" {
			picked = fallbackChartPalette[i%len(fallbackChartPalette)]
		}
		out[i].Color = picked
		used[picked] = true
	}
	return out
}

func collectionEffectiveFormat(row collectionRow) string {
	canon := canonicalFormat(row.Format)
	lowerCfg := strings.ToLower(row.ConfigName)
	if canon == "template-v1" && (strings.Contains(lowerCfg, "template_v1_raw") || strings.Contains(lowerCfg, "template-v1-raw")) {
		return "template-v1-raw"
	}
	if canon != "" {
		return canon
	}
	return canonicalFormat(strings.ReplaceAll(displayConfigWithoutIndex(row.ConfigName), " ", "-"))
}

func collectionLifecycleSeriesIdentity(row collectionRow) (key, label, order string) {
	family := collectionEngineFamily(row)
	rawVariant := displayConfigWithoutIndex(row.ConfigName)
	canonVariant := canonicalFormat(strings.ReplaceAll(rawVariant, " ", "-"))
	if canonVariant == "" {
		canonVariant = collectionEffectiveFormat(row)
	}
	if canonVariant == "native" {
		canonVariant = "native-columns"
	}

	variantLabel := displayLifecycleFormat(canonVariant)
	if family != "" {
		if variantLabel == "" {
			return family, family, collectionSeriesOrder(family, "")
		}
		return family + "\x00" + canonVariant, family + " " + variantLabel, collectionSeriesOrder(family, canonVariant)
	}
	fallback := firstNonEmpty(rawVariant, row.ConfigName, row.Format)
	return fallback, fallback, "2_99_" + strings.ToLower(fallback)
}

func displayLifecycleFormat(canon string) string {
	switch canon {
	case "template-v1":
		return "Template V1"
	case "template-v1-raw":
		return "Template V1 Raw"
	case "bson":
		return "BSON"
	case "json":
		return "JSON"
	case "native-columns", "native":
		return "Native"
	case "":
		return ""
	default:
		return chartLabel(strings.ReplaceAll(canon, "-", " "))
	}
}

func displayConfigWithEngine(config, engine string, includeIndex bool) string {
	row := collectionRow{ConfigName: config, Engine: engine}
	family := collectionEngineFamily(row)
	base := displayConfigWithoutIndex(config)
	canon := canonicalFormat(strings.ReplaceAll(base, " ", "-"))
	variantLabel := displayLifecycleFormat(canon)
	if variantLabel == "" {
		variantLabel = base
	}
	label := variantLabel
	if family != "" {
		if label != "" {
			label = family + " " + label
		} else {
			label = family
		}
	}
	if includeIndex {
		if idx := collectionIndexFromConfig(config); idx >= 0 {
			label += fmt.Sprintf(" / i%d", idx)
		}
	}
	if strings.TrimSpace(label) == "" {
		return displayConfig(config)
	}
	return label
}

// niceLinearAxis computes a human-friendly rounded upper bound and tick list
// (e.g., 0, 100, 200, 300, 400 instead of 0, 107, 214, 321, 428).
func niceLinearAxis(maxVal float64, unit string) (float64, []float64) {
	if !isFinite(maxVal) || maxVal <= 0 {
		return 1, []float64{0, 0.25, 0.5, 0.75, 1}
	}
	if unit == "bytes" {
		scale := 1.0
		switch {
		case maxVal >= 1024*1024*1024:
			scale = 1024 * 1024 * 1024
		case maxVal >= 1024*1024:
			scale = 1024 * 1024
		case maxVal >= 1024:
			scale = 1024
		}
		scaledMax, scaledTicks := niceDecimalAxis(maxVal / scale)
		ticks := make([]float64, len(scaledTicks))
		for i, t := range scaledTicks {
			ticks[i] = t * scale
		}
		return scaledMax * scale, ticks
	}
	return niceDecimalAxis(maxVal)
}

func niceDecimalAxis(maxVal float64) (float64, []float64) {
	if !isFinite(maxVal) || maxVal <= 0 {
		return 1, []float64{0, 0.25, 0.5, 0.75, 1}
	}
	rawStep := maxVal / 5.0
	mag := math.Pow(10, math.Floor(math.Log10(rawStep)))
	multipliers := []float64{1, 2, 2.5, 5, 10, 20}
	step := mag
	nSteps := 5
	for _, m := range multipliers {
		cand := m * mag
		n := int(math.Ceil(maxVal/cand - 1e-12))
		if n < 1 {
			n = 1
		}
		if n <= 5 {
			step = cand
			nSteps = n
			break
		}
	}
	// If maxVal sits within 2% of the top tick, leave a clean step only when nSteps < 4;
	// otherwise keep the crisp rounded boundary (e.g., 397 -> 400).
	axisMax := float64(nSteps) * step
	ticks := make([]float64, 0, nSteps+1)
	for i := 0; i <= nSteps; i++ {
		ticks = append(ticks, float64(i)*step)
	}
	return axisMax, ticks
}

func seriesPositiveRange(series []chartSeries) (minPos, maxPos float64, ok bool) {
	for _, s := range series {
		for _, v := range s.Values {
			if !isFinite(v) || v <= 0 {
				continue
			}
			if !ok {
				minPos = v
				maxPos = v
				ok = true
				continue
			}
			if v < minPos {
				minPos = v
			}
			if v > maxPos {
				maxPos = v
			}
		}
	}
	return minPos, maxPos, ok
}

func supportsLogScale(series []chartSeries) bool {
	minPos, maxPos, ok := seriesPositiveRange(series)
	return ok && minPos > 0 && maxPos/minPos >= 4.0
}

// niceLogAxis computes clean Log10 bounds and tick positions covering [minPos, maxPos]
// with lower headroom so even minPos has a substantial visible bar height.
func niceLogAxis(minPos, maxPos float64, unit string) (logMin, logMax float64, ticks []float64) {
	if !isFinite(minPos) || !isFinite(maxPos) || minPos <= 0 || maxPos <= minPos {
		return 1, 10, []float64{1, 2, 5, 10}
	}
	scale := 1.0
	if unit == "bytes" && maxPos/minPos < 1024 {
		switch {
		case minPos >= 1024*1024*1024:
			scale = 1024 * 1024 * 1024
		case minPos >= 1024*1024:
			scale = 1024 * 1024
		case minPos >= 1024:
			scale = 1024
		}
	}
	sMin := minPos / scale
	sMax := maxPos / scale

	// Pick largest {1, 2, 5}*10^k <= sMin*0.85 for lower bound.
	targetLow := sMin * 0.85
	lowExp := int(math.Floor(math.Log10(targetLow)))
	logMinScaled := math.Pow(10, float64(lowExp))
	for _, m := range []float64{5, 2, 1} {
		cand := m * math.Pow(10, float64(lowExp))
		if cand <= targetLow {
			logMinScaled = cand
			break
		}
	}
	if logMinScaled > targetLow {
		logMinScaled = 5 * math.Pow(10, float64(lowExp-1))
	}

	// Pick smallest {1, 2, 5}*10^k >= sMax*1.05 for upper bound.
	targetHigh := sMax * 1.05
	highExp := int(math.Floor(math.Log10(targetHigh)))
	logMaxScaled := math.Pow(10, float64(highExp+1))
	for _, m := range []float64{1, 2, 5, 10} {
		cand := m * math.Pow(10, float64(highExp))
		if cand >= targetHigh {
			logMaxScaled = cand
			break
		}
	}

	decades := math.Log10(logMaxScaled / logMinScaled)
	var mults []float64
	switch {
	case decades <= 2.4:
		mults = []float64{1, 2, 5}
	case decades <= 3.6:
		mults = []float64{1, 5}
	default:
		mults = []float64{1}
	}

	seen := make(map[string]bool)
	addTick := func(v float64) {
		if v < logMinScaled*(1-1e-9) || v > logMaxScaled*(1+1e-9) {
			return
		}
		k := fmt.Sprintf("%.6g", v)
		if !seen[k] {
			seen[k] = true
			ticks = append(ticks, v*scale)
		}
	}
	addTick(logMinScaled)
	minE := int(math.Floor(math.Log10(logMinScaled))) - 1
	maxE := int(math.Ceil(math.Log10(logMaxScaled))) + 1
	for e := minE; e <= maxE; e++ {
		base := math.Pow(10, float64(e))
		for _, m := range mults {
			addTick(m * base)
		}
	}
	addTick(logMaxScaled)
	sort.Float64s(ticks)
	return logMinScaled * scale, logMaxScaled * scale, ticks
}

func formatAxisTick(v float64, unit string) string {
	if math.Abs(v) < 1e-12 {
		return "0"
	}
	if unit == "bytes" {
		return formatCompactBytes(v, true)
	}
	if unit == "ratio" {
		return trimTrailingZeros(fmt.Sprintf("%.2f", v)) + "x"
	}
	abs := math.Abs(v)
	switch {
	case abs >= 1_000_000_000:
		return trimTrailingZeros(fmt.Sprintf("%.2f", v/1_000_000_000)) + "B"
	case abs >= 1_000_000:
		return trimTrailingZeros(fmt.Sprintf("%.2f", v/1_000_000)) + "M"
	case abs >= 1_000:
		return trimTrailingZeros(fmt.Sprintf("%.1f", v/1_000)) + "K"
	case abs >= 10:
		return trimTrailingZeros(fmt.Sprintf("%.1f", v))
	default:
		return trimTrailingZeros(fmt.Sprintf("%.2f", v))
	}
}

func formatBarLabelValue(v float64, unit string) string {
	if !isFinite(v) || v <= 0 {
		return ""
	}
	switch unit {
	case "bytes":
		return formatCompactBytes(v, false)
	case "ratio":
		return fmt.Sprintf("%.2fx", v)
	case "B/doc":
		if v < 100 {
			return fmt.Sprintf("%.1f", v)
		}
		return fmt.Sprintf("%.0f", v)
	default:
		abs := math.Abs(v)
		switch {
		case abs >= 1_000_000_000:
			return trimTrailingZeros(fmt.Sprintf("%.2f", v/1_000_000_000)) + "B"
		case abs >= 1_000_000:
			return trimTrailingZeros(fmt.Sprintf("%.2f", v/1_000_000)) + "M"
		case abs >= 10_000:
			return trimTrailingZeros(fmt.Sprintf("%.0f", v/1_000)) + "K"
		case abs >= 1_000:
			return trimTrailingZeros(fmt.Sprintf("%.1f", v/1_000)) + "K"
		case abs >= 100:
			return fmt.Sprintf("%.0f", v)
		default:
			return trimTrailingZeros(fmt.Sprintf("%.1f", v))
		}
	}
}

func formatExactChartValue(v float64, unit string) string {
	if !isFinite(v) || v <= 0 {
		return "-"
	}
	switch unit {
	case "bytes":
		exactBytes := int64(math.Round(v))
		if exactBytes < 1024 {
			return commaInt(exactBytes) + " B"
		}
		return commaInt(exactBytes) + " B (" + fmtBytes(float64(exactBytes)) + ")"
	case "ratio":
		return fmt.Sprintf("%.2fx", v)
	case "B/doc":
		return commaFloat(v, 1) + " B/doc"
	case "docs/sec", "ops/sec":
		return commaInt(int64(math.Round(v))) + " " + unit
	default:
		if math.Abs(v-math.Round(v)) < 1e-6 {
			return commaInt(int64(math.Round(v))) + " " + unit
		}
		return commaFloat(v, 2) + " " + unit
	}
}

func formatCompactBytes(v float64, spaced bool) string {
	sep := ""
	if spaced {
		sep = " "
	}
	switch {
	case v >= 1024*1024*1024:
		u := "G"
		if spaced {
			u = "GiB"
		}
		return trimTrailingZeros(fmt.Sprintf("%.1f", v/(1024*1024*1024))) + sep + u
	case v >= 1024*1024:
		u := "M"
		if spaced {
			u = "MiB"
		}
		return trimTrailingZeros(fmt.Sprintf("%.1f", v/(1024*1024))) + sep + u
	case v >= 1024:
		u := "K"
		if spaced {
			u = "KiB"
		}
		return trimTrailingZeros(fmt.Sprintf("%.1f", v/1024)) + sep + u
	default:
		return fmt.Sprintf("%.0f%sB", v, sep)
	}
}

func trimTrailingZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}

const minVisibleBarHeight = 3.0

func barGeometry(v, axisY, top, plotH float64, yFn func(float64) float64) (yy, barH float64) {
	yy = yFn(v)
	if yy < top {
		yy = top
	}
	barH = axisY - yy
	if barH < minVisibleBarHeight {
		barH = minVisibleBarHeight
		yy = axisY - barH
	}
	return yy, barH
}

func writeBarValueLabel(b *strings.Builder, x, yy, barW, top float64, label string, seriesIdx int) {
	if label == "" {
		return
	}
	cx := x + barW/2
	if barW >= 26 {
		ly := math.Max(12.0, yy-5.0)
		b.WriteString(fmt.Sprintf("<text class=\"bar-val\" data-series=\"%d\" x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", seriesIdx, cx, ly, esc(label)))
		return
	}
	fontSize := "9.5"
	if barW < 10 {
		fontSize = "8.5"
	}
	ly := math.Max(top-4.0, yy-4.0)
	// Anchor right above the bar; when bar is shorter than top, keep label directly above bar top.
	if yy > top {
		ly = yy - 4.0
	}
	b.WriteString(fmt.Sprintf("<text class=\"bar-val\" data-series=\"%d\" transform=\"translate(%.1f %.1f) rotate(-90)\" text-anchor=\"start\" dominant-baseline=\"central\" font-size=\"%s\" font-weight=\"700\" fill=\"#344256\">%s</text>", seriesIdx, cx, ly, fontSize, esc(label)))
}

func writeYAxisTicks(b *strings.Builder, ticks []float64, left, plotW float64, unit, fontSize string, yFn func(float64) float64) {
	lastY := math.Inf(1)
	for _, value := range ticks {
		yy := yFn(value)
		b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#edf2f7\"/>", left, yy, left+plotW, yy))
		// Avoid overlapping adjacent log tick labels when tightly spaced.
		if math.Abs(lastY-yy) < 15.0 {
			continue
		}
		lastY = yy
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"end\" font-size=\"%s\" font-weight=\"700\" fill=\"#344256\">%s</text>", left-7, yy+4, fontSize, esc(formatAxisTick(value, unit))))
	}
}

func chartHeaderWithScale(title string, series []chartSeries, hasLog bool) string {
	var b strings.Builder
	b.WriteString("<div class=\"chart\" data-scale=\"linear\">")
	b.WriteString("<div class=\"chart-head\">")
	b.WriteString("<div class=\"chart-head-top\">")
	b.WriteString("<div class=\"chart-title\">" + esc(chartLabel(title)) + "</div>")
	if hasLog {
		b.WriteString("<div class=\"chart-controls\"><div class=\"scale-toggle\" role=\"group\" aria-label=\"Y-axis scale\">")
		b.WriteString("<button type=\"button\" class=\"scale-btn active\" data-scale-target=\"linear\" aria-pressed=\"true\">Linear</button>")
		b.WriteString("<button type=\"button\" class=\"scale-btn\" data-scale-target=\"log\" aria-pressed=\"false\">Log</button>")
		b.WriteString("</div></div>")
	}
	b.WriteString("</div>")
	b.WriteString(legend(series))
	b.WriteString("</div>")
	return b.String()
}

type chartMetaPayload struct {
	XAxisLabel string            `json:"xAxisLabel"`
	Unit       string            `json:"unit"`
	Categories []string          `json:"categories"`
	Series     []chartMetaSeries `json:"series"`
}

type chartMetaSeries struct {
	Name      string     `json:"name"`
	Color     string     `json:"color"`
	Values    []*float64 `json:"values"`
	Formatted []string   `json:"formatted"`
}

func chartMetaScript(xAxisLabel string, categories []string, series []chartSeries, unit string) string {
	payload := chartMetaPayload{
		XAxisLabel: chartLabel(xAxisLabel),
		Unit:       unit,
		Categories: make([]string, len(categories)),
		Series:     make([]chartMetaSeries, len(series)),
	}
	for i, cat := range categories {
		payload.Categories[i] = chartLabel(cat)
	}
	for i, s := range series {
		vals := make([]*float64, len(categories))
		fmts := make([]string, len(categories))
		for c := range categories {
			if c < len(s.Values) && isFinite(s.Values[c]) && s.Values[c] > 0 {
				v := s.Values[c]
				vals[c] = &v
				fmts[c] = formatExactChartValue(v, unit)
			} else {
				fmts[c] = "-"
			}
		}
		payload.Series[i] = chartMetaSeries{
			Name:      chartLabel(s.Name),
			Color:     s.Color,
			Values:    vals,
			Formatted: fmts,
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return "<script type=\"application/json\" class=\"chart-meta\">" + string(raw) + "</script>"
}

func chartExactValuesDetails(xAxisLabel string, categories []string, series []chartSeries, unit string) string {
	if len(categories) == 0 || len(series) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<details class=\"chart-values\"><summary>Exact values</summary>")
	b.WriteString("<div class=\"table-wrap mini-table-wrap\"><table class=\"mini-table\"><thead><tr>")
	b.WriteString("<th>" + esc(chartLabel(xAxisLabel)) + "</th>")
	for _, s := range series {
		b.WriteString("<th class=\"num\"><span class=\"swatch\" style=\"background:" + esc(s.Color) + "\"></span>" + esc(chartLabel(s.Name)) + "</th>")
	}
	b.WriteString("</tr></thead><tbody>")
	for c, cat := range categories {
		b.WriteString("<tr><td>" + esc(chartLabel(cat)) + "</td>")
		for _, s := range series {
			cell := "-"
			if c < len(s.Values) && isFinite(s.Values[c]) && s.Values[c] > 0 {
				cell = formatExactChartValue(s.Values[c], unit)
			}
			b.WriteString("<td class=\"num\">" + esc(cell) + "</td>")
		}
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table></div></details>")
	return b.String()
}

type benchmarkContextPill struct {
	Label string
	Value string
	Tone  string // "durable", "relaxed", "fast", "neutral"
}

func collectBenchmarkContext(data reportData) []benchmarkContextPill {
	var pills []benchmarkContextPill
	add := func(label, value, tone string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		pills = append(pills, benchmarkContextPill{Label: label, Value: value, Tone: tone})
	}

	meta := data.RunMetadata
	metaVal := func(keys ...string) string {
		if len(meta) == 0 {
			return ""
		}
		for _, k := range keys {
			if v := strings.TrimSpace(meta[k]); v != "" {
				return v
			}
		}
		return ""
	}

	if tier := metaVal("tier"); tier != "" {
		add("Tier", tier, "neutral")
	}

	durabilityTier := metaVal("durability_tier")
	if durabilityTier != "" && !strings.EqualFold(durabilityTier, "default") {
		add("Durability tier", durabilityTier, durabilityTone(durabilityTier))
	}

	openMode := metaVal("mongo_treedb_open_mode", "treedb_open_mode")
	if openMode == "" {
		openMode = observedLoadModeField(data.MongoLoadModes, func(r loadModeRow) string { return r.TreeDBOpenMode })
	}
	add("TreeDB open mode", openMode, "neutral")

	durability := metaVal("treedb_durability_profile", "mongo_treedb_profile", "treedb_profile")
	collProfile := metaVal("collection_treedb_profile")
	if durability == "" && collProfile != "" {
		durability = collProfile
	}
	if durability == "" || strings.EqualFold(durability, "default") {
		if obs := observedTreeDBProfile(data); obs != "" {
			if durability == "" {
				durability = obs
			} else if !strings.EqualFold(obs, durability) {
				durability = durability + " (" + obs + ")"
			}
		}
	}
	if durability != "" {
		add("TreeDB durability", durability, durabilityTone(durability))
	}
	if collProfile != "" && !strings.EqualFold(collProfile, "default") && !strings.EqualFold(collProfile, durability) && !strings.Contains(strings.ToLower(durability), strings.ToLower(collProfile)) {
		add("TreeDB collections", collProfile, durabilityTone(collProfile))
	}

	if memtable := metaVal("mongo_treedb_memtable_mode", "treedb_memtable_mode"); memtable != "" {
		add("TreeDB memtable", memtable, "neutral")
	}

	mongoImg := metaVal("mongo_image")
	mongoMode := metaVal("mongo_mode")
	if mongoImg != "" {
		val := mongoImg
		if mongoMode != "" {
			val += " (" + mongoMode + ")"
		}
		add("MongoDB", val, "neutral")
	} else if mongoMode != "" {
		add("MongoDB mode", mongoMode, "neutral")
	}

	writeConcern := metaVal("mongo_write_concern", "write_concern")
	if writeConcern == "" || strings.EqualFold(writeConcern, "default") {
		if obsWC := observedLoadModeField(data.MongoLoadModes, func(r loadModeRow) string { return r.WriteConcern }); obsWC != "" {
			writeConcern = obsWC
		}
	}
	if writeConcern != "" {
		formattedWC := formatMongoWriteConcernContext(writeConcern, durabilityTier)
		add("MongoDB write concern", formattedWC, durabilityTone(formattedWC))
	}

	sqliteMode := formatSQLiteDurabilityContext(meta, data.Collections)
	if sqliteMode != "" {
		add("SQLite journal/sync", sqliteMode, durabilityTone(sqliteMode))
	}

	return pills
}

func formatMongoWriteConcernContext(raw, durabilityTier string) string {
	trimmed := strings.TrimSpace(raw)
	switch strings.ToLower(trimmed) {
	case "":
		return ""
	case "default":
		return "w:1 (default)"
	case "w0", "unacknowledged":
		return "w:0 (unacknowledged)"
	case "w1":
		if strings.EqualFold(durabilityTier, "fast") {
			return "w:1, j:false (Mongo 8 fastest ack; --nojournal removed)"
		}
		return "w:1, j:false (w1)"
	case "w1_j", "journal":
		return "w:1, j:true (w1_j)"
	case "majority_j":
		return "w:majority, j:true (majority_j)"
	default:
		return trimmed
	}
}

func durabilityContextNote(data reportData) string {
	if len(data.RunMetadata) > 0 {
		if note := strings.TrimSpace(data.RunMetadata["durability_note"]); note != "" && !strings.EqualFold(note, "none") {
			return note
		}
		if strings.EqualFold(strings.TrimSpace(data.RunMetadata["durability_tier"]), "fast") {
			return "Fast tier note: MongoDB 8 removed --nojournal, so w:1 (j:false) is MongoDB 8's fastest acknowledged write mode."
		}
	}
	return ""
}

func observedLoadModeField(rows []loadModeRow, get func(loadModeRow) string) string {
	seen := make(map[string]bool)
	var vals []string
	for _, r := range rows {
		v := strings.TrimSpace(get(r))
		if v != "" && !seen[v] {
			seen[v] = true
			vals = append(vals, v)
		}
	}
	sort.Strings(vals)
	return strings.Join(vals, ", ")
}

func observedTreeDBProfile(data reportData) string {
	seen := make(map[string]bool)
	var vals []string
	record := func(v string) {
		v = strings.TrimSpace(v)
		if v == "" || seen[v] {
			return
		}
		seen[v] = true
		vals = append(vals, v)
	}
	for _, p := range data.Profiles.Mongo {
		record(p.TreeDBProfile)
	}
	for _, r := range data.MongoLoadModes {
		record(r.TreeDBProfile)
	}
	for _, r := range data.Collections {
		if collectionEngineFamily(r) == "TreeDB" {
			eng := strings.TrimSpace(r.Engine)
			if strings.Contains(eng, "wal") || strings.Contains(eng, "durable") || strings.Contains(eng, "fast") || strings.Contains(eng, "unsafe") {
				record(eng)
			}
		}
	}
	for _, r := range data.RawEngine {
		record(r.Profile)
	}
	sort.Strings(vals)
	return strings.Join(vals, ", ")
}

func formatSQLiteDurabilityContext(meta runMetadata, rows []collectionRow) string {
	if len(meta) > 0 {
		jm := strings.TrimSpace(meta["sqlite_journal_mode"])
		sm := strings.TrimSpace(meta["sqlite_synchronous"])
		if jm != "" && sm != "" {
			return strings.ToUpper(jm) + " / " + strings.ToUpper(sm)
		}
		if mode := strings.TrimSpace(meta["sqlite_mode"]); mode != "" {
			return mode
		}
		if jm != "" {
			return strings.ToUpper(jm)
		}
	}
	seen := make(map[string]bool)
	var modes []string
	for _, row := range rows {
		if collectionEngineFamily(row) != "SQLite" {
			continue
		}
		var desc string
		jm := strings.TrimSpace(row.Extra["sqlite_journal_mode"])
		sm := strings.TrimSpace(row.Extra["sqlite_synchronous"])
		if jm != "" && sm != "" {
			desc = strings.ToUpper(jm) + " / " + strings.ToUpper(sm)
		} else {
			eng := strings.TrimSpace(row.Engine)
			switch strings.ToLower(eng) {
			case "sqlite_wal_normal":
				desc = "WAL / NORMAL (sqlite_wal_normal)"
			case "sqlite_wal_full":
				desc = "WAL / FULL (sqlite_wal_full)"
			case "sqlite_wal_off":
				desc = "WAL / OFF (sqlite_wal_off)"
			case "sqlite_delete_full":
				desc = "DELETE / FULL (sqlite_delete_full)"
			case "sqlite_delete_normal":
				desc = "DELETE / NORMAL (sqlite_delete_normal)"
			default:
				desc = eng
			}
		}
		if desc != "" && !seen[desc] {
			seen[desc] = true
			modes = append(modes, desc)
		}
	}
	sort.Strings(modes)
	return strings.Join(modes, ", ")
}

func durabilityTone(val string) string {
	lower := strings.ToLower(strings.TrimSpace(val))
	switch {
	case strings.Contains(lower, "durable") || strings.Contains(lower, "full") || strings.Contains(lower, "j:true") || strings.Contains(lower, "journal=true") || strings.Contains(lower, "w1_j") || strings.Contains(lower, "majority_j") || lower == "journal":
		return "durable"
	case strings.Contains(lower, "no_wal") || strings.Contains(lower, "unsafe") || strings.Contains(lower, "off") || lower == "fast" || lower == "w0" || strings.Contains(lower, "w:0") || strings.Contains(lower, "unacknowledged"):
		return "fast"
	case strings.Contains(lower, "relaxed") || strings.Contains(lower, "wal") || strings.Contains(lower, "normal") || strings.Contains(lower, "w:1") || strings.Contains(lower, "w=1") || lower == "w1":
		return "relaxed"
	default:
		return "neutral"
	}
}

func renderBenchmarkContextBanner(b *strings.Builder, data reportData) {
	pills := collectBenchmarkContext(data)
	note := durabilityContextNote(data)
	if len(pills) == 0 && note == "" {
		return
	}
	b.WriteString("<div class=\"context-banner\" aria-label=\"Benchmark Configuration and Durability Contract\">")
	b.WriteString("<span class=\"context-heading\">Benchmark Configuration &amp; Durability Contract</span>")
	b.WriteString("<div class=\"context-pills\">")
	for _, pill := range pills {
		toneClass := "tone-" + pill.Tone
		b.WriteString("<span class=\"context-pill " + esc(toneClass) + "\"><span class=\"context-label\">" + esc(pill.Label) + ":</span> <strong>" + esc(pill.Value) + "</strong></span>")
	}
	b.WriteString("</div>")
	if note != "" {
		b.WriteString("<div class=\"context-note\" style=\"margin-top:6px;font-size:12px;color:var(--muted);\">" + esc(note) + "</div>")
	}
	b.WriteString("</div>")
}

func parseWriteConcernRaw(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var asStr string
	if err := json.Unmarshal(raw, &asStr); err == nil {
		return strings.TrimSpace(asStr)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err == nil {
		var parts []string
		if w, ok := obj["w"]; ok && w != nil {
			parts = append(parts, fmt.Sprintf("w:%v", w))
		}
		if j, ok := obj["j"]; ok && j != nil {
			parts = append(parts, fmt.Sprintf("j:%v", j))
		} else if jr, ok := obj["journal_requested"]; ok {
			if jr == nil {
				parts = append(parts, "j:unspecified")
			} else {
				parts = append(parts, fmt.Sprintf("j:%v", jr))
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, ", ")
		}
	}
	return ""
}

func reportInteractiveJS() string {
	return `<div id="chart-tooltip" class="chart-tooltip" role="tooltip" aria-hidden="true"></div>
<script>
(function() {
  var tip = document.getElementById('chart-tooltip');
  if (!tip) return;

  function parseMeta(chart) {
    if (chart._meta !== undefined) return chart._meta;
    var el = chart.querySelector('script.chart-meta');
    if (!el) { chart._meta = null; return null; }
    try { chart._meta = JSON.parse(el.textContent || '{}'); }
    catch (e) { chart._meta = null; }
    return chart._meta;
  }

  function escHtml(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  function formatRatio(other, base) {
    if (!(other > 0) || !(base > 0)) return '';
    var r = other / base;
    if (r >= 100) return r.toFixed(0) + 'x';
    if (r >= 10) return r.toFixed(1) + 'x';
    return r.toFixed(2) + 'x';
  }

  var activeItem = null;

  function positionTipAt(x, y) {
    var pad = 14;
    var vw = window.innerWidth || document.documentElement.clientWidth;
    var vh = window.innerHeight || document.documentElement.clientHeight;
    var rect = tip.getBoundingClientRect();
    var left = x + pad;
    var top = y + pad;
    if (left + rect.width > vw - 8) left = Math.max(8, x - rect.width - pad);
    if (top + rect.height > vh - 8) top = Math.max(8, y - rect.height - pad);
    tip.style.left = left + 'px';
    tip.style.top = top + 'px';
  }

  function positionTipForElement(item) {
    var box = item.getBoundingClientRect();
    var vw = window.innerWidth || document.documentElement.clientWidth;
    var vh = window.innerHeight || document.documentElement.clientHeight;
    var rect = tip.getBoundingClientRect();
    var left = box.left + box.width / 2 - rect.width / 2;
    var top = box.top - rect.height - 10;
    if (top < 8) top = box.bottom + 10;
    if (left + rect.width > vw - 8) left = vw - rect.width - 8;
    if (left < 8) left = 8;
    if (top + rect.height > vh - 8) top = Math.max(8, vh - rect.height - 8);
    tip.style.left = left + 'px';
    tip.style.top = top + 'px';
  }

  function clearDescribedBy() {
    if (activeItem) {
      activeItem.removeAttribute('aria-describedby');
      activeItem = null;
    }
  }

  function showTip(item, ev) {
    var chart = item.closest('.chart');
    if (!chart) return;
    var meta = parseMeta(chart);
    if (!meta || !meta.series) return;
    var sIdx = parseInt(item.getAttribute('data-series'), 10);
    var cIdx = parseInt(item.getAttribute('data-cat'), 10);
    if (isNaN(sIdx) || isNaN(cIdx) || !meta.series[sIdx]) return;

    var hovered = meta.series[sIdx];
    var catName = (meta.categories && meta.categories[cIdx] !== undefined) ? meta.categories[cIdx] : '';
    var header = catName;
    if (meta.xAxisLabel && /^[0-9]+$/.test(catName)) {
      header = meta.xAxisLabel + ': ' + catName;
    }
    var val = hovered.values ? hovered.values[cIdx] : null;
    var fmt = hovered.formatted ? hovered.formatted[cIdx] : '-';

    var html = '<div class="tt-head">' + escHtml(header) + '</div>';
    html += '<div class="tt-row tt-primary">' +
      '<span class="tt-name"><span class="tt-swatch" style="background:' + escHtml(hovered.color) + '"></span>' + escHtml(hovered.name) + '</span>' +
      '<span class="tt-val">' + escHtml(fmt) + '</span></div>';

    var others = [];
    for (var i = 0; i < meta.series.length; i++) {
      if (i === sIdx) continue;
      var s = meta.series[i];
      var ov = s.values ? s.values[cIdx] : null;
      if (ov === null || ov === undefined) continue;
      var ratio = formatRatio(ov, val);
      others.push('<div class="tt-row">' +
        '<span class="tt-name"><span class="tt-swatch" style="background:' + escHtml(s.color) + '"></span>' + escHtml(s.name) + '</span>' +
        '<span class="tt-val">' + escHtml(s.formatted[cIdx]) + (ratio ? ' <span class="tt-ratio">(' + escHtml(ratio) + ')</span>' : '') + '</span></div>');
    }
    if (others.length > 0) {
      html += '<div class="tt-sep"></div>' + others.join('');
    }
    clearDescribedBy();
    activeItem = item;
    item.setAttribute('aria-describedby', 'chart-tooltip');
    tip.innerHTML = html;
    tip.classList.add('visible');
    tip.setAttribute('aria-hidden', 'false');
    if (ev && typeof ev.clientX === 'number' && typeof ev.clientY === 'number') {
      positionTipAt(ev.clientX, ev.clientY);
    } else {
      positionTipForElement(item);
    }
  }

  function hideTip() {
    clearDescribedBy();
    tip.classList.remove('visible');
    tip.setAttribute('aria-hidden', 'true');
  }

  document.addEventListener('pointerover', function(ev) {
    var item = ev.target && ev.target.closest ? ev.target.closest('.chart-item') : null;
    if (item) showTip(item, ev);
  });
  document.addEventListener('pointermove', function(ev) {
    if (tip.classList.contains('visible') && (!activeItem || document.activeElement !== activeItem)) {
      positionTipAt(ev.clientX, ev.clientY);
    }
  });
  document.addEventListener('pointerout', function(ev) {
    var item = ev.target && ev.target.closest ? ev.target.closest('.chart-item') : null;
    if (item && document.activeElement !== item) hideTip();
  });
  document.addEventListener('focusin', function(ev) {
    var item = ev.target && ev.target.closest ? ev.target.closest('.chart-item') : null;
    if (item) showTip(item, null);
  });
  document.addEventListener('focusout', function(ev) {
    var item = ev.target && ev.target.closest ? ev.target.closest('.chart-item') : null;
    if (item) hideTip();
  });
  document.addEventListener('keydown', function(ev) {
    if (ev.key === 'Escape' && tip.classList.contains('visible')) {
      hideTip();
    }
  });

  // Scale toggle (Linear / Log)
  document.addEventListener('click', function(ev) {
    var btn = ev.target && ev.target.closest ? ev.target.closest('.scale-btn') : null;
    if (btn) {
      var chart = btn.closest('.chart');
      var target = btn.getAttribute('data-scale-target');
      if (!chart || !target) return;
      chart.setAttribute('data-scale', target);
      var buttons = chart.querySelectorAll('.scale-btn');
      for (var i = 0; i < buttons.length; i++) {
        var active = buttons[i].getAttribute('data-scale-target') === target;
        buttons[i].classList.toggle('active', active);
        buttons[i].setAttribute('aria-pressed', active ? 'true' : 'false');
      }
      return;
    }

    // Legend series toggle (Click to toggle; Shift+click to solo)
    var leg = ev.target && ev.target.closest ? ev.target.closest('.legend-item') : null;
    if (leg) {
      var chart = leg.closest('.chart');
      if (!chart) return;
      var sIdx = leg.getAttribute('data-series');
      var allBtns = chart.querySelectorAll('.legend-item');
      if (ev.shiftKey) {
        var alreadySolo = !leg.classList.contains('is-hidden');
        for (var i = 0; i < allBtns.length; i++) {
          if (allBtns[i] !== leg && !allBtns[i].classList.contains('is-hidden')) {
            alreadySolo = false;
          }
        }
        for (var i = 0; i < allBtns.length; i++) {
          var hide = alreadySolo ? false : (allBtns[i] !== leg);
          allBtns[i].classList.toggle('is-hidden', hide);
          allBtns[i].setAttribute('aria-pressed', hide ? 'false' : 'true');
        }
      } else {
        var nowHidden = !leg.classList.contains('is-hidden');
        leg.classList.toggle('is-hidden', nowHidden);
        leg.setAttribute('aria-pressed', nowHidden ? 'false' : 'true');
      }
      var hiddenMap = {};
      for (var i = 0; i < allBtns.length; i++) {
        if (allBtns[i].classList.contains('is-hidden')) {
          hiddenMap[allBtns[i].getAttribute('data-series')] = true;
        }
      }
      var els = chart.querySelectorAll('svg [data-series]');
      for (var j = 0; j < els.length; j++) {
        var sid = els[j].getAttribute('data-series');
        els[j].classList.toggle('series-hidden', !!hiddenMap[sid]);
      }
    }
  });
})();
</script>
`
}
