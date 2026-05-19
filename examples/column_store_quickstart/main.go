package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	collectionName = "activity_events"
	narrativeWidth = 96
)

type querySpec struct {
	Name        string
	Title       string
	KeyHeader   string
	ValueHeader string
	Limit       int
	Request     collections.ColumnPhysicalQueryRequest
}

type queryRun struct {
	Spec   querySpec
	Plan   collections.ColumnQueryPlan
	Result collections.ColumnPhysicalQueryResult
}

type activityEvent struct {
	TimeUS  int64  `json:"time_us"`
	Action  string `json:"action"`
	Actor   string `json:"actor"`
	Client  string `json:"client"`
	Subject string `json:"subject"`
}

func main() {
	var rows int
	var dir string
	var keep bool
	flag.IntVar(&rows, "rows", 48, "number of activity events to write")
	flag.StringVar(&dir, "dir", "", "TreeDB directory; empty uses a temporary directory")
	flag.BoolVar(&keep, "keep", false, "keep the temporary directory after the run")
	flag.Parse()

	if rows <= 0 {
		log.Fatal("-rows must be positive")
	}

	if dir == "" {
		tmp, err := os.MkdirTemp("", "gomap-column-store-quickstart-*")
		if err != nil {
			log.Fatal(err)
		}
		dir = tmp
		if !keep {
			defer func() { _ = os.RemoveAll(tmp) }()
		}
	} else if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Fatal(err)
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		log.Fatal(err)
	}

	events, ids, docs, err := buildEvents(rows)
	if err != nil {
		log.Fatal(err)
	}

	db, cleanup, err := treedb.OpenBackend(backendOptions(absDir))
	if err != nil {
		log.Fatal(err)
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(collectionMeta()); err != nil {
		_ = cleanup()
		log.Fatal(err)
	}
	collection, err := manager.OpenCollection(collectionName)
	if err != nil {
		_ = cleanup()
		log.Fatal(err)
	}
	if _, err := collection.InsertBatch(ids, docs); err != nil {
		_ = cleanup()
		log.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		_ = cleanup()
		log.Fatal(err)
	}
	if err := cleanup(); err != nil {
		log.Fatal(err)
	}

	reopened, reopenedCleanup, err := treedb.OpenBackend(backendOptions(absDir))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := reopenedCleanup(); err != nil {
			log.Printf("close TreeDB: %v", err)
		}
	}()
	reopenedCollection, err := collections.NewCollectionManager(reopened).OpenCollection(collectionName)
	if err != nil {
		log.Fatal(err)
	}

	printTitle("Activity event column store")
	printNarrative("Write a short activity stream as JSON, keep the hot dimensions in column lanes, then reopen it and scan those lanes for rollups.")
	fmt.Println()
	printBox("Dataset", keyValueLines([][2]string{
		{"db", absDir},
		{"collection", collectionName},
		{"rows ingested", fmt.Sprintf("%d", len(events))},
		{"column lanes", "time_us, action, actor"},
		{"retained JSON", "client, subject"},
	}))

	sampleID := ids[min(7, len(ids)-1)]
	reconstructed, err := reopenedCollection.Get(sampleID)
	if err != nil {
		log.Fatal(err)
	}

	specs := []querySpec{
		{
			Name:        "events_by_action",
			Title:       "Events by Action",
			KeyHeader:   "action",
			ValueHeader: "events",
			Request: collections.ColumnPhysicalQueryRequest{
				Kind:        collections.ColumnPhysicalQueryGroupCount,
				GroupColumn: "action",
			},
		},
		{
			Name:        "active_window_by_actor",
			Title:       "Active Window by Actor",
			KeyHeader:   "actor",
			ValueHeader: "span_us",
			Limit:       8,
			Request: collections.ColumnPhysicalQueryRequest{
				Kind:        collections.ColumnPhysicalQueryGroupInt64Span,
				GroupColumn: "actor",
				ValueColumn: "time_us",
			},
		},
	}
	runs := make([]queryRun, 0, len(specs))
	for _, spec := range specs {
		run, err := runPlannedPhysicalQuery(reopenedCollection, spec, rows)
		if err != nil {
			log.Fatal(err)
		}
		runs = append(runs, run)
	}

	var sample activityEvent
	if err := json.Unmarshal(reconstructed, &sample); err != nil {
		log.Fatal(err)
	}

	fmt.Println()
	printTitle("Primary read after reopen")
	printNarrative("The row is still fetched by id as a complete JSON document; the columnized fields are stitched back into the document.")
	fmt.Println()
	printBox("Reconstructed row", keyValueLines([][2]string{
		{"id", string(sampleID)},
		{"time_us", fmt.Sprintf("%d", sample.TimeUS)},
		{"action", sample.Action},
		{"actor", sample.Actor},
		{"client", sample.Client},
		{"subject", sample.Subject},
	}))
	printQuerySummary(runs)
	for _, run := range runs {
		printGroupTable(run)
	}
}

func backendOptions(dir string) treedb.Options {
	opts := treedb.OptionsFor(treedb.ProfileDurable, dir)
	opts.CommandWAL = true
	opts.CommandWALStatsScan = true
	opts.DisableBackgroundPrune = true
	return opts
}

func collectionMeta() *collections.CollectionMeta {
	return &collections.CollectionMeta{
		Name: collectionName,
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore: &collections.ColumnStoreConfig{
				Enabled: true,
				Columns: []collections.ColumnStoreColumn{
					{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
					{Name: "action", Path: "action", ValueType: collections.ColumnStoreValueString, Dictionary: true},
					{Name: "actor", Path: "actor", ValueType: collections.ColumnStoreValueString, Dictionary: true},
				},
				SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
				AggregateMetadata: []collections.ColumnAggregateMetadata{
					{Name: "min_time_us", Column: "time_us", Kind: collections.ColumnAggregateMin},
					{Name: "max_time_us", Column: "time_us", Kind: collections.ColumnAggregateMax},
				},
				RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
				Reconstruction:  collections.ColumnReconstructionRetainedPayloadAndColumns,
				ProfileSupport:  collections.ColumnStoreProfileDurableOnly,
			},
		},
	}
}

func buildEvents(rows int) ([]activityEvent, [][]byte, [][]byte, error) {
	const baseTimeUS = int64(1_700_000_000_000_000)
	actors := []string{
		"did:plc:alice",
		"did:plc:bruno",
		"did:plc:chandra",
		"did:plc:daria",
		"did:plc:eli",
		"did:plc:fran",
		"did:plc:gita",
		"did:plc:hugo",
		"did:plc:ines",
	}
	clients := []string{"web", "ios", "android"}
	subjects := []string{
		"feed:launch-notes",
		"feed:storage-updates",
		"feed:index-health",
		"feed:ops-review",
	}

	events := make([]activityEvent, rows)
	ids := make([][]byte, rows)
	docs := make([][]byte, rows)
	for i := 0; i < rows; i++ {
		event := activityEvent{
			TimeUS:  baseTimeUS + int64(i*17)*1_000_000 + int64((i%5)*1000),
			Action:  actionFor(i),
			Actor:   actors[(i*5+i/3)%len(actors)],
			Client:  clients[i%len(clients)],
			Subject: subjects[(i*7)%len(subjects)],
		}
		doc, err := json.Marshal(event)
		if err != nil {
			return nil, nil, nil, err
		}
		events[i] = event
		ids[i] = []byte(fmt.Sprintf("activity_%04d", i))
		docs[i] = doc
	}
	return events, ids, docs, nil
}

func actionFor(i int) string {
	switch {
	case i%6 == 0:
		return "graph.follow"
	case i%5 == 0:
		return "post.reposted"
	case i%2 == 0:
		return "post.liked"
	default:
		return "post.created"
	}
}

func runPlannedPhysicalQuery(collection *collections.Collection, spec querySpec, rows int) (queryRun, error) {
	plan, err := collection.PlanColumnQuery(collections.ColumnQueryPlanRequest{
		Name:             spec.Name,
		ProjectedColumns: projectedColumns(spec.Request),
		EstimatedRows:    rows,
		ForceKind:        collections.ColumnQueryPlanSerialColumnScan,
	})
	if err != nil {
		return queryRun{}, err
	}
	if !plan.Supported {
		return queryRun{}, fmt.Errorf("serial column scan unsupported for %s: %s", spec.Name, plan.Diagnostics.UnsupportedPlanReason)
	}
	result, err := collection.RunColumnPhysicalQuery(spec.Request)
	if err != nil {
		return queryRun{}, err
	}
	return queryRun{Spec: spec, Plan: plan, Result: result}, nil
}

func projectedColumns(req collections.ColumnPhysicalQueryRequest) []string {
	seen := make(map[string]struct{}, 3)
	out := make([]string, 0, 3)
	for _, column := range []string{req.GroupColumn, req.ValueColumn, req.DistinctColumn} {
		if column == "" {
			continue
		}
		if _, ok := seen[column]; ok {
			continue
		}
		seen[column] = struct{}{}
		out = append(out, column)
	}
	return out
}

func printQuerySummary(runs []queryRun) {
	fmt.Println()
	printTitle("Column scans over the same rows")
	printNarrative("Both scans read the physical column lanes directly, so the rollups avoid reconstructing every JSON document.")
	printNarrative("The plan is forced to serial_column_scan so the physical column path is explicit.")
	fmt.Println()

	rows := make([][]string, 0, len(runs))
	for _, run := range runs {
		diag := run.Result.Diagnostics
		rows = append(rows, []string{
			run.Spec.Name,
			string(run.Plan.Kind),
			fmt.Sprintf("%d", diag.RowsScanned),
			fmt.Sprintf("%d", diag.ResultGroups),
			fmt.Sprintf("%d", diag.PhysicalBytesScanned),
			fmt.Sprintf("%.1f", mibPerSecond(diag.PhysicalBytesScanned, diag.ScanNanos)),
			fmt.Sprintf("%d", diag.RowMaterializations),
			fmt.Sprintf("%d", diag.ManifestGeneration),
		})
	}
	printBoxedTable("Scan plan and counters", []string{"query", "plan", "rows", "groups", "bytes", "MiB/s", "JSON rows", "manifest"}, rows)
}

func printGroupTable(run queryRun) {
	groups := append([]collections.ColumnPhysicalQueryGroup(nil), run.Result.Groups...)
	sort.Slice(groups, func(i, j int) bool { return groups[i].Key < groups[j].Key })
	limit := run.Spec.Limit
	if limit <= 0 || limit > len(groups) {
		limit = len(groups)
	}

	fmt.Println()
	switch run.Spec.Name {
	case "events_by_action":
		printTitle("What activity is in this feed slice?")
		printNarrative("The first scan groups on the dictionary-coded action lane.")
		fmt.Println()
	case "active_window_by_actor":
		printTitle("How spread out is each actor's activity?")
		printNarrative("The second scan groups on actor and computes max(time_us)-min(time_us) for each actor.")
		fmt.Println()
	default:
		printTitle(run.Spec.Title)
		fmt.Println()
	}

	rows := make([][]string, 0, limit)
	for i := 0; i < limit; i++ {
		rows = append(rows, []string{
			groups[i].Key,
			fmt.Sprintf("%d", queryGroupValue(groups[i], run.Spec.Request.Kind)),
		})
	}
	printBoxedTable(run.Spec.Title, []string{run.Spec.KeyHeader, run.Spec.ValueHeader}, rows)
	if limit < len(groups) {
		remaining := len(groups) - limit
		fmt.Printf("%s %d more %s present in this slice.\n", bold("Note:"), remaining, plural("group is", "groups are", remaining))
	}
}

func plural(singular, plural string, count int) string {
	if count == 1 {
		return singular
	}
	return plural
}

func printTitle(title string) {
	fmt.Println(bold(title))
}

func printNarrative(text string) {
	for _, line := range wrapText(text, narrativeWidth) {
		fmt.Println(line)
	}
}

func wrapText(text string, width int) []string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return []string{""}
	}

	lines := make([]string, 0, 2)
	line := words[0]
	for _, word := range words[1:] {
		if visibleLen(line)+1+visibleLen(word) > width {
			lines = append(lines, line)
			line = word
			continue
		}
		line += " " + word
	}
	lines = append(lines, line)
	return lines
}

func keyValueLines(rows [][2]string) []string {
	keyWidth := 0
	for _, row := range rows {
		if width := len(row[0]) + 1; width > keyWidth {
			keyWidth = width
		}
	}

	lines := make([]string, 0, len(rows))
	for _, row := range rows {
		label := row[0] + ":"
		lines = append(lines, bold(label)+strings.Repeat(" ", keyWidth-len(label)+2)+row[1])
	}
	return lines
}

func printBoxedTable(title string, headers []string, rows [][]string) {
	widths := make([]int, len(headers))
	for i, header := range headers {
		widths[i] = visibleLen(header)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i >= len(widths) {
				break
			}
			if width := visibleLen(cell); width > widths[i] {
				widths[i] = width
			}
		}
	}

	separators := make([]string, len(widths))
	for i, width := range widths {
		separators[i] = strings.Repeat("─", width)
	}

	lines := make([]string, 0, len(rows)+2)
	lines = append(lines, formatTableRow(headers, widths, true))
	lines = append(lines, strings.Join(separators, "  "))
	for _, row := range rows {
		lines = append(lines, formatTableRow(row, widths, false))
	}
	printBox(title, lines)
}

func formatTableRow(cells []string, widths []int, header bool) string {
	out := make([]string, len(widths))
	for i := range widths {
		cell := ""
		if i < len(cells) {
			cell = cells[i]
		}
		if header {
			cell = bold(cell)
		}
		out[i] = padRight(cell, widths[i])
	}
	return strings.Join(out, "  ")
}

func printBox(title string, lines []string) {
	boxLines := lines
	if title != "" {
		boxLines = make([]string, 0, len(lines)+2)
		boxLines = append(boxLines, bold(title), "")
		boxLines = append(boxLines, lines...)
	}

	width := 0
	for _, line := range boxLines {
		if lineWidth := visibleLen(line); lineWidth > width {
			width = lineWidth
		}
	}

	fmt.Println(boxTop(width))
	for _, line := range boxLines {
		fmt.Printf("│  %s%s  │\n", line, strings.Repeat(" ", width-visibleLen(line)))
	}
	fmt.Println(boxBottom(width))
}

func boxTop(width int) string {
	return "╭" + strings.Repeat("─", width+4) + "╮"
}

func boxBottom(width int) string {
	return "╰" + strings.Repeat("─", width+4) + "╯"
}

func padRight(s string, width int) string {
	padding := width - visibleLen(s)
	if padding <= 0 {
		return s
	}
	return s + strings.Repeat(" ", padding)
}

func visibleLen(s string) int {
	width := 0
	inEscape := false
	for i := 0; i < len(s); i++ {
		switch {
		case !inEscape && s[i] == '\x1b' && i+1 < len(s) && s[i+1] == '[':
			inEscape = true
			i++
		case inEscape:
			if s[i] >= '@' && s[i] <= '~' {
				inEscape = false
			}
		default:
			_, size := utf8.DecodeRuneInString(s[i:])
			if size > 1 {
				i += size - 1
			}
			width++
		}
	}
	return width
}

func bold(s string) string {
	if os.Getenv("NO_COLOR") != "" {
		return s
	}
	return "\x1b[1m" + s + "\x1b[0m"
}

func queryGroupValue(group collections.ColumnPhysicalQueryGroup, kind collections.ColumnPhysicalQueryKind) int64 {
	switch kind {
	case collections.ColumnPhysicalQueryGroupCount,
		collections.ColumnPhysicalQueryGroupCountDistinct,
		collections.ColumnPhysicalQueryHourCount:
		return int64(group.Count)
	default:
		return group.Int64
	}
}

func mibPerSecond(bytes int64, nanos int64) float64 {
	if bytes <= 0 || nanos <= 0 {
		return 0
	}
	return float64(bytes) / (1024 * 1024) / (float64(nanos) / 1e9)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
