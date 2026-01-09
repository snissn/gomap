package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
)

type traceEvent struct {
	Op         string `json:"op"`
	Phase      string `json:"phase"`
	KeyLen     int    `json:"key_len"`
	ValueLen   int    `json:"value_len"`
	BatchOps   int    `json:"batch_ops"`
	BatchBytes int    `json:"batch_bytes"`
	IterKind   string `json:"iter_kind"`
	IterNexts  int    `json:"iter_nexts"`
	IterMillis int64  `json:"iter_ms"`
	StartLen   int    `json:"iter_start_len"`
	EndLen     int    `json:"iter_end_len"`
}

type distSummary struct {
	Count int     `json:"count"`
	Min   int     `json:"min"`
	P50   int     `json:"p50"`
	P90   int     `json:"p90"`
	P99   int     `json:"p99"`
	Max   int     `json:"max"`
	Avg   float64 `json:"avg"`
}

type phaseSummary struct {
	Ops            map[string]int `json:"ops"`
	BatchOps       distSummary    `json:"batch_ops"`
	BatchBytes     distSummary    `json:"batch_bytes"`
	IterNexts      distSummary    `json:"iter_nexts"`
	IterMillis     distSummary    `json:"iter_ms"`
	GetKeyLens     distSummary    `json:"get_key_lens"`
	GetValueLens   distSummary    `json:"get_value_lens"`
	SetKeyLens     distSummary    `json:"set_key_lens"`
	SetValueLens   distSummary    `json:"set_value_lens"`
	IterStartLens  distSummary    `json:"iter_start_lens"`
	IterEndLens    distSummary    `json:"iter_end_lens"`
	IterCreateKind map[string]int `json:"iter_create_kind"`
}

type summary struct {
	TotalEvents int                     `json:"total_events"`
	Phases      map[string]phaseSummary `json:"phases"`
}

type distBuilder struct {
	values []int
	sum    int64
}

func (d *distBuilder) add(v int) {
	if v < 0 {
		return
	}
	d.values = append(d.values, v)
	d.sum += int64(v)
}

func (d *distBuilder) summarize() distSummary {
	n := len(d.values)
	if n == 0 {
		return distSummary{}
	}
	sort.Ints(d.values)
	return distSummary{
		Count: n,
		Min:   d.values[0],
		P50:   percentile(d.values, 0.50),
		P90:   percentile(d.values, 0.90),
		P99:   percentile(d.values, 0.99),
		Max:   d.values[n-1],
		Avg:   float64(d.sum) / float64(n),
	}
}

func percentile(vals []int, p float64) int {
	if len(vals) == 0 {
		return 0
	}
	if p <= 0 {
		return vals[0]
	}
	if p >= 1 {
		return vals[len(vals)-1]
	}
	pos := int(float64(len(vals)-1) * p)
	if pos < 0 {
		pos = 0
	}
	if pos >= len(vals) {
		pos = len(vals) - 1
	}
	return vals[pos]
}

type phaseAgg struct {
	ops            map[string]int
	batchOps       distBuilder
	batchBytes     distBuilder
	iterNexts      distBuilder
	iterMillis     distBuilder
	getKeyLens     distBuilder
	getValueLens   distBuilder
	setKeyLens     distBuilder
	setValueLens   distBuilder
	iterStartLens  distBuilder
	iterEndLens    distBuilder
	iterCreateKind map[string]int
}

func newPhaseAgg() *phaseAgg {
	return &phaseAgg{
		ops:            make(map[string]int),
		iterCreateKind: make(map[string]int),
	}
}

func (p *phaseAgg) addEvent(ev traceEvent) {
	phase := ev.Phase
	_ = phase
	p.ops[ev.Op]++
	switch ev.Op {
	case "batch_write":
		p.batchOps.add(ev.BatchOps)
		p.batchBytes.add(ev.BatchBytes)
	case "iter_create":
		p.iterStartLens.add(ev.StartLen)
		p.iterEndLens.add(ev.EndLen)
		if ev.IterKind != "" {
			p.iterCreateKind[ev.IterKind]++
		}
	case "iter_close":
		p.iterNexts.add(ev.IterNexts)
		p.iterMillis.add(int(ev.IterMillis))
	case "get":
		p.getKeyLens.add(ev.KeyLen)
		p.getValueLens.add(ev.ValueLen)
	case "set", "set_sync":
		p.setKeyLens.add(ev.KeyLen)
		p.setValueLens.add(ev.ValueLen)
	}
}

func (p *phaseAgg) summarize() phaseSummary {
	return phaseSummary{
		Ops:            p.ops,
		BatchOps:       p.batchOps.summarize(),
		BatchBytes:     p.batchBytes.summarize(),
		IterNexts:      p.iterNexts.summarize(),
		IterMillis:     p.iterMillis.summarize(),
		GetKeyLens:     p.getKeyLens.summarize(),
		GetValueLens:   p.getValueLens.summarize(),
		SetKeyLens:     p.setKeyLens.summarize(),
		SetValueLens:   p.setValueLens.summarize(),
		IterStartLens:  p.iterStartLens.summarize(),
		IterEndLens:    p.iterEndLens.summarize(),
		IterCreateKind: p.iterCreateKind,
	}
}

func main() {
	tracePath := flag.String("trace", "", "Path to treedb trace JSONL file")
	outPath := flag.String("out", "", "Path to write JSON summary (optional)")
	flag.Parse()

	if *tracePath == "" {
		fmt.Fprintln(os.Stderr, "missing -trace")
		os.Exit(2)
	}
	f, err := os.Open(*tracePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open trace: %v\n", err)
		os.Exit(2)
	}
	defer f.Close()

	phases := make(map[string]*phaseAgg)
	total := 0
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)
	for scanner.Scan() {
		var ev traceEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		phase := ev.Phase
		if phase == "" {
			phase = "unknown"
		}
		agg := phases[phase]
		if agg == nil {
			agg = newPhaseAgg()
			phases[phase] = agg
		}
		agg.addEvent(ev)
		total++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan trace: %v\n", err)
		os.Exit(2)
	}

	out := summary{
		TotalEvents: total,
		Phases:      make(map[string]phaseSummary, len(phases)),
	}
	for phase, agg := range phases {
		out.Phases[phase] = agg.summarize()
	}

	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal summary: %v\n", err)
		os.Exit(2)
	}

	if *outPath != "" {
		if err := os.WriteFile(*outPath, data, 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write summary: %v\n", err)
			os.Exit(2)
		}
	} else {
		fmt.Println(string(data))
	}
}
