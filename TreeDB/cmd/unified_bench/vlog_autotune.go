package main

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/caching"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

type vlogAutotuneReport struct {
	Suite    string                   `json:"suite"`
	Cases    []vlogAutotuneCaseReport `json:"cases"`
	Failures int                      `json:"failures"`
}

type vlogAutotuneCaseReport struct {
	Name   string                   `json:"name"`
	Modes  []vlogAutotuneModeReport `json:"modes"`
	Marks  []benchMark              `json:"marks"`
	Detail string                   `json:"detail,omitempty"`
	Kind   string                   `json:"kind"`
}

type vlogAutotuneModeReport struct {
	Mode   string                          `json:"mode"`
	Result caching.VlogAutotuneBenchResult `json:"result"`
}

type benchMark struct {
	Name   string `json:"name"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail,omitempty"`
}

type vlogAutotuneCase struct {
	Name     string
	Kind     string
	Segments []vlogSegmentSpec
}

type vlogSegmentSpec struct {
	Name               string
	Workload           string
	ValueSize          int
	Records            int
	EncodeNsPerRawByte float64
	IoNsPerStoredByte  float64
}

const (
	vlogAutotuneStrictThroughputRatio = 1.01
	vlogAutotuneIOThroughputRatio     = 1.15
	vlogAutotuneMarqueeRatio          = 1.10
)

func runVlogAutotuneSuite(caseName string) (*vlogAutotuneReport, error) {
	report := &vlogAutotuneReport{Suite: "vlog_autotune"}
	cases := buildVlogAutotuneCases()
	for _, c := range cases {
		if caseName != "" && c.Name != caseName {
			continue
		}
		caseReport, err := runVlogAutotuneCase(c)
		if err != nil {
			return nil, err
		}
		report.Cases = append(report.Cases, caseReport)
		for _, mark := range caseReport.Marks {
			if !mark.Pass {
				report.Failures++
			}
		}
	}
	if caseName != "" && len(report.Cases) == 0 {
		return nil, fmt.Errorf("unknown case %q", caseName)
	}
	return report, nil
}

func buildVlogAutotuneCases() []vlogAutotuneCase {
	return []vlogAutotuneCase{
		{
			Name: "cpu_bound_compressible_1k",
			Kind: "cpu_bound_compressible",
			Segments: []vlogSegmentSpec{{
				Name:               "cpu_bound_compressible",
				Workload:           "highly_compressible_tail64",
				ValueSize:          1 << 10,
				Records:            2048,
				EncodeNsPerRawByte: 25,
				IoNsPerStoredByte:  2,
			}},
		},
		{
			Name: "cpu_bound_compressible_16k",
			Kind: "cpu_bound_compressible",
			Segments: []vlogSegmentSpec{{
				Name:               "cpu_bound_compressible",
				Workload:           "highly_compressible_tail64",
				ValueSize:          16 << 10,
				Records:            256,
				EncodeNsPerRawByte: 25,
				IoNsPerStoredByte:  2,
			}},
		},
		{
			Name: "io_bound_compressible_1k",
			Kind: "io_bound_compressible",
			Segments: []vlogSegmentSpec{{
				Name:               "io_bound_compressible",
				Workload:           "highly_compressible_tail64",
				ValueSize:          1 << 10,
				Records:            2048,
				EncodeNsPerRawByte: 5,
				IoNsPerStoredByte:  200,
			}},
		},
		{
			Name: "io_bound_compressible_16k",
			Kind: "io_bound_compressible",
			Segments: []vlogSegmentSpec{{
				Name:               "io_bound_compressible",
				Workload:           "highly_compressible_tail64",
				ValueSize:          16 << 10,
				Records:            256,
				EncodeNsPerRawByte: 5,
				IoNsPerStoredByte:  200,
			}},
		},
		{
			Name: "io_bound_incompressible_1k",
			Kind: "io_bound_incompressible",
			Segments: []vlogSegmentSpec{{
				Name:               "io_bound_incompressible",
				Workload:           "incompressible",
				ValueSize:          1 << 10,
				Records:            2048,
				EncodeNsPerRawByte: 5,
				IoNsPerStoredByte:  200,
			}},
		},
		{
			Name: "io_bound_incompressible_16k",
			Kind: "io_bound_incompressible",
			Segments: []vlogSegmentSpec{{
				Name:               "io_bound_incompressible",
				Workload:           "incompressible",
				ValueSize:          16 << 10,
				Records:            256,
				EncodeNsPerRawByte: 5,
				IoNsPerStoredByte:  200,
			}},
		},
		{
			Name: "template_vs_dict_1k",
			Kind: "template_vs_dict",
			Segments: []vlogSegmentSpec{
				{
					Name:               "io_bound_compressible",
					Workload:           "highly_compressible_tail64",
					ValueSize:          1 << 10,
					Records:            4096,
					EncodeNsPerRawByte: 5,
					IoNsPerStoredByte:  200,
				},
				{
					Name:               "io_bound_template_friendly",
					Workload:           "template_friendly_mid",
					ValueSize:          1 << 10,
					Records:            16384,
					EncodeNsPerRawByte: 5,
					IoNsPerStoredByte:  200,
				},
			},
		},
		{
			Name: "marquee_regime_shift",
			Kind: "marquee",
			Segments: []vlogSegmentSpec{
				{
					Name:               "segment_a_compressible",
					Workload:           "highly_compressible_tail64",
					ValueSize:          1 << 10,
					Records:            2048,
					EncodeNsPerRawByte: 5,
					IoNsPerStoredByte:  200,
				},
				{
					Name:               "segment_b_incompressible",
					Workload:           "incompressible",
					ValueSize:          1 << 10,
					Records:            2048,
					EncodeNsPerRawByte: 5,
					IoNsPerStoredByte:  200,
				},
				{
					Name:               "segment_c_compressible",
					Workload:           "highly_compressible_tail64",
					ValueSize:          1 << 10,
					Records:            2048,
					EncodeNsPerRawByte: 5,
					IoNsPerStoredByte:  200,
				},
			},
		},
	}
}

func runVlogAutotuneCase(c vlogAutotuneCase) (vlogAutotuneCaseReport, error) {
	modes := []caching.VlogAutotuneBenchMode{
		caching.VlogAutotuneBenchOff,
		caching.VlogAutotuneBenchNoDictFixed,
		caching.VlogAutotuneBenchDictFixed,
		caching.VlogAutotuneBenchTemplate,
		caching.VlogAutotuneBenchAutotune,
	}
	caseReport := vlogAutotuneCaseReport{Name: c.Name, Kind: c.Kind}
	for _, mode := range modes {
		segments, err := buildSegments(c.Segments)
		if err != nil {
			return vlogAutotuneCaseReport{}, err
		}
		res, err := caching.RunVlogAutotuneBench(caching.VlogAutotuneBenchRequest{
			Mode:     mode,
			FixedK:   4,
			Segments: segments,
		})
		if err != nil {
			return vlogAutotuneCaseReport{}, err
		}
		caseReport.Modes = append(caseReport.Modes, vlogAutotuneModeReport{
			Mode:   string(mode),
			Result: *res,
		})
	}
	caseReport.Marks = evalVlogAutotuneMarks(c, caseReport.Modes)
	return caseReport, nil
}

func buildSegments(specs []vlogSegmentSpec) ([]caching.VlogAutotuneBenchSegment, error) {
	segments := make([]caching.VlogAutotuneBenchSegment, 0, len(specs))
	for _, spec := range specs {
		w, ok := valuelog.LookupAutotuneWorkload(spec.Workload)
		if !ok {
			return nil, fmt.Errorf("unknown workload %q", spec.Workload)
		}
		segments = append(segments, caching.VlogAutotuneBenchSegment{
			Name:               spec.Name,
			Workload:           w,
			ValueSize:          spec.ValueSize,
			Records:            spec.Records,
			EncodeNsPerRawByte: spec.EncodeNsPerRawByte,
			IoNsPerStoredByte:  spec.IoNsPerStoredByte,
		})
	}
	return segments, nil
}

func evalVlogAutotuneMarks(c vlogAutotuneCase, modes []vlogAutotuneModeReport) []benchMark {
	var marks []benchMark
	auto := findMode(modes, string(caching.VlogAutotuneBenchAutotune))
	off := findMode(modes, string(caching.VlogAutotuneBenchOff))
	if auto == nil {
		return []benchMark{{Name: "autotune_present", Pass: false, Detail: "missing autotune result"}}
	}
	if off == nil {
		marks = append(marks, benchMark{Name: "off_present", Pass: false, Detail: "missing off result"})
	}

	marks = append(marks, sanityMarks(auto.Result)...)

	switch c.Kind {
	case "cpu_bound_compressible":
		if off != nil && len(auto.Result.Segments) > 0 && len(off.Result.Segments) > 0 {
			autoThroughput := auto.Result.Segments[0].ThroughputRawMBps
			offThroughput := off.Result.Segments[0].ThroughputRawMBps
			marks = append(marks, benchMark{
				Name:   "cpu_bound_throughput",
				Pass:   throughputRatioPass(autoThroughput, offThroughput, vlogAutotuneStrictThroughputRatio),
				Detail: throughputRatioDetail(autoThroughput, offThroughput, vlogAutotuneStrictThroughputRatio),
			})
			autoKept := auto.Result.Segments[0].KeptFrac
			marks = append(marks, benchMark{
				Name:   "cpu_bound_kept_frac",
				Pass:   autoKept <= 0.10,
				Detail: fmt.Sprintf("kept=%.3f", autoKept),
			})
		}
	case "io_bound_compressible":
		if off != nil && len(auto.Result.Segments) > 0 && len(off.Result.Segments) > 0 {
			autoThroughput := auto.Result.Segments[0].ThroughputRawMBps
			offThroughput := off.Result.Segments[0].ThroughputRawMBps
			marks = append(marks, benchMark{
				Name:   "io_bound_throughput",
				Pass:   throughputRatioPass(autoThroughput, offThroughput, vlogAutotuneIOThroughputRatio),
				Detail: throughputRatioDetail(autoThroughput, offThroughput, vlogAutotuneIOThroughputRatio),
			})
			autoKept := auto.Result.Segments[0].KeptFrac
			marks = append(marks, benchMark{
				Name:   "io_bound_kept_frac",
				Pass:   autoKept >= 0.50,
				Detail: fmt.Sprintf("kept=%.3f", autoKept),
			})
			state := auto.Result.Segments[0].State
			marks = append(marks, benchMark{
				Name:   "io_bound_state_active",
				Pass:   strings.EqualFold(state, "ACTIVE"),
				Detail: fmt.Sprintf("state=%s", state),
			})
		}
	case "io_bound_incompressible":
		if len(auto.Result.Segments) > 0 {
			autoKept := auto.Result.Segments[0].KeptFrac
			autoAttempted := auto.Result.Segments[0].AttemptedFrac
			allowedAttempted := 0.10
			if frames := auto.Result.Segments[0].FramesTotal; frames > 0 {
				minAttempt := 1.0 / float64(frames)
				if minAttempt > allowedAttempted {
					allowedAttempted = minAttempt
				}
			}
			marks = append(marks, benchMark{
				Name:   "io_incompressible_kept_frac",
				Pass:   autoKept <= 0.02,
				Detail: fmt.Sprintf("kept=%.3f", autoKept),
			})
			marks = append(marks, benchMark{
				Name:   "io_incompressible_attempted_frac",
				Pass:   autoAttempted <= allowedAttempted,
				Detail: fmt.Sprintf("attempted=%.3f allowed=%.3f frames=%d", autoAttempted, allowedAttempted, auto.Result.Segments[0].FramesTotal),
			})
		}
	case "marquee":
		marks = append(marks, marqueeMarks(auto.Result, off)...)
	}
	return marks
}

func sanityMarks(result caching.VlogAutotuneBenchResult) []benchMark {
	var marks []benchMark
	for i, seg := range result.Segments {
		segLabel := fmt.Sprintf("segment_%d", i)
		validFractions := seg.KeptFrac >= 0 && seg.AttemptedFrac >= 0 && seg.KeptFrac <= seg.AttemptedFrac && seg.AttemptedFrac <= 1.0
		marks = append(marks, benchMark{
			Name:   segLabel + "_fractions",
			Pass:   validFractions,
			Detail: fmt.Sprintf("kept=%.3f attempted=%.3f", seg.KeptFrac, seg.AttemptedFrac),
		})
		ratioOK := seg.ObservedRatio > 0 && seg.ObservedRatio <= 1.05
		marks = append(marks, benchMark{
			Name:   segLabel + "_ratio",
			Pass:   ratioOK,
			Detail: fmt.Sprintf("ratio=%.3f", seg.ObservedRatio),
		})
		marks = append(marks, benchMark{
			Name:   segLabel + "_publish_order",
			Pass:   seg.PublishOrderingOK,
			Detail: fmt.Sprintf("dict_id=%d", seg.DictID),
		})
	}
	return marks
}

func marqueeMarks(auto caching.VlogAutotuneBenchResult, off *vlogAutotuneModeReport) []benchMark {
	var marks []benchMark
	if len(auto.Segments) >= 3 {
		segA := auto.Segments[0]
		segB := auto.Segments[1]
		segC := auto.Segments[2]
		marks = append(marks, benchMark{
			Name:   "marquee_seg_a_active",
			Pass:   strings.EqualFold(segA.State, "ACTIVE"),
			Detail: fmt.Sprintf("state=%s", segA.State),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_a_kept_frac",
			Pass:   segA.KeptFrac >= 0.50,
			Detail: fmt.Sprintf("kept=%.3f", segA.KeptFrac),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_a_dict",
			Pass:   segA.DictID != 0,
			Detail: fmt.Sprintf("dict_id=%d", segA.DictID),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_b_paused",
			Pass:   strings.EqualFold(segB.State, "PAUSED"),
			Detail: fmt.Sprintf("state=%s", segB.State),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_b_kept_frac",
			Pass:   segB.KeptFrac <= 0.02,
			Detail: fmt.Sprintf("kept=%.3f", segB.KeptFrac),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_b_attempted_frac",
			Pass:   segB.AttemptedFrac <= 0.10,
			Detail: fmt.Sprintf("attempted=%.3f", segB.AttemptedFrac),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_c_active",
			Pass:   strings.EqualFold(segC.State, "ACTIVE"),
			Detail: fmt.Sprintf("state=%s", segC.State),
		})
		marks = append(marks, benchMark{
			Name:   "marquee_seg_c_kept_frac",
			Pass:   segC.KeptFrac >= 0.30,
			Detail: fmt.Sprintf("kept=%.3f", segC.KeptFrac),
		})
	}
	if off != nil {
		throughput := auto.ThroughputMB
		offThroughput := off.Result.ThroughputMB
		marks = append(marks, benchMark{
			Name:   "marquee_throughput_gain",
			Pass:   throughputRatioPass(throughput, offThroughput, vlogAutotuneMarqueeRatio),
			Detail: throughputRatioDetail(throughput, offThroughput, vlogAutotuneMarqueeRatio),
		})
	}
	return marks
}

func throughputRatioPass(candidate, baseline, minRatio float64) bool {
	return baseline > 0 && candidate/baseline > minRatio
}

func throughputRatioDetail(candidate, baseline, minRatio float64) string {
	if baseline <= 0 {
		return fmt.Sprintf("candidate=%.3f baseline=%.3f ratio=nan min=>%.2fx", candidate, baseline, minRatio)
	}
	return fmt.Sprintf("candidate=%.3f baseline=%.3f ratio=%.4fx min=>%.2fx", candidate, baseline, candidate/baseline, minRatio)
}

func findMode(modes []vlogAutotuneModeReport, name string) *vlogAutotuneModeReport {
	for i := range modes {
		if modes[i].Mode == name {
			return &modes[i]
		}
	}
	return nil
}

func printVlogAutotuneReport(report *vlogAutotuneReport) {
	fmt.Printf("suite: %s\n", report.Suite)
	for _, c := range report.Cases {
		fmt.Printf("case: %s\n", c.Name)
		for _, mode := range c.Modes {
			fmt.Printf("  mode: %-12s throughput=%.3f MB/s raw=%d stored=%d wall_ns=%d\n",
				mode.Mode,
				mode.Result.ThroughputMB,
				mode.Result.RawBytes,
				mode.Result.StoredBytes,
				mode.Result.WallTimeNs,
			)
			for _, seg := range mode.Result.Segments {
				fmt.Printf("    segment: %-26s kept=%.3f attempted=%.3f ratio=%.3f state=%s k=%d dict_id=%d history=%d\n",
					seg.Name,
					seg.KeptFrac,
					seg.AttemptedFrac,
					seg.ObservedRatio,
					seg.State,
					seg.K,
					seg.DictID,
					seg.HistoryBytes,
				)
			}
		}
		if len(c.Marks) > 0 {
			for _, mark := range c.Marks {
				status := "PASS"
				if !mark.Pass {
					status = "FAIL"
				}
				if mark.Detail != "" {
					fmt.Printf("  mark: %-32s %s (%s)\n", mark.Name, status, mark.Detail)
				} else {
					fmt.Printf("  mark: %-32s %s\n", mark.Name, status)
				}
			}
		}
	}
	if report.Failures > 0 {
		fmt.Printf("marks_failed: %d\n", report.Failures)
	} else {
		fmt.Printf("marks_failed: 0\n")
	}
}
