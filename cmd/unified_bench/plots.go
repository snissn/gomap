package main

import (
	"fmt"
	"image/color"
	"math"
	"os"
	"path/filepath"
	"strings"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
)

func writeReadmePlots(outDir string, pointRuns, scanRuns []BenchRun) (pointOpsPath, batchScanPath string, err error) {
	if outDir == "" {
		return "", "", nil
	}
	if filepath.IsAbs(outDir) {
		return "", "", fmt.Errorf("outdir must be repo-relative (got absolute path %q)", outDir)
	}
	if len(pointRuns) == 0 || len(scanRuns) == 0 {
		return "", "", fmt.Errorf("missing benchmark data for plots")
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", fmt.Errorf("create outdir %q: %w", outDir, err)
	}

	pointOpsFile := filepath.Join(outDir, "unified_bench_point_ops.png")
	batchScanFile := filepath.Join(outDir, "unified_bench_batch_scans.png")

	if err := writePlotGridPNG(pointOpsFile, [][]plotSpec{
		{
			{Title: "Sequential Write", TestName: "sequential_write", Engines: []string{"HashDB", "TreeDB", "Badger", "LevelDB"}, ShowLegend: true},
		},
		{
			{Title: "Random Write", TestName: "random_write", Engines: []string{"HashDB", "TreeDB", "Badger", "LevelDB"}},
		},
		{
			{Title: "Random Read", TestName: "random_read", Engines: []string{"HashDB", "TreeDB", "Badger", "LevelDB"}},
		},
	}, pointRuns); err != nil {
		return "", "", err
	}

	if err := writePlotGridPNG(batchScanFile, [][]plotSpec{
		{
			{Title: "Batch Write", TestName: "batch_write", Engines: []string{"TreeDB", "Badger", "LevelDB"}, ShowLegend: true},
		},
		{
			{Title: "Full Scan", TestName: "full_scan", Engines: []string{"TreeDB", "Badger", "LevelDB"}},
		},
		{
			{Title: "Prefix Scan", TestName: "prefix_scan", Engines: []string{"TreeDB", "Badger", "LevelDB"}},
		},
	}, scanRuns); err != nil {
		return "", "", err
	}

	return filepath.ToSlash(pointOpsFile), filepath.ToSlash(batchScanFile), nil
}

type plotSpec struct {
	Title    string
	TestName string
	Engines  []string

	ShowLegend bool
}

func writePlotGridPNG(path string, specs [][]plotSpec, runs []BenchRun) error {
	if len(specs) == 0 {
		return fmt.Errorf("empty plot spec for %q", path)
	}
	if len(runs) == 0 {
		return fmt.Errorf("empty runs for %q", path)
	}
	cols := len(specs[0])
	if cols == 0 {
		return fmt.Errorf("empty plot spec row for %q", path)
	}
	for r := range specs {
		if len(specs[r]) != cols {
			return fmt.Errorf("inconsistent plot spec columns for %q", path)
		}
	}

	plots := make([][]*plot.Plot, len(specs))
	for r := range specs {
		plots[r] = make([]*plot.Plot, len(specs[r]))
		for c := range specs[r] {
			p, err := newScalingPlot(specs[r][c], runs)
			if err != nil {
				return fmt.Errorf("plot %q: %w", path, err)
			}
			plots[r][c] = p
		}
	}

	const (
		dpi = 150
		w   = 12 * vg.Inch
		h   = 9 * vg.Inch
	)
	img := vgimg.NewWith(vgimg.UseWH(w, h), vgimg.UseDPI(dpi))
	dc := draw.New(img)

	t := draw.Tiles{Rows: len(plots), Cols: cols}
	canvases := plot.Align(plots, t, dc)

	for r := range plots {
		for c := range plots[r] {
			plots[r][c].Draw(canvases[r][c])
		}
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %q: %w", path, err)
	}
	defer f.Close()

	png := vgimg.PngCanvas{Canvas: img}
	if _, err := png.WriteTo(f); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

func newScalingPlot(spec plotSpec, runs []BenchRun) (*plot.Plot, error) {
	p := plot.New()
	p.Title.Text = spec.Title
	p.X.Label.Text = "keys"
	p.Y.Label.Text = "ops/sec"

	p.X.Scale = plot.LogScale{}
	p.X.Tick.Marker = plot.LogTicks{}
	p.Y.Scale = plot.LogScale{}
	p.Y.Tick.Marker = plot.LogTicks{}
	p.Add(plotter.NewGrid())

	for _, engine := range spec.Engines {
		xy := make(plotter.XYs, 0, len(runs))
		for _, run := range runs {
			v := run.Results[spec.TestName][engine]
			if math.IsNaN(v) || v <= 0 {
				continue
			}
			xy = append(xy, plotter.XY{X: float64(run.Config.Keys), Y: v})
		}
		if len(xy) == 0 {
			continue
		}

		line, points, err := plotter.NewLinePoints(xy)
		if err != nil {
			return nil, err
		}
		line.Color = engineColor(engine)
		points.Color = engineColor(engine)
		points.Radius = vg.Points(2.5)

		p.Add(line, points)
		if spec.ShowLegend {
			p.Legend.Add(engine, line)
		}
	}

	// Only the top plot in each grid gets a legend to reduce clutter.
	if spec.ShowLegend {
		p.Legend.Top = true
		p.Legend.Left = true
		p.Legend.YOffs = -vg.Points(3)
	} else {
		p.Legend = plot.NewLegend()
	}

	return p, nil
}

func engineColor(engine string) color.RGBA {
	switch strings.ToLower(engine) {
	case "hashdb":
		return color.RGBA{R: 31, G: 119, B: 180, A: 255} // blue
	case "treedb":
		return color.RGBA{R: 255, G: 127, B: 14, A: 255} // orange
	case "badger":
		return color.RGBA{R: 214, G: 39, B: 40, A: 255} // red
	case "leveldb":
		return color.RGBA{R: 148, G: 103, B: 189, A: 255} // purple
	default:
		return color.RGBA{R: 127, G: 127, B: 127, A: 255} // gray
	}
}
