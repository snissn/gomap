package benchmark

import (
	"fmt"
	"image/color"
	"os"
	"path/filepath"

	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func plotResults(results []BenchmarkResult) {
	if err := plotRPS(results, "SET", "benchmark/set_rps.png"); err != nil {
		fmt.Printf("Error plotting SET RPS: %v\n", err)
	}
	if err := plotRPS(results, "GET", "benchmark/get_rps.png"); err != nil {
		fmt.Printf("Error plotting GET RPS: %v\n", err)
	}
}

func plotRPS(results []BenchmarkResult, mode string, filename string) error {
	p := plot.New()
	p.Title.Text = fmt.Sprintf("%s RPS vs Keys", mode)
	p.X.Label.Text = "Keys"
	p.Y.Label.Text = "Requests Per Second"

	engineData := make(map[string]plotter.XYs)
	for _, r := range results {
		val := 0.0
		if mode == "SET" {
			val = r.SetRPS
		} else {
			val = r.GetRPS
		}
		engineData[r.Engine] = append(engineData[r.Engine], plotter.XY{X: float64(r.KeyCount), Y: val})
	}

	i := 0
	for engine, pts := range engineData {
		line, points, err := plotter.NewLinePoints(pts)
		if err != nil {
			return err
		}
		line.Color = plotutil.Color(i)
		points.Shape = plotutil.Shape(i)
		points.Color = line.Color
		p.Add(line, points)
		p.Legend.Add(engine, line)
		i++
	}

	p.BackgroundColor = color.White
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return err
	}
	return p.Save(8*vg.Inch, 4*vg.Inch, filename)
}
