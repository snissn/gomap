package benchmark

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type BenchmarkResult struct {
	Engine   string
	KeyCount int
	SetRPS   float64
	GetRPS   float64
	SetP50   float64
	GetP50   float64
}

func SaveResultsToCSV(filename string, results []BenchmarkResult) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"Engine", "KeyCount", "SET RPS", "SET p50", "GET RPS", "GET p50"})
	for _, r := range results {
		w.Write([]string{
			r.Engine,
			strconv.Itoa(r.KeyCount),
			fmt.Sprintf("%.2f", r.SetRPS),
			fmt.Sprintf("%.3f", r.SetP50),
			fmt.Sprintf("%.2f", r.GetRPS),
			fmt.Sprintf("%.3f", r.GetP50),
		})
	}
	return nil
}

func PrintResultsTable(results []BenchmarkResult) {
	fmt.Printf("\n%-8s | %-8s | %-10s | %-7s | %-10s | %-7s\n", "Engine", "Keys", "SET RPS", "SET p50", "GET RPS", "GET p50")
	fmt.Println(strings.Repeat("-", 60))
	for _, r := range results {
		fmt.Printf("%-8s | %-8d | %-10.2f | %-7.3f | %-10.2f | %-7.3f\n",
			r.Engine, r.KeyCount, r.SetRPS, r.SetP50, r.GetRPS, r.GetP50)
	}
}
