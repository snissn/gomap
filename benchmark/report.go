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
	fmt.Println("\nEngine   | Keys       | SET RPS        | SET p50   | GET RPS        | GET p50")
	fmt.Println("--------------------------------------------------------------------------")
	for _, r := range results {
		fmt.Printf("%-8s | %10d | %14.2f | %9.3f | %14.2f | %9.3f\n",
			r.Engine, r.KeyCount, r.SetRPS, r.SetP50, r.GetRPS, r.GetP50)
	}
	fmt.Println("")
}
