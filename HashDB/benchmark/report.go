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
	Scenario string
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

	w.Write([]string{"Engine", "Scenario", "KeyCount", "SET RPS", "SET p50", "GET RPS", "GET p50"})
	for _, r := range results {
		w.Write([]string{
			r.Engine,
			r.Scenario,
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
	fmt.Println("\nEngine   | Scenario        | Keys       | SET RPS        | SET p50   | GET RPS        | GET p50")
	fmt.Println("---------------------------------------------------------------------------------------------")
	for _, r := range results {
		fmt.Printf("%-8s | %-15s | %10s | %14s | %9.3f | %14s | %9.3f\n",
			r.Engine,
			r.Scenario,
			formatInt(r.KeyCount),
			formatFloat(r.SetRPS),
			r.SetP50,
			formatFloat(r.GetRPS),
			r.GetP50)
	}
	fmt.Println("")
}

func formatInt(n int) string {
	s := strconv.Itoa(n)
	return addCommas(s)
}

func formatFloat(f float64) string {
	s := fmt.Sprintf("%.2f", f)
	parts := strings.Split(s, ".")
	parts[0] = addCommas(parts[0])
	return strings.Join(parts, ".")
}

func addCommas(s string) string {
	if len(s) <= 3 {
		return s
	}
	var res []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			res = append(res, ',')
		}
		res = append(res, byte(c))
	}
	return string(res)
}
