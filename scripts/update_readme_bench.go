package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	readmePath := flag.String("readme", "README.md", "Path to README file to update")
	startMarker := flag.String("start", "<!-- BENCHMARK_START -->", "Start marker line")
	endMarker := flag.String("end", "<!-- BENCHMARK_END -->", "End marker line")
	flag.Parse()

	snippetBytes, err := io.ReadAll(os.Stdin)
	if err != nil {
		fatalf("read stdin: %v", err)
	}
	snippet := strings.TrimRight(string(snippetBytes), "\n")
	if strings.TrimSpace(snippet) == "" {
		fatalf("empty benchmark snippet (pipe unified_bench markdown output into this script)")
	}
	snippetLines := splitLines(snippet)

	readmeBytes, err := os.ReadFile(*readmePath)
	if err != nil {
		fatalf("read %s: %v", *readmePath, err)
	}
	readmeLines := splitLines(string(readmeBytes))

	startIdx := indexOfLine(readmeLines, *startMarker)
	endIdx := indexOfLine(readmeLines, *endMarker)
	if startIdx == -1 || endIdx == -1 || startIdx >= endIdx {
		fatalf("markers not found or in wrong order: %q .. %q", *startMarker, *endMarker)
	}

	out := make([]string, 0, len(readmeLines)+len(snippetLines))
	out = append(out, readmeLines[:startIdx+1]...)
	out = append(out, snippetLines...)
	out = append(out, readmeLines[endIdx:]...)
	outText := strings.Join(out, "\n")
	if !strings.HasSuffix(outText, "\n") {
		outText += "\n"
	}

	if err := os.WriteFile(*readmePath, []byte(outText), 0o644); err != nil {
		fatalf("write %s: %v", *readmePath, err)
	}
}

func splitLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], "\r")
	}
	return lines
}

func indexOfLine(lines []string, marker string) int {
	for i, line := range lines {
		if strings.TrimSpace(line) == marker {
			return i
		}
	}
	return -1
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, "update_readme_bench: "+format+"\n", args...)
	os.Exit(2)
}
