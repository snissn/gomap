package main

import (
	"bytes"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

func TestUnifiedBenchDBsHelp(t *testing.T) {
	const childEnv = "GOMAP_DBS_HELP_TEST"
	if os.Getenv(childEnv) == "1" {
		// Other tests register their own DBs. Read the expected selections from
		// the fresh registry in the subprocess that runs the CLI.
		defaults := resolveDBs("all", "")
		fmt.Printf("DBs included in 'all'\n%s\n", strings.Join(defaults, ", "))
		fmt.Println("Explicit variants")
		for _, name := range slices.Sorted(maps.Keys(dbFactories)) {
			if !slices.Contains(defaults, name) {
				fmt.Println(name)
			}
		}
		fmt.Println("Aliases")
		for _, alias := range slices.Sorted(maps.Keys(dbAliases)) {
			if targets := resolveDBs(alias, ""); len(targets) == 1 {
				fmt.Printf("%s -> %s\n", alias, targets[0])
			}
		}
		os.Args = []string{os.Args[0], "-help"}
		main()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestUnifiedBenchDBsHelp$")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	var expected, help bytes.Buffer
	cmd.Stdout, cmd.Stderr = &expected, &help
	if err := cmd.Run(); err != nil {
		t.Fatalf("-help failed: %v\n%s", err, help.String())
	}
	_, flags, ok := strings.Cut(help.String(), "\n  -dbs ")
	if !ok {
		t.Fatal("-help did not list the -dbs flag")
	}
	block, _, _ := strings.Cut(flags, "\n  -")
	_, description, _ := strings.Cut(block, "\n")
	usage, details, _ := strings.Cut(description, "\n")
	if !strings.HasSuffix(usage, "(default all)") {
		t.Fatalf("-dbs description should include the default: %q", usage)
	}
	lines := strings.Split(strings.TrimSpace(details), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	if got, want := strings.Join(lines, "\n"), strings.TrimSpace(expected.String()); got != want {
		t.Fatalf("-dbs help selections differ from the registry\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestTreeDBUsageGroup(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{name: "treedb-flush-threshold", want: "TreeDB Main Knobs"},
		{name: "treedb-vlog-compression", want: "TreeDB Compression Knobs"},
		{name: "treedb-vlog-dict-train-bytes", want: "TreeDB Advanced Tuning"},
		{name: "treedb-disable-wal", want: "TreeDB Unsafe Knobs"},
	}

	for _, tc := range cases {
		if got := treeDBUsageGroup(tc.name); got != tc.want {
			t.Fatalf("treeDBUsageGroup(%q)=%q want %q", tc.name, got, tc.want)
		}
	}
}

func TestStyleUsageText(t *testing.T) {
	if got := styleUsageText("-flag", false); got != "-flag" {
		t.Fatalf("plain style=%q want %q", got, "-flag")
	}
	if got := styleUsageText("-flag", true); got != "\x1b[1m-flag\x1b[0m" {
		t.Fatalf("bold style=%q", got)
	}
}
