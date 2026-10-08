package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestTreeDBPrimaryDirectoryOptionalOption(t *testing.T) {
	for _, requested := range []bool{false, true} {
		t.Run(fmt.Sprint(requested), func(t *testing.T) {
			opts := struct{ IndexPrimaryDirectory bool }{IndexPrimaryDirectory: !requested}
			if err := configureTreeDBPrimaryDirectory(reflect.ValueOf(&opts).Elem(), requested); err != nil {
				t.Fatal(err)
			}
			if opts.IndexPrimaryDirectory != requested {
				t.Fatalf("built option=%t want %t", opts.IndexPrimaryDirectory, requested)
			}
			// Reporting reads the built value, independently of the request.
			opts.IndexPrimaryDirectory = !requested
			report := treeDBPrimaryDirectoryReport(reflect.ValueOf(opts), requested)
			for _, want := range []string{fmt.Sprintf("index_primary_directory_requested=%t", requested), "index_primary_directory_supported=true", fmt.Sprintf("index_primary_directory_configured_enabled=%t", !requested)} {
				if !strings.Contains(report, want) {
					t.Fatalf("report missing %q: %s", want, report)
				}
			}
			absent := struct{}{}
			err := configureTreeDBPrimaryDirectory(reflect.ValueOf(&absent).Elem(), requested)
			if requested && (err == nil || !strings.Contains(err.Error(), "does not support")) {
				t.Fatalf("unsupported enable error=%v", err)
			}
			if !requested && err != nil {
				t.Fatalf("unsupported default: %v", err)
			}
			for _, invalid := range []reflect.Value{reflect.ValueOf(&struct{ IndexPrimaryDirectory int }{}).Elem(), reflect.ValueOf(struct{ IndexPrimaryDirectory bool }{})} {
				if err := configureTreeDBPrimaryDirectory(invalid, requested); err == nil || !strings.Contains(err.Error(), "settable bool") {
					t.Fatalf("invalid option error=%v", err)
				}
			}
		})
	}
}

func TestTreeDBPrimaryDirectorySuiteOpenFailsClosed(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbIndexPrimaryDirectory = true
	for name, open := range map[string]func(string) (*backenddb.DB, error){
		"column_store":       openColumnStoreSuiteDB,
		"collection_storage": openCollectionStorageDB,
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "must-not-open")
			db, err := open(dir)
			if db != nil {
				defer db.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "does not support -treedb-index-primary-directory") {
				t.Fatalf("suite Open rejection=%v", err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("suite Open touched directory: %v", err)
			}
		})
	}
}

func TestTreeDBPrimaryDirectorySuitePreflight(t *testing.T) {
	if os.Getenv("GOMAP_PRIMARY_SUITE_PREFLIGHT_TEST") == "1" {
		main()
		return
	}
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	for _, suite := range []string{"column_store", "column-store", "collection_storage", "collection-storage", " COLLECTION_STORAGE "} {
		if err := validateTreeDBPrimaryDirectoryRequest(suite); err != nil {
			t.Fatalf("default false suite %q: %v", suite, err)
		}
		*treedbIndexPrimaryDirectory = true
		if err := validateTreeDBPrimaryDirectoryRequest(suite); err == nil {
			t.Fatalf("enabled suite %q accepted", suite)
		}
		*treedbIndexPrimaryDirectory = false
		dir := filepath.Join(t.TempDir(), "must-not-create-artifacts")
		cmd := exec.Command(os.Args[0], "-test.run=^TestTreeDBPrimaryDirectorySuitePreflight$", "-suite="+suite, "-treedb-index-primary-directory=true", "-profile-dir="+dir)
		cmd.Env = append(os.Environ(), "GOMAP_PRIMARY_SUITE_PREFLIGHT_TEST=1")
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "does not support -treedb-index-primary-directory") {
			t.Fatalf("CLI suite %q rejection=%v output=%s", suite, err, out)
		}
		if strings.Contains(string(out), "Unified Benchmark Runner") {
			t.Fatalf("CLI printed measurement banner before rejecting suite %q: %s", suite, out)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("CLI touched artifacts for suite %q: %v", suite, err)
		}
	}
}

func TestBuildTreeDBOptionsPrimaryDirectory(t *testing.T) {
	f := flag.Lookup("treedb-index-primary-directory")
	if f == nil || f.DefValue != "false" {
		t.Fatalf("opt-in flag=%v", f)
	}
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	field := fieldByPath(reflect.ValueOf(treedb.Options{}), "IndexPrimaryDirectory")
	supported := field.IsValid() && field.Kind() == reflect.Bool
	for _, requested := range []bool{false, true} {
		*treedbIndexPrimaryDirectory = requested
		for _, cfg := range []treeDBOptionsBuildConfig{{}, {forceWALOn: true}, {forceBenchmarkUnsafe: true}} {
			opts, rep, err := buildTreeDBOptionsWithConfig("", cfg)
			if requested && !supported {
				if err == nil || !strings.Contains(err.Error(), "does not support") {
					t.Fatalf("unsupported build error=%v", err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			built := fieldByPath(reflect.ValueOf(opts), "IndexPrimaryDirectory")
			if supported && built.Bool() != requested {
				t.Fatalf("option=%t want %t", built.Bool(), requested)
			}
			// Changing the global flag must not rewrite an existing report's request.
			*treedbIndexPrimaryDirectory = !requested
			report := rep.formatText("")
			*treedbIndexPrimaryDirectory = requested
			for _, want := range []string{fmt.Sprintf("index_primary_directory_requested=%t", requested), fmt.Sprintf("index_primary_directory_supported=%t", supported), fmt.Sprintf("index_primary_directory_configured_enabled=%t", requested && supported), "configuration is not runtime path qualification"} {
				if !strings.Contains(report, want) {
					t.Fatalf("built report missing %q: %s", want, report)
				}
			}
		}
	}
	if !supported {
		*treedbIndexPrimaryDirectory = true
		dir := filepath.Join(t.TempDir(), "must-not-open")
		for _, open := range []func(string) error{
			func(dir string) error { _, err := NewTreeDB(dir); return err },
			func(dir string) error { _, err := NewTreeDBPublicCommandWAL(dir); return err },
			func(dir string) error { _, err := NewTreeDBBenchUnsafe(dir); return err },
		} {
			if err := open(dir); err == nil || !strings.Contains(err.Error(), "does not support") {
				t.Fatalf("Open rejection=%v", err)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatalf("Open touched directory: %v", err)
			}
		}
		if _, err := treeDBResolvedOptionsText(""); err == nil {
			t.Fatal("resolved-options preflight accepted unsupported enable")
		}
	}
}

func TestTreeDBPrimaryDirectoryReportsHiddenAdapters(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	savedDBs, savedExclude := *dbsArg, *dbsExcludeArg
	defer func() { *dbsArg, *dbsExcludeArg = savedDBs, savedExclude }()
	resetTreeDBIndexFlagsForTest()
	supported := fieldByPath(reflect.ValueOf(treedb.Options{}), "IndexPrimaryDirectory").IsValid()
	names := []string{"treedbcached", "treedb_cached_command_wal", "treedb_command_wal"}
	for name := range dbFactories {
		if name == "treedb" || strings.HasPrefix(name, "treedb_") {
			names = append(names, name)
		}
	}
	names = append(names, "pebble")
	for _, name := range names {
		for _, requested := range []bool{false, true} {
			if requested && !supported {
				continue
			}
			t.Run(fmt.Sprintf("%s/%t", name, requested), func(t *testing.T) {
				*treedbIndexPrimaryDirectory = requested
				*dbsArg, *dbsExcludeArg = name, ""
				resolved := resolveDBs(name, "")
				if len(resolved) != 1 {
					t.Fatalf("adapter did not resolve: %v", resolved)
				}
				run := BenchRun{Config: BenchConfig{DBsArg: name}, Instances: []*DBInstance{{Name: resolved[0], Wrapper: &fixedNameDB{name: resolved[0]}}}}
				stderr, err := os.CreateTemp(t.TempDir(), "stderr")
				if err != nil {
					t.Fatal(err)
				}
				oldStderr := os.Stderr
				os.Stderr = stderr
				logResolvedTreeDBOptions()
				os.Stderr = oldStderr
				if _, err := stderr.Seek(0, 0); err != nil {
					t.Fatal(err)
				}
				banner, err := io.ReadAll(stderr)
				if err != nil {
					t.Fatal(err)
				}
				if err := stderr.Close(); err != nil {
					t.Fatal(err)
				}
				for kind, text := range map[string]string{"banner": string(banner), "single": renderMarkdownSingle(run), "sweep": renderMarkdownSweep([]BenchRun{run})} {
					for _, want := range []string{fmt.Sprintf("index_primary_directory_requested=%t", requested), fmt.Sprintf("index_primary_directory_supported=%t", supported), fmt.Sprintf("index_primary_directory_configured_enabled=%t", requested && supported)} {
						if got := strings.Contains(text, want); got != (name != "pebble") {
							t.Errorf("%s report contains %q=%t; output=%s", kind, want, got, text)
						}
					}
					if name != "pebble" {
						canonical := resolved[0] == "treedb"
						for _, field := range []string{"profile_resolved=", "benchmark_unsafe=", "read_integrity=", "vlog.compression="} {
							if got := strings.Contains(text, field); got != canonical {
								t.Errorf("%s generic field %q=%t for canonical=%t", kind, field, got, canonical)
							}
						}
						if !canonical && !strings.Contains(text, "report_scope=primary_directory_selector_for_selected_treedb_adapters") {
							t.Errorf("%s missing selector scope", kind)
						}
					}

				}
			})
		}
	}
}

func TestTreeDBPrimaryDirectoryVariantReportIgnoresOverriddenFlags(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbDisableWAL = true
	*treedbAllowUnsafe = false
	for _, cfg := range []treeDBOptionsBuildConfig{{forceWALOn: true}, {forceBenchmarkUnsafe: true}} {
		opts, _, err := buildTreeDBOptionsWithConfig("", cfg)
		if err != nil {
			t.Fatalf("actual variant builder: %v", err)
		}
		text, err := treeDBSelectedOptionsText("", false)
		if err != nil {
			t.Errorf("shared selector report rejected valid variant: %v", err)
			continue
		}
		want := treeDBPrimaryDirectoryReport(reflect.ValueOf(opts), false)
		if !strings.Contains(text, want) {
			t.Errorf("report does not describe actual variant selector: %s", text)
		}
	}
}
