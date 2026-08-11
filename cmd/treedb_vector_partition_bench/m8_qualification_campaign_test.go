package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/nativewire"
	"github.com/snissn/gomap/TreeDB/vectorpartition"
)

func TestM8QualificationCampaignBindsThreeHashedRepeatsV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, head := testM8QualificationGitCheckoutV1(t, root)
	base := m8QualificationFrozenBaseSHAV1
	fixture := m8QualificationFixturesV1[0]
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: base, HeadSHA: head}
	write := func(name string, matrix m8ProductionMatrixV1) {
		run := strings.TrimSuffix(name, ".json")
		matrixDirectory := filepath.Join(root, "matrices", run)
		if err := os.MkdirAll(matrixDirectory, 0o755); err != nil {
			t.Fatal(err)
		}
		testM8QualificationSetBaseV1(t, &matrix, base)
		testM8QualificationExecutionIDsV1(&matrix, len(campaign.Runs))
		testM8QualificationProfilesV1(t, matrixDirectory, run, &matrix)
		testM8QualificationSetSourceCheckoutV1(t, root, &matrix)
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join("matrices", run, name)
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
	}
	for i := 0; i < 3; i++ {
		write("repeat-"+string(rune('1'+i))+".json", testM8QualificationMatrixV1(t, head, fixture, 125+float64(i)*75))
	}
	summary, err := testM8ValidateQualificationCampaignV1(root, campaign)
	if err != nil {
		t.Fatal(err)
	}
	profileCalls := 0
	if _, err := m8ValidateQualificationCampaignWithVerifiersV1(root, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, func(string, m8ProductionReportV1) ([][]m8CanonicalResultV1, error) { return nil, nil }, func(m8ProductionProfileEvidenceV1) bool { profileCalls++; return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if profileCalls != len(campaign.Runs)*len(m8RequiredVariantIDsV1) {
		t.Fatalf("profile verifier calls=%d want=%d", profileCalls, len(campaign.Runs)*len(m8RequiredVariantIDsV1))
	}
	if summary.P2QPSMedian != 200 || summary.P16QPSMedian != 100 || summary.P2P95Min != 87 || summary.P2P95Median != 162 || summary.P2P95Max != 237 || summary.P16P95Min != 101 || summary.P16P95Median != 176 || summary.P16P95Max != 251 {
		t.Fatalf("summary=%+v", summary)
	}
	t.Run("publication_completion", func(t *testing.T) {
		var matrix m8ProductionMatrixV1
		raw, err := os.ReadFile(filepath.Join(root, campaign.Runs[0].Path))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		for name, completed := range map[string]time.Time{
			"missing": {},
			"equal":   matrix.ExecutionCompletedAt,
			"before":  matrix.ExecutionCompletedAt.Add(-time.Nanosecond),
		} {
			t.Run(name, func(t *testing.T) {
				bad := campaign
				bad.Runs = slices.Clone(campaign.Runs)
				bad.Runs[0].PublicationCompletedAt = completed
				if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil || !strings.Contains(err.Error(), "post-publication completion") {
					t.Fatalf("completion err=%v", err)
				}
			})
		}
	})
	t.Run("topology_variant_drift", func(t *testing.T) {
		write := func(root string, leaderDrift bool) m8QualificationCampaignV1 {
			_, campaignHead := testM8QualificationGitCheckoutV1(t, root)
			campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: campaignHead, HeadSHA: campaignHead}
			for i := 0; i < 3; i++ {
				matrix := testM8QualificationMatrixV1(t, campaignHead, fixture, 125)
				testM8QualificationExecutionIDsV1(&matrix, i)
				matrix.Variants[1].Topology.ReadySetDigest = strings.Repeat("e", 64)
				matrix.Variants[1].Topology.MetaLeader = fmt.Sprintf("elected-meta-leader-%d", i)
				testM8BindRouterSessionsVariantV1(&matrix.Variants[1].RouterSessions, *matrix.Variants[1].Variant, matrix.Variants[1].Topology.ReadySetDigest)
				if leaderDrift {
					matrix.Variants[1].Topology.Groups[0].LeaderID = "different-leader"
				}
				name := fmt.Sprintf("topology-drift-%d.json", i)
				testM8QualificationProfilesV1(t, root, strings.TrimSuffix(name, ".json"), &matrix)
				raw, err := json.Marshal(matrix)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
			}
			return campaign
		}
		matchingRoot := t.TempDir()
		if _, err := testM8ValidateQualificationCampaignV1(matchingRoot, write(matchingRoot, false)); err != nil {
			t.Fatalf("rejected matching serving layouts with distinct variant ready sets: %v", err)
		}
		driftRoot := t.TempDir()
		driftCampaign := write(driftRoot, true)
		if _, err := testM8ValidateQualificationCampaignV1(driftRoot, driftCampaign); err == nil || !strings.Contains(err.Error(), "changes retained topology") {
			t.Fatalf("topology drift err=%v", err)
		}
	})
	t.Run("edited_command", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, 3)
		testM8QualificationProfilesV1(t, root, "edited-command", &matrix)
		for i, arg := range matrix.Variants[0].Command {
			if arg == "-probes" {
				matrix.Variants[0].Command[i+1] = "16"
				break
			}
		}
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		path := "edited-command.json"
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = slices.Clone(campaign.Runs)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted edited retained command")
		}
	})
	t.Run("edited_matrix_command", func(t *testing.T) {
		for name, mutate := range map[string]func(*m8ProductionMatrixV1){
			"empty_dataset": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-dataset" {
						matrix.Command[i+1] = ""
						return
					}
				}
				t.Fatal("missing dataset flag")
			},
			"changed_dataset": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-dataset" {
						matrix.Command[i+1] = t.TempDir()
						return
					}
				}
				t.Fatal("missing dataset flag")
			},
			"missing_variant_db": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-m8-variant-dbs" {
						matrix.Command = append(matrix.Command[:i:i], matrix.Command[i+2:]...)
						return
					}
				}
				t.Fatal("missing variant DB flag")
			},
			"changed_variant_db": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-m8-variant-dbs" {
						matrix.Command[i+1] = t.TempDir() + "," + strings.Join(strings.Split(matrix.Command[i+1], ",")[1:], ",")
						return
					}
				}
				t.Fatal("missing variant DB flag")
			},
			"changed_profiles": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-profiles" {
						matrix.Command[i+1] = t.TempDir()
						return
					}
				}
				t.Fatal("missing profiles flag")
			},
			"changed_truth_digest": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-m8-truth-cache-sha256" {
						matrix.Command[i+1] = strings.Repeat("e", 64)
						return
					}
				}
				t.Fatal("missing truth-cache digest flag")
			},
			"omitted_truth_digest": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-m8-truth-cache-sha256" {
						matrix.Command = append(matrix.Command[:i:i], matrix.Command[i+2:]...)
						return
					}
				}
				t.Fatal("missing truth-cache digest flag")
			},
			"changed_out": func(matrix *m8ProductionMatrixV1) {
				testM8QualificationReplaceCommandFlagV1(t, matrix.Command, "-out", t.TempDir())
			},
			"shared_out": func(matrix *m8ProductionMatrixV1) {
				shared := filepath.Join(root, "shared-matrix-output")
				if err := os.MkdirAll(shared, 0o755); err != nil {
					t.Fatal(err)
				}
				testM8QualificationReplaceCommandFlagV1(t, matrix.Command, "-out", shared)
			},
			"source_checkout_out": func(matrix *m8ProductionMatrixV1) {
				checkout, err := m8CanonicalPathV1(".")
				if err != nil {
					t.Fatal(err)
				}
				testM8QualificationReplaceCommandFlagV1(t, matrix.Command, "-out", checkout)
			},
			"changed_config": func(matrix *m8ProductionMatrixV1) {
				for i, arg := range matrix.Command {
					if arg == "-probes" {
						matrix.Command[i+1] = "16"
						return
					}
				}
				t.Fatal("missing probes flag")
			},
		} {
			t.Run(name, func(t *testing.T) {
				matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
				testM8QualificationExecutionIDsV1(&matrix, 3)
				testM8QualificationProfilesV1(t, root, "matrix-command-"+name, &matrix)
				mutate(&matrix)
				raw, err := json.Marshal(matrix)
				if err != nil {
					t.Fatal(err)
				}
				path := "matrix-command-" + name + ".json"
				if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				bad := campaign
				bad.Runs = slices.Clone(campaign.Runs)
				bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
				if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
					t.Fatalf("accepted %s", name)
				}
			})
		}
	})
	t.Run("edited_artifact_command_flags", func(t *testing.T) {
		for name, mutate := range map[string]func(*m8ProductionReportV1){
			"changed_dataset": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-dataset" {
						report.Command[i+1] = t.TempDir()
						return
					}
				}
				t.Fatal("missing dataset flag")
			},
			"omitted_existing_db": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-m8-existing-db" {
						report.Command = append(report.Command[:i:i], report.Command[i+2:]...)
						return
					}
				}
				t.Fatal("missing existing DB flag")
			},
			"changed_existing_db": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-m8-existing-db" {
						report.Command[i+1] = t.TempDir()
						return
					}
				}
				t.Fatal("missing existing DB flag")
			},
			"changed_profiles": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-profiles" {
						report.Command[i+1] = t.TempDir()
						return
					}
				}
				t.Fatal("missing profiles flag")
			},
			"omitted_profiles": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-profiles" {
						report.Command = append(report.Command[:i:i], report.Command[i+2:]...)
						return
					}
				}
				t.Fatal("missing profiles flag")
			},
			"changed_out": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-out", t.TempDir())
			},
			"changed_matrix_out": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-m8-matrix-out", t.TempDir())
			},
			"changed_matrix_profiles": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-m8-matrix-profiles", t.TempDir())
			},
			"changed_truth_cache_digest": func(report *m8ProductionReportV1) {
				report.TruthCache.Status, report.TruthCache.ComputeNanos, report.TruthCache.LoadNanos = "reused", 0, 1
				if !m8QualificationCommandWithExecutableV1(root, root, *report, func(string, string, string, string) bool { return true }) {
					t.Fatal("rejected bound reused truth-cache command")
				}
				for i, arg := range report.Command {
					if arg == "-m8-truth-cache-sha256" {
						report.Command[i+1] = strings.Repeat("e", 64)
						return
					}
				}
				t.Fatal("missing truth-cache digest flag")
			},
			"omitted_truth_cache_digest": func(report *m8ProductionReportV1) {
				for i, arg := range report.Command {
					if arg == "-m8-truth-cache-sha256" {
						report.Command = append(report.Command[:i:i], report.Command[i+2:]...)
						return
					}
				}
				t.Fatal("missing truth-cache digest flag")
			},
			"insufficient_max_vectors": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-max-vectors", "1")
			},
			"off_plan_max_vectors": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-max-vectors", strconv.Itoa(report.Dataset.Vectors+1))
			},
			"off_plan_max_fixture_bytes": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes-1, 10))
			},
			"omitted_max_fixture_bytes": func(report *m8ProductionReportV1) {
				testM8QualificationRemoveCommandFlagV1(t, &report.Command, "-max-fixture-bytes")
			},
			"omitted_base_sha": func(report *m8ProductionReportV1) {
				testM8QualificationRemoveCommandFlagV1(t, &report.Command, "-base-sha")
			},
			"changed_head_sha": func(report *m8ProductionReportV1) {
				testM8QualificationReplaceCommandFlagV1(t, report.Command, "-head-sha", strings.Repeat("e", 40))
			},
			"duplicate_base_sha": func(report *m8ProductionReportV1) {
				report.Command = append(report.Command, "-base-sha", report.BaseSHA)
			},
		} {
			t.Run(name, func(t *testing.T) {
				matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
				testM8QualificationExecutionIDsV1(&matrix, 3)
				testM8QualificationProfilesV1(t, root, "edited-"+name, &matrix)
				mutate(&matrix.Variants[0])
				raw, err := json.Marshal(matrix)
				if err != nil {
					t.Fatal(err)
				}
				path := "edited-" + name + ".json"
				if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				bad := campaign
				bad.Runs = slices.Clone(campaign.Runs)
				bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
				if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
					t.Fatalf("accepted %s", name)
				}
			})
		}
	})
	t.Run("matrix_fixture_admission_caps", func(t *testing.T) {
		for _, corpus := range m8QualificationFixturesV1 {
			matrixDirectory, err := m8CanonicalPathV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			_, commandHead := testM8QualificationGitCheckoutV1(t, matrixDirectory)
			matrix := testM8QualificationMatrixV1(t, commandHead, corpus, 125)
			testM8QualificationProfilesV1(t, matrixDirectory, "matrix-admission", &matrix)
			testM8QualificationSetSourceCheckoutV1(t, matrixDirectory, &matrix)
			verify := func(string, string, string, string) bool { return true }
			if !m8QualificationMatrixCommandWithExecutableV1(matrixDirectory, matrixDirectory, matrix, verify) {
				t.Fatalf("rejected exact %dk caps", corpus.Vectors)
			}
			for _, flag := range []string{"-max-vectors", "-max-fixture-bytes", "-base-sha", "-head-sha"} {
				bad := matrix
				bad.Command = slices.Clone(matrix.Command)
				testM8QualificationRemoveCommandFlagV1(t, &bad.Command, flag)
				if m8QualificationMatrixCommandWithExecutableV1(matrixDirectory, matrixDirectory, bad, verify) {
					t.Fatalf("accepted omitted %s for %dk", flag, corpus.Vectors)
				}
			}
			for name, mutate := range map[string]func([]string){
				"insufficient_max_vectors": func(args []string) {
					testM8QualificationReplaceCommandFlagV1(t, args, "-max-vectors", strconv.Itoa(corpus.Vectors-1))
				},
				"excessive_max_vectors": func(args []string) {
					testM8QualificationReplaceCommandFlagV1(t, args, "-max-vectors", strconv.Itoa(corpus.Vectors+1))
				},
				"off_plan_max_fixture_bytes": func(args []string) {
					testM8QualificationReplaceCommandFlagV1(t, args, "-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes-1, 10))
				},
				"changed_base_sha": func(args []string) {
					testM8QualificationReplaceCommandFlagV1(t, args, "-base-sha", strings.Repeat("e", 40))
				},
				"duplicate_head_sha": func(args []string) {
					args = append(args, "-head-sha", strings.Repeat("a", 40))
				},
			} {
				bad := matrix
				bad.Command = slices.Clone(matrix.Command)
				if name == "duplicate_head_sha" {
					bad.Command = append(bad.Command, "-head-sha", matrix.HeadSHA)
				} else {
					mutate(bad.Command)
				}
				if m8QualificationMatrixCommandWithExecutableV1(matrixDirectory, matrixDirectory, bad, verify) {
					t.Fatalf("accepted %s for %dk", name, corpus.Vectors)
				}
			}
		}
	})
	t.Run("off_plan_router_configuration", func(t *testing.T) {
		root := t.TempDir()
		_, campaignHead := testM8QualificationGitCheckoutV1(t, root)
		campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: campaignHead, HeadSHA: campaignHead}
		for repeat := 0; repeat < 3; repeat++ {
			matrix := testM8QualificationMatrixV1(t, campaignHead, fixture, 125)
			testM8QualificationExecutionIDsV1(&matrix, repeat)
			for i := range matrix.Variants {
				matrix.Variants[i].Variant.RouterConfig.BranchFactor++
				refreshTestM3VariantIdentityV1(t, matrix.Variants[i].Variant)
				matrix.Variants[i].GateLedger = m8ProductionGateLedgerForReportV1(matrix.Variants[i])
			}
			var err error
			matrix, err = m8BuildProductionMatrixV1(config{baseSHA: campaignHead, headSHA: campaignHead, partitions: 16, command: []string{"m8-test"}}, fixture, matrix.Variants)
			if err != nil {
				t.Fatal(err)
			}
			name := fmt.Sprintf("off-plan-router-%d.json", repeat)
			testM8QualificationProfilesV1(t, root, strings.TrimSuffix(name, ".json"), &matrix)
			raw, err := json.Marshal(matrix)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
		}
		if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "off-plan M3 construction") {
			t.Fatalf("off-plan router configuration err=%v", err)
		}
	})
	for name, mutate := range map[string]func(*m8ProductionRowV1){
		"edited_qps":     func(row *m8ProductionRowV1) { row.QPS++ },
		"edited_elapsed": func(row *m8ProductionRowV1) { row.ElapsedNanos++ },
		"zero_elapsed":   func(row *m8ProductionRowV1) { row.ElapsedNanos = 0 },
		"elapsed_shorter_than_slowest_request": func(row *m8ProductionRowV1) {
			row.ElapsedNanos = row.MaxTotalNanos - 1
			row.QPS, _ = m8ProductionQPSV1(row.Samples, row.ElapsedNanos)
		},
		"elapsed_shorter_than_percentile_distribution": func(row *m8ProductionRowV1) {
			row.ElapsedNanos = row.MaxTotalNanos
			row.QPS, _ = m8ProductionQPSV1(row.Samples, row.ElapsedNanos)
		},
	} {
		t.Run(name, func(t *testing.T) {
			report := testM8QualificationReportV1(t, head, fixture, testM3VariantDescriptorV1(t.TempDir()), 125)
			mutate(&report.Rows[0])
			err := testM8ValidateProductionReportV1(report)
			if err == nil {
				t.Fatalf("accepted %s", name)
			}
			if name == "elapsed_shorter_than_slowest_request" && !strings.Contains(err.Error(), "elapsed is shorter than its slowest request") {
				t.Fatalf("elapsed boundary err=%v", err)
			}
			if name == "elapsed_shorter_than_percentile_distribution" && !strings.Contains(err.Error(), "percentile-derived aggregate") {
				t.Fatalf("percentile aggregate boundary err=%v", err)
			}
		})
	}
	singleCorpusIndex, err := json.Marshal(m8QualificationIndexV1{SchemaVersion: 2, ResultKind: "vector_partition_structured_qualification_index_v2", BaseSHA: base, HeadSHA: head, Campaigns: []m8QualificationCampaignV1{campaign}})
	if err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(root, "campaign.json")
	if err := os.WriteFile(indexPath, singleCorpusIndex, 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout strings.Builder
	if err := run([]string{"validate-qualification", "-index", indexPath}, &stdout); err == nil {
		t.Fatal("accepted 100k-only qualification index")
	}
	fixture250 := m8QualificationFixturesV1[1]
	campaign250 := m8QualificationCampaignV1{FixtureChecksum: fixture250.Checksum, BaseSHA: base, HeadSHA: head}
	for i := 0; i < 3; i++ {
		name := "250k-repeat-" + string(rune('1'+i)) + ".json"
		matrix := testM8QualificationMatrixV1(t, head, fixture250, 125+float64(i)*75)
		testM8QualificationSetBaseV1(t, &matrix, base)
		testM8QualificationExecutionIDsV1(&matrix, i)
		testM8QualificationProfilesV1(t, root, strings.TrimSuffix(name, ".json"), &matrix)
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign250.Runs = append(campaign250.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
	}
	qualificationIndex := m8QualificationIndexV1{SchemaVersion: 2, ResultKind: "vector_partition_structured_qualification_index_v2", BaseSHA: base, HeadSHA: head, Campaigns: []m8QualificationCampaignV1{campaign, campaign250}}
	indexSummary, err := testM8ValidateQualificationIndexV1(root, qualificationIndex)
	if err != nil || indexSummary.Status != "qualified" || indexSummary.BaseSHA != base || indexSummary.HeadSHA != head || len(indexSummary.Campaigns) != 2 || indexSummary.Campaigns[fixture.Checksum].P2QPSMedian != 200 || indexSummary.Campaigns[fixture250.Checksum].P2QPSMedian != 200 {
		t.Fatalf("index summary err=%v summary=%+v", err, indexSummary)
	}
	t.Run("rehashed_overlapping_matrix_execution", func(t *testing.T) {
		var prior, overlap m8ProductionMatrixV1
		priorRaw, err := os.ReadFile(filepath.Join(root, campaign.Runs[0].Path))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(priorRaw, &prior); err != nil {
			t.Fatal(err)
		}
		overlapRaw, err := os.ReadFile(filepath.Join(root, campaign250.Runs[0].Path))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(overlapRaw, &overlap); err != nil {
			t.Fatal(err)
		}
		overlap.ExecutionStartedAt, overlap.ExecutionCompletedAt = prior.ExecutionStartedAt, prior.ExecutionCompletedAt
		if err := m8ValidateQualificationMatrixDerivationV1(overlap); err != nil {
			t.Fatalf("self-consistent overlap matrix rejected before aggregate serial guard: %v", err)
		}
		overlapRaw, err = json.Marshal(overlap)
		if err != nil {
			t.Fatal(err)
		}
		const path = "250k-overlapping-execution.json"
		if err := os.WriteFile(filepath.Join(root, path), overlapRaw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(overlapRaw)
		bad := qualificationIndex
		bad.Campaigns = slices.Clone(qualificationIndex.Campaigns)
		bad.Campaigns[1].Runs = slices.Clone(campaign250.Runs)
		bad.Campaigns[1].Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: overlap.ExecutionCompletedAt.Add(time.Nanosecond)}
		if _, err := testM8ValidateQualificationIndexV1(root, bad); err == nil || !strings.Contains(err.Error(), "overlapping matrix executions") {
			t.Fatalf("overlap err=%v", err)
		}
	})
	t.Run("alternate_frozen_base", func(t *testing.T) {
		root, alternateBase := t.TempDir(), strings.Repeat("b", 40)
		campaigns := make([]m8QualificationCampaignV1, 0, len(m8QualificationFixturesV1))
		for _, corpus := range m8QualificationFixturesV1 {
			campaign := m8QualificationCampaignV1{FixtureChecksum: corpus.Checksum, BaseSHA: alternateBase, HeadSHA: alternateBase}
			for repeat := 0; repeat < 3; repeat++ {
				matrix := testM8QualificationMatrixV1(t, alternateBase, corpus, 125+float64(repeat)*75)
				testM8QualificationExecutionIDsV1(&matrix, repeat)
				name := fmt.Sprintf("alternate-base-%d-%d.json", corpus.Vectors, repeat)
				testM8QualificationProfilesV1(t, root, strings.TrimSuffix(name, ".json"), &matrix)
				raw, err := json.Marshal(matrix)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
			}
			campaigns = append(campaigns, campaign)
		}
		alternate := m8QualificationIndexV1{SchemaVersion: 2, ResultKind: "vector_partition_structured_qualification_index_v2", BaseSHA: alternateBase, HeadSHA: alternateBase, Campaigns: campaigns}
		if _, err := testM8ValidateQualificationIndexV1(root, alternate); err == nil || !strings.Contains(err.Error(), "frozen base revision") {
			t.Fatalf("alternate frozen base err=%v", err)
		}
	})
	index, err := json.Marshal(qualificationIndex)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, index, 0o644); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := run([]string{"validate-qualification", "-index", indexPath}, &stdout); err == nil {
		t.Fatalf("descriptor-only CLI validation err=%v output=%q", err, stdout.String())
	}
	for name, mutate := range map[string]func(*m8QualificationIndexV1){
		"duplicate_100k": func(index *m8QualificationIndexV1) { index.Campaigns[1] = index.Campaigns[0] },
		"unknown_corpus": func(index *m8QualificationIndexV1) { index.Campaigns[1].FixtureChecksum = strings.Repeat("b", 64) },
		"missing_base":   func(index *m8QualificationIndexV1) { index.BaseSHA = "" },
		"malformed_head": func(index *m8QualificationIndexV1) { index.HeadSHA = "not-a-sha" },
		"mixed_base":     func(index *m8QualificationIndexV1) { index.Campaigns[1].BaseSHA = strings.Repeat("b", 40) },
		"mixed_head":     func(index *m8QualificationIndexV1) { index.Campaigns[1].HeadSHA = strings.Repeat("c", 40) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := qualificationIndex
			bad.Campaigns = slices.Clone(qualificationIndex.Campaigns)
			mutate(&bad)
			if _, err := testM8ValidateQualificationIndexV1(root, bad); err == nil {
				t.Fatalf("accepted %s qualification index", name)
			}
		})
	}
	t.Run("manifest_mismatch", func(t *testing.T) {
		var matrix m8ProductionMatrixV1
		raw, err := os.ReadFile(filepath.Join(root, campaign250.Runs[0].Path))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		matrix.Dataset.Fixture = "wrong-fixture"
		raw, err = json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		const path = "250k-manifest-mismatch.json"
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := qualificationIndex
		bad.Campaigns = slices.Clone(qualificationIndex.Campaigns)
		bad.Campaigns[1].Runs = slices.Clone(campaign250.Runs)
		bad.Campaigns[1].Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign250.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationIndexV1(root, bad); err == nil {
			t.Fatal("accepted manifest-mismatched corpus")
		}
	})
	t.Run("different_executable_across_corpora", func(t *testing.T) {
		bad := qualificationIndex
		bad.Campaigns = slices.Clone(qualificationIndex.Campaigns)
		bad.Campaigns[1].Runs = slices.Clone(campaign250.Runs)
		for i, run := range bad.Campaigns[1].Runs {
			var matrix m8ProductionMatrixV1
			raw, err := os.ReadFile(filepath.Join(root, run.Path))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &matrix); err != nil {
				t.Fatal(err)
			}
			matrix.ExecutableSHA256 = strings.Repeat("b", 64)
			for j := range matrix.Variants {
				matrix.Variants[j].ExecutableSHA256 = matrix.ExecutableSHA256
				matrix.Variants[j].Variant.ExecutableSHA256 = matrix.ExecutableSHA256
				refreshTestM3VariantIdentityV1(t, matrix.Variants[j].Variant)
			}
			testM8QualificationProfilesV1(t, root, fmt.Sprintf("250k-other-executable-%d", i), &matrix)
			raw, err = json.Marshal(matrix)
			if err != nil {
				t.Fatal(err)
			}
			path := fmt.Sprintf("250k-other-executable-%d.json", i)
			if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			bad.Campaigns[1].Runs[i] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign250.Runs[i].PublicationCompletedAt}
		}
		if _, err := testM8ValidateQualificationIndexV1(root, bad); err == nil || !strings.Contains(err.Error(), "different benchmark executables") {
			t.Fatalf("cross-corpus executable drift err=%v", err)
		}
	})

	for name, mutate := range map[string]func(*m8QualificationCampaignV1){
		"traversal":        func(c *m8QualificationCampaignV1) { c.Runs[0].Path = "../repeat-1.json" },
		"duplicate_path":   func(c *m8QualificationCampaignV1) { c.Runs[1].Path = c.Runs[0].Path },
		"duplicate_digest": func(c *m8QualificationCampaignV1) { c.Runs[1].SHA256 = c.Runs[0].SHA256 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := campaign
			bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
			mutate(&bad)
			if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
				t.Fatalf("accepted %s campaign identity", name)
			}
		})
	}
	t.Run("execution_identity_whitespace", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, 0)
		testM8QualificationProfilesV1(t, root, "execution-id-whitespace", &matrix)
		matrix.Variants[0].ExecutionID += " "
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "execution-id-whitespace.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: "execution-id-whitespace.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted whitespace execution identity")
		}
	})
	t.Run("reserialized_reidentified_profile_copy", func(t *testing.T) {
		var copied m8ProductionMatrixV1
		original, err := os.ReadFile(filepath.Join(root, campaign.Runs[1].Path))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(original, &copied); err != nil {
			t.Fatal(err)
		}
		for i := range copied.Variants {
			copied.Variants[i].ExecutionID = strings.Repeat(string("abc"[i]), 32)
			digest, err := m8ProductionExecutionEvidenceDigestV1(copied.Variants[i].ExecutionID, copied.Variants[i].Profiles.Artifacts, copied.Variants[i].MeasurementTranscript.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			copied.Variants[i].ExecutionEvidenceDigest = digest
		}
		raw, err := json.MarshalIndent(copied, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "execution-id-copy.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[2] = m8QualificationCampaignRunV1{Path: "execution-id-copy.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[2].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatalf("reserialized copy err=%v", err)
		}
	})
	t.Run("tampered_execution_evidence_digest", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, 0)
		testM8QualificationProfilesV1(t, root, "execution-evidence-tamper", &matrix)
		matrix.Variants[0].ExecutionEvidenceDigest = strings.Repeat("0", 64)
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "execution-evidence-tamper.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: "execution-evidence-tamper.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted tampered execution evidence digest")
		}
	})
	t.Run("profile_reuse_across_variants", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationSetBaseV1(t, &matrix, base)
		testM8QualificationExecutionIDsV1(&matrix, 0)
		testM8QualificationProfilesV1(t, root, "profile-reuse-variants", &matrix)
		matrix.Variants[1].Profiles = matrix.Variants[0].Profiles
		matrix.Variants[1].Command = append(testM8QualificationCommandV1(matrix.Variants[1], root), "-m8-matrix-out", root, "-m8-matrix-profiles", filepath.Dir(matrix.Variants[1].Profiles.Directory))
		digest, err := m8ProductionExecutionEvidenceDigestV1(matrix.Variants[1].ExecutionID, matrix.Variants[1].Profiles.Artifacts, matrix.Variants[1].MeasurementTranscript.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		matrix.Variants[1].ExecutionEvidenceDigest = digest
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "profile-reuse-variants.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest256 := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: "profile-reuse-variants.json", SHA256: hex.EncodeToString(digest256[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil || !strings.Contains(err.Error(), "reuses profile artifact set") {
			t.Fatalf("profile reuse err=%v", err)
		}
	})
	t.Run("forged_router_representative_count", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, 0)
		testM8QualificationProfilesV1(t, root, "router-count-forgery", &matrix)
		matrix.Variants[0].RouterRepresentatives++
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "router-count-forgery.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: "router-count-forgery.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted forged router representative count")
		}
	})
	invalid := testM8QualificationMatrixV1(t, head, fixture, 125)
	invalid.Variants[0].Rows[0].RouterCandidates = 0
	raw, err := json.Marshal(invalid)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "invalid-child.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	bad := campaign
	bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
	bad.Runs[0] = m8QualificationCampaignRunV1{Path: "invalid-child.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
	if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
		t.Fatal("accepted an invalid child report")
	}
	derivedTamper := testM8QualificationMatrixV1(t, head, fixture, 125)
	testM8QualificationProfilesV1(t, root, "derived-tamper", &derivedTamper)
	derivedTamper.Variants[0].GateLedger.Balance = "fail"
	derivedTamper, err = m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: []string{"m8-test"}}, fixture, derivedTamper.Variants)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(derivedTamper)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "derived-tamper.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(raw)
	bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
	bad.Runs[0] = m8QualificationCampaignRunV1{Path: "derived-tamper.json", SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
	if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
		t.Fatal("accepted coordinated stale child and matrix ledgers")
	}
	for name, mutate := range map[string]func(*m8ProductionMatrixV1){
		"wrong_corpus": func(matrix *m8ProductionMatrixV1) {
			matrix.Dataset.Fixture = "untrusted"
			for i := range matrix.Variants {
				matrix.Variants[i].Dataset = matrix.Dataset
			}
		},
		"wrong_config": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.TopK++
			}
		},
		"off_plan_router_candidates": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.RouterCandidates = 64
			}
		},
		"wrong_truth_cap": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.MaxExactTruthVisits++
			}
		},
		"wrong_m3_build_cap": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Variant.PartitionMaxDistanceWork++
				refreshTestM3VariantIdentityV1(t, matrix.Variants[i].Variant)
			}
		},
		"wrong_m3_visit_cap": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Variant.M3MaxBenchmarkVisits++
				refreshTestM3VariantIdentityV1(t, matrix.Variants[i].Variant)
			}
		},
		"incomplete_oracle": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Rows[0].Attribution.OracleStagesComplete = false
			}
		},
		"router_digest_drift": func(matrix *m8ProductionMatrixV1) {
			matrix.Variants[0].Variant.RouterModelDigest = strings.Repeat("e", 64)
		},
		"router_session_namespace_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants[0].RouterSessions.AfterWarmup {
				matrix.Variants[0].RouterSessions.AfterWarmup[i].Identity.Database = "different-database"
			}
			for i := range matrix.Variants[0].RouterSessions.AfterMeasured {
				matrix.Variants[0].RouterSessions.AfterMeasured[i].Identity.Database = "different-database"
			}
		},
		"topology_leader_drift": func(matrix *m8ProductionMatrixV1) {
			matrix.Variants[0].Topology.Groups[0].LeaderID = "different-leader"
		},
		"resource_cap_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Resources.PeakRSSCapBytes--
			}
		},
		"host_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Host.CPUModel = "different-host"
			}
		},
		"gomaxprocs_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].GOMAXPROCS = 2
			}
		},
		"gomemlimit_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].GoMemoryLimitBytes = 2
			}
		},
		"wrong_repeat_schedule": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Config.Probes = []int{4, 8, 16}
			}
		},
		"p95_gate": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				for j := range matrix.Variants[i].Rows {
					if matrix.Variants[i].Rows[j].Probes == 4 {
						matrix.Variants[i].Rows[j].P50Nanos, matrix.Variants[i].Rows[j].P95Nanos, matrix.Variants[i].Rows[j].P99Nanos, matrix.Variants[i].Rows[j].MaxTotalNanos = 199, 200, 201, 202
					}
				}
			}
		},
		"unprofiled": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Profiles = m8ProductionProfileEvidenceV1{Status: "not_captured"}
				matrix.Variants[i].ExecutionEvidenceDigest = ""
				matrix.Variants[i].Command = testM8QualificationCommandV1(matrix.Variants[i], root)
			}
		},
		"missing_profile": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Profiles.Artifacts = matrix.Variants[i].Profiles.Artifacts[:len(matrix.Variants[i].Profiles.Artifacts)-1]
			}
		},
		"profile_mode_drift": func(matrix *m8ProductionMatrixV1) {
			for i := range matrix.Variants {
				matrix.Variants[i].Profiles.Scope = "different capture mode"
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
			testM8QualificationProfilesV1(t, root, name, &matrix)
			mutate(&matrix)
			raw, err := json.Marshal(matrix)
			if err != nil {
				t.Fatal(err)
			}
			path := name + ".json"
			if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			bad := campaign
			bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
			bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
			_, err = testM8ValidateQualificationCampaignV1(root, bad)
			if err == nil {
				t.Fatalf("accepted %s qualification matrix: %v", name, err)
			}
		})
	}
	t.Run("tampered_profile", func(t *testing.T) {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationProfilesV1(t, root, "tampered-profile", &matrix)
		if err := os.WriteFile(matrix.Variants[0].Profiles.Artifacts[0].Path, []byte("tampered"), 0o644); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		path := "tampered-profile.json"
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0] = m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: campaign.Runs[0].PublicationCompletedAt}
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted tampered profile")
		}
	})

	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape.json")); err == nil {
		bad := campaign
		bad.Runs = append([]m8QualificationCampaignRunV1(nil), campaign.Runs...)
		bad.Runs[0].Path = "escape.json"
		bad.Runs[0].SHA256 = hex.EncodeToString(digest[:])
		if _, err := testM8ValidateQualificationCampaignV1(root, bad); err == nil {
			t.Fatal("accepted a symlink escaping campaign root")
		}
	}

	broken := testM8QualificationMatrixV1(t, head, fixture, 110)
	testM8QualificationProfilesV1(t, root, "broken", &broken)
	p2, p16 := m8QualificationRowsV1(broken.Variants[1])
	if p2 == nil || p16 == nil || p2.QPS >= p16.QPS*1.15 {
		t.Fatalf("broken selected rows p2=%+v p16=%+v", p2, p16)
	}
	campaign.Runs = campaign.Runs[:2]
	write("broken.json", broken)
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "misses the selected p2/p16 gate") {
		t.Fatalf("under-target p2 QPS error=%v", err)
	}
}

func TestM8QualificationFullLadderCountsShortfallRowsV1(t *testing.T) {
	qualified := func(probes int) m8ProductionRowV1 {
		return m8ProductionRowV1{Status: "pass", Probes: probes, EfSearch: 128, Concurrency: 1, RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterCandidates: m8QualificationRouterCandidatesV1, Attribution: m8ProductionAttributionV1{OracleStagesComplete: true}}
	}
	report := m8ProductionReportV1{Rows: []m8ProductionRowV1{
		qualified(1), qualified(2), qualified(4), qualified(8), {Status: "candidate_coverage_shortfall", Probes: 16},
	}}
	if !m8QualificationHasFullLadderV1(report) {
		t.Fatal("recorded p1/2/4/8/16 ladder treated as omitted")
	}
	if p2, p16 := m8QualificationRowsV1(report); p2 == nil || p16 != nil {
		t.Fatalf("shortfall must reach the selected p2/p16 gate as an unqualified p16: p2=%+v p16=%+v", p2, p16)
	}
}

func TestM8QualificationCommandBindsBenchmarkExecutableV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, head := testM8QualificationGitCheckoutV1(t, root)
	fixture := m8QualificationFixturesV1[0]
	matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
	matrixDirectory := root
	testM8QualificationProfilesV1(t, matrixDirectory, "command", &matrix)
	report := matrix.Variants[0]
	wanted := report.Command[0]
	verify := func(_root, command, revision, digest string) bool {
		return command == wanted && revision == head && digest == matrix.ExecutableSHA256
	}
	if !m8QualificationCommandWithExecutableV1(root, matrixDirectory, report, verify) || !m8QualificationMatrixCommandWithExecutableV1(root, matrixDirectory, matrix, verify) {
		t.Fatal("rejected canonical benchmark command")
	}
	missingSource := report
	testM8QualificationRemoveCommandFlagV1(t, &missingSource.Command, "-source-checkout")
	if m8QualificationCommandWithExecutableV1(root, matrixDirectory, missingSource, verify) {
		t.Fatal("accepted child command without source checkout")
	}
	changedSource := matrix
	testM8QualificationReplaceCommandFlagV1(t, changedSource.Command, "-source-checkout", t.TempDir())
	if m8QualificationMatrixCommandWithExecutableV1(root, matrixDirectory, changedSource, verify) {
		t.Fatal("accepted matrix command with changed source checkout")
	}
	duplicateSource := report
	duplicateSource.Command = append(append([]string(nil), duplicateSource.Command...), "-source-checkout", filepath.Join(root, "source"))
	if m8QualificationCommandWithExecutableV1(root, matrixDirectory, duplicateSource, verify) {
		t.Fatal("accepted child command with duplicate source checkout")
	}
	report.Command[0] = filepath.Join(t.TempDir(), "unrelated")
	if m8QualificationCommandWithExecutableV1(root, matrixDirectory, report, verify) {
		t.Fatal("accepted unrelated child executable")
	}
	matrix.Command[0] = filepath.Join(t.TempDir(), "unrelated")
	if m8QualificationMatrixCommandWithExecutableV1(root, matrixDirectory, matrix, verify) {
		t.Fatal("accepted unrelated matrix executable")
	}
	report.Command[0] = ""
	if m8QualificationCommandWithExecutableV1(root, matrixDirectory, report, verify) {
		t.Fatal("accepted blank child executable")
	}
	report = matrix.Variants[0]
	report.ExecutableSHA256 = strings.Repeat("b", 64)
	if m8QualificationCommandWithExecutableV1(root, matrixDirectory, report, verify) {
		t.Fatal("accepted child executable digest drift")
	}
	matrix.ExecutableSHA256 = strings.Repeat("b", 64)
	if m8QualificationMatrixCommandWithExecutableV1(root, matrixDirectory, matrix, verify) {
		t.Fatal("accepted matrix executable digest drift")
	}
}

func testM8QualificationGitCheckoutV1(t *testing.T, root string) (string, string) {
	t.Helper()
	root, err := m8CanonicalPathV1(root)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit := func(args ...string) {
		t.Helper()
		command := exec.Command("git", args...)
		command.Dir = source
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	runGit("init", "-q")
	runGit("config", "user.name", "M8 Test")
	runGit("config", "user.email", "m8@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "tracked.txt"), []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit("add", "tracked.txt")
	runGit("commit", "-qm", "initial")
	headRaw, err := exec.Command("git", "-C", source, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return source, strings.TrimSpace(string(headRaw))
}

func testM8QualificationSetBaseV1(t *testing.T, matrix *m8ProductionMatrixV1, base string) {
	t.Helper()
	for i := range matrix.Variants {
		report := &matrix.Variants[i]
		report.BaseSHA = base
		report.Variant.BaseSHA = base
		refreshTestM3VariantIdentityV1(t, report.Variant)
		report.GateLedger = m8ProductionGateLedgerForReportV1(*report)
	}
	built, err := m8BuildProductionMatrixV1(config{baseSHA: base, headSHA: matrix.HeadSHA, partitions: 16, command: []string{"m8-test"}}, matrix.Dataset, matrix.Variants)
	if err != nil {
		t.Fatal(err)
	}
	*matrix = built
}

func TestM8QualificationSourceCheckoutV1(t *testing.T) {
	newCheckout := func(t *testing.T) (string, string, string) {
		t.Helper()
		root, err := m8CanonicalPathV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		source, head := testM8QualificationGitCheckoutV1(t, root)
		return root, source, head
	}
	valid := func(root, source, head string) bool {
		return m8QualificationSourceCheckoutV1(root, []string{"-source-checkout", source}, config{sourceCheckout: source, headSHA: head})
	}
	t.Run("clean", func(t *testing.T) {
		root, source, head := newCheckout(t)
		if !valid(root, source, head) {
			t.Fatal("rejected clean retained source checkout")
		}
	})
	for name, mutate := range map[string]func(*testing.T, string, string){
		"deleted": func(t *testing.T, _ string, source string) {
			if err := os.RemoveAll(source); err != nil {
				t.Fatal(err)
			}
		},
		"non_git": func(t *testing.T, _ string, source string) {
			if err := os.RemoveAll(filepath.Join(source, ".git")); err != nil {
				t.Fatal(err)
			}
		},
		"wrong_head": func(t *testing.T, _ string, source string) {
			command := exec.Command("git", "-C", source, "commit", "--allow-empty", "-qm", "next")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("advance source head: %v: %s", err, output)
			}
		},
		"dirty": func(t *testing.T, _ string, source string) {
			if err := os.WriteFile(filepath.Join(source, "untracked.txt"), []byte("dirty\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			root, source, head := newCheckout(t)
			mutate(t, root, source)
			if valid(root, source, head) {
				t.Fatalf("accepted %s retained source checkout", name)
			}
		})
	}
}

func TestM8QualificationM3BuildCapsV1(t *testing.T) {
	for _, fixture := range m8QualificationFixturesV1 {
		partitionConfig, routerConfig, visits, ok := m8QualificationM3BuildConfigV1(fixture)
		if !ok {
			t.Fatalf("fixture=%d expected config", fixture.Vectors)
		}
		definition := partitionCollectionMetaWithDegree(m3BenchmarkCollection, fixture.Dimensions, partitionConfig.Degree).VectorIndexes[0]
		variant := m3VariantDescriptorV1{IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition), PartitionHNSWM: partitionConfig.Degree, PartitionConfig: partitionConfig, PartitionMaxDistanceWork: partitionConfig.MaxDistanceWork, RouterMaxScalarWork: routerConfig.MaxScalarWork, RouterConfig: routerConfig, M3MaxBenchmarkVisits: visits}
		if !m8QualificationM3BuildCapsV1(variant, fixture) {
			t.Fatalf("fixture=%d rejected expected config", fixture.Vectors)
		}
		defaultCandidate := variant
		definition.EfConstruction = 256
		defaultCandidate.IndexDefinitionDigest = collections.VectorIndexDefinitionDigestV1(definition)
		if m8QualificationM3BuildCapsV1(defaultCandidate, fixture) {
			t.Fatalf("fixture=%d accepted M16/eFC256 local definition", fixture.Vectors)
		}
		definition.EfConstruction = 128
		candidate := variant
		candidate.PartitionHNSWM = 18
		definition.M, definition.EfConstruction = 18, 256
		candidate.IndexDefinitionDigest = collections.VectorIndexDefinitionDigestV1(definition)
		if !m8QualificationM3BuildCapsV1(candidate, fixture) {
			t.Fatalf("fixture=%d rejected M18/eFC256 local candidate", fixture.Vectors)
		}
		candidate.IndexDefinitionDigest = variant.IndexDefinitionDigest
		if m8QualificationM3BuildCapsV1(candidate, fixture) {
			t.Fatalf("fixture=%d accepted M18 with the default local definition", fixture.Vectors)
		}
		wantRouterMaxVectors, ok := m8QualificationRouterMaxVectorsV1(fixture.Vectors)
		if !ok || routerConfig.MaxVectors != wantRouterMaxVectors {
			t.Fatalf("fixture=%d router max vectors=%d want=%d", fixture.Vectors, routerConfig.MaxVectors, wantRouterMaxVectors)
		}
		oldDefault, err := parseConfig([]string{
			"-stage", "overlap,partition_index", "-dataset", ".", "-out", ".", "-probes", "1", "-partitions", "16", "-seed", strconv.FormatInt(fixture.Seed, 10),
			"-partition-max-distance-work", strconv.FormatInt(partitionConfig.MaxDistanceWork, 10), "-router-max-scalar-work", strconv.FormatInt(routerConfig.MaxScalarWork, 10),
			"-m3-max-benchmark-visits", strconv.FormatInt(visits, 10),
		})
		if err != nil {
			t.Fatal(err)
		}
		oldVariant := variant
		oldVariant.PartitionConfig, oldVariant.RouterConfig = oldDefault.partition, oldDefault.routerConfig
		if m8QualificationM3BuildCapsV1(oldVariant, fixture) {
			t.Fatalf("fixture=%d accepted old default-1M descriptor config", fixture.Vectors)
		}
		missingRouter, err := parseConfig([]string{
			"-stage", "overlap,partition_index", "-dataset", ".", "-out", ".", "-probes", "1", "-partitions", "16", "-seed", strconv.FormatInt(fixture.Seed, 10),
			"-max-vectors", strconv.Itoa(fixture.Vectors), "-partition-max-distance-work", strconv.FormatInt(partitionConfig.MaxDistanceWork, 10), "-router-max-scalar-work", strconv.FormatInt(routerConfig.MaxScalarWork, 10),
			"-m3-max-benchmark-visits", strconv.FormatInt(visits, 10),
		})
		if err != nil {
			t.Fatal(err)
		}
		missingRouterVariant := variant
		missingRouterVariant.PartitionConfig, missingRouterVariant.RouterConfig = missingRouter.partition, missingRouter.routerConfig
		if m8QualificationM3BuildCapsV1(missingRouterVariant, fixture) {
			t.Fatalf("fixture=%d accepted descriptor without expanded router cap", fixture.Vectors)
		}
		variant.RouterMaxScalarWork++
		if m8QualificationM3BuildCapsV1(variant, fixture) {
			t.Fatalf("fixture=%d accepted wrong cap", fixture.Vectors)
		}
		variant.RouterMaxScalarWork--
		variant.M3MaxBenchmarkVisits++
		if m8QualificationM3BuildCapsV1(variant, fixture) {
			t.Fatalf("fixture=%d accepted wrong M3 visit cap", fixture.Vectors)
		}
		variant.M3MaxBenchmarkVisits--
		variant.RouterConfig.BranchFactor++
		if m8QualificationM3BuildCapsV1(variant, fixture) {
			t.Fatalf("fixture=%d accepted off-plan router config", fixture.Vectors)
		}
		variant.RouterConfig.BranchFactor--
		variant.RouterConfig.MaxVectors--
		if m8QualificationM3BuildCapsV1(variant, fixture) {
			t.Fatalf("fixture=%d accepted off-plan router membership cap", fixture.Vectors)
		}
		variant.RouterConfig.MaxVectors++
		for name, mutate := range map[string]func(*m3VariantDescriptorV1){
			"graph degree":      func(v *m3VariantDescriptorV1) { v.PartitionConfig.Degree++ },
			"graph pivots":      func(v *m3VariantDescriptorV1) { v.PartitionConfig.Pivots++ },
			"graph repetitions": func(v *m3VariantDescriptorV1) { v.PartitionConfig.Repetitions++ },
			"graph imbalance":   func(v *m3VariantDescriptorV1) { v.PartitionConfig.Imbalance += .01 },
			"local HNSW M":      func(v *m3VariantDescriptorV1) { v.PartitionHNSWM-- },
		} {
			candidate := variant
			mutate(&candidate)
			if m8QualificationM3BuildCapsV1(candidate, fixture) {
				t.Fatalf("fixture=%d accepted off-plan %s", fixture.Vectors, name)
			}
		}
	}
}

func TestM8QualificationImmutableTopologyCopiesGroupsV1(t *testing.T) {
	head := strings.Repeat("a", 40)
	matrix := testM8QualificationMatrixV1(t, head, m8QualificationFixturesV1[0], 125)
	before := matrix.Variants[0].Topology.Groups[0]
	_ = m8QualificationImmutableTopologyV1(matrix.Variants[0].Topology)
	if got := matrix.Variants[0].Topology.Groups[0]; got.Endpoint != before.Endpoint || got.CommitIndex != before.CommitIndex || got.ReadIndex != before.ReadIndex || got.AppliedIndex != before.AppliedIndex || got.EndpointHits != before.EndpointHits {
		t.Fatalf("immutable topology mutated report group: got=%+v want=%+v", got, before)
	}
	if err := m8ValidateQualificationMatrixDerivationV1(matrix); err != nil {
		t.Fatalf("matrix derivation lost original topology evidence: %v", err)
	}
}

func TestCommitted4027StructuredQualificationPlanV1(t *testing.T) {
	root := filepath.Join("..", "..")
	raw, err := os.ReadFile(filepath.Join(root, "TreeDB", "docs", "spec", "artifacts", "vector-partition-qualification-4027-plan.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan struct {
		SchemaVersion int    `json:"schema_version"`
		ResultKind    string `json:"result_kind"`
		Status        string `json:"status"`
		BaseSHA       string `json:"base_sha"`
		Candidate     struct {
			Variant            string `json:"variant"`
			AssignmentBackend  string `json:"assignment_backend"`
			KaHIPPythonSHA256  string `json:"kahip_python_sha256"`
			KaHIPAdapterSHA256 string `json:"kahip_adapter_sha256"`
			RouterCandidates   int    `json:"router_candidates"`
			Probes             []int  `json:"probes"`
			RepeatedProbes     []int  `json:"repeated_probes"`
			EFSearch           int    `json:"ef_search"`
			Repetitions        int    `json:"repetitions"`
		} `json:"candidate"`
		Corpora []struct {
			ID               string `json:"id"`
			DatasetSource    string `json:"dataset_source"`
			Dataset          string `json:"dataset"`
			Checksum         string `json:"checksum"`
			TruthCacheSource string `json:"truth_cache_source"`
			TruthCache       string `json:"truth_cache"`
			TruthIdentity    string `json:"truth_cache_identity"`
			TruthArtifact    string `json:"truth_cache_sha256"`
			TruthSHA         string `json:"truth_sha256"`
			Vectors          int    `json:"vectors"`
			MaxVectors       int    `json:"max_vectors"`
			MaxFixtureBytes  int64  `json:"max_fixture_bytes"`
			GraphCap         int64  `json:"partition_max_distance_work"`
			RouterCap        int64  `json:"router_max_scalar_work"`
			RouterMaxVectors int    `json:"router_max_vectors"`
			M3Cap            int64  `json:"m3_max_benchmark_visits"`
			M8Cap            int64  `json:"m8_max_exact_truth_visits"`
		} `json:"corpora"`
		Commands       map[string]string `json:"commands"`
		Validation     string            `json:"validation"`
		SourceCheckout string            `json:"source_checkout"`
	}
	if err := json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.SchemaVersion != 1 || plan.ResultKind != "vector_partition_structured_qualification_campaign_plan_v1" || plan.Status != "planned_no_measurement" || plan.BaseSHA != m8QualificationFrozenBaseSHAV1 || plan.Candidate.Variant != "graph-overlap-020-v1" || plan.Candidate.AssignmentBackend != "kahip_python_3.25_eco_symmetrized_v1_seed_<seed>" || plan.Candidate.KaHIPPythonSHA256 != m8QualificationKaHIPPythonSHA256V1 || plan.Candidate.KaHIPAdapterSHA256 != kahipAdapterSHA256 || plan.Candidate.RouterCandidates != m8QualificationRouterCandidatesV1 || !slices.Equal(plan.Candidate.Probes, []int{1, 2, 4, 8, 16}) || !slices.Equal(plan.Candidate.RepeatedProbes, []int{1, 2, 4, 8, 16}) || plan.Candidate.EFSearch != 128 || plan.Candidate.Repetitions != 3 || len(plan.Corpora) != 2 {
		t.Fatalf("plan=%+v", plan)
	}
	anchor100, ok100 := m8QualificationTruthCacheAnchorV1(m8QualificationFixturesV1[0])
	anchor250, ok250 := m8QualificationTruthCacheAnchorV1(m8QualificationFixturesV1[1])
	if !ok100 || !ok250 || plan.Corpora[0].DatasetSource != "/mnt/fast4tb/gomap-4015-fixtures/embedding_mixture_100k" || plan.Corpora[0].Dataset != "<campaign-root>/100k/dataset" || plan.Corpora[0].TruthCacheSource != "/mnt/fast4tb/gomap-4027-truth-oracles-a5364e5b/100k/truth-cache" || plan.Corpora[0].TruthCache != "<campaign-root>/100k/truth-cache" || plan.Corpora[0].TruthIdentity != anchor100.Identity || plan.Corpora[0].TruthArtifact != anchor100.ArtifactSHA256 || plan.Corpora[0].TruthSHA != anchor100.TruthSHA256 || plan.Corpora[0].MaxVectors != 100000 || plan.Corpora[0].RouterMaxVectors != 120000 || plan.Corpora[0].MaxFixtureBytes != maxFixtureBytes || plan.Corpora[0].GraphCap != 20000000000 || plan.Corpora[0].RouterCap != 20000000000 || plan.Corpora[1].DatasetSource != "<campaign-root>/source/testdata/vector_partition_qualification_embedding_mixture_250k" || plan.Corpora[1].Dataset != "<campaign-root>/250k/dataset" || plan.Corpora[1].TruthCacheSource != "/mnt/fast4tb/gomap-4027-truth-oracles-a5364e5b/250k/truth-cache" || plan.Corpora[1].TruthCache != "<campaign-root>/250k/truth-cache" || plan.Corpora[1].TruthIdentity != anchor250.Identity || plan.Corpora[1].TruthArtifact != anchor250.ArtifactSHA256 || plan.Corpora[1].TruthSHA != anchor250.TruthSHA256 || plan.Corpora[1].Vectors != 250000 || plan.Corpora[1].MaxVectors != 250000 || plan.Corpora[1].RouterMaxVectors != 300000 || plan.Corpora[1].MaxFixtureBytes != maxFixtureBytes || plan.Corpora[1].Checksum != "d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69" || plan.Corpora[1].GraphCap != 50000000000 || plan.Corpora[1].RouterCap != 50000000000 || plan.Corpora[1].M3Cap != 900000000 || plan.Corpora[1].M8Cap != 1500000000 {
		t.Fatalf("250k plan=%+v", plan.Corpora[1])
	}
	if !strings.Contains(plan.Commands["stage_dataset"], "<dataset-source>/fixture_manifest.json") || !strings.Contains(plan.Commands["stage_existing_truth_cache"], "<truth-cache-source>/m8_canonical_truth_<truth-cache-identity>.json") || !strings.Contains(plan.Commands["stage_source"], "git clone --no-local --no-checkout <repository-source> <campaign-root>/source") || !strings.Contains(plan.Commands["stage_source"], "git -C <campaign-root>/source checkout --detach <head-sha>") || !strings.Contains(plan.Commands["stage_source"], "test -d <campaign-root>/source/.git") || !strings.Contains(plan.Commands["stage_source"], "test ! -e <campaign-root>/source/.git/objects/info/alternates") {
		t.Fatalf("plan does not stage retained inputs: %+v", plan.Commands)
	}
	if !strings.Contains(plan.Commands["generate_truth_cache_100k"], "-max-exact-truth-visits 100000000") || !strings.Contains(plan.Commands["generate_truth_cache_250k"], "-max-exact-truth-visits 250000000") || !strings.Contains(plan.Commands["generate_truth_cache_250k"], "-dataset <campaign-root>/source/testdata/vector_partition_qualification_embedding_mixture_250k") || strings.Contains(plan.Commands["generate_truth_cache_250k"], "-dataset testdata/") {
		t.Fatalf("plan does not freeze independent truth generation caps: %+v", plan.Commands)
	}
	if strings.Contains(plan.Commands["build_benchmark"], "go build -o <campaign-root>/bin/treedb_vector_partition_bench") || !strings.Contains(plan.Commands["build_benchmark"], "install -d <campaign-root>/bin &&") || !strings.Contains(plan.Commands["build_benchmark"], "go -C <campaign-root>/source build -buildvcs=true -o <campaign-root>/bin/treedb_vector_partition_bench") || !strings.Contains(plan.Commands["build_benchmark"], "treedb_vector_partition_bench.sha256") || !strings.Contains(plan.Commands["build_benchmark"], "treedb_vector_partition_bench.buildinfo.txt") || !strings.Contains(plan.Commands["build_benchmark"], "vcs.revision=<head-sha>") || !strings.Contains(plan.Commands["build_benchmark"], "vcs.modified=false") {
		t.Fatalf("plan does not build the retained benchmark executable: %+v", plan.Commands)
	}
	for _, command := range []string{plan.Commands["m3_graph_disjoint"], plan.Commands["m3_graph_overlap"], plan.Commands["m3_stable_hash_disjoint"], plan.Commands["m8_matrix_repeats_full_ladder"], plan.Validation} {
		if strings.Contains(command, "go run") || !strings.Contains(command, "<campaign-root>/bin/treedb_vector_partition_bench") {
			t.Fatalf("plan does not invoke the retained benchmark executable: %q", command)
		}
	}
	if !strings.Contains(plan.Commands["m3_graph_disjoint"], "-partition-max-distance-work <graph-cap>") || !strings.Contains(plan.Commands["m3_graph_disjoint"], "-router-max-scalar-work <router-cap>") || !strings.Contains(plan.Commands["m3_graph_overlap"], "-partition-max-distance-work <graph-cap>") || !strings.Contains(plan.Commands["m3_graph_overlap"], "-router-max-scalar-work <router-cap>") || !strings.Contains(plan.Commands["m3_stable_hash_disjoint"], "-partition-max-distance-work <graph-cap>") || !strings.Contains(plan.Commands["m3_stable_hash_disjoint"], "-router-max-scalar-work <router-cap>") {
		t.Fatalf("graph commands do not bind corpus-specific scalar cap: %+v", plan.Commands)
	}
	for _, command := range []string{plan.Commands["m3_graph_disjoint"], plan.Commands["m3_graph_overlap"], plan.Commands["m3_stable_hash_disjoint"]} {
		if !strings.Contains(command, "-max-vectors <max-vectors>") || !strings.Contains(command, "-router-max-vectors <router-max-vectors>") || !strings.Contains(command, "-max-fixture-bytes <max-fixture-bytes>") || !strings.Contains(command, "-base-sha <base-sha>") || !strings.Contains(command, "-head-sha <head-sha>") || !strings.Contains(command, "-source-checkout <campaign-root>/source") {
			t.Fatalf("plan command does not bind fixture admission caps: %q", command)
		}
	}
	if !strings.Contains(plan.Commands["m8_matrix_repeats_full_ladder"], "-base-sha <base-sha>") || !strings.Contains(plan.Commands["m8_matrix_repeats_full_ladder"], "-head-sha <head-sha>") || !strings.Contains(plan.Commands["m8_matrix_repeats_full_ladder"], "-source-checkout <campaign-root>/source") || !strings.Contains(plan.SourceCheckout, "-source-checkout <campaign-root>/source") {
		t.Fatalf("plan M8 command does not bind explicit provenance: %q", plan.Commands["m8_matrix_repeats_full_ladder"])
	}
	if !strings.Contains(plan.Commands["m8_matrix_repeats_full_ladder"], "-router-candidates 256") || !strings.Contains(plan.Commands["m8_matrix_repeats_full_ladder"], "-ef-search 128") {
		t.Fatalf("plan M8 command does not bind all retained router representatives: %q", plan.Commands["m8_matrix_repeats_full_ladder"])
	}
	if !strings.Contains(plan.Commands["record_matrix_publication"], "after the foreground m8_matrix_repeats_full_ladder child exits successfully") || !strings.Contains(plan.Commands["record_matrix_publication"], "publication_completed_at") || !strings.Contains(plan.Commands["record_matrix_publication"], "strictly after the matrix execution_completed_at") {
		t.Fatalf("plan does not bind post-publication matrix completion: %q", plan.Commands["record_matrix_publication"])
	}
	script, err := filepath.Abs(filepath.Join(root, "scripts", "treedb_kahip_partition.py"))
	if err != nil {
		t.Fatal(err)
	}
	for _, corpus := range plan.Corpora {
		partition, router, visits, ok := m8QualificationM3BuildConfigV1(fixtureManifest{Vectors: corpus.Vectors, Seed: map[int]int64{100000: 4017, 250000: 4016}[corpus.Vectors]})
		if !ok {
			t.Fatalf("missing expected M3 config for %dk", corpus.Vectors)
		}
		replace := strings.NewReplacer("<campaign-root>/source/scripts/treedb_kahip_partition.py", script, "<campaign-root>", t.TempDir(), "<corpus>", corpus.ID, "<dataset>", t.TempDir(), "<graph-cap>", strconv.FormatInt(corpus.GraphCap, 10), "<router-cap>", strconv.FormatInt(corpus.RouterCap, 10), "<router-max-vectors>", strconv.Itoa(corpus.RouterMaxVectors), "<m3-cap>", strconv.FormatInt(corpus.M3Cap, 10), "<max-vectors>", strconv.Itoa(corpus.MaxVectors), "<max-fixture-bytes>", strconv.FormatInt(corpus.MaxFixtureBytes, 10), "<seed>", strconv.FormatInt(map[int]int64{100000: 4017, 250000: 4016}[corpus.Vectors], 10), "<base-sha>", m8QualificationFrozenBaseSHAV1, "<head-sha>", strings.Repeat("a", 40), "/mnt/fast4tb/gomap-4024-kahip-3.25/bin/python", os.Args[0])
		for _, name := range []string{"m3_graph_disjoint", "m3_graph_overlap", "m3_stable_hash_disjoint"} {
			args := strings.Fields(replace.Replace(plan.Commands[name]))
			cfg, err := parseConfig(args[1:])
			if err != nil || cfg.baseSHA != m8QualificationFrozenBaseSHAV1 || cfg.headSHA != strings.Repeat("a", 40) || cfg.maxVectors != corpus.MaxVectors || cfg.maxBytes != corpus.MaxFixtureBytes || cfg.partition != partition || cfg.routerConfig != router || cfg.m3MaxBenchmarkVisits != visits {
				t.Fatalf("%dk %s config err=%v cfg=%+v", corpus.Vectors, name, err, cfg)
			}
		}
		m8Replace := strings.NewReplacer("<campaign-root>", t.TempDir(), "<corpus>", corpus.ID, "<dataset>", t.TempDir(), "<truth-cache>", t.TempDir(), "<truth-cache-sha256>", strings.Repeat("b", 64), "<graph-disjoint-db>", t.TempDir(), "<graph-overlap-020-db>", t.TempDir(), "<stable-id-hash-disjoint-db>", t.TempDir(), "<m8-exact-truth-cap>", strconv.FormatInt(corpus.M8Cap, 10), "<max-vectors>", strconv.Itoa(corpus.MaxVectors), "<max-fixture-bytes>", strconv.FormatInt(corpus.MaxFixtureBytes, 10), "<seed>", strconv.FormatInt(map[int]int64{100000: 4017, 250000: 4016}[corpus.Vectors], 10), "<base-sha>", m8QualificationFrozenBaseSHAV1, "<head-sha>", strings.Repeat("a", 40))
		args := strings.Fields(m8Replace.Replace(plan.Commands["m8_matrix_repeats_full_ladder"]))
		cfg, err := parseConfig(args[1:])
		if err != nil || cfg.routerCandidates != m8QualificationRouterCandidatesV1 || !slices.Equal(cfg.efSearch, []int{128}) {
			t.Fatalf("%dk M8 config err=%v cfg=%+v", corpus.Vectors, err, cfg)
		}
	}
	if !strings.Contains(plan.Validation, "regular retained inputs below that root") || !strings.Contains(plan.Validation, "one benchmark executable SHA-256") || !strings.Contains(plan.Validation, "every campaign, M8 child, M8 matrix, and M3 descriptor must match it") || !strings.Contains(plan.Validation, "exactly one explicit canonical -base-sha/-head-sha pair") || !strings.Contains(plan.Validation, "after each foreground matrix child exits") || !strings.Contains(plan.Validation, "publication_completed_at strictly after") || !strings.Contains(plan.Validation, "post-publication intervals") {
		t.Fatalf("plan does not bind the aggregate revision: %q", plan.Validation)
	}
}

func testM8QualificationMatrixV1(t *testing.T, head string, fixture fixtureManifest, p4QPS float64) m8ProductionMatrixV1 {
	t.Helper()
	variants := []struct {
		id, assignment string
		overlap        float64
	}{
		{"graph-disjoint-v1", partitionAssignmentGraphV1, 0},
		{"graph-overlap-020-v1", partitionAssignmentGraphV1, .2},
		{"stable-id-hash-disjoint-v1", partitionAssignmentStableIDHashV1, 0},
	}
	matrix := m8ProductionMatrixV1{Variants: make([]m8ProductionReportV1, 0, len(variants))}
	datasetDirectory, truthCacheDirectory := "", ""
	for _, v := range variants {
		descriptor := testM3VariantDescriptorV1(t.TempDir())
		descriptor.VariantID, descriptor.AssignmentBasis, descriptor.OverlapRatio = v.id, v.assignment, v.overlap
		descriptor.DatabaseDirectory = filepath.Join(os.TempDir(), "gomap-m8-qualification-retained-"+strings.ReplaceAll(t.Name(), "/", "-")+"-"+fixture.Checksum[:8], v.id)
		if err := os.MkdirAll(descriptor.DatabaseDirectory, 0o755); err != nil {
			t.Fatal(err)
		}
		if v.assignment == partitionAssignmentStableIDHashV1 {
			descriptor.ArtifactSHA256 = strings.Repeat("c", 64)
			descriptor.ArtifactBackend = "stable_id_hash_baseline_v1"
		} else {
			descriptor.ArtifactBackend = fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
			descriptor.KaHIPPythonSHA256 = m8QualificationKaHIPPythonSHA256V1
			descriptor.KaHIPAdapterSHA256 = kahipAdapterSHA256
		}
		refreshTestM3VariantIdentityV1(t, &descriptor)
		report := testM8QualificationReportV1(t, head, fixture, descriptor, p4QPS)
		if _, err := m3ReadVariantDescriptorV1(report.Variant.DatabaseDirectory); err != nil {
			if err := os.Remove(filepath.Join(report.Variant.DatabaseDirectory, m3VariantDescriptorFileV1)); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if err := m3WriteVariantDescriptorV1(report.Variant.DatabaseDirectory, *report.Variant); err != nil {
				t.Fatal(err)
			}
		}
		if datasetDirectory == "" {
			datasetDirectory = report.DatasetDirectory
		} else {
			report.DatasetDirectory = datasetDirectory
		}
		if truthCacheDirectory == "" {
			truthCacheDirectory = report.TruthCacheDirectory
		} else {
			report.TruthCacheDirectory = truthCacheDirectory
		}
		report.GateLedger = m8ProductionGateLedgerForReportV1(report)
		matrix.Variants = append(matrix.Variants, report)
	}
	sourceCheckout := filepath.Join(filepath.Dir(datasetDirectory), "source")
	if err := os.MkdirAll(sourceCheckout, 0o755); err != nil {
		t.Fatal(err)
	}
	built, err := m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: commandWithProvenanceAndSourceCheckoutV1("m8-test", nil, head, head, sourceCheckout)}, fixture, matrix.Variants)
	if err != nil {
		t.Fatal(err)
	}
	return built
}

func testM8ValidateQualificationCampaignV1(root string, campaign m8QualificationCampaignV1) (m8QualificationCampaignSummaryV1, error) {
	return m8ValidateQualificationCampaignWithVerifiersV1(root, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, func(string, m8ProductionReportV1) ([][]m8CanonicalResultV1, error) { return nil, nil }, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	})
}

func testM8ValidateQualificationIndexV1(root string, index m8QualificationIndexV1) (m8QualificationIndexSummaryV1, error) {
	return m8ValidateQualificationIndexWithVerifiersV1(root, index, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, func(string, m8ProductionReportV1) ([][]m8CanonicalResultV1, error) { return nil, nil }, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	})
}

func TestM8QualificationBoundedJSONEvidenceV1(t *testing.T) {
	t.Run("index_cap_and_decode", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "index.json")
		if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := runValidateQualification([]string{"-index", path}, io.Discard); err == nil || !strings.Contains(err.Error(), "decode qualification index") {
			t.Fatalf("malformed index err=%v", err)
		}
		if err := os.WriteFile(path, []byte("{}{}"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := runValidateQualification([]string{"-index", path}, io.Discard); err == nil || !strings.Contains(err.Error(), "decode qualification index") {
			t.Fatalf("trailing index err=%v", err)
		}
		if err := os.Truncate(path, m8QualificationIndexMaxBytesV1+1); err != nil {
			t.Fatal(err)
		}
		if err := runValidateQualification([]string{"-index", path}, io.Discard); err == nil || !strings.Contains(err.Error(), "byte length") {
			t.Fatalf("oversized index err=%v", err)
		}
	})

	t.Run("matrix_cap_and_decode", func(t *testing.T) {
		root, head := t.TempDir(), m8QualificationFrozenBaseSHAV1
		path := filepath.Join(root, "repeat.json")
		campaign := m8QualificationCampaignV1{FixtureChecksum: m8QualificationFixturesV1[0].Checksum, BaseSHA: head, HeadSHA: head, Runs: []m8QualificationCampaignRunV1{
			{Path: "repeat.json", SHA256: strings.Repeat("a", 64)},
			{Path: "repeat-2.json", SHA256: strings.Repeat("b", 64)},
			{Path: "repeat-3.json", SHA256: strings.Repeat("c", 64)},
		}}
		if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs[0].SHA256 = hex.EncodeToString(digest[:])
		if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "decode qualification matrix") {
			t.Fatalf("malformed matrix err=%v", err)
		}
		if err := os.WriteFile(path, []byte("{}{}"), 0o644); err != nil {
			t.Fatal(err)
		}
		raw, err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		digest = sha256.Sum256(raw)
		campaign.Runs[0].SHA256 = hex.EncodeToString(digest[:])
		if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "decode qualification matrix") {
			t.Fatalf("trailing matrix err=%v", err)
		}
		if err := os.Truncate(path, m8QualificationMatrixMaxBytesV1+1); err != nil {
			t.Fatal(err)
		}
		if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "byte length") {
			t.Fatalf("oversized matrix err=%v", err)
		}
	})

	t.Run("transcript_cap_and_decode", func(t *testing.T) {
		report := testM8QualificationReportV1(t, m8QualificationFrozenBaseSHAV1, m8QualificationFixturesV1[0], testM3VariantDescriptorV1(t.TempDir()), 125)
		path := report.MeasurementTranscript.Path
		write := func(raw []byte) {
			t.Helper()
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			report.MeasurementTranscript.Bytes = int64(len(raw))
			report.MeasurementTranscript.SHA256 = hex.EncodeToString(digest[:])
		}
		write([]byte("{"))
		if validM8ProductionMeasurementTranscriptV1(report) {
			t.Fatal("accepted malformed transcript")
		}
		write([]byte("{}{}"))
		if validM8ProductionMeasurementTranscriptV1(report) {
			t.Fatal("accepted trailing transcript")
		}
		if err := os.Truncate(path, m8QualificationTranscriptMaxBytesV1+1); err != nil {
			t.Fatal(err)
		}
		if validM8ProductionMeasurementTranscriptV1(report) {
			t.Fatal("accepted oversized transcript")
		}
	})
	t.Run("transcript_outcome_shape", func(t *testing.T) {
		report := testM8QualificationReportV1(t, m8QualificationFrozenBaseSHAV1, m8QualificationFixturesV1[0], testM3VariantDescriptorV1(t.TempDir()), 125)
		raw, err := os.ReadFile(report.MeasurementTranscript.Path)
		if err != nil {
			t.Fatal(err)
		}
		var transcript m8ProductionMeasurementTranscriptV1
		if err := json.Unmarshal(raw, &transcript); err != nil {
			t.Fatal(err)
		}
		write := func(value m8ProductionMeasurementTranscriptV1) {
			t.Helper()
			raw, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(report.MeasurementTranscript.Path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			report.MeasurementTranscript.Bytes, report.MeasurementTranscript.SHA256 = int64(len(raw)), hex.EncodeToString(digest[:])
		}
		for name, mutate := range map[string]func(*m8ProductionMeasurementTranscriptV1){
			"missing":     func(value *m8ProductionMeasurementTranscriptV1) { value.Outcomes = nil },
			"rss_missing": func(value *m8ProductionMeasurementTranscriptV1) { value.PeakRSSObservations = nil },
			"rss_zero":    func(value *m8ProductionMeasurementTranscriptV1) { value.PeakRSSObservations[0] = 0 },
			"rss_extra": func(value *m8ProductionMeasurementTranscriptV1) {
				value.PeakRSSObservations = append(value.PeakRSSObservations, 1)
			},
			"duplicate": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TopKIDs[0][1] = value.Outcomes[0].TopKIDs[0][0]
			},
			"out_of_domain": func(value *m8ProductionMeasurementTranscriptV1) { value.Outcomes[0].TopKIDs[0][0] = "doc-999999" },
			"cell_mismatch": func(value *m8ProductionMeasurementTranscriptV1) { value.Outcomes[0].Probes++ },
			"timing_count": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TotalNanos = value.Outcomes[0].TotalNanos[:len(value.Outcomes[0].TotalNanos)-1]
			},
			"timing_zero": func(value *m8ProductionMeasurementTranscriptV1) { value.Outcomes[0].TotalNanos[0] = 0 },
			"score_count": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TopKScoreBits[0] = value.Outcomes[0].TopKScoreBits[0][:len(value.Outcomes[0].TopKScoreBits[0])-1]
			},
			"score_nonfinite": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TopKScoreBits[0][0] = math.Float32bits(float32(math.Inf(1)))
			},
		} {
			t.Run(name, func(t *testing.T) {
				value := transcript
				value.Outcomes = append([]m8ProductionRowOutcomesV1(nil), transcript.Outcomes...)
				value.Outcomes[0].TopKIDs = append([][]string(nil), transcript.Outcomes[0].TopKIDs...)
				value.Outcomes[0].TopKIDs[0] = append([]string(nil), transcript.Outcomes[0].TopKIDs[0]...)
				value.Outcomes[0].TopKScoreBits = append([][]uint32(nil), transcript.Outcomes[0].TopKScoreBits...)
				value.Outcomes[0].TopKScoreBits[0] = append([]uint32(nil), transcript.Outcomes[0].TopKScoreBits[0]...)
				value.Outcomes[0].TotalNanos = append([]uint64(nil), transcript.Outcomes[0].TotalNanos...)
				value.PeakRSSObservations = append([]uint64(nil), transcript.PeakRSSObservations...)
				mutate(&value)
				write(value)
				if validM8ProductionMeasurementTranscriptV1(report) {
					t.Fatal("accepted malformed transcript outcomes")
				}
			})
		}
	})
	t.Run("transcript_timings_recompute_percentiles", func(t *testing.T) {
		report := testM8QualificationReportV1(t, m8QualificationFrozenBaseSHAV1, m8QualificationFixturesV1[0], testM3VariantDescriptorV1(t.TempDir()), 125)
		raw, err := os.ReadFile(report.MeasurementTranscript.Path)
		if err != nil {
			t.Fatal(err)
		}
		var transcript m8ProductionMeasurementTranscriptV1
		if err := json.Unmarshal(raw, &transcript); err != nil {
			t.Fatal(err)
		}
		for name, mutate := range map[string]func(*m8ProductionRowV1){
			"percentile": func(row *m8ProductionRowV1) { row.P95Nanos-- },
			"maximum":    func(row *m8ProductionRowV1) { row.MaxTotalNanos-- },
		} {
			t.Run(name, func(t *testing.T) {
				value := report
				value.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
				mutate(&value.Rows[0])
				copy := transcript
				copy.Rows = append([]m8ProductionRowV1(nil), value.Rows...)
				raw, err := json.Marshal(copy)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(value.MeasurementTranscript.Path, raw, 0o644); err != nil {
					t.Fatal(err)
				}
				digest := sha256.Sum256(raw)
				value.MeasurementTranscript.Bytes, value.MeasurementTranscript.SHA256 = int64(len(raw)), hex.EncodeToString(digest[:])
				if validM8ProductionMeasurementTranscriptV1(value) {
					t.Fatal("accepted self-consistent retained percentile")
				}
			})
		}
	})
	t.Run("transcript_timings_bind_elapsed", func(t *testing.T) {
		report := testM8QualificationReportV1(t, m8QualificationFrozenBaseSHAV1, m8QualificationFixturesV1[0], testM3VariantDescriptorV1(t.TempDir()), 125)
		raw, err := os.ReadFile(report.MeasurementTranscript.Path)
		if err != nil {
			t.Fatal(err)
		}
		var transcript m8ProductionMeasurementTranscriptV1
		if err := json.Unmarshal(raw, &transcript); err != nil {
			t.Fatal(err)
		}
		var total uint64
		for _, duration := range transcript.Outcomes[0].TotalNanos {
			total += duration
		}
		report.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
		report.Rows[0].ElapsedNanos = total - 1
		report.Rows[0].QPS, _ = m8ProductionQPSV1(report.Rows[0].Samples, report.Rows[0].ElapsedNanos)
		transcript.Rows = append([]m8ProductionRowV1(nil), report.Rows...)
		raw, err = json.Marshal(transcript)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(report.MeasurementTranscript.Path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		report.MeasurementTranscript.Bytes, report.MeasurementTranscript.SHA256 = int64(len(raw)), hex.EncodeToString(digest[:])
		if validM8ProductionMeasurementTranscriptV1(report) {
			t.Fatal("accepted transcript with elapsed shorter than retained sequential totals")
		}
	})
}

func TestM8ProductionMeasurementTranscriptFrozenShapeV5(t *testing.T) {
	report := testM8QualificationReportV1(t, m8QualificationFrozenBaseSHAV1, m8QualificationFixturesV1[0], testM3VariantDescriptorV1(t.TempDir()), 125)
	if len(report.Rows) != 5 || report.Rows[0].Samples != 1000 || report.Config.TopK != 10 {
		t.Fatalf("unexpected frozen transcript shape: rows=%d samples=%d top_k=%d", len(report.Rows), report.Rows[0].Samples, report.Config.TopK)
	}
	if report.MeasurementTranscript.Bytes > m8QualificationTranscriptMaxBytesV1 {
		t.Fatalf("frozen transcript bytes=%d, cap=%d", report.MeasurementTranscript.Bytes, m8QualificationTranscriptMaxBytesV1)
	}
	if !validM8ProductionMeasurementTranscriptV1(report) {
		t.Fatal("rejected frozen v5 transcript")
	}
}

func TestM8QualificationTranscriptPeakRSSBindingV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, head := testM8QualificationGitCheckoutV1(t, root)
	fixture := m8QualificationFixturesV1[0]
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: head, HeadSHA: head}
	for repeat := 0; repeat < 3; repeat++ {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, repeat)
		dir := filepath.Join(root, fmt.Sprintf("repeat-%d", repeat))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		testM8QualificationProfilesV1(t, dir, "profiles", &matrix)
		testM8QualificationSetSourceCheckoutV1(t, root, &matrix)
		path := filepath.Join(dir, "matrix.json")
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: filepath.Join(filepath.Base(dir), "matrix.json"), SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
	}
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err != nil {
		t.Fatalf("rejected ordinary campaign: %v", err)
	}
	for i, run := range campaign.Runs {
		path := filepath.Join(root, run.Path)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		for j := range matrix.Variants {
			report := &matrix.Variants[j]
			raw, err := os.ReadFile(report.MeasurementTranscript.Path)
			if err != nil {
				t.Fatal(err)
			}
			var transcript m8ProductionMeasurementTranscriptV1
			if err := json.Unmarshal(raw, &transcript); err != nil {
				t.Fatal(err)
			}
			transcript.PeakRSSObservations[0] = m8QualificationPeakRSSCapBytesV1 + 1
			raw, err = json.Marshal(transcript)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(report.MeasurementTranscript.Path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			report.MeasurementTranscript.Bytes, report.MeasurementTranscript.SHA256 = int64(len(raw)), hex.EncodeToString(digest[:])
			// Forge the report back under the cap and refresh every report-derived
			// value; qualification must still use the retained raw observation.
			report.Resources.PeakRSSBytes = 1
			testM8CompleteResourceLimitsV1(t, report)
			report.GateLedger = m8ProductionGateLedgerForReportV1(*report)
			report.ExecutionEvidenceDigest, err = m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, report.Profiles.Artifacts, report.MeasurementTranscript.SHA256)
			if err != nil {
				t.Fatal(err)
			}
		}
		matrix, err = m8BuildProductionMatrixWithExecutionIntervalV1(config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: matrix.Command}, matrix.Dataset, matrix.Variants, matrix.ExecutionStartedAt, matrix.ExecutionCompletedAt)
		if err != nil {
			t.Fatal(err)
		}
		raw, err = json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs[i].SHA256 = hex.EncodeToString(digest[:])
	}
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "unbound environment or resources") {
		t.Fatalf("accepted fully rehashed lowered-RSS campaign: %v", err)
	}
}

func TestM8QualificationTruthCacheAnchorV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture := m8QualificationFixturesV1[0]
	dir, evidence := testM8QualificationTruthCacheV1(t, root, fixture)
	report := m8ProductionReportV1{Dataset: fixture, TruthCacheDirectory: dir, TruthCache: evidence, Config: m8ProductionConfigEvidenceV1{TopK: 10}, Variant: &m3VariantDescriptorV1{SourceRows: uint64(fixture.Vectors)}}
	anchor := m8QualificationTruthCacheAnchorV1
	path := m8TruthCacheArtifactPathV1(dir, evidence.Identity)
	truth, artifact, err := m8ReadTruthCacheV1(path, fixture, fixture.Queries, 10, uint64(fixture.Vectors), evidence.ArtifactSHA256)
	if err != nil {
		t.Fatal(err)
	}
	truthSHA, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	localAnchor := m8QualificationTruthAnchorV1{Identity: evidence.Identity, ArtifactSHA256: artifact, TruthSHA256: truthSHA}
	if err := m8QualificationTruthCacheWithAnchorV1(root, report, localAnchor); err != nil {
		t.Fatalf("rejected matching bounded-decoded cache: %v", err)
	}
	if _, ok := anchor(fixture); !ok {
		t.Fatal("missing frozen 100k cache anchor")
	}
	var cache m8TruthCacheFileV1
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cache); err != nil {
		t.Fatal(err)
	}
	cache.Truth[0][0].Score += .125
	cache.TruthSHA256, err = m8TruthContentSHA256V1(cache.Truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	report.TruthCache.ArtifactSHA256 = hex.EncodeToString(digest[:])
	if err := m8QualificationTruthCacheWithAnchorV1(root, report, localAnchor); err == nil || !strings.Contains(err.Error(), "frozen corpus anchor") {
		t.Fatalf("accepted self-consistent forged truth cache/report: %v", err)
	}

	// Exercise the retained matrix/campaign path as well: all report-side
	// hashes, commands, transcripts, and matrix digests are refreshed after the
	// cache changes, but the independently frozen anchor must still win.
	campaignRoot := t.TempDir()
	_, campaignHead := testM8QualificationGitCheckoutV1(t, campaignRoot)
	cacheDir, cacheEvidence := testM8QualificationTruthCacheV1(t, campaignRoot, fixture)
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: campaignHead, HeadSHA: campaignHead}
	write := func(name string, matrix m8ProductionMatrixV1) {
		for i := range matrix.Variants {
			matrix.Variants[i].TruthCacheDirectory, matrix.Variants[i].TruthCache = cacheDir, cacheEvidence
			matrix.Variants[i].Command = testM8QualificationCommandV1(matrix.Variants[i], root)
		}
		testM8QualificationExecutionIDsV1(&matrix, len(campaign.Runs))
		testM8QualificationProfilesV1(t, campaignRoot, strings.TrimSuffix(name, ".json"), &matrix)
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(campaignRoot, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
	}
	for i := 0; i < 3; i++ {
		write(fmt.Sprintf("repeat-%d.json", i), testM8QualificationMatrixV1(t, campaign.HeadSHA, fixture, 125))
	}
	verify := func(root string, report m8ProductionReportV1) ([][]m8CanonicalResultV1, error) {
		return m8QualificationReadTruthCacheWithAnchorV1(root, report, localAnchor)
	}
	if _, err := m8ValidateQualificationCampaignWithVerifiersV1(campaignRoot, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, verify, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	}); err != nil {
		t.Fatalf("rejected anchored campaign: %v", err)
	}
	// Refresh every mutable report, transcript, execution, matrix, and campaign
	// digest after lowering a p4 percentile. The retained raw timings remain
	// untouched, so qualification must reject the self-consistent bundle.
	for i, run := range campaign.Runs {
		raw, err := os.ReadFile(filepath.Join(campaignRoot, run.Path))
		if err != nil {
			t.Fatal(err)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		for j := range matrix.Variants {
			report := &matrix.Variants[j]
			for row := range report.Rows {
				if report.Rows[row].Probes == 4 {
					report.Rows[row].P95Nanos--
				}
			}
			raw, err := os.ReadFile(report.MeasurementTranscript.Path)
			if err != nil {
				t.Fatal(err)
			}
			var transcript m8ProductionMeasurementTranscriptV1
			if err := json.Unmarshal(raw, &transcript); err != nil {
				t.Fatal(err)
			}
			transcript.Rows = report.Rows
			raw, err = json.Marshal(transcript)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(report.MeasurementTranscript.Path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			report.MeasurementTranscript.Bytes, report.MeasurementTranscript.SHA256 = int64(len(raw)), hex.EncodeToString(digest[:])
			report.GateLedger = m8ProductionGateLedgerForReportV1(*report)
			digestText, err := m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, report.Profiles.Artifacts, report.MeasurementTranscript.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			report.ExecutionEvidenceDigest = digestText
		}
		matrix, err = m8BuildProductionMatrixWithExecutionIntervalV1(config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: matrix.Command}, matrix.Dataset, matrix.Variants, matrix.ExecutionStartedAt, matrix.ExecutionCompletedAt)
		if err != nil {
			t.Fatal(err)
		}
		raw, err = json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(campaignRoot, run.Path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs[i].SHA256 = hex.EncodeToString(digest[:])
	}
	if _, err := m8ValidateQualificationCampaignWithVerifiersV1(campaignRoot, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, verify, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	}); err == nil {
		t.Fatal("accepted self-consistent lowered-p95 report/transcript/matrix/campaign")
	}
	// Refresh every mutable digest around a favorable p4 aggregate while leaving
	// the retained query outcomes and anchored truth untouched. Qualification
	// must reject at the outcome comparison, not accept the self-consistent
	// report/matrix/campaign hashes.
	for i, run := range campaign.Runs {
		raw, err := os.ReadFile(filepath.Join(campaignRoot, run.Path))
		if err != nil {
			t.Fatal(err)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		for j := range matrix.Variants {
			report := &matrix.Variants[j]
			for row := range report.Rows {
				if report.Rows[row].Probes == 4 {
					report.Rows[row].RecallAtK = .95
					report.Rows[row].Attribution.EndToEndRecallAtK = .95
					report.Rows[row].Attribution.ApproximateLocalToEndToEndLossAtK = .05
					report.Rows[row].Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(report.Rows[row].Attribution)
					report.Rows[row].Attribution.StageOwners = m8AttributionStageOwnersV1(report.Rows[row].Attribution)
				}
			}
			report.GateLedger = m8ProductionGateLedgerForReportV1(*report)
			if err := os.Remove(report.MeasurementTranscript.Path); err != nil {
				t.Fatal(err)
			}
			testM8QualificationTranscriptV1(t, filepath.Dir(report.MeasurementTranscript.Path), report)
			digest, err := m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, report.Profiles.Artifacts, report.MeasurementTranscript.SHA256)
			if err != nil {
				t.Fatal(err)
			}
			report.ExecutionEvidenceDigest = digest
		}
		matrix, err = m8BuildProductionMatrixWithExecutionIntervalV1(config{baseSHA: matrix.BaseSHA, headSHA: matrix.HeadSHA, partitions: matrix.Variants[0].Config.Partitions, command: matrix.Command}, matrix.Dataset, matrix.Variants, matrix.ExecutionStartedAt, matrix.ExecutionCompletedAt)
		if err != nil {
			t.Fatal(err)
		}
		raw, err = json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(campaignRoot, run.Path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs[i].SHA256 = hex.EncodeToString(digest[:])
	}
	if _, err := m8ValidateQualificationCampaignWithVerifiersV1(campaignRoot, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, verify, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "query outcomes") {
		t.Fatalf("accepted self-consistent forged recall/report/transcript/matrix/campaign: %v", err)
	}
	cachePath := m8TruthCacheArtifactPathV1(cacheDir, cacheEvidence.Identity)
	raw, err = os.ReadFile(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cache); err != nil {
		t.Fatal(err)
	}
	cache.Truth[0][0].Score += .125
	cache.TruthSHA256, err = m8TruthContentSHA256V1(cache.Truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cachePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(raw)
	cacheEvidence.ArtifactSHA256 = hex.EncodeToString(digest[:])
	for i, run := range campaign.Runs {
		raw, err := os.ReadFile(filepath.Join(campaignRoot, run.Path))
		if err != nil {
			t.Fatal(err)
		}
		var matrix m8ProductionMatrixV1
		if err := json.Unmarshal(raw, &matrix); err != nil {
			t.Fatal(err)
		}
		for j := range matrix.Variants {
			matrix.Variants[j].TruthCache.ArtifactSHA256 = cacheEvidence.ArtifactSHA256
			matrix.Variants[j].Command = testM8QualificationCommandV1(matrix.Variants[j], campaignRoot)
		}
		testM8QualificationProfilesV1(t, campaignRoot, fmt.Sprintf("forged-%d", i), &matrix)
		raw, err = json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(campaignRoot, run.Path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs[i].SHA256 = hex.EncodeToString(digest[:])
	}
	if _, err := m8ValidateQualificationCampaignWithVerifiersV1(campaignRoot, campaign, func(string, m8ProductionReportV1) error { return nil }, func(string, string, string, string) bool { return true }, verify, func(m8ProductionProfileEvidenceV1) bool { return true }, func(string, m8ProductionReportV1, [][]m8CanonicalResultV1, m8ProductionMeasurementTranscriptV1) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "frozen corpus anchor") {
		t.Fatalf("accepted self-consistent forged cache/report/matrix/campaign: %v", err)
	}
}

// testM8QualificationRetainedDescriptorV1 builds only the persisted M3 asset
// phase.  Qualification must open a real router/manifest, not accept a
// descriptor-shaped directory.
func testM8QualificationRetainedDescriptorV1(t *testing.T, dir, head string, fixture fixtureManifest, variantID, assignment string, ratio float64, sourceID ...func(int) string) m3VariantDescriptorV1 {
	t.Helper()
	const partitions = 16
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	vectors := fixtureVectors(fixture)
	input := make([]vectorpartition.Vector, len(vectors))
	valuesByID := make(map[string][]float64, len(vectors))
	for i := range vectors {
		id := fmt.Sprintf("doc-%06d", i)
		if len(sourceID) != 0 {
			id = sourceID[0](i)
		}
		input[i] = vectorpartition.Vector{ID: id, Values: vectors[i]}
		valuesByID[id] = vectors[i]
	}
	partition := vectorpartition.DefaultConfig()
	partition.Partitions, partition.Seed, partition.MaxDistanceWork = partitions, fixture.Seed, 20_000_000_000
	artifact, err := vectorpartition.BuildWithPartitioner(input, partition, vectorpartition.Source{SourceID: "qualification-test:" + fixture.Checksum}, vectorpartition.ReferencePartitioner{})
	if err != nil {
		t.Fatal(err)
	}
	graphArtifact := artifact
	if assignment == partitionAssignmentStableIDHashV1 {
		artifact, err = vectorpartition.BuildStableIDHashBaseline(artifact)
		if err != nil {
			t.Fatal(err)
		}
	}
	capacity, err := m3OverlapCapacityV1(artifact, ratio)
	if err != nil {
		t.Fatal(err)
	}
	overlap, err := vectorpartition.BuildOverlap(artifact, vectorpartition.OverlapConfig{Ratio: ratio, Capacity: capacity, RequireExact: true})
	if err != nil {
		t.Fatal(err)
	}
	if assignment == partitionAssignmentStableIDHashV1 {
		artifact.Backend = "stable_id_hash_baseline_v1"
	} else {
		artifact.Backend = fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
	}
	artifactDigest, err := vectorpartition.Digest(artifact)
	if err != nil {
		t.Fatal(err)
	}
	graphDigest, err := vectorpartition.Digest(graphArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if assignment == partitionAssignmentGraphV1 {
		graphDigest = artifactDigest
	}
	graphBuildDigest, err := m3GraphBuildSHA256V1(graphArtifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatal(err)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	meta := partitionCollectionMeta(m3BenchmarkCollection, fixture.Dimensions)
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(m3BenchmarkCollection)
	if err != nil {
		t.Fatal(err)
	}
	if len(sourceID) == 0 {
		if err := insertM3SourceRows(col, vectors); err != nil {
			t.Fatal(err)
		}
	} else {
		ids := make([][]byte, len(vectors))
		documents := make([][]byte, len(vectors))
		for i := range vectors {
			raw, err := json.Marshal(struct {
				TimeUS    int64     `json:"time_us"`
				Embedding []float32 `json:"embedding"`
			}{TimeUS: int64(i + 1), Embedding: m3Float32Vector(vectors[i])})
			if err != nil {
				t.Fatal(err)
			}
			ids[i], documents[i] = []byte(sourceID[0](i)), raw
		}
		if _, err := col.InsertBatch(ids, documents); err != nil {
			t.Fatal(err)
		}
	}
	if err := col.Flush(); err != nil {
		t.Fatal(err)
	}
	if status, err := col.RebuildVectorIndex(partitionHNSWIndex); err != nil || !status.Loaded {
		t.Fatalf("rebuild source index: status=%+v err=%v", status, err)
	}
	source, sourceRows, err := col.VectorPartitionSourceOrdinalsV1(partitionHNSWIndex)
	if err != nil {
		t.Fatal(err)
	}
	sourceOrdinals, err := m3SourceOrdinalsByArtifactID(artifact, sourceRows)
	if err != nil {
		t.Fatal(err)
	}
	routerConfig := vectorpartition.DefaultRouterConfigV1()
	routerConfig.Seed = fixture.Seed
	partitionConfig := partition
	descriptor := m3VariantDescriptorV1{SchemaVersion: 5, ResultKind: "m3_persistent_variant_descriptor_v5", FixtureChecksum: fixture.Checksum, ExecutableSHA256: strings.Repeat("e", 64), BaseSHA: head, HeadSHA: head, VariantID: variantID, AssignmentBasis: assignment, OverlapRatio: ratio, ArtifactSHA256: artifactDigest, GraphArtifactSHA256: graphDigest, GraphBuildSHA256: graphBuildDigest, ArtifactBackend: artifact.Backend, Source: artifact.Source, DatabaseDirectory: dir, IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]), PartitionHNSWM: partitionHNSWDegree, PartitionConfig: partitionConfig, PartitionMaxDistanceWork: partitionConfig.MaxDistanceWork, RouterMaxScalarWork: 20_000_000_000, RouterConfig: routerConfig, M3MaxBenchmarkVisits: 400_000_000, Capacity: overlap.Capacity, OverlapRequested: overlap.Budget, OverlapUseful: overlap.Useful, OverlapFiller: overlap.Filler, EdgeCutBefore: overlap.EdgeCutBefore, EdgeCutAfter: overlap.EdgeCutAfter}
	if assignment == partitionAssignmentGraphV1 && strings.HasPrefix(artifact.Backend, "kahip_python_") {
		descriptor.KaHIPPythonSHA256 = m8QualificationKaHIPPythonSHA256V1
		descriptor.KaHIPAdapterSHA256 = kahipAdapterSHA256
	}
	descriptor.BuildIdentityDigest, err = m3VariantBuildIdentityDigestV1(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	generation := source.Generation + 1
	manifest, _, err := m3BuildingManifest(*meta, source, artifact, overlap, sourceOrdinals, generation, descriptor.BuildIdentityDigest)
	if err != nil {
		t.Fatal(err)
	}
	routerVectors := make([][]float64, len(artifact.IDs))
	for ordinal, id := range artifact.IDs {
		routerVectors[ordinal] = valuesByID[id]
	}
	routerPartitions, err := m3RouterPartitions(artifact, overlap, sourceOrdinals, routerVectors)
	if err != nil {
		t.Fatal(err)
	}
	fileID, err := m3PartitionAssetFileID(generation)
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]collections.VectorPartitionSearchAssetV1, partitions)
	for partition := range inputs {
		inputs[partition] = collections.VectorPartitionSearchAssetV1{Source: source, Generation: generation, PartitionID: uint32(partition), Dimensions: fixture.Dimensions}
	}
	assets, resources, err := col.MaterializeVectorPartitionLocalSearchAssetsV1(partitionHNSWIndex, manifest, fileID, inputs)
	if err != nil {
		t.Fatal(err)
	}
	if resources != nil {
		resources.Release()
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		t.Fatal(err)
	}
	routerFileID, err := m3RouterAssetFileID(generation)
	if err != nil {
		t.Fatal(err)
	}
	routerBuild, err := col.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, routerPartitions, collections.VectorPartitionRouterBuildOptionsV1{Config: routerConfig, AssetFileID: routerFileID, AssetPartID: uint64(partitions) + 1, M: partitionHNSWDegree, EfConstruction: 128, EfSearch: 128})
	if err != nil {
		t.Fatal(err)
	}
	if routerBuild.Generation != generation {
		t.Fatalf("router generation=%d want %d", routerBuild.Generation, generation)
	}
	router, _, err := col.OpenVectorPartitionRouterV1(partitionHNSWIndex)
	if err != nil {
		t.Fatal(err)
	}
	routerStatus := router.Status()
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	var persistent uint64
	for _, asset := range routerStatus.Manifest.Assets {
		persistent += asset.Bytes
	}
	persistent += routerStatus.Manifest.RouterAsset.Bytes
	descriptor.ManifestIntegrity, descriptor.ReadySetDigest = routerStatus.Manifest.IntegrityDigest, routerStatus.Manifest.ReadySetDigest
	descriptor.RouterAssetChecksum, descriptor.RouterModelDigest = routerStatus.Manifest.RouterAsset.Checksum, routerStatus.ModelDigest
	descriptor.SourceGeneration, descriptor.SourceChecksum, descriptor.SourceSchemaHash, descriptor.SourceRows = routerStatus.Manifest.SourceGeneration, routerStatus.Manifest.SourceChecksum, routerStatus.Manifest.SourceSchemaHash, routerStatus.Manifest.SourceRowCount
	descriptor.PartitionGeneration, descriptor.RouterGeneration, descriptor.Partitions = routerStatus.Manifest.Generation, routerStatus.Manifest.RouterGeneration, routerStatus.Manifest.PartitionCount
	descriptor.OverlapPolicy, descriptor.OverlapRealized, descriptor.OverlapRejected = routerStatus.Manifest.BalancePolicy, overlap.Used, overlap.Unspent
	descriptor.OverlapUnusedCapacity = descriptor.Capacity*int(descriptor.Partitions) - int(descriptor.SourceRows) - descriptor.OverlapRealized
	descriptor.PartitionLoads = append([]int(nil), overlap.Loads...)
	descriptor.OverlapMemberships, descriptor.RouterRepresentatives, descriptor.PersistentAssetBytes = len(routerStatus.Manifest.OverlapMemberships), uint64(len(routerStatus.Manifest.Representatives)), persistent
	if err := validateM3VariantDescriptorV1(descriptor); err != nil {
		t.Fatalf("descriptor: %v: %+v", err, descriptor)
	}
	if err := m3DescriptorMatchesManifestV1(descriptor, fixture, routerStatus.Manifest, routerStatus.ModelDigest, routerStatus.Config); err != nil {
		t.Fatal(err)
	}
	if err := m3WriteVariantDescriptorV1(dir, descriptor); err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func testM8QualificationDatasetDirectoryV1(t *testing.T, root string, fixture fixtureManifest) string {
	t.Helper()
	dir := filepath.Join(root, "dataset")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture_manifest.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	dir, err = m8CanonicalPathV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func testM8QualificationTruthCacheV1(t *testing.T, root string, fixture fixtureManifest) (string, m8TruthCacheEvidenceV1) {
	t.Helper()
	dir := filepath.Join(root, "truth-cache")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	dir, err := m8CanonicalPathV1(dir)
	if err != nil {
		t.Fatal(err)
	}
	identity := m8TruthCacheIdentityV1(fixture, 10)
	truth := make([][]m8CanonicalResultV1, fixture.Queries)
	for query := range truth {
		truth[query] = make([]m8CanonicalResultV1, 10)
		for rank := range truth[query] {
			truth[query][rank] = m8CanonicalResultV1{ID: fmt.Sprintf("doc-%06d", 9-rank), Score: float32(10 - rank)}
		}
	}
	truthSHA256, err := m8TruthContentSHA256V1(truth)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(m8TruthCacheFileV1{SchemaVersion: 1, Identity: identity, Contract: m8CanonicalTruthContractV1, DatasetChecksum: fixture.Checksum, Dimensions: fixture.Dimensions, Metric: fixture.Metric, TopK: 10, TruthSHA256: truthSHA256, Truth: truth})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(m8TruthCacheArtifactPathV1(dir, identity), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	return dir, m8TruthCacheEvidenceV1{Status: "reused", Identity: identity, ArtifactSHA256: hex.EncodeToString(digest[:]), LoadNanos: 1}
}

func TestM8QualificationRetainedVariantV1(t *testing.T) {
	if !collections.VectorPartitionNamespacePersistenceSupportedV1() {
		t.Skip("vector partition namespace persistence unsupported")
	}
	head, fixture := strings.Repeat("a", 40), m8QualificationFixturesV1[0]
	fixture.Vectors, fixture.Dimensions, fixture.Queries = 256, 8, 8
	_, queries := fixtureData(fixture)
	fixture.Checksum = fixtureChecksumFromData(fixtureVectors(fixture), queries)
	newReport := func(t *testing.T) (string, m8ProductionReportV1) {
		t.Helper()
		root := t.TempDir()
		descriptor := testM8QualificationRetainedDescriptorV1(t, filepath.Join(root, "m3"), head, fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0)
		truthDir, truth := testM8QualificationTruthCacheV1(t, root, fixture)
		return root, m8ProductionReportV1{Dataset: fixture, DatasetDirectory: testM8QualificationDatasetDirectoryV1(t, root, fixture), TruthCacheDirectory: truthDir, TruthCache: truth, Config: m8ProductionConfigEvidenceV1{TopK: 10}, Variant: &descriptor}
	}
	reject := func(t *testing.T, root string, report m8ProductionReportV1) {
		t.Helper()
		if err := m8QualificationRetainedVariantV1(root, report); err == nil {
			t.Fatal("accepted unavailable or tampered retained M3 assets")
		}
	}
	t.Run("valid", func(t *testing.T) {
		root, report := newReport(t)
		if err := m8QualificationRetainedVariantV1(root, report); err != nil {
			t.Fatalf("rejected real retained M3 assets: %v", err)
		}
	})
	t.Run("noncanonical_source_document_id", func(t *testing.T) {
		root := t.TempDir()
		descriptor := testM8QualificationRetainedDescriptorV1(t, filepath.Join(root, "m3"), head, fixture, "graph-disjoint-v1", partitionAssignmentGraphV1, 0, func(ordinal int) string {
			if ordinal == 128 { // Outside this fixture's retained truth IDs.
				return "doc-128"
			}
			return fmt.Sprintf("doc-%06d", ordinal)
		})
		truthDir, truth := testM8QualificationTruthCacheV1(t, root, fixture)
		report := m8ProductionReportV1{Dataset: fixture, DatasetDirectory: testM8QualificationDatasetDirectoryV1(t, root, fixture), TruthCacheDirectory: truthDir, TruthCache: truth, Config: m8ProductionConfigEvidenceV1{TopK: 10}, Variant: &descriptor}
		err := m8QualificationRetainedVariantV1(root, report)
		if err == nil || !strings.Contains(err.Error(), "invalid fixture document ID") {
			t.Fatalf("noncanonical retained source ID err=%v", err)
		}
	})
	t.Run("mutated_source_row", func(t *testing.T) {
		root, report := newReport(t)
		db, err := backenddb.Open(backenddb.Options{Dir: report.Variant.DatabaseDirectory, DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		manager := collections.NewCollectionManager(db)
		collection, err := manager.OpenCollection(m3BenchmarkCollection)
		if err == nil {
			err = collection.Delete([]byte("doc-000000"))
		}
		closeErr := db.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("mutate retained source row: %v %v", err, closeErr)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil {
			t.Fatal("accepted mutated retained source row")
		}
	})
	t.Run("mismatched_source_rows", func(t *testing.T) {
		root, report := newReport(t)
		report.Dataset.Seed++
		vectors, queries := fixtureData(report.Dataset)
		report.Dataset.Checksum = fixtureChecksumFromData(vectors, queries)
		raw, err := json.Marshal(report.Dataset)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(report.DatasetDirectory, "fixture_manifest.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		report.TruthCacheDirectory, report.TruthCache = testM8QualificationTruthCacheV1(t, root, report.Dataset)
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "verify retained M3 source rows") {
			t.Fatalf("mismatched retained source rows err=%v", err)
		}
	})
	t.Run("copied_descriptor_only", func(t *testing.T) {
		root, report := newReport(t)
		copy := *report.Variant
		copy.DatabaseDirectory = filepath.Join(root, "copied-descriptor")
		if err := os.MkdirAll(copy.DatabaseDirectory, 0o755); err != nil {
			t.Fatal(err)
		}
		refreshTestM3VariantIdentityV1(t, &copy)
		if err := m3WriteVariantDescriptorV1(copy.DatabaseDirectory, copy); err != nil {
			t.Fatal(err)
		}
		report.Variant = &copy
		reject(t, root, report)
	})
	t.Run("missing_descriptor", func(t *testing.T) {
		root, report := newReport(t)
		if err := os.Remove(filepath.Join(report.Variant.DatabaseDirectory, m3VariantDescriptorFileV1)); err != nil {
			t.Fatal(err)
		}
		reject(t, root, report)
	})
	t.Run("missing_partition_router_packs", func(t *testing.T) {
		root, report := newReport(t)
		if err := os.RemoveAll(backenddb.ColumnAssetRootDirPath(report.Variant.DatabaseDirectory)); err != nil {
			t.Fatal(err)
		}
		reject(t, root, report)
	})
	t.Run("tampered_partition_router_pack", func(t *testing.T) {
		root, report := newReport(t)
		var pack string
		err := filepath.Walk(backenddb.ColumnAssetRootDirPath(report.Variant.DatabaseDirectory), func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil || pack != "" || info.IsDir() {
				return walkErr
			}
			pack = path
			return nil
		})
		if err != nil || pack == "" {
			t.Fatalf("find retained partition/router pack: %v path=%q", err, pack)
		}
		if err := os.WriteFile(pack, []byte("tampered"), 0o600); err != nil {
			t.Fatal(err)
		}
		reject(t, root, report)
	})
	t.Run("replays_retained_attribution", func(t *testing.T) {
		root := t.TempDir()
		vectors := fixtureVectors(fixture)
		built, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 16)
		if err != nil {
			t.Fatal(err)
		}
		assetDirectory := filepath.Join(root, "m3")
		built.owned = false
		builtDirectory := built.dir
		if err := built.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(builtDirectory, assetDirectory); err != nil {
			t.Fatal(err)
		}
		report := m8ProductionReportV1{Dataset: fixture, Config: m8ProductionConfigEvidenceV1{TopK: 10}, Variant: &m3VariantDescriptorV1{DatabaseDirectory: assetDirectory}}
		assets, err := openM8ProductionExistingAssetSetV1(assetDirectory)
		if err != nil {
			t.Fatal(err)
		}
		report.Config.RouterCandidates = int(assets.status.Representatives)
		_, queries := fixtureData(report.Dataset)
		truth, err := m8ExactTruthFixtureV1(vectors, queries, report.Config.TopK)
		if err != nil {
			_ = assets.Close()
			t.Fatal(err)
		}
		primaryHomes, finalMemberships, err := m8TruthPartitionMembershipsByDocumentIDV1(assets, truth)
		if err != nil {
			_ = assets.Close()
			t.Fatal(err)
		}
		harness, err := newM8AttributionHarnessV1(assets)
		if err != nil {
			_ = assets.Close()
			t.Fatal(err)
		}
		oracles, err := m8MembershipOracleRecallCacheV1(truth, primaryHomes, finalMemberships, len(harness.searchers), 1)
		if err != nil {
			_ = harness.Close()
			_ = assets.Close()
			t.Fatal(err)
		}
		cell, err := m8BuildAttributionV1(context.Background(), assets, primaryHomes, finalMemberships, queries, truth, oracles, 1, 64, report.Config.TopK, int(assets.status.Representatives), make([][]m8CanonicalResultV1, len(queries)), harness)
		if closeErr := errors.Join(harness.Close(), assets.Close()); err == nil && closeErr != nil {
			err = closeErr
		}
		if err != nil {
			t.Fatal(err)
		}
		var recall float64
		for i := range truth {
			recall += m8CanonicalRecallV1(truth[i], cell.Local[i])
		}
		row := m8ProductionRowV1{Status: "pass", Probes: 1, EfSearch: 64, Samples: len(queries), RecallAtK: recall / float64(len(truth))}
		if err := m8AttachAttributionV1(&row, cell, cell.Local); err != nil {
			t.Fatal(err)
		}
		report.Rows = []m8ProductionRowV1{row}
		outcome := m8ProductionRowOutcomesV1{TopKIDs: make([][]string, len(cell.Local)), TopKScoreBits: make([][]uint32, len(cell.Local))}
		for query := range cell.Local {
			outcome.TopKIDs[query] = m8CanonicalIDsV1(cell.Local[query])
			outcome.TopKScoreBits[query] = make([]uint32, len(cell.Local[query]))
			for result := range cell.Local[query] {
				outcome.TopKScoreBits[query][result] = math.Float32bits(cell.Local[query][result].Score)
			}
		}
		transcript := m8ProductionMeasurementTranscriptV1{Outcomes: []m8ProductionRowOutcomesV1{outcome}}
		if err := m8QualificationRetainedAttributionV1(root, report, truth, transcript); err != nil {
			t.Fatalf("rejected retained attribution replay: %v", err)
		}
		for name, mutate := range map[string]func(*m8ProductionMeasurementTranscriptV1){
			"coordinator_id": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TopKIDs[0][0], value.Outcomes[0].TopKIDs[0][1] = value.Outcomes[0].TopKIDs[0][1], value.Outcomes[0].TopKIDs[0][0]
			},
			"coordinator_score": func(value *m8ProductionMeasurementTranscriptV1) {
				value.Outcomes[0].TopKScoreBits[0][0] ^= 1
			},
		} {
			t.Run(name, func(t *testing.T) {
				value := transcript
				value.Outcomes = append([]m8ProductionRowOutcomesV1(nil), transcript.Outcomes...)
				value.Outcomes[0].TopKIDs = append([][]string(nil), transcript.Outcomes[0].TopKIDs...)
				value.Outcomes[0].TopKIDs[0] = append([]string(nil), transcript.Outcomes[0].TopKIDs[0]...)
				value.Outcomes[0].TopKScoreBits = append([][]uint32(nil), transcript.Outcomes[0].TopKScoreBits...)
				value.Outcomes[0].TopKScoreBits[0] = append([]uint32(nil), transcript.Outcomes[0].TopKScoreBits[0]...)
				mutate(&value)
				if err := m8QualificationRetainedAttributionV1(root, report, truth, value); err == nil || !strings.Contains(err.Error(), "retained attribution") {
					t.Fatalf("accepted forged retained coordinator %s parity: %v", name, err)
				}
			})
		}
		report.Rows[0].Attribution.FinalMembershipOracleRecallAtK = 0
		report.Rows[0].Attribution.FinalMembershipOracleRegretAtK = 1
		report.Rows[0].Attribution.PrimaryToFinalMembershipGainAtK = -report.Rows[0].Attribution.PrimaryHomeOracleRecallAtK
		report.Rows[0].Attribution.FinalMembershipToExactLossAtK = -report.Rows[0].Attribution.ExactRepresentativeRecallAtK
		report.Rows[0].Attribution.ResidualLossOwners = m8AttributionLossOwnersV1(report.Rows[0].Attribution)
		report.Rows[0].Attribution.StageOwners = m8AttributionStageOwnersV1(report.Rows[0].Attribution)
		if err := m8QualificationRetainedAttributionV1(root, report, truth, transcript); err == nil || !strings.Contains(err.Error(), "retained attribution") {
			t.Fatalf("accepted forged retained attribution: %v", err)
		}
	})
}

func TestM8QualificationRetainedInputBoundaryV1(t *testing.T) {
	fixture := m8QualificationFixturesV1[0]
	newReport := func(t *testing.T) (string, m8ProductionReportV1) {
		t.Helper()
		root := t.TempDir()
		truthDir, truth := testM8QualificationTruthCacheV1(t, root, fixture)
		return root, m8ProductionReportV1{Dataset: fixture, DatasetDirectory: testM8QualificationDatasetDirectoryV1(t, root, fixture), TruthCacheDirectory: truthDir, TruthCache: truth, Config: m8ProductionConfigEvidenceV1{TopK: 10}, Variant: &m3VariantDescriptorV1{DatabaseDirectory: filepath.Join(root, "m3")}}
	}
	rewriteTruthCache := func(t *testing.T, report *m8ProductionReportV1, mutate func(*m8TruthCacheFileV1)) {
		t.Helper()
		path := m8TruthCacheArtifactPathV1(report.TruthCacheDirectory, report.TruthCache.Identity)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var cache m8TruthCacheFileV1
		if err := json.Unmarshal(raw, &cache); err != nil {
			t.Fatal(err)
		}
		mutate(&cache)
		raw, err = json.Marshal(cache)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		report.TruthCache.ArtifactSHA256 = hex.EncodeToString(digest[:])
	}
	t.Run("valid_dataset_reaches_database_check", func(t *testing.T) {
		root, report := newReport(t)
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "database is not a directory") {
			t.Fatalf("valid dataset boundary err=%v", err)
		}
	})
	t.Run("missing_dataset_manifest", func(t *testing.T) {
		root, report := newReport(t)
		if err := os.Remove(filepath.Join(report.DatasetDirectory, "fixture_manifest.json")); err != nil {
			t.Fatal(err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "dataset manifest") {
			t.Fatalf("missing dataset manifest err=%v", err)
		}
	})
	t.Run("changed_dataset_manifest", func(t *testing.T) {
		root, report := newReport(t)
		changed := fixture
		changed.Seed++
		raw, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(report.DatasetDirectory, "fixture_manifest.json"), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "does not match report") {
			t.Fatalf("changed dataset manifest err=%v", err)
		}
	})
	t.Run("dataset_outside_campaign_root", func(t *testing.T) {
		root, report := newReport(t)
		report.DatasetDirectory = testM8QualificationDatasetDirectoryV1(t, t.TempDir(), fixture)
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "dataset is outside qualification root") {
			t.Fatalf("outside dataset err=%v", err)
		}
	})
	t.Run("dataset_manifest_symlink", func(t *testing.T) {
		root, report := newReport(t)
		manifest := filepath.Join(report.DatasetDirectory, "fixture_manifest.json")
		raw, err := os.ReadFile(manifest)
		if err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "fixture_manifest.json")
		if err := os.WriteFile(outside, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(manifest); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, manifest); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "dataset manifest is not a regular file") {
			t.Fatalf("linked dataset manifest err=%v", err)
		}
	})
	t.Run("truth_cache_outside_campaign_root", func(t *testing.T) {
		root, report := newReport(t)
		report.TruthCacheDirectory, report.TruthCache = testM8QualificationTruthCacheV1(t, t.TempDir(), fixture)
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "truth cache is outside qualification root") {
			t.Fatalf("outside truth cache err=%v", err)
		}
	})
	t.Run("truth_cache_artifact_symlink", func(t *testing.T) {
		root, report := newReport(t)
		path := m8TruthCacheArtifactPathV1(report.TruthCacheDirectory, report.TruthCache.Identity)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), filepath.Base(path))
		if err := os.WriteFile(outside, raw, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, path); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "truth-cache artifact is not a regular file") {
			t.Fatalf("linked truth cache err=%v", err)
		}
	})
	t.Run("truth_cache_malformed_json", func(t *testing.T) {
		root, report := newReport(t)
		path := m8TruthCacheArtifactPathV1(report.TruthCacheDirectory, report.TruthCache.Identity)
		if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "validate canonical truth-cache artifact") {
			t.Fatalf("malformed truth cache err=%v", err)
		}
	})
	t.Run("truth_cache_semantic_invalid", func(t *testing.T) {
		root, report := newReport(t)
		rewriteTruthCache(t, &report, func(cache *m8TruthCacheFileV1) {
			cache.Truth[0][1].ID = cache.Truth[0][0].ID
			cache.TruthSHA256, _ = m8TruthContentSHA256V1(cache.Truth)
		})
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "semantic mismatch") {
			t.Fatalf("semantic truth cache err=%v", err)
		}
	})
	t.Run("truth_cache_identity_mismatch", func(t *testing.T) {
		root, report := newReport(t)
		rewriteTruthCache(t, &report, func(cache *m8TruthCacheFileV1) { cache.Identity = strings.Repeat("a", 64) })
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "identity/schema mismatch") {
			t.Fatalf("identity truth cache err=%v", err)
		}
	})
	t.Run("truth_cache_content_digest_mismatch", func(t *testing.T) {
		root, report := newReport(t)
		rewriteTruthCache(t, &report, func(cache *m8TruthCacheFileV1) { cache.TruthSHA256 = strings.Repeat("a", 64) })
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "truth_sha256 mismatch") {
			t.Fatalf("truth digest cache err=%v", err)
		}
	})
	t.Run("database_outside_campaign_root", func(t *testing.T) {
		root, report := newReport(t)
		report.Variant.DatabaseDirectory = t.TempDir()
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "outside qualification root") {
			t.Fatalf("outside retained database err=%v", err)
		}
	})
	t.Run("database_symlink_outside_campaign_root", func(t *testing.T) {
		root, report := newReport(t)
		link := filepath.Join(root, "escaped-m3")
		if err := os.Symlink(t.TempDir(), link); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		report.Variant.DatabaseDirectory = link
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "outside qualification root") {
			t.Fatalf("escaping retained database symlink err=%v", err)
		}
	})
	t.Run("database_subtree_symlink", func(t *testing.T) {
		root, report := newReport(t)
		if err := os.MkdirAll(report.Variant.DatabaseDirectory, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(report.Variant.DatabaseDirectory, "column_assets")); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if err := m8QualificationRetainedVariantV1(root, report); err == nil || !strings.Contains(err.Error(), "contains symlink") {
			t.Fatalf("retained database subtree symlink err=%v", err)
		}
	})
}

func TestM8QualificationRejectsDirtyM3VariantV1(t *testing.T) {
	root := t.TempDir()
	_, head := testM8QualificationGitCheckoutV1(t, root)
	fixture := m8QualificationFixturesV1[0]
	write := func(name string, matrix m8ProductionMatrixV1) m8QualificationCampaignRunV1 {
		testM8QualificationProfilesV1(t, root, strings.TrimSuffix(name, ".json"), &matrix)
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		return m8QualificationCampaignRunV1{Path: name, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)}
	}
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: head, HeadSHA: head}
	for i := 0; i < 3; i++ {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, i)
		campaign.Runs = append(campaign.Runs, write(fmt.Sprintf("clean-%d.json", i), matrix))
	}
	dirty := testM8QualificationMatrixV1(t, head, fixture, 125)
	testM8QualificationExecutionIDsV1(&dirty, 4)
	dirty.Variants[0].Variant.BuildDirty = true
	refreshTestM3VariantIdentityV1(t, dirty.Variants[0].Variant)
	dirty.Variants[0].GateLedger = m8ProductionGateLedgerForReportV1(dirty.Variants[0])
	var err error
	dirty, err = m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: []string{"m8-test"}}, fixture, dirty.Variants)
	if err != nil {
		t.Fatal(err)
	}
	campaign.Runs[0] = write("dirty.json", dirty)
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil {
		t.Fatal("accepted self-consistent dirty M3 descriptor")
	}
	mismatched := testM8QualificationMatrixV1(t, head, fixture, 125)
	testM8QualificationExecutionIDsV1(&mismatched, 3)
	for i := range mismatched.Variants {
		descriptor := mismatched.Variants[i].Variant
		descriptor.BaseSHA, descriptor.HeadSHA = strings.Repeat("b", 40), strings.Repeat("c", 40)
		refreshTestM3VariantIdentityV1(t, descriptor)
		mismatched.Variants[i].GateLedger = m8ProductionGateLedgerForReportV1(mismatched.Variants[i])
	}
	mismatched, err = m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: []string{"m8-test"}}, fixture, mismatched.Variants)
	if err != nil {
		t.Fatal(err)
	}
	campaign.Runs[0] = write("mismatched-m3-revision.json", mismatched)
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "retained M3 revision") {
		t.Fatalf("mismatched M3 revision err=%v", err)
	}
	wrongExecutable := testM8QualificationMatrixV1(t, head, fixture, 125)
	testM8QualificationExecutionIDsV1(&wrongExecutable, 2)
	for i := range wrongExecutable.Variants {
		descriptor := wrongExecutable.Variants[i].Variant
		descriptor.ExecutableSHA256 = strings.Repeat("b", 64)
		refreshTestM3VariantIdentityV1(t, descriptor)
		wrongExecutable.Variants[i].GateLedger = m8ProductionGateLedgerForReportV1(wrongExecutable.Variants[i])
	}
	wrongExecutable, err = m8BuildProductionMatrixV1(config{baseSHA: head, headSHA: head, partitions: 16, command: []string{"m8-test"}}, fixture, wrongExecutable.Variants)
	if err != nil {
		t.Fatal(err)
	}
	campaign.Runs[0] = write("mismatched-m3-executable.json", wrongExecutable)
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil || !strings.Contains(err.Error(), "retained M3 executable") {
		t.Fatalf("mismatched M3 executable err=%v", err)
	}
}

func TestM8QualificationRejectsEscapingTranscriptSymlinkV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, head := testM8QualificationGitCheckoutV1(t, root)
	fixture := m8QualificationFixturesV1[0]
	campaign := m8QualificationCampaignV1{FixtureChecksum: fixture.Checksum, BaseSHA: head, HeadSHA: head}
	var transcriptPath string
	for i := 0; i < 3; i++ {
		matrix := testM8QualificationMatrixV1(t, head, fixture, 125)
		testM8QualificationExecutionIDsV1(&matrix, i)
		testM8QualificationProfilesV1(t, root, fmt.Sprintf("transcript-%d", i), &matrix)
		if i == 0 {
			transcriptPath = matrix.Variants[0].MeasurementTranscript.Path
			if !m8QualificationMeasurementTranscriptV1(root, matrix.Variants[0]) {
				t.Fatal("rejected ordinary resolved in-root transcript")
			}
		}
		raw, err := json.Marshal(matrix)
		if err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("transcript-%d.json", i)
		if err := os.WriteFile(filepath.Join(root, path), raw, 0o644); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		campaign.Runs = append(campaign.Runs, m8QualificationCampaignRunV1{Path: path, SHA256: hex.EncodeToString(digest[:]), PublicationCompletedAt: matrix.ExecutionCompletedAt.Add(time.Nanosecond)})
	}
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err != nil {
		t.Fatalf("ordinary campaign err=%v", err)
	}
	raw, err := os.ReadFile(transcriptPath)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "transcript.json")
	if err := os.WriteFile(outside, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(transcriptPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, transcriptPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := testM8ValidateQualificationCampaignV1(root, campaign); err == nil {
		t.Fatal("accepted transcript symlink escaping campaign root")
	}
}

func TestM8QualificationProfilesResolveArtifactTargetsV1(t *testing.T) {
	root, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	matrix := testM8QualificationMatrixV1(t, strings.Repeat("a", 40), m8QualificationFixturesV1[0], 125)
	testM8QualificationProfilesV1(t, root, "profile-targets", &matrix)
	profiles := matrix.Variants[0].Profiles
	if _, ok := m8QualificationProfilesV1(root, profiles); !ok {
		t.Fatal("rejected ordinary in-root profiles")
	}
	path := profiles.Artifacts[0].Path
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.WriteFile(outside, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, ok := m8QualificationProfilesV1(root, profiles); ok {
		t.Fatal("accepted profile artifact symlink escaping campaign root")
	}
}

func TestM8QualificationVariantBackendV1(t *testing.T) {
	for _, fixture := range m8QualificationFixturesV1 {
		kahip := fmt.Sprintf("kahip_python_3.25_eco_symmetrized_v1_seed_%d", fixture.Seed)
		for _, variant := range []m3VariantDescriptorV1{
			{VariantID: "graph-disjoint-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: kahip, KaHIPPythonSHA256: m8QualificationKaHIPPythonSHA256V1, KaHIPAdapterSHA256: kahipAdapterSHA256},
			{VariantID: "graph-overlap-020-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: kahip, KaHIPPythonSHA256: m8QualificationKaHIPPythonSHA256V1, KaHIPAdapterSHA256: kahipAdapterSHA256},
			{VariantID: "stable-id-hash-disjoint-v1", AssignmentBasis: partitionAssignmentStableIDHashV1, ArtifactBackend: "stable_id_hash_baseline_v1"},
		} {
			if !m8QualificationVariantBackendV1(variant, fixture) {
				t.Fatalf("fixture=%d rejected expected backend %+v", fixture.Vectors, variant)
			}
		}
		for name, variant := range map[string]m3VariantDescriptorV1{
			"reference graph":  {VariantID: "graph-disjoint-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: "reference_deterministic_v1"},
			"wrong seed":       {VariantID: "graph-overlap-020-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: kahip + "0"},
			"missing python":   {VariantID: "graph-disjoint-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: kahip, KaHIPAdapterSHA256: kahipAdapterSHA256},
			"wrong adapter":    {VariantID: "graph-disjoint-v1", AssignmentBasis: partitionAssignmentGraphV1, ArtifactSHA256: "same", GraphArtifactSHA256: "same", ArtifactBackend: kahip, KaHIPPythonSHA256: m8QualificationKaHIPPythonSHA256V1, KaHIPAdapterSHA256: strings.Repeat("a", 64)},
			"stable relabel":   {VariantID: "stable-id-hash-disjoint-v1", AssignmentBasis: partitionAssignmentStableIDHashV1, ArtifactBackend: kahip},
			"stable execution": {VariantID: "stable-id-hash-disjoint-v1", AssignmentBasis: partitionAssignmentStableIDHashV1, ArtifactBackend: "stable_id_hash_baseline_v1", KaHIPPythonSHA256: m8QualificationKaHIPPythonSHA256V1},
		} {
			if m8QualificationVariantBackendV1(variant, fixture) {
				t.Fatalf("fixture=%d accepted %s backend", fixture.Vectors, name)
			}
		}
	}
}

func testM8QualificationExecutionIDsV1(matrix *m8ProductionMatrixV1, repeat int) {
	for i := range matrix.Variants {
		matrix.Variants[i].ExecutionID = strings.Repeat(string("123456789abcdef"[repeat*len(matrix.Variants)+i]), 32)
	}
}

func testM8QualificationReportV1(t *testing.T, head string, fixture fixtureManifest, descriptor m3VariantDescriptorV1, p4QPS float64) m8ProductionReportV1 {
	t.Helper()
	datasetDirectory, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	truthCacheDirectory, err := m8CanonicalPathV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wantOverlap := int(float64(fixture.Vectors) * descriptor.OverlapRatio)
	partitionConfig, routerConfig, visits, ok := m8QualificationM3BuildConfigV1(fixture)
	if !ok {
		t.Fatal("qualification build config")
	}
	loads := make([]uint64, 16)
	for row := 0; row < fixture.Vectors+wantOverlap; row++ {
		loads[row%len(loads)]++
	}
	descriptor.Partitions, descriptor.SourceRows, descriptor.Source.Vectors = 16, uint64(fixture.Vectors), fixture.Vectors
	descriptor.OverlapMemberships = wantOverlap
	descriptor.EdgeCutBefore = wantOverlap + 1
	descriptor.BaseSHA, descriptor.HeadSHA = head, head
	descriptor.PartitionConfig, descriptor.PartitionMaxDistanceWork = partitionConfig, partitionConfig.MaxDistanceWork
	descriptor.RouterConfig, descriptor.RouterMaxScalarWork, descriptor.M3MaxBenchmarkVisits = routerConfig, routerConfig.MaxScalarWork, visits
	descriptor.IndexDefinitionDigest = collections.VectorIndexDefinitionDigestV1(partitionCollectionMetaWithDegree(m3BenchmarkCollection, fixture.Dimensions, descriptor.PartitionHNSWM).VectorIndexes[0])
	descriptor.PartitionLoads = make([]int, len(loads))
	for i, load := range loads {
		descriptor.PartitionLoads[i] = int(load)
		descriptor.Capacity = max(descriptor.Capacity, int(load))
	}
	descriptor.FixtureChecksum, descriptor.PersistentAssetBytes = fixture.Checksum, 1
	refreshTestM3VariantIdentityV1(t, &descriptor)
	group := func(id string) nativewire.VectorPartitionM8ProductionGroupEvidenceV1 {
		return nativewire.VectorPartitionM8ProductionGroupEvidenceV1{GroupID: id, LeaderID: id + "-leader", NodeIDs: []string{id + "-a", id + "-b", id + "-c"}, CommitIndex: 1, ReadIndex: 1, AppliedIndex: 1, ReadEvidenceKind: "production", ProvesProductionConsensus: true, EndpointHits: 1}
	}
	identity := nativewire.VectorPartitionCoordinatorRouterSessionIdentityV1{Database: "default", Catalog: "default", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: "index-digest", SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: uint64(fixture.Vectors), PartitionGeneration: 5, ReadySetDigest: "ready-digest", RouterModelDigest: "model-digest"}
	warm := nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{Identity: identity, ColdOpens: 1, ManifestOpenAttempts: 1, Misses: 1, ReaderPins: 1, LeasePins: 1, LeaseReleases: 1}
	measured := warm
	rowProbes := []int{1, 2, 4, 8, 16}
	measured.Hits, measured.LeasePins, measured.LeaseReleases = uint64(fixture.Queries*len(rowProbes)), uint64(fixture.Queries*len(rowProbes)+1), uint64(fixture.Queries*len(rowProbes)+1)
	row := func(probes int, qps float64) m8ProductionRowV1 {
		attribution := m8ProductionAttributionV1{Contract: m8CanonicalResultContractV1, GlobalExactRecallAtK: 1, OracleStagesComplete: true, PrimaryHomeOracleRecallAtK: 1, FinalMembershipOracleRecallAtK: 1, ExactRepresentativeTruthHomeCoverageAtK: 1, TruthNeighborHomePairColocationAtK: 1, ExactRepresentativeFinalMembershipCoverageAtK: 1, TruthNeighborFinalMembershipPairColocationAtK: 1, ExactRepresentativeOverlapTruthContributionAtK: 1, ExactRepresentativeDuplicateMembershipCoverageAtK: 1, TruthNeighborRankRetentionAtK: slices.Repeat([]float64{1}, 10), ExhaustivePartitionRecallAtK: 1, ExhaustivePartitionIDParity: true, ExhaustivePartitionScoreParity: true, ExactRepresentativeRecallAtK: 1, ApproximateRepresentativeRecallAtK: 1, LocalHNSWRecallAtK: 1, ApproximateLocalHNSWRecallAtK: 1, EndToEndRecallAtK: 1, CoordinatorMergeIDParity: true, CoordinatorMergeScoreParity: true, ApproximateRouterCandidateBudget: m8QualificationRouterCandidatesV1, ApproximateRouterPartitionCoverageComplete: true, LocalHNSWSearches: uint64(fixture.Queries * probes), LocalHNSWCandidates: 1, ApproximateLocalHNSWSearches: uint64(fixture.Queries * probes), ApproximateLocalHNSWCandidates: 1, ResidualLossOwners: []string{"none_observed"}}
		attribution.StageOwners = m8AttributionStageOwnersV1(attribution)
		p95 := uint64(80 + probes + int(p4QPS-120))
		elapsedNanos := uint64(float64(fixture.Queries) * float64(time.Second) / qps)
		derivedQPS, ok := m8ProductionQPSV1(fixture.Queries, elapsedNanos)
		if !ok {
			t.Fatal("derive qualification QPS")
		}
		return m8ProductionRowV1{Status: "pass", VariantID: descriptor.VariantID, Overlap: descriptor.OverlapRatio, Probes: probes, EfSearch: 128, Concurrency: 1, Samples: fixture.Queries, RecallAtK: 1, QPS: derivedQPS, ElapsedNanos: elapsedNanos, P50Nanos: p95 - 1, P95Nanos: p95, P99Nanos: p95 + 1, MaxTotalNanos: p95 + 2, RouterMode: collections.VectorPartitionRouterModeApproxV1, RouterCandidates: m8QualificationRouterCandidatesV1, ExactParityChecked: probes == 16, ExactParityPassed: probes == 16, NoPartialResults: true, Attribution: attribution}
	}
	diagnostics := make([]m8PartitionPackDiagnosticsV1, len(loads))
	for i, load := range loads {
		diagnostics[i] = m8PartitionPackDiagnosticsV1{PartitionID: uint32(i), Rows: load, ReachableRows: load, TraversalRoots: 1}
	}
	rows := make([]m8ProductionRowV1, 0, len(rowProbes))
	for _, probes := range rowProbes {
		qps := 100.0
		if probes == 2 {
			qps = p4QPS
		}
		rows = append(rows, row(probes, qps))
	}
	report := m8ProductionReportV1{SchemaVersion: 4, ResultKind: "m8_production_multi_group_evidence_v4", Mode: m8ProductionMultiGroupModeV1, ProductionEvidence: true, GeneratedAt: time.Now(), ExecutionID: strings.Repeat("f", 32), Command: []string{"m8-test"}, ExecutableSHA256: descriptor.ExecutableSHA256, BaseSHA: head, HeadSHA: head, GoVersion: "go1.test", GOOS: "linux", GOARCH: "amd64", LogicalCPUs: 1, GOMAXPROCS: 1, GoMemoryLimitBytes: 1, Host: m8ProductionHostEvidenceV1{CPUModel: "test"}, Dataset: fixture, DatasetDirectory: datasetDirectory, TruthCacheDirectory: truthCacheDirectory, Variant: &descriptor, Config: m8ProductionConfigEvidenceV1{RaftGroups: 4, RaftNodesPerGroup: 3, Partitions: 16, Probes: rowProbes, Overlap: []float64{descriptor.OverlapRatio}, TopK: 10, RecallTarget: .90, Concurrency: []int{1}, Warmup: 0, EffectiveWarmup: 0, EfSearch: []int{128}, RouterCandidates: m8QualificationRouterCandidatesV1, MaxExactTruthVisits: m8QualificationExactTruthCapV1(fixture), Seed: fixture.Seed}, BuildNanos: 1, Topology: nativewire.VectorPartitionM8ProductionMultiGroupEvidenceV1{Network: "tcp_loopback_serialized_m5_v1", LifecycleState: "active", ReadySetDigest: strings.Repeat("c", 64), MetaGroup: "meta", MetaLeader: "meta-leader", MetaNodes: []string{"meta-a", "meta-b", "meta-c"}, MaxConcurrentShardRequests: 1, Groups: []nativewire.VectorPartitionM8ProductionGroupEvidenceV1{group("group-a"), group("group-b"), group("group-c"), group("group-d")}}, RouterSessions: m8ProductionRouterSessionEvidenceV1{AfterWarmup: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{warm}, AfterMeasured: []nativewire.VectorPartitionCoordinatorRouterSessionStatsV1{measured}}, Rows: rows, PackDiagnostics: diagnostics, UntimedBoundary: m8ProductionResourceBoundaryV1{SelectedPartitions: 16, EfSearch: 10, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 1, RPCs: 1, RequestBytes: 1, ShardPartitions: 1, ShardRequestBytes: 1}}, Failure: m8ProductionFailureEvidenceV1{Passed: true, Error: "unavailable", ResourceBoundary: m8ProductionFaultResourceBoundaryV1{SelectedPartitions: 16, EfSearch: 4096, WallClockNanos: 1, Maxima: m8ProductionResourceObservedMaximaV1{Requests: 1, RPCs: 1, RequestBytes: 1, ShardPartitions: 1, ShardRequestBytes: 1}}}, GateLedger: m8ProductionGateLedgerV1{ExhaustiveParity: "pass", FailureHonesty: "pass", PartitionPackReachability: "pass", Recall: "pass", ProbeReduction: "pass", EndToEndQPS: "pass", TailLatency: "pass", Balance: "pass", ResourceBounds: "pass"}, Resources: m8ProductionResourceEvidenceV1{PersistentAssetBytes: descriptor.PersistentAssetBytes, PersistentAssetCap: m8QualificationPersistentAssetCapBytesV1, PartitionLoads: loads, PeakRSSBytes: 1, PeakRSSCapBytes: m8QualificationPeakRSSCapBytesV1, PeakRSSMeasured: true, PeakRSSScope: m8PeakRSSScopeV1, OverlapMemberships: wantOverlap, MaxPartitionLoad: uint64((fixture.Vectors + wantOverlap + 15) / 16), BalanceHardCap: uint64((fixture.Vectors + wantOverlap + 15) / 16), LimitComparisons: []m8ProductionResourceLimitComparisonV1{{Name: "test", Configured: 1, Passed: true}}}, TruthCache: m8TruthCacheEvidenceV1{Status: "computed", Identity: m8TruthCacheIdentityV1(fixture, 10), ArtifactSHA256: strings.Repeat("d", 64), ComputeNanos: 1}, TimedBoundary: "measured", Limitations: []string{"test"}}
	report.RouterRepresentatives = descriptor.RouterRepresentatives
	report.Topology.ReadySetDigest = descriptor.ReadySetDigest
	testM8BindRouterSessionsVariantV1(&report.RouterSessions, descriptor, report.Topology.ReadySetDigest)
	testM8CompleteResourceLimitsV1(t, &report)
	report.Command = testM8QualificationCommandV1(report, t.TempDir())
	if err := validateM3VariantDescriptorV1(descriptor); err != nil {
		t.Fatalf("qualification descriptor: %v: %+v", err, descriptor)
	}
	testM8QualificationTranscriptV1(t, t.TempDir(), &report)
	if err := validateM8ProductionReportV1(report, m8QualificationResourceCapsV1()); err != nil {
		t.Fatalf("valid qualification report rejected: %v", err)
	}
	return report
}

func testM8QualificationCommandV1(report m8ProductionReportV1, out string) []string {
	cfg := report.Config
	args := []string{
		filepath.Join(report.DatasetDirectory, "treedb_vector_partition_bench"), "-mode", m8ProductionMultiGroupModeV1,
		"-source-checkout", filepath.Join(out, "source"),
		"-dataset", report.DatasetDirectory, "-m8-existing-db", report.Variant.DatabaseDirectory, "-m8-truth-cache", report.TruthCacheDirectory, "-out", out, "-partitions", "16", "-probes", "1,2,4,8,16",
		"-overlap", strconv.FormatFloat(report.Variant.OverlapRatio, 'g', -1, 64), "-top-k", "10",
		"-recall-target", ".9", "-seed", strconv.FormatInt(cfg.Seed, 10), "-raft-groups", "4",
		"-raft-nodes-per-group", "3", "-concurrency", "1", "-warmup", "0", "-ef-search", "128",
		"-router-candidates", strconv.Itoa(m8QualificationRouterCandidatesV1),
		"-m8-max-rss-bytes", strconv.FormatUint(report.Resources.PeakRSSCapBytes, 10),
		"-m8-max-persistent-asset-bytes", strconv.FormatUint(report.Resources.PersistentAssetCap, 10),
		"-m8-max-exact-truth-visits", strconv.FormatInt(cfg.MaxExactTruthVisits, 10),
		"-max-vectors", strconv.Itoa(report.Dataset.Vectors),
		"-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10),
	}
	if report.Profiles.Directory != "" {
		args = append(args, "-profiles", report.Profiles.Directory)
	}
	args = append(args, "-m8-truth-cache-sha256", report.TruthCache.ArtifactSHA256)
	args = append(args, "-base-sha", report.BaseSHA, "-head-sha", report.HeadSHA)
	return args
}

func testM8QualificationReplaceCommandFlagV1(t *testing.T, args []string, flag, value string) {
	t.Helper()
	for i := range args {
		if args[i] == flag && i+1 < len(args) {
			args[i+1] = value
			return
		}
	}
	t.Fatalf("missing %s", flag)
}

func testM8QualificationRemoveCommandFlagV1(t *testing.T, args *[]string, flag string) {
	t.Helper()
	for i := range *args {
		if (*args)[i] == flag && i+1 < len(*args) {
			*args = append((*args)[:i:i], (*args)[i+2:]...)
			return
		}
	}
	t.Fatalf("missing %s", flag)
}

func testM8QualificationProfilesV1(t *testing.T, root, run string, matrix *m8ProductionMatrixV1) {
	t.Helper()
	var err error
	root, err = m8CanonicalPathV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatal(err)
	}
	templateDir := filepath.Join(root, "profile-template")
	templatePaths := make([]string, 0, len(m8ProfileArtifactNamesV1))
	for _, name := range m8ProfileArtifactNamesV1 {
		templatePaths = append(templatePaths, filepath.Join(templateDir, name))
	}
	created := false
	if _, err := os.Stat(templatePaths[0]); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(templateDir, 0o755); err != nil {
			t.Fatal(err)
		}
		capture, err := startM8ProfileCaptureV1(templateDir)
		if err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
		if _, err := capture.Stop(); err != nil {
			t.Fatal(err)
		}
		templateDir = capture.dir
		for i, name := range m8ProfileArtifactNamesV1 {
			templatePaths[i] = filepath.Join(templateDir, name)
		}
		created = true
	} else if err != nil {
		t.Fatal(err)
	}
	var templateArtifacts []m8ProductionProfileArtifactV1
	if created {
		var err error
		templateArtifacts, err = m8ProfileArtifactsV1(templatePaths)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		templateArtifacts = make([]m8ProductionProfileArtifactV1, 0, len(templatePaths))
		for _, path := range templatePaths {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			templateArtifacts = append(templateArtifacts, m8ProductionProfileArtifactV1{Path: path, Bytes: info.Size(), SHA256: hex.EncodeToString(digest[:])})
		}
	}
	profileSets := make(map[string]bool, len(matrix.Variants))
	for i := range matrix.Variants {
		report := &matrix.Variants[i]
		directory := filepath.Join(root, "profiles", run, report.Variant.VariantID)
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		directory, err := m8CanonicalPathV1(directory)
		if err != nil {
			t.Fatal(err)
		}
		artifacts := make([]m8ProductionProfileArtifactV1, 0, len(templateArtifacts))
		captured := make([]string, 0, len(templateArtifacts))
		for _, artifact := range templateArtifacts {
			raw, err := os.ReadFile(artifact.Path)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(directory, filepath.Base(artifact.Path))
			if err := os.WriteFile(path, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			artifact.Path = path
			artifacts = append(artifacts, artifact)
			captured = append(captured, path)
		}
		for j := range artifacts {
			if filepath.Base(artifacts[j].Path) != "allocs.pprof" {
				continue
			}
			if err := writeM8RuntimeProfileV1("allocs", artifacts[j].Path); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(artifacts[j].Path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(artifacts[j].Path)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(raw)
			artifacts[j].Bytes, artifacts[j].SHA256 = info.Size(), hex.EncodeToString(digest[:])
			break
		}
		profileSet, err := m8ProductionProfileSetDigestV1(artifacts)
		if err != nil || profileSets[profileSet] {
			t.Fatalf("profile set=%q err=%v", profileSet, err)
		}
		profileSets[profileSet] = true
		report.Profiles = m8ProductionProfileEvidenceV1{Directory: directory, Captured: captured, Artifacts: artifacts, Status: "captured_production_query_and_fault_boundary", Scope: "test profile capture"}
		report.Command = append(testM8QualificationCommandV1(*report, root), "-m8-matrix-out", root, "-m8-matrix-profiles", filepath.Dir(directory))
		testM8QualificationReplaceCommandFlagV1(t, report.Command, "-source-checkout", filepath.Join(root, "source"))
		testM8QualificationTranscriptV1(t, directory, report)
		digest, err := m8ProductionExecutionEvidenceDigestV1(report.ExecutionID, artifacts, report.MeasurementTranscript.SHA256)
		if err != nil {
			t.Fatal(err)
		}
		report.ExecutionEvidenceDigest = digest
	}
	matrix.Command = testM8QualificationMatrixCommandV1(*matrix, root)
}

func testM8QualificationSetSourceCheckoutV1(t *testing.T, root string, matrix *m8ProductionMatrixV1) {
	t.Helper()
	root, err := m8CanonicalPathV1(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range matrix.Variants {
		testM8QualificationReplaceCommandFlagV1(t, matrix.Variants[i].Command, "-source-checkout", filepath.Join(root, "source"))
	}
	testM8QualificationReplaceCommandFlagV1(t, matrix.Command, "-source-checkout", filepath.Join(root, "source"))
}

func testM8QualificationMatrixCommandV1(matrix m8ProductionMatrixV1, out string) []string {
	report := matrix.Variants[0]
	variantDBs := make([]string, 0, len(m8RequiredVariantIDsV1))
	for _, variantID := range m8RequiredVariantIDsV1 {
		for i := range matrix.Variants {
			if matrix.Variants[i].Variant.VariantID == variantID {
				variantDBs = append(variantDBs, matrix.Variants[i].Variant.DatabaseDirectory)
				break
			}
		}
	}
	cfg := report.Config
	args := []string{
		filepath.Join(report.DatasetDirectory, "treedb_vector_partition_bench"), "-mode", m8ProductionMultiGroupModeV1,
		"-source-checkout", filepath.Join(out, "source"),
		"-dataset", report.DatasetDirectory, "-m8-truth-cache", report.TruthCacheDirectory, "-m8-variant-dbs", strings.Join(variantDBs, ","), "-out", out, "-partitions", "16", "-probes", "1,2,4,8,16",
		"-overlap", "0,.2", "-top-k", "10", "-recall-target", ".9", "-seed", strconv.FormatInt(cfg.Seed, 10),
		"-raft-groups", "4", "-raft-nodes-per-group", "3", "-concurrency", "1", "-warmup", "0", "-ef-search", "128", "-router-candidates", strconv.Itoa(m8QualificationRouterCandidatesV1),
		"-m8-max-rss-bytes", strconv.FormatUint(report.Resources.PeakRSSCapBytes, 10),
		"-m8-max-persistent-asset-bytes", strconv.FormatUint(report.Resources.PersistentAssetCap, 10),
		"-m8-max-exact-truth-visits", strconv.FormatInt(cfg.MaxExactTruthVisits, 10),
		"-max-vectors", strconv.Itoa(report.Dataset.Vectors),
		"-max-fixture-bytes", strconv.FormatInt(maxFixtureBytes, 10),
	}
	if report.Profiles.Directory != "" {
		args = append(args, "-profiles", filepath.Dir(report.Profiles.Directory))
	}
	args = append(args, "-m8-truth-cache-sha256", report.TruthCache.ArtifactSHA256)
	args = append(args, "-base-sha", matrix.BaseSHA, "-head-sha", matrix.HeadSHA)
	return args
}

func testM8QualificationTranscriptV1(t *testing.T, dir string, report *m8ProductionReportV1) {
	t.Helper()
	evidence, err := m8WriteProductionMeasurementTranscriptV1(dir, *report, testM8MeasurementCellsV1(*report))
	if err != nil {
		t.Fatal(err)
	}
	report.MeasurementTranscript = evidence
}

func testM8MeasurementCellsV1(report m8ProductionReportV1) []m8MeasuredCellV1 {
	cells := make([]m8MeasuredCellV1, 0, len(report.Rows))
	for rowIndex, row := range report.Rows {
		if row.Status != "pass" && row.Status != "fail" && row.Status != "candidate_coverage_shortfall" {
			continue
		}
		results := make([][]m8CanonicalResultV1, row.Samples)
		if row.Status != "candidate_coverage_shortfall" {
			for query := range results {
				results[query] = make([]m8CanonicalResultV1, min(report.Config.TopK, report.Dataset.Vectors))
				for rank := range results[query] {
					results[query][rank] = m8CanonicalResultV1{ID: fmt.Sprintf("doc-%06d", len(results[query])-1-rank)}
				}
			}
		}
		var durations []uint64
		if row.Status == "pass" || row.Status == "fail" {
			durations = make([]uint64, row.Samples)
			i50, ok50 := m8NearestRankPercentileIndexV1(uint64(row.Samples), 50)
			i95, ok95 := m8NearestRankPercentileIndexV1(uint64(row.Samples), 95)
			i99, ok99 := m8NearestRankPercentileIndexV1(uint64(row.Samples), 99)
			if !ok50 || !ok95 || !ok99 {
				panic("invalid test timing shape")
			}
			for i := range durations {
				switch {
				case uint64(i) <= i50:
					durations[i] = row.P50Nanos
				case uint64(i) <= i95:
					durations[i] = row.P95Nanos
				case uint64(i) <= i99:
					durations[i] = row.P99Nanos
				default:
					durations[i] = row.MaxTotalNanos
				}
			}
		}
		cells = append(cells, m8MeasuredCellV1{rowIndex: rowIndex, probes: row.Probes, efSearch: row.EfSearch, results: results, durations: durations})
	}
	return cells
}
