package docs_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type commandWALNativeWireAlignment struct {
	Version            int                                  `json:"version"`
	Owner              string                               `json:"owner"`
	Tracker            string                               `json:"tracker"`
	RelationshipValues []string                             `json:"relationship_values"`
	AckRecoverability  map[string]string                    `json:"ack_recoverability"`
	Entries            []commandWALNativeWireAlignmentEntry `json:"entries"`
}

type commandWALNativeWireAlignmentEntry struct {
	NativeWireCommand       string `json:"nativewire_command"`
	NativeWireFixture       string `json:"nativewire_fixture"`
	NativeWireFixtureSHA256 string `json:"nativewire_fixture_sha256"`
	CommandWALKind          string `json:"command_wal_kind"`
	CommandWALPayloadFormat string `json:"command_wal_payload_format"`
	LocalFixture            string `json:"local_fixture"`
	LocalFixtureSHA256      string `json:"local_fixture_sha256"`
	SupportMatrixStatus     string `json:"support_matrix_status"`
	Relationship            string `json:"relationship"`
	Notes                   string `json:"notes"`
}

func TestCommandWALNativeWireAlignmentManifestCoverage(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	matrix := loadCommandWALSupportMatrix(t)
	matrixByNativeWire := make(map[string]commandWALSupportEntry)
	for _, entry := range matrix.Entries {
		if entry.Surface == "nativewire" {
			matrixByNativeWire[entry.EntryPoint] = entry
		}
	}
	seen := make(map[string]struct{}, len(alignment.Entries))
	for _, entry := range alignment.Entries {
		if entry.NativeWireCommand == "" || entry.CommandWALKind == "" || entry.CommandWALPayloadFormat == "" {
			t.Fatalf("incomplete alignment entry: %+v", entry)
		}
		if _, ok := seen[entry.NativeWireCommand]; ok {
			t.Fatalf("duplicate native-wire alignment entry for %s", entry.NativeWireCommand)
		}
		seen[entry.NativeWireCommand] = struct{}{}
		matrixEntry, ok := matrixByNativeWire[entry.NativeWireCommand]
		if !ok {
			t.Fatalf("alignment entry %s missing from command WAL support matrix", entry.NativeWireCommand)
		}
		if matrixEntry.Status != entry.SupportMatrixStatus || matrixEntry.Command != entry.CommandWALKind {
			t.Fatalf("alignment entry %s disagrees with support matrix: alignment=%+v matrix=%+v", entry.NativeWireCommand, entry, matrixEntry)
		}
		if strings.HasPrefix(entry.Relationship, "local_only_") || entry.Relationship == "read_rejected_v1" {
			if entry.NativeWireFixture != "" || entry.NativeWireFixtureSHA256 != "" {
				t.Fatalf("%s relationship %s must not declare deterministic fixture %s", entry.NativeWireCommand, entry.Relationship, entry.NativeWireFixture)
			}
		} else {
			assertHexFixtureDigest(t, entry.NativeWireFixture, entry.NativeWireFixtureSHA256)
		}
		if entry.SupportMatrixStatus == "WAL-supported" && (entry.Relationship == "lowered_equivalent_v1" || entry.Relationship == "lowered_kind_only_v1") {
			if entry.LocalFixture == "" || entry.LocalFixtureSHA256 == "" {
				t.Fatalf("%s is WAL-supported without local command-WAL fixture", entry.NativeWireCommand)
			}
			assertHexFixtureDigest(t, entry.LocalFixture, entry.LocalFixtureSHA256)
		}
	}
	for command := range matrixByNativeWire {
		if _, ok := seen[command]; !ok {
			t.Fatalf("native-wire matrix command %s missing alignment entry", command)
		}
	}
}

func TestNativeWireAndLocalCommandDigestStable(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	allowed := make(map[string]struct{}, len(alignment.RelationshipValues))
	for _, value := range alignment.RelationshipValues {
		allowed[value] = struct{}{}
	}
	fixtureDigests := make(map[string]string)
	for _, entry := range alignment.Entries {
		if _, ok := allowed[entry.Relationship]; !ok {
			t.Fatalf("%s has unknown relationship %q", entry.NativeWireCommand, entry.Relationship)
		}
		if entry.Notes == "" {
			t.Fatalf("%s missing alignment notes", entry.NativeWireCommand)
		}
		if (entry.NativeWireFixture == "") != (entry.NativeWireFixtureSHA256 == "") {
			t.Fatalf("%s native-wire fixture and digest must both be empty or both be set: fixture=%q sha256=%q", entry.NativeWireCommand, entry.NativeWireFixture, entry.NativeWireFixtureSHA256)
		}
		if (entry.LocalFixture == "") != (entry.LocalFixtureSHA256 == "") {
			t.Fatalf("%s local fixture and digest must both be empty or both be set: fixture=%q sha256=%q", entry.NativeWireCommand, entry.LocalFixture, entry.LocalFixtureSHA256)
		}
		recordFixtureDigest(t, fixtureDigests, entry.NativeWireFixture, entry.NativeWireFixtureSHA256)
		recordFixtureDigest(t, fixtureDigests, entry.LocalFixture, entry.LocalFixtureSHA256)
		if (entry.Relationship == "future_rejected_v1" || entry.Relationship == "local_only_rejected_v1") && entry.LocalFixture != "" {
			t.Fatalf("%s is rejected/future but has local fixture %s", entry.NativeWireCommand, entry.LocalFixture)
		}
		if (entry.Relationship == "lowered_equivalent_v1" || entry.Relationship == "lowered_kind_only_v1") && entry.SupportMatrixStatus != "WAL-supported" {
			t.Fatalf("%s lowered relationship requires WAL-supported status", entry.NativeWireCommand)
		}
	}
	if len(fixtureDigests) == 0 {
		t.Fatal("alignment has no fixture digests")
	}
	for fixture, want := range fixtureDigests {
		assertHexFixtureDigest(t, fixture, want)
	}
}

func recordFixtureDigest(t *testing.T, digests map[string]string, fixture, sha256 string) {
	t.Helper()
	if fixture == "" {
		return
	}
	if existing, ok := digests[fixture]; ok && existing != sha256 {
		t.Fatalf("%s has conflicting fixture digests %s and %s", fixture, existing, sha256)
	}
	digests[fixture] = sha256
}

func TestNativeWireAckFlushedRequiresRootPublishAndAppliedLSN(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	for _, key := range []string{"visible", "flushed", "synced", "raft_committed"} {
		requireAckRecoverability(t, alignment, key)
	}
	if got := requireAckRecoverability(t, alignment, "visible"); !strings.Contains(got, "does not require root publication or AppliedLSN") {
		t.Fatalf("visible recoverability rule = %q", got)
	}
	if got := requireAckRecoverability(t, alignment, "flushed"); !strings.Contains(got, "roots and AppliedLSN") || !strings.Contains(got, "same backend commit") {
		t.Fatalf("flushed recoverability rule = %q", got)
	}
	protocol := readRepoText(t, "TreeDB/docs/spec/native-wire-protocol.md")
	normalizedProtocol := collapseWhitespace(protocol)
	assertContainsAll(t, normalizedProtocol, "visible recoverability doc",
		"`visible` means",
		"command-WAL recoverability",
		"does not require root publication",
		"`AppliedLSN` advancement",
	)
	assertContainsAll(t, normalizedProtocol, "flushed recoverability doc",
		"`flushed` means",
		"backend roots",
		"`AppliedLSN` advanced",
		"same backend commit",
	)
}

func requireAckRecoverability(t *testing.T, alignment commandWALNativeWireAlignment, key string) string {
	t.Helper()
	value, ok := alignment.AckRecoverability[key]
	if !ok {
		t.Fatalf("native-wire alignment missing ack_recoverability[%q]", key)
	}
	if value == "" {
		t.Fatalf("native-wire alignment has empty ack_recoverability[%q]", key)
	}
	return value
}

func TestNativeWirePostFramePublishFailureCommitAmbiguous(t *testing.T) {
	protocol := readRepoText(t, "TreeDB/docs/spec/native-wire-protocol.md")
	normalizedProtocol := collapseWhitespace(protocol)
	assertContainsAll(t, normalizedProtocol, "post-frame failure doc",
		"complete command frame",
		"required local boundary",
		"root publication",
		"`AppliedLSN` advancement",
		"`commit_ambiguous`",
		"`not_committed` after a complete command frame",
	)
}

func TestRaftApplyDoesNotReportRecoverableBeforeCommandWALAppliedLSN(t *testing.T) {
	walSpec := readRepoText(t, "TreeDB/docs/spec/user-command-wal.md")
	normalizedSpec := collapseWhitespace(walSpec)
	for _, required := range []string{
		"Native-wire and Raft alignment",
		"must lower to a local command-WAL frame and satisfy the requested local ack boundary before reporting local recoverability",
		"`raft_committed` is not local WAL append",
		"For the R3a local apply harness tracked by #1654",
		"lowers supported entries to local `CommandEnvelope` payloads",
		"advances future `ApplyProgress` or applied-index metadata only after the selected local recoverability boundary is satisfied",
	} {
		if !strings.Contains(normalizedSpec, required) {
			t.Fatalf("user-command WAL spec missing Raft/local recoverability rule: %q", required)
		}
	}
}

func TestR3aApplyCloseout3043DocumentsLocalBoundary(t *testing.T) {
	closeout := readRepoText(t, "TreeDB/docs/spec/r3a-apply-closeout-3043.md")
	normalized := collapseWhitespace(closeout)
	assertContainsAll(t, normalized, "R3a apply closeout #3043",
		"local-only harness evidence",
		"does not expose networked Raft",
		"records result/idempotency and apply-progress metadata only after local `AppliedCommandLSN` coverage is present",
		"`before-local-wal-append-v1`",
		"`after-local-wal-append-before-visible-v1`",
		"`after-visible-before-result-record-v1`",
		"`after-result-record-before-progress-v1`",
		"`after-progress-record-v1`",
		"`create_collection`",
		"`insert_batch`",
		"`replace_batch`",
		"`delete_batch`",
		"`update_bson_set` are not accepted",
		"`raft_committed` acknowledgement",
		"BenchmarkApplyCommittedEntryCloseout3043",
	)
}

func TestRaftRoadmapUsesCommandWALRecoverabilityBoundary(t *testing.T) {
	roadmap := readRepoText(t, "TreeDB/docs/spec/native-query-raft-roadmap.md")
	normalizedRoadmap := collapseWhitespace(roadmap)
	for _, forbidden := range []string{
		"CollectionWALRecoverable",
		"physical/root-delta WAL as the active plan",
	} {
		if strings.Contains(roadmap, forbidden) {
			t.Fatalf("native-query Raft roadmap still uses stale recoverability language %q", forbidden)
		}
	}
	assertContainsAll(t, normalizedRoadmap, "R3a command-WAL roadmap boundary",
		"The current R3a planning slice (#1654, with executable children #3037-#3043)",
		"user-command WAL `CommandEnvelope` payload",
		"selected local command-WAL/`AppliedLSN` recoverability boundary",
		"local command-WAL frame, normal executor effects, and selected root/`AppliedLSN` boundary",
	)
}

func TestRaftCommandEntryAndLocalCommandPayloadUseSharedCanonicalSchema(t *testing.T) {
	alignment := loadCommandWALNativeWireAlignment(t)
	for _, entry := range alignment.Entries {
		switch entry.Relationship {
		case "lowered_equivalent_v1", "lowered_kind_only_v1":
			if entry.SupportMatrixStatus != "WAL-supported" {
				t.Fatalf("%s lowered entry must stay supported: %+v", entry.NativeWireCommand, entry)
			}
		case "future_rejected_v1":
			if entry.SupportMatrixStatus != "WAL-rejected" {
				t.Fatalf("%s future entry must stay explicitly rejected: %+v", entry.NativeWireCommand, entry)
			}
		case "local_only_rejected_v1":
			if entry.SupportMatrixStatus != "WAL-rejected" {
				t.Fatalf("%s local-only rejected entry must document rejection: %+v", entry.NativeWireCommand, entry)
			}
		case "local_only_barrier_v1":
			if entry.SupportMatrixStatus != "WAL-supported" {
				t.Fatalf("%s local-only barrier entry must document barrier semantics: %+v", entry.NativeWireCommand, entry)
			}
		case "read_rejected_v1":
			if entry.SupportMatrixStatus != "read-only" {
				t.Fatalf("%s read-only entry must stay read-only: %+v", entry.NativeWireCommand, entry)
			}
		default:
			t.Fatalf("%s has unrecognized relationship %q", entry.NativeWireCommand, entry.Relationship)
		}
	}
}

func loadCommandWALNativeWireAlignment(t *testing.T) commandWALNativeWireAlignment {
	t.Helper()
	path := repoPath(t, "TreeDB/docs/spec/command-wal-nativewire-alignment.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read native-wire alignment manifest: %v", err)
	}
	var alignment commandWALNativeWireAlignment
	if err := json.Unmarshal(raw, &alignment); err != nil {
		t.Fatalf("decode native-wire alignment manifest: %v", err)
	}
	if alignment.Version != 1 || alignment.Owner == "" || alignment.Tracker == "" {
		t.Fatalf("incomplete native-wire alignment manifest: %+v", alignment)
	}
	if len(alignment.Entries) == 0 {
		t.Fatal("native-wire alignment manifest has no entries")
	}
	return alignment
}

func assertHexFixtureDigest(t *testing.T, fixture, want string) {
	t.Helper()
	if fixture == "" || want == "" {
		t.Fatalf("missing fixture digest: fixture=%q sha256=%q", fixture, want)
	}
	raw := readRepoText(t, fixture)
	compact := strings.Join(strings.Fields(raw), "")
	fixtureBytes, err := hex.DecodeString(compact)
	if err != nil {
		t.Fatalf("%s is not valid hex fixture: %v", fixture, err)
	}
	sum := sha256.Sum256(fixtureBytes)
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("%s sha256=%s want %s", fixture, got, want)
	}
}

func assertContainsAll(t *testing.T, haystack, label string, needles ...string) {
	t.Helper()
	for _, needle := range needles {
		if !strings.Contains(haystack, needle) {
			t.Fatalf("%s missing %q", label, needle)
		}
	}
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func readRepoText(t *testing.T, rel string) string {
	t.Helper()
	raw, err := os.ReadFile(repoPath(t, rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(raw)
}

func repoPath(t *testing.T, rel string) string {
	t.Helper()
	_, repoRoot := repoRoots(t)
	clean := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(clean) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		t.Fatalf("repo-relative path %q escapes repo root", rel)
	}
	path := filepath.Join(repoRoot, clean)
	relative, err := filepath.Rel(repoRoot, path)
	if err != nil {
		t.Fatalf("compute repo-relative path for %q: %v", rel, err)
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		t.Fatalf("repo-relative path %q resolves outside repo root: %s", rel, path)
	}
	return path
}
