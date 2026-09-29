package nativewire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// The owner has completed real shard work and is held before copying its wire
// response. The catalog leader then commits INVALIDATED before that reply is
// released. This must not return the old ACTIVE generation to the public caller.
func TestMultiOwnerTCPDomainSearchRejectsInFlightActiveInvalidationV1(t *testing.T) {
	faultDir := t.TempDir()
	t.Setenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL", faultDir)
	testMultiOwnerTCPDomainSearchUsesOnlyHostedAssetsV1(t, false, false, false)
}

func fixedPeerActiveInvalidationChildV1(runtime *FixedPeerTCPRuntimeV1) {
	faultDir := os.Getenv("GOMAP_FIXED_PEER_ACTIVE_INVALIDATION_CONTROL")
	if faultDir == "" || runtime == nil || runtime.vector == nil {
		return
	}
	switch runtime.config.NodeID {
	case "owner-c":
		go func() {
			for {
				runtime.vector.initMu.Lock()
				topology := runtime.vector.topology
				if topology != nil {
					service := topology.services["group-c"]
					if service != nil {
						service.testBeforeResponseCopy = func() {
							if _, err := os.Stat(filepath.Join(faultDir, "arm")); err != nil {
								return
							}
							if err := os.WriteFile(filepath.Join(faultDir, "owner-held"), nil, 0600); err != nil {
								_ = os.WriteFile(filepath.Join(faultDir, "fault-error"), []byte(err.Error()), 0600)
								return
							}
							deadline := time.After(30 * time.Second)
							for {
								if _, err := os.Stat(filepath.Join(faultDir, "release")); err == nil {
									return
								}
								select {
								case <-deadline:
									_ = os.WriteFile(filepath.Join(faultDir, "fault-error"), []byte("owner reply was never released"), 0600)
									return
								case <-time.After(5 * time.Millisecond):
								}
							}
						}
						_ = os.WriteFile(filepath.Join(faultDir, "hook-ready"), nil, 0600)
						runtime.vector.initMu.Unlock()
						return
					}
				}
				runtime.vector.initMu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	case "source-holder":
		go func() {
			for {
				if _, err := os.Stat(filepath.Join(faultDir, "invalidate")); err == nil {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			coordinator := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: runtime.authority, Committer: runtime.meta}
			identity := runtime.config.Vector.Identity
			_, err := coordinator.InvalidateGenerationBeforeRelevantMutationV1(context.Background(), identity, "in-flight strict search fault")
			if err == nil {
				var record raftplacement.VectorPartitionLifecycleRecordV1
				var ok bool
				record, ok = runtime.authority.VectorPartitionLifecycleRecordV1(identity)
				if !ok || record.State != raftplacement.VectorPartitionLifecycleInvalidatedV1 {
					err = fmt.Errorf("catalog leader did not apply INVALIDATED: record=%+v present=%t", record, ok)
				}
			}
			if err != nil {
				_ = os.WriteFile(filepath.Join(faultDir, "fault-error"), []byte(err.Error()), 0600)
				return
			}
			status, ok := runtime.authority.Status()
			if !ok || status.AppliedIndex == 0 {
				_ = os.WriteFile(filepath.Join(faultDir, "fault-error"), []byte("catalog leader has no applied invalidation index"), 0600)
				return
			}
			if err := os.WriteFile(filepath.Join(faultDir, "invalidated.tmp"), []byte(strconv.FormatUint(status.AppliedIndex, 10)), 0600); err == nil {
				err = os.Rename(filepath.Join(faultDir, "invalidated.tmp"), filepath.Join(faultDir, "invalidated"))
			}
			if err != nil {
				_ = os.WriteFile(filepath.Join(faultDir, "fault-error"), []byte(err.Error()), 0600)
			}
		}()
	}
}

func fixedPeerWaitActiveInvalidationFileV1(t *testing.T, ctx context.Context, dir, name string) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if raw, err := os.ReadFile(filepath.Join(dir, "fault-error")); err == nil {
			t.Fatalf("ACTIVE invalidation fixture: %s", raw)
		}
		if _, err := os.Stat(filepath.Join(dir, name)); err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

func fixedPeerAssertInFlightActiveInvalidationV1(t *testing.T, ctx context.Context, dir string, control *FixedPeerTCPClientV1, client *Client, request public.SearchRequestV1) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "arm"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	defer os.WriteFile(filepath.Join(dir, "release"), nil, 0600)
	request.Deadline = time.Now().Add(30 * time.Second)
	type result struct {
		response public.SearchResponseV1
		err      error
	}
	finished := make(chan result, 1)
	go func() {
		response, err := client.VectorSearchStrictV1(ctx, request)
		finished <- result{response: response, err: err}
	}()
	fixedPeerWaitActiveInvalidationFileV1(t, ctx, dir, "owner-held")
	select {
	case got := <-finished:
		t.Fatalf("strict search finished before held owner reply: response=%+v err=%v", got.response, got.err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "invalidate"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	fixedPeerWaitActiveInvalidationFileV1(t, ctx, dir, "invalidated")
	raw, err := os.ReadFile(filepath.Join(dir, "invalidated"))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil || applied == 0 {
		t.Fatalf("invalid catalog apply index %q: %v", raw, err)
	}
	fixedPeerWaitV1(t, ctx, func() bool {
		status, err := control.Status(ctx, "ingress")
		return err == nil && status.Catalog.AppliedIndex >= applied
	})
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-finished:
		if !hasPublicVectorErrorCodeV1(got.err, public.ErrorGenerationMismatchV1) || len(got.response.Neighbors) != 0 {
			t.Fatalf("strict search returned stale ACTIVE result after committed invalidation: response=%+v err=%v", got.response, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("strict search did not finish after owner release: %v", ctx.Err())
	}
	request.Deadline = time.Now().Add(30 * time.Second)
	retry, err := client.VectorSearchStrictV1(ctx, request)
	if !hasPublicVectorErrorCodeV1(err, public.ErrorGenerationMismatchV1) || len(retry.Neighbors) != 0 {
		t.Fatalf("strict search re-entered invalidated ACTIVE generation: response=%+v err=%v", retry, err)
	}
}
