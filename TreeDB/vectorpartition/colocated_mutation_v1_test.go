package vectorpartition

import (
	"context"
	"errors"
	"testing"
	"time"
)

// #4977 capability-absence exception: Replace/Delete and their typed product
// requests do not exist on the construction base. These contract tests are
// written first; their initial compile failure is capability evidence, not a
// runnable semantic red. The coordinator owns all Go execution.
type colocatedMutationBackendTestV1 struct {
	*serviceBackendV1
	replace func(context.Context, ReplaceRequestV1) (MutationResponseV1, error)
	delete  func(context.Context, DeleteRequestV1) (MutationResponseV1, error)
}

func (b *colocatedMutationBackendTestV1) ReplaceVectorPartitionV1(ctx context.Context, r ReplaceRequestV1) (MutationResponseV1, error) {
	return b.replace(ctx, r)
}
func (b *colocatedMutationBackendTestV1) DeleteVectorPartitionV1(ctx context.Context, r DeleteRequestV1) (MutationResponseV1, error) {
	return b.delete(ctx, r)
}

func TestServiceV1ColocatedMutationContractV1(t *testing.T) {
	request := ReplaceRequestV1{Version: 1, Generation: GenerationIDV1{Index: "embedding", Generation: 7}, IdempotencyKey: []byte("replace-attempt"), ID: []byte("doc"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1]}`), Deadline: time.Now().Add(time.Minute)}
	proof := MutationResponseV1{Generation: request.Generation, OwnerGroup: "owner", CommitTerm: 2, CommitIndex: 9, AppliedIndex: 9, ProductionConsensus: true, LiveRevision: 3, Coverage: 8, Matched: 1, Modified: 1, VisibilityToken: []byte("durable-scoped-floor"), Counters: MutationCountersV1{Routes: 1, Commits: 1, Replications: 1, Applies: 1, VisibilityProofs: 1}}
	backend := &colocatedMutationBackendTestV1{serviceBackendV1: &serviceBackendV1{}}
	backend.replace = func(ctx context.Context, r ReplaceRequestV1) (MutationResponseV1, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("mutation deadline not applied")
		}
		r.ID[0] = 'x'
		r.Document[0] = 'x'
		r.Vector[0] = 1
		r.IdempotencyKey[0] = 'x'
		return proof, nil
	}
	backend.delete = func(context.Context, DeleteRequestV1) (MutationResponseV1, error) {
		response := proof
		response.Matched = 0
		response.Modified = 0
		response.Deleted = 1
		return response, nil
	}
	service, err := NewServiceV1(backend)
	if err != nil {
		t.Fatal(err)
	}
	got, err := service.Replace(t.Context(), request)
	if err != nil || got.Modified != 1 {
		t.Fatalf("replace=%+v err=%v", got, err)
	}
	if string(request.ID) != "doc" || request.Document[0] != '{' || request.Vector[0] != 0 || string(request.IdempotencyKey) != "replace-attempt" {
		t.Fatal("backend borrowed caller mutation buffers")
	}
	deletion := DeleteRequestV1{Version: 1, Generation: request.Generation, IdempotencyKey: []byte("delete-attempt"), ID: request.ID, Deadline: request.Deadline}
	if got, err := service.Delete(t.Context(), deletion); err != nil || got.Deleted != 1 {
		t.Fatalf("delete=%+v err=%v", got, err)
	}
	backend.replace = func(context.Context, ReplaceRequestV1) (MutationResponseV1, error) {
		bad := proof
		bad.AppliedIndex = 8
		return bad, nil
	}
	if _, err := service.Replace(t.Context(), request); !errors.As(err, new(*ErrorV1)) {
		t.Fatalf("incomplete postcommit proof err=%v", err)
	} else {
		var typed *ErrorV1
		errors.As(err, &typed)
		if typed.Code != ErrorCommitAmbiguousV1 {
			t.Fatalf("proof error=%v", err)
		}
	}
	backend.replace = func(context.Context, ReplaceRequestV1) (MutationResponseV1, error) {
		noop := proof
		noop.Matched = 0
		noop.Modified = 0
		noop.LiveRevision = 0
		return noop, nil
	}
	if got, err := service.Replace(t.Context(), request); err != nil || got.Modified != 0 || got.LiveRevision != 0 {
		t.Fatalf("missing/no-op=%+v err=%v", got, err)
	}
	backend.delete = func(context.Context, DeleteRequestV1) (MutationResponseV1, error) {
		t.Fatal("invalid delete reached backend")
		return MutationResponseV1{}, nil
	}
	deletion.ID = []byte{0xff}
	if _, err := service.Delete(t.Context(), deletion); err == nil {
		t.Fatal("invalid stable ID accepted")
	}
}
