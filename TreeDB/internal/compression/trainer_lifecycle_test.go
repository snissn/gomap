package compression

import (
	"bytes"
	"testing"
	"time"

	"github.com/snissn/compress/zstd"
)

func TestTrainerCollectOwnsSampleBeforeDelayedConsumption(t *testing.T) {
	trainer := NewTrainer(TrainConfig{TrainBytes: 1 << 20, MinRecords: 100, MaxRecordBytes: 1024, SampleStride: 1}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}
	defer trainer.Close()
	want := bytes.Repeat([]byte("owned-sample/"), 16)
	input := bytes.Clone(want)
	// Block the real queue consumer at its existing sample-storage lock. Collect
	// itself does not take this lock; it must own the sample before returning.
	trainer.mu.Lock()
	trainer.Collect(input)
	for i := range input {
		input[i] = 'X'
	}
	trainer.mu.Unlock()
	trainer.Close()
	done := make(chan struct{})
	go func() { trainer.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("delayed sample consumer did not drain")
	}
	if trainer.Stats().Enqueued != 1 || len(trainer.samples) != 1 || !bytes.Equal(trainer.samples[0], want) {
		t.Fatalf("queued sample did not own pre-mutation bytes: enqueued=%d samples=%q", trainer.Stats().Enqueued, trainer.samples)
	}
}

func TestTrainerWaitDrainsClosedWorker(t *testing.T) {
	trainer := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		MinRecords:     100,
		MaxRecordBytes: 1024,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}
	for i := 0; i < 32; i++ {
		trainer.Collect([]byte("authoritative-resource-trainer-drain"))
	}
	trainer.Close()
	done := make(chan struct{})
	go func() {
		trainer.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Trainer.Wait did not observe worker shutdown")
	}
	if trainer.ShouldCollect() {
		t.Fatal("closed trainer still collects samples")
	}
}

func TestTrainerWaitIncludesSpawnedTrainingJobs(t *testing.T) {
	trainer := NewTrainer(TrainConfig{
		TrainBytes:     1 << 20,
		MinRecords:     100,
		MaxRecordBytes: 1024,
	}, Config{Kind: KindZSTD, Level: zstd.SpeedFastest}, false, false)
	if trainer == nil {
		t.Fatal("NewTrainer returned nil")
	}

	// Model a job already spawned by appendSample. The queue worker owns Add,
	// so closing and draining sampleCh guarantees there can be no later Add when
	// run reaches its Wait.
	jobRelease := make(chan struct{})
	trainer.trainWG.Add(1)
	go func() {
		defer trainer.trainWG.Done()
		<-jobRelease
	}()
	trainer.Close()

	waitDone := make(chan struct{})
	go func() {
		trainer.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
		t.Fatal("Trainer.Wait returned while a spawned training job was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(jobRelease)
	select {
	case <-waitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Trainer.Wait did not observe spawned training job completion")
	}
}
