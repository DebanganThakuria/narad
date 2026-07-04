package topics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// createResult carries a CreateTopic outcome across the goroutine
// boundary in the gate tests.
type createResult struct {
	created topic.Topic
	err     error
}

func TestCreateGate_ArmedBlocksCreateUntilRelease(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, nil)
	manager.ArmCreateGate()

	done := make(chan createResult, 1)
	go func() {
		created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
		done <- createResult{created: created, err: err}
	}()

	// The create must not complete while the gate is armed.
	select {
	case res := <-done:
		t.Fatalf("CreateTopic() completed before gate release: (%+v, %v)", res.created, res.err)
	case <-time.After(50 * time.Millisecond):
	}
	if ms.lastCreatedTopic.Name != "" {
		t.Fatalf("metastore CreateTopic() called before gate release, topic = %q", ms.lastCreatedTopic.Name)
	}

	manager.ReleaseCreateGate()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("CreateTopic() after release error = %v", res.err)
		}
		if res.created.Name != testTopicName {
			t.Fatalf("CreateTopic() name = %q, want %q", res.created.Name, testTopicName)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CreateTopic() still blocked after ReleaseCreateGate()")
	}
	if ms.lastCreatedTopic.Name != testTopicName {
		t.Fatalf("metastore CreateTopic() topic = %q, want %q", ms.lastCreatedTopic.Name, testTopicName)
	}
}

func TestCreateGate_ArmedCancelledContextReturnsCtxError(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, nil)
	manager.ArmCreateGate()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan createResult, 1)
	go func() {
		created, err := manager.CreateTopic(ctx, CreateOpts{Name: testTopicName})
		done <- createResult{created: created, err: err}
	}()

	cancel()

	select {
	case res := <-done:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("CreateTopic() error = %v, want wrapped %v", res.err, context.Canceled)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CreateTopic() did not return promptly after ctx cancellation")
	}
	if ms.lastCreatedTopic.Name != "" {
		t.Fatalf("metastore CreateTopic() called despite cancelled ctx, topic = %q", ms.lastCreatedTopic.Name)
	}
}

func TestCreateGate_DefaultUnarmedDoesNotBlock(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, nil)

	// Constructors never arm the gate, so CreateTopic must proceed
	// immediately even with a context that could otherwise win a select.
	created, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName})
	if err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if created.Name != testTopicName {
		t.Fatalf("CreateTopic() name = %q, want %q", created.Name, testTopicName)
	}
}

func TestCreateGate_ReleaseIsIdempotentAndSafeWhenUnarmed(t *testing.T) {
	manager := newTestManager(t, newFakeMetastore(), nil)

	// Release without arming must be a no-op.
	manager.ReleaseCreateGate()

	manager.ArmCreateGate()
	manager.ArmCreateGate() // arming twice keeps a single gate
	manager.ReleaseCreateGate()
	manager.ReleaseCreateGate() // double release must not panic

	if _, err := manager.CreateTopic(context.Background(), CreateOpts{Name: testTopicName}); err != nil {
		t.Fatalf("CreateTopic() after release error = %v", err)
	}
}
