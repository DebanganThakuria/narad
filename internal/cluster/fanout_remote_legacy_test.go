package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A link attached to a target topic created before topic IDs records no
// target ID. A topic never gains an ID except by being recreated, so an
// ID the target reports later means a new topic (or another cluster
// behind the remote's URL): the link stops in target_replaced before
// anything lands there.
func TestRemoteChildToAnIDlessTargetStopsWhenTheTargetGainsAnID(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{legacyTarget: true, rigSourceOpts: rigSourceOpts{stallRetry: 300 * time.Millisecond}})
	if rg.stub.Remote.TargetID != "" {
		t.Fatalf("stub target ID %q, want none", rg.stub.Remote.TargetID)
	}
	rg.src.start()
	defer rg.src.stop()
	rg.src.produce(t, 0, 10, 2, 0)
	rg.waitState(t, 0, topic.RemoteStateTargetReplaced, 15*time.Second)
	if n := len(rg.target.records(t, "orders")); n != 0 {
		t.Fatalf("%d records landed in a target that gained an ID", n)
	}
}

// Resume applies the same rule: a link that recorded no target ID and
// finds one now resumes only with accept_target.
func TestRemoteChildResumeOntoAnIDlessTargetThatGainedAnIDNeedsAcceptTarget(t *testing.T) {
	s := linksRig(t)
	s.checks.targetID = ""
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	if res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b"}); res.Status != http.StatusOK {
		t.Fatalf("pause: %d %s", res.Status, res.Body)
	}
	s.checks.targetID = "tid-new"
	if res := s.write(t, nodewire.RemoteSubResume, map[string]any{"parent": "orders", "child": "orders-to-b"}); res.Status != http.StatusConflict {
		t.Fatalf("resume onto a target that gained an ID: %d %s, want 409", res.Status, res.Body)
	}
	if res := s.write(t, nodewire.RemoteSubResume, map[string]any{"parent": "orders", "child": "orders-to-b", "accept_target": true}); res.Status != http.StatusOK {
		t.Fatalf("resume with accept_target: %d %s", res.Status, res.Body)
	}
	stub, _ := s.store.GetTopic(context.Background(), "orders-to-b")
	if stub.Remote.Paused || stub.Remote.TargetID != "tid-new" {
		t.Fatalf("after resume: %+v", stub.Remote)
	}
}
