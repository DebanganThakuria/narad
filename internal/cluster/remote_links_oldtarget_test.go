package cluster

import (
	"net/http"
	"strings"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// An attach (dry run or not) to a target whose batch produce takes 100
// messages (v3.1.0, with its 1 MiB batch body) warns that a record near
// the source's 1 MiB limit will block the link until the target is
// upgraded; a target on this release gets no such warning.
func TestAttachWarnsAboutAnOlderTargetsBatchBodyLimit(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.target.createTopic(t, "orders2", 3)
	warned := func(res nodewire.Response) bool {
		ws, _ := bodyOf(t, res)["warnings"].([]any)
		for _, w := range ws {
			if s, _ := w.(string); strings.Contains(s, "1 MiB") && strings.Contains(s, "record_too_large") {
				return true
			}
		}
		return false
	}
	attach := map[string]any{"parent": "orders", "child": "orders-to-b2", "remote": "b", "remote_topic": "orders2", "dry_run": true}
	res := rg.src.write(t, nodewire.RemoteSubAttach, attach)
	if res.Status != http.StatusOK || warned(res) {
		t.Fatalf("dry run to a current target: %d %s, want no batch-limit warning", res.Status, res.Body)
	}
	rg.target.faults.set("max100")
	res = rg.src.write(t, nodewire.RemoteSubAttach, attach)
	if res.Status != http.StatusOK || !warned(res) {
		t.Fatalf("dry run to a v3.1.0 target: %d %s, want the batch-limit warning", res.Status, res.Body)
	}
	delete(attach, "dry_run")
	res = rg.src.write(t, nodewire.RemoteSubAttach, attach)
	if res.Status != http.StatusCreated || !warned(res) {
		t.Fatalf("attach to a v3.1.0 target: %d %s, want the batch-limit warning", res.Status, res.Body)
	}
}
