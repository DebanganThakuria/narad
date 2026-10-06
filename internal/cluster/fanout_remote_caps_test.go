package cluster

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// A target that took 1,000-message chunks and then answers "too many
// messages" (rolled back to v3.1.0, or an older pod behind its load
// balancer) is a capability change, not a refused record: the link
// drops to 100-message chunks at once, counts no rejected_record, and
// later slabs are not refused again.
func TestRemoteChildTargetThatShrinksItsBatchLimitIsReprobed(t *testing.T) {
	rg := newRemoteRig(t, remoteRigOpts{})
	rg.src.start()
	defer rg.src.stop()
	first := rg.src.produce(t, 0, 5, 2, 0)
	rg.waitDelivered(t, first, 15*time.Second)

	rg.target.faults.set("max100")
	more := rg.src.produce(t, 0, 300, 7, 1000)
	rg.waitDelivered(t, append(first, more...), 30*time.Second)
	later := rg.src.produce(t, 0, 300, 7, 5000)
	rg.waitDelivered(t, append(append(first, more...), later...), 30*time.Second)

	if v := counterValue(rg.src.metrics.RemoteLink.ErrorsTotal.WithLabelValues("b", topic.RemoteStateRejectedRecord)); v != 0 {
		t.Fatalf("rejected_record errors = %v for records the target accepts", v)
	}
	if n := rg.target.faults.tooMany.Load(); n > 1 {
		t.Fatalf("the target refused %d chunks as too many messages, want at most the first", n)
	}
}
