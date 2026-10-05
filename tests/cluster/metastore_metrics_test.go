//go:build cluster

package cluster

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestNodeExportsMetastoreAndRaftMetrics: `narad serve` hands its
// registry to the metastore, so a node's /metrics carries the Raft state
// and the metadata database's series beside the broker's.
func TestNodeExportsMetastoreAndRaftMetrics(t *testing.T) {
	metricsPorts, _ := allocPorts(t)
	metricsAddr := fmt.Sprintf("127.0.0.1:%d", metricsPorts[0])
	singleNode(t, map[string]string{"NARAD_HTTP_METRICS_ADDR": metricsAddr})

	client := &http.Client{Timeout: 5 * time.Second}
	var body string
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(body, `narad_raft_state{state="leader"} 1`) {
		if time.Now().After(deadline) {
			t.Fatalf("/metrics never reported this single node as the Raft leader; last scrape:\n%s", tail(body, 4000))
		}
		time.Sleep(200 * time.Millisecond)
		resp, err := client.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body = string(raw)
	}
	for _, want := range []string{
		"narad_raft_has_leader 1",
		"narad_raft_voters 1",
		"narad_metastore_fsm_bytes ",
		"narad_metastore_apply_stopped 0",
		"narad_metastore_snapshot_failures_total 0",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics lacks %q", want)
		}
	}
}
