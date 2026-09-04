package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type recordingMetrics struct {
	ops      []string
	outcomes []string
}

func (m *recordingMetrics) ObserveRPC(op, outcome string, elapsed time.Duration) {
	if elapsed < 0 {
		panic("negative elapsed")
	}
	m.ops = append(m.ops, op)
	m.outcomes = append(m.outcomes, outcome)
}

type scriptedTransport struct {
	status int
	err    error
}

func (s scriptedTransport) RequestOnLane(context.Context, string, clusterrpc.Lane, clusterwire.StreamFrameType, []byte) (clusterwire.StreamFrame, error) {
	if s.err != nil {
		return clusterwire.StreamFrame{}, s.err
	}
	payload, _ := nodewire.EncodeResponse(nodewire.Response{Status: s.status})
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, Payload: payload}, nil
}

func TestPeerClientRecordsRPCMetrics(t *testing.T) {
	for _, tc := range []struct {
		transport scriptedTransport
		want      string
	}{
		{scriptedTransport{status: http.StatusOK}, rpcOutcomeOK},
		{scriptedTransport{status: http.StatusNoContent}, rpcOutcomeOK},
		{scriptedTransport{status: http.StatusNotFound}, rpcOutcomeRejected},
		{scriptedTransport{status: http.StatusInternalServerError}, rpcOutcomeFailed},
		{scriptedTransport{err: context.DeadlineExceeded}, rpcOutcomeTimeout},
		{scriptedTransport{err: errors.New("boom")}, rpcOutcomeError},
	} {
		rec := &recordingMetrics{}
		client := &PeerClient{frames: tc.transport}
		client.SetMetrics(rec)
		_, _ = client.Ack(context.Background(), "peer", nodewire.AckRequest{})
		if len(rec.ops) != 1 || rec.ops[0] != "ack" || rec.outcomes[0] != tc.want {
			t.Fatalf("transport %+v: recorded %v/%v, want ack/%s", tc.transport, rec.ops, rec.outcomes, tc.want)
		}
	}
	// No hook: nothing to record, no panic.
	client := &PeerClient{frames: scriptedTransport{status: http.StatusOK}}
	if _, err := client.Ack(context.Background(), "peer", nodewire.AckRequest{}); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}
}

func TestPrometheusRPCMetricsRegistersSeries(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewPrometheusRPCMetrics(reg)
	m.ObserveRPC("consume", rpcOutcomeOK, 3*time.Millisecond)
	m.ObserveRPC("consume", rpcOutcomeTimeout, 5*time.Second)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	got := map[string]bool{}
	for _, family := range families {
		got[family.GetName()] = true
		if family.GetName() == "narad_cluster_rpc_requests_total" && len(family.GetMetric()) != 2 {
			t.Fatalf("requests_total series = %d, want 2 (one per outcome)", len(family.GetMetric()))
		}
	}
	for _, name := range []string{"narad_cluster_rpc_requests_total", "narad_cluster_rpc_request_seconds"} {
		if !got[name] {
			t.Fatalf("metric %s not registered; got %v", name, got)
		}
	}
}
