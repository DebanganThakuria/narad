package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/errs"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/remote"
)

// recordingLeader is a RegistryLeader and a LinkLeader that records
// what reached it.
type recordingLeader struct {
	name  string
	got   []string
	check []string
}

func (l *recordingLeader) ServeWrite(_ context.Context, req nodewire.RemoteWriteRequest) nodewire.Response {
	l.got = append(l.got, req.SubOp)
	return jsonResponse(http.StatusOK, map[string]string{"by": l.name})
}

func (l *recordingLeader) ServeCheck(_ context.Context, req nodewire.RemoteCheckRequest) nodewire.Response {
	l.check = append(l.check, req.Mode)
	return jsonResponse(http.StatusOK, map[string]string{"by": l.name})
}

func (l *recordingLeader) ServeUnshipped(_ context.Context, req nodewire.RemoteCheckRequest) nodewire.Response {
	l.check = append(l.check, req.Mode)
	return jsonResponse(http.StatusOK, map[string]string{"by": l.name})
}

func rpcErrorOf(t *testing.T, res nodewire.Response) string {
	t.Helper()
	var e struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(res.Body, &e)
	return e.Error
}

func TestRemoteOpsWithoutAPlaneAnswer501(t *testing.T) {
	s := NewRPCServer(nil, nil, nil)
	write, _ := nodewire.EncodeRemoteWriteRequest(nodewire.RemoteWriteRequest{SubOp: nodewire.RemoteSubCreate})
	check, _ := nodewire.EncodeRemoteCheckRequest(nodewire.RemoteCheckRequest{Mode: nodewire.RemoteCheckStatus})
	for _, payload := range [][]byte{write, check} {
		if res := s.dispatch(context.Background(), requestKey{}, payload); res.Status != http.StatusNotImplemented {
			t.Fatalf("status %d, want 501", res.Status)
		}
	}
	for _, payload := range [][]byte{{byte(nodewire.OpRemoteWrite)}, {byte(nodewire.OpRemoteCheck), 0, 0}} {
		if res := s.dispatch(context.Background(), requestKey{}, payload); res.Status != http.StatusBadRequest {
			t.Fatalf("malformed payload: status %d, want 400", res.Status)
		}
	}
}

func TestRemotePlaneDispatch(t *testing.T) {
	reg := &recordingLeader{name: "registry"}
	links := &recordingLeader{name: "links"}
	p := NewRemotePlane(nil, nil, nil, "n0", nil)
	p.Registry, p.Links = reg, links
	s := NewRPCServer(nil, nil, nil)
	s.SetRemotePlane(p)

	for subOp, by := range map[string]string{
		nodewire.RemoteSubCreate: "registry", nodewire.RemoteSubUpdate: "registry",
		nodewire.RemoteSubDelete: "registry", nodewire.RemoteSubReencrypt: "registry",
		nodewire.RemoteSubAttach: "links", nodewire.RemoteSubPause: "links", nodewire.RemoteSubResume: "links",
		nodewire.RemoteSubSkip: "links", nodewire.RemoteSubDetach: "links", nodewire.RemoteSubTopicDelete: "links",
	} {
		payload, _ := nodewire.EncodeRemoteWriteRequest(nodewire.RemoteWriteRequest{SubOp: subOp, Actor: "alice", RequestID: remote.NewRequestID()})
		res := s.dispatch(context.Background(), requestKey{}, payload)
		var body map[string]string
		_ = json.Unmarshal(res.Body, &body)
		if res.Status != http.StatusOK || body["by"] != by {
			t.Fatalf("%s: %d %v, want served by %s", subOp, res.Status, body, by)
		}
	}
	for _, subOp := range []string{"", "remote", "topic.create", "user.create"} {
		payload, _ := nodewire.EncodeRemoteWriteRequest(nodewire.RemoteWriteRequest{SubOp: subOp})
		res := s.dispatch(context.Background(), requestKey{}, payload)
		if res.Status != http.StatusBadRequest || rpcErrorOf(t, res) != "unsupported remote write" {
			t.Fatalf("sub-op %q: %d %q", subOp, res.Status, res.Body)
		}
	}
	for mode, by := range map[string]string{
		nodewire.RemoteCheckRun: "registry", nodewire.RemoteCheckStatus: "registry", nodewire.RemoteCheckUnshipped: "links",
	} {
		payload, _ := nodewire.EncodeRemoteCheckRequest(nodewire.RemoteCheckRequest{Mode: mode})
		res := s.dispatch(context.Background(), requestKey{}, payload)
		var body map[string]string
		_ = json.Unmarshal(res.Body, &body)
		if res.Status != http.StatusOK || body["by"] != by {
			t.Fatalf("mode %s: %d %v, want served by %s", mode, res.Status, body, by)
		}
	}
	payload, _ := nodewire.EncodeRemoteCheckRequest(nodewire.RemoteCheckRequest{Mode: "purge"})
	if res := s.dispatch(context.Background(), requestKey{}, payload); res.Status != http.StatusBadRequest {
		t.Fatalf("unknown mode: %d", res.Status)
	}
}

func TestUnsupportedOperationDetection(t *testing.T) {
	older := NewRPCServer(nil, nil, nil)
	// A payload with an op byte this server does not know stands in for
	// an older leader receiving OpRemoteWrite.
	res := older.dispatch(context.Background(), requestKey{}, []byte{0xfe})
	if !unsupportedOperation(res) {
		t.Fatalf("an unknown op's answer was not recognised: %d %q", res.Status, res.Body)
	}
	for _, other := range []nodewire.Response{
		errorResponse(http.StatusBadRequest, "unsupported remote write"),
		errorResponse(http.StatusBadRequest, "invalid remote write request"),
		errorResponse(http.StatusPreconditionFailed, "unsupported rpc operation 33"),
		{Status: http.StatusOK},
	} {
		if unsupportedOperation(other) {
			t.Fatalf("%d %q read as an older leader", other.Status, other.Body)
		}
	}
}

func TestBrokerErrorStatusMapsRemoteChildConflictTo409(t *testing.T) {
	s := NewRPCServer(nil, nil, nil)
	for _, err := range []error{
		errs.ErrRemoteChildConflict,
		errs.ErrRemoteAwareDeleteRequired,
		fmt.Errorf("stub %q: %w", "orders-to-b", errs.ErrRemoteChildLocal),
	} {
		if status, _ := s.brokerErrorStatus("op", err); status != http.StatusConflict {
			t.Fatalf("brokerErrorStatus(%v) = %d, want 409", err, status)
		}
	}
}
