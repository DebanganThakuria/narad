package replication

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	replicationwire "github.com/debanganthakuria/narad/internal/protocol/replication"
)

type fakeClusterStore struct {
	followerAddr string
}

func (fakeClusterStore) GetAssignment(topicName string, partition int) (metastore.Assignment, error) {
	return metastore.Assignment{
		Topic:      topicName,
		Partition:  partition,
		OwnerID:    "node-a",
		FollowerID: "node-b",
	}, nil
}

func (s fakeClusterStore) GetMember(podID string) (metastore.Member, error) {
	addr := s.followerAddr
	if addr == "" {
		addr = podID + ".example:8080"
	}
	return metastore.Member{
		ID:     podID,
		Addr:   addr,
		Status: metastore.MemberAlive,
	}, nil
}

type clusterRoundTripFunc func(*http.Request) (*http.Response, error)

func (f clusterRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestClusterReplicateSendsRawPayloadRequest(t *testing.T) {
	var captured *http.Request
	client := &http.Client{Transport: clusterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		captured = req
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if string(body) != `{"id":1}` {
			t.Fatalf("request body = %s, want raw payload", body)
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}
	cluster := NewCluster("node-a", fakeClusterStore{}, client)

	if err := cluster.Replicate(context.Background(), "orders", 2, 42, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Replicate() error = %v", err)
	}

	if captured == nil {
		t.Fatal("client did not receive request")
	}
	if got, want := captured.URL.String(), "http://node-b.example:8080/internal/v1/replicate"; got != want {
		t.Fatalf("request URL = %q, want %q", got, want)
	}
	if got := captured.Header.Get("Content-Type"); got != replicationwire.RawContentType {
		t.Fatalf("Content-Type = %q, want %q", got, replicationwire.RawContentType)
	}
	if got := captured.Header.Get(replicationwire.HeaderTopic); got != "orders" {
		t.Fatalf("topic header = %q, want orders", got)
	}
	if got := captured.Header.Get(replicationwire.HeaderPartition); got != "2" {
		t.Fatalf("partition header = %q, want 2", got)
	}
	if got := captured.Header.Get(replicationwire.HeaderOffset); got != "42" {
		t.Fatalf("offset header = %q, want 42", got)
	}
	if got := captured.Header.Get(replicationwire.HeaderLeaderID); got != "node-a" {
		t.Fatalf("leader header = %q, want node-a", got)
	}
}

func TestClusterReplicateBatchSendsBatchPayloadRequest(t *testing.T) {
	var captured *http.Request
	client := &http.Client{Transport: clusterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		captured = req
		payloads, err := replicationwire.DecodeBatchPayload(req.Body, 0)
		if err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		if len(payloads) != 2 || string(payloads[0]) != `{"id":1}` || string(payloads[1]) != `{"id":2}` {
			t.Fatalf("request payloads = %q", payloads)
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}
	cluster := NewCluster("node-a", fakeClusterStore{}, client)

	err := cluster.ReplicateBatch(context.Background(), "orders", 2, []Record{
		{Offset: 42, Payload: []byte(`{"id":1}`)},
		{Offset: 43, Payload: []byte(`{"id":2}`)},
	})
	if err != nil {
		t.Fatalf("ReplicateBatch() error = %v", err)
	}

	if captured == nil {
		t.Fatal("client did not receive request")
	}
	if got := captured.Header.Get("Content-Type"); got != replicationwire.BatchContentType {
		t.Fatalf("Content-Type = %q, want %q", got, replicationwire.BatchContentType)
	}
	if got := captured.Header.Get(replicationwire.HeaderOffset); got != "42" {
		t.Fatalf("offset header = %q, want 42", got)
	}
	if got := captured.Header.Get(replicationwire.HeaderRecordCount); got != "2" {
		t.Fatalf("record count header = %q, want 2", got)
	}
}

func TestClusterReplicateReturnsOffsetMismatchHint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(replicationwire.HeaderReplicaNextOffset, "7")
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()
	cluster := NewCluster("node-a", fakeClusterStore{followerAddr: server.URL}, server.Client())

	err := cluster.Replicate(context.Background(), "orders", 2, 42, []byte(`{"id":1}`))
	var mismatch *OffsetMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Replicate() error = %v, want OffsetMismatchError", err)
	}
	if mismatch.RequestedOffset != 42 || mismatch.ReplicaNextOffset != 7 {
		t.Fatalf("OffsetMismatchError = %+v, want requested=42 replica_next=7", mismatch)
	}
}

func TestClusterCatchUpUsesFollowerNextHint(t *testing.T) {
	var posted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		posted = append(posted, r.Header.Get(replicationwire.HeaderOffset)+":"+string(body))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cluster := NewCluster("node-a", fakeClusterStore{followerAddr: server.URL}, server.Client())
	hint := int64(1)
	log := &memoryLeaderLog{
		records: [][]byte{
			[]byte(`{"id":0}`),
			[]byte(`{"id":1}`),
			[]byte(`{"id":2}`),
		},
		hwm: 1,
	}

	if err := cluster.CatchUp(context.Background(), "orders", 2, log, CatchUpOptions{FollowerNextOffset: &hint}); err != nil {
		t.Fatalf("CatchUp() error = %v", err)
	}
	want := []string{`1:{"id":1}`, `2:{"id":2}`}
	if len(posted) != len(want) {
		t.Fatalf("posted = %v, want %v", posted, want)
	}
	for i := range want {
		if posted[i] != want[i] {
			t.Fatalf("posted[%d] = %q, want %q", i, posted[i], want[i])
		}
	}
	if log.HighWatermark() != 3 {
		t.Fatalf("HighWatermark() = %d, want 3", log.HighWatermark())
	}
}

type memoryLeaderLog struct {
	records [][]byte
	hwm     int64
}

func (l *memoryLeaderLog) Read(offset int64) ([]byte, error) {
	if offset < 0 || offset >= int64(len(l.records)) {
		return nil, io.EOF
	}
	out := append([]byte(nil), l.records[offset]...)
	return out, nil
}

func (l *memoryLeaderLog) HighWatermark() int64 {
	return l.hwm
}

func (l *memoryLeaderLog) NextOffset() int64 {
	return int64(len(l.records))
}

func (l *memoryLeaderLog) AdvanceHighWatermark(newHWM int64) error {
	if newHWM > l.hwm {
		l.hwm = newHWM
	}
	return nil
}
