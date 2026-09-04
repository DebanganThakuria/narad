package cluster

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// laneRecorder is a frameTransport that records the lane of every request
// and answers with an empty 200 (or a JSON body when the caller decodes
// one).
type laneRecorder struct {
	lanes []clusterrpc.Lane
	body  []byte
}

func (r *laneRecorder) RequestOnLane(_ context.Context, _ string, lane clusterrpc.Lane, _ clusterwire.StreamFrameType, _ []byte) (clusterwire.StreamFrame, error) {
	r.lanes = append(r.lanes, lane)
	payload, err := nodewire.EncodeResponse(nodewire.Response{Status: http.StatusOK, ContentType: nodewire.ContentTypeJSON, Body: r.body})
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: payload}, nil
}

// Every operation must ride its designated lane. Passing operation names
// as lanes (the previous bug) normalized every bulk op onto the 4-stream
// control lane and serialized produce, consume, and ack traffic there.
func TestPeerClientOperationLanes(t *testing.T) {
	ctx := context.Background()
	ack := nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2}
	for name, tc := range map[string]struct {
		body string
		call func(c *PeerClient) error
		want clusterrpc.Lane
	}{
		"produce": {
			call: func(c *PeerClient) error {
				_, err := c.Produce(ctx, "peer", nodewire.ProduceRequest{Topic: "orders", Payload: []byte("x")})
				return err
			},
			want: clusterrpc.LaneProduce,
		},
		"commit_produce": {
			call: func(c *PeerClient) error {
				_, err := c.CommitProduce(ctx, "peer", nodewire.CommitProduceRequest{Topic: "orders", Payload: []byte("x")})
				return err
			},
			want: clusterrpc.LaneProduce,
		},
		"commit_produce_batch": {
			call: func(c *PeerClient) error {
				_, err := c.CommitProduceBatch(ctx, "peer", nodewire.CommitProduceBatchRequest{})
				return err
			},
			want: clusterrpc.LaneProduce,
		},
		"consume": {
			call: func(c *PeerClient) error {
				_, err := c.Consume(ctx, "peer", nodewire.ConsumeRequest{Topic: "orders"})
				return err
			},
			want: clusterrpc.LaneConsume,
		},
		"ack": {
			call: func(c *PeerClient) error { _, err := c.Ack(ctx, "peer", ack); return err },
			want: clusterrpc.LaneAck,
		},
		"extend_ack": {
			call: func(c *PeerClient) error { _, err := c.ExtendAck(ctx, "peer", ack); return err },
			want: clusterrpc.LaneAck,
		},
		"nack": {
			call: func(c *PeerClient) error { _, err := c.Nack(ctx, "peer", ack); return err },
			want: clusterrpc.LaneAck,
		},
		"fanout_cursors": {
			body: "[]",
			call: func(c *PeerClient) error { _, err := c.FanoutCursors(ctx, "peer", "orders"); return err },
			want: clusterrpc.LaneProduce,
		},
		"fetch_segment_chunk": {
			call: func(c *PeerClient) error {
				_, err := c.FetchSegmentChunk(ctx, "peer", "orders", 0, 0, 0, 1)
				return err
			},
			want: clusterrpc.LaneProduce,
		},
		"prepare_handoff": {
			body: "{}",
			call: func(c *PeerClient) error {
				_, err := c.PrepareHandoff(ctx, "peer", "orders", 0, time.Second)
				return err
			},
			want: clusterrpc.LaneProduce,
		},
		"list_partition_segments": {
			body: "{}",
			call: func(c *PeerClient) error { _, err := c.ListPartitionSegments(ctx, "peer", "orders", 0); return err },
			want: clusterrpc.LaneControl,
		},
		"create_topic": {
			call: func(c *PeerClient) error { _, err := c.CreateTopic(ctx, "peer", []byte("{}")); return err },
			want: clusterrpc.LaneControl,
		},
		"alter_topic": {
			call: func(c *PeerClient) error { _, err := c.AlterTopic(ctx, "peer", "orders", []byte("{}")); return err },
			want: clusterrpc.LaneControl,
		},
		"delete_topic": {
			call: func(c *PeerClient) error { _, err := c.DeleteTopic(ctx, "peer", "orders"); return err },
			want: clusterrpc.LaneControl,
		},
		"get_topic": {
			call: func(c *PeerClient) error { _, err := c.GetTopic(ctx, "peer", "orders"); return err },
			want: clusterrpc.LaneControl,
		},
		"purge_topic": {
			call: func(c *PeerClient) error { _, err := c.PurgeTopic(ctx, "peer", "orders"); return err },
			want: clusterrpc.LaneControl,
		},
		"attach_child": {
			call: func(c *PeerClient) error { _, err := c.AttachChild(ctx, "peer", "p", "c", 0); return err },
			want: clusterrpc.LaneControl,
		},
		"detach_child": {
			call: func(c *PeerClient) error { _, err := c.DetachChild(ctx, "peer", "p", "c"); return err },
			want: clusterrpc.LaneControl,
		},
		"topic_partition_stats": {
			body: `{"index":0}`,
			call: func(c *PeerClient) error { _, err := c.TopicPartitionStats(ctx, "peer", "orders", 0); return err },
			want: clusterrpc.LaneControl,
		},
		"join_cluster": {
			call: func(c *PeerClient) error {
				_, err := c.JoinCluster(ctx, "peer", nodewire.JoinClusterRequest{})
				return err
			},
			want: clusterrpc.LaneControl,
		},
		"register_member": {
			call: func(c *PeerClient) error {
				_, err := c.RegisterMember(ctx, "peer", nodewire.MemberRequest{})
				return err
			},
			want: clusterrpc.LaneControl,
		},
		"create_user": {
			call: func(c *PeerClient) error { _, err := c.CreateUser(ctx, "peer", []byte("{}")); return err },
			want: clusterrpc.LaneControl,
		},
		"update_user": {
			call: func(c *PeerClient) error { _, err := c.UpdateUser(ctx, "peer", "u", []byte("{}")); return err },
			want: clusterrpc.LaneControl,
		},
		"delete_user": {
			call: func(c *PeerClient) error { _, err := c.DeleteUser(ctx, "peer", "u"); return err },
			want: clusterrpc.LaneControl,
		},
		"decommission_member": {
			call: func(c *PeerClient) error { _, err := c.DecommissionMember(ctx, "peer", "n", false); return err },
			want: clusterrpc.LaneControl,
		},
		"complete_move": {
			call: func(c *PeerClient) error { return c.CompleteMove(ctx, "peer", "orders", 0, "a", "b") },
			want: clusterrpc.LaneControl,
		},
		"abort_move": {
			call: func(c *PeerClient) error { return c.AbortMove(ctx, "peer", "orders", 0, "b") },
			want: clusterrpc.LaneControl,
		},
		"get_assignment": {
			body: "{}",
			call: func(c *PeerClient) error { _, err := c.GetAssignment(ctx, "peer", "orders", 0); return err },
			want: clusterrpc.LaneControl,
		},
	} {
		t.Run(name, func(t *testing.T) {
			rec := &laneRecorder{body: []byte(tc.body)}
			client := &PeerClient{frames: rec}
			if err := tc.call(client); err != nil {
				t.Fatalf("%s error = %v", name, err)
			}
			if len(rec.lanes) != 1 || rec.lanes[0] != tc.want {
				t.Fatalf("%s lanes = %v, want [%s]", name, rec.lanes, tc.want)
			}
		})
	}
}

func TestPeerClientNilSafety(t *testing.T) {
	var client *PeerClient
	if _, err := client.Ack(context.Background(), "peer", nodewire.AckRequest{}); err == nil {
		t.Fatal("nil client Ack succeeded")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("nil client Close() error = %v", err)
	}
	if err := NewPeerClient(0, "").Close(); err != nil {
		t.Fatalf("Close() of an undialed client error = %v", err)
	}
}
