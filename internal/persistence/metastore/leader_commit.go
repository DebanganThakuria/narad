package metastore

// Where a follower learns the LEADER's commit index. Raft's local
// commit_index is not that number: after a restart it is 0 (only
// lastApplied is restored from the snapshot), and it only rises when an
// AppendEntries carrying LeaderCommitIndex > 0 arrives. Heartbeats carry
// no LeaderCommitIndex at all, yet they do refresh LastContact, so a
// restarted follower that has heard exactly one heartbeat satisfies
// "fresh contact AND applied >= commit" (0 >= 0) with its FSM still at
// the snapshot. The leader's replication goroutine can back off for up
// to ~10s after a node was down, so that window is real, and it is
// exactly when the ownership latch (ListAssignments) must NOT set.
//
// Even the first real AppendEntries is not enough on its own: the
// follower sets commit_index = min(LeaderCommitIndex, its own last log
// index), so a node that is far behind and receiving entries in batches
// reads applied >= commit after every batch while the leader's commit
// index is still far ahead.
//
// The fix records the highest LeaderCommitIndex ever received on the
// Raft transport, before Raft sees the request. AppliedCaughtUp then
// requires applied_index >= that value for a follower: the FSM has
// applied everything the leader had committed as of the last real
// AppendEntries, which is the "caught up once since start" the latch
// needs. hashicorp/raft has no observer for AppendEntries, so the hook
// is a thin Transport wrapper over the consumer channel.

import (
	"io"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

// commitObservingTransport wraps a raft.Transport, relaying every
// inbound RPC unchanged while recording the highest LeaderCommitIndex
// seen on AppendEntries requests. It also forwards the optional
// WithClose and WithPreVote capabilities so Raft keeps pre-vote enabled
// and can close the underlying transport on shutdown.
type commitObservingTransport struct {
	inner raft.Transport
	out   chan raft.RPC
	done  chan struct{}

	// leaderCommit is the highest LeaderCommitIndex observed since this
	// process started. 0 means no real AppendEntries has arrived yet.
	leaderCommit atomic.Uint64
}

func newCommitObservingTransport(inner raft.Transport) *commitObservingTransport {
	t := &commitObservingTransport{
		inner: inner,
		out:   make(chan raft.RPC),
		done:  make(chan struct{}),
	}
	go t.relay()
	return t
}

// relay copies RPCs from the inner consumer channel to the outer one,
// observing AppendEntries on the way. It exits when the transport is
// closed; Raft consumes from out until then.
func (t *commitObservingTransport) relay() {
	in := t.inner.Consumer()
	for {
		select {
		case <-t.done:
			return
		case rpc := <-in:
			t.observe(rpc.Command)
			select {
			case t.out <- rpc:
			case <-t.done:
				return
			}
		}
	}
}

// observe records the leader's commit index carried by a real
// AppendEntries. Heartbeats carry 0 and are ignored, as is everything
// that is not an AppendEntries.
func (t *commitObservingTransport) observe(command any) {
	ae, ok := command.(*raft.AppendEntriesRequest)
	if !ok || ae.LeaderCommitIndex == 0 {
		return
	}
	for {
		cur := t.leaderCommit.Load()
		if ae.LeaderCommitIndex <= cur || t.leaderCommit.CompareAndSwap(cur, ae.LeaderCommitIndex) {
			return
		}
	}
}

// LeaderCommitIndex returns the highest commit index any leader has
// reported to this node since the process started, or 0 if no real
// AppendEntries has arrived yet.
func (t *commitObservingTransport) LeaderCommitIndex() uint64 {
	return t.leaderCommit.Load()
}

// raft.Transport.

func (t *commitObservingTransport) Consumer() <-chan raft.RPC { return t.out }

func (t *commitObservingTransport) LocalAddr() raft.ServerAddress { return t.inner.LocalAddr() }

func (t *commitObservingTransport) AppendEntriesPipeline(id raft.ServerID, target raft.ServerAddress) (raft.AppendPipeline, error) {
	return t.inner.AppendEntriesPipeline(id, target)
}

func (t *commitObservingTransport) AppendEntries(id raft.ServerID, target raft.ServerAddress, args *raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	return t.inner.AppendEntries(id, target, args, resp)
}

func (t *commitObservingTransport) RequestVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	return t.inner.RequestVote(id, target, args, resp)
}

func (t *commitObservingTransport) InstallSnapshot(id raft.ServerID, target raft.ServerAddress, args *raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse, data io.Reader) error {
	return t.inner.InstallSnapshot(id, target, args, resp, data)
}

func (t *commitObservingTransport) EncodePeer(id raft.ServerID, addr raft.ServerAddress) []byte {
	return t.inner.EncodePeer(id, addr)
}

func (t *commitObservingTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return t.inner.DecodePeer(buf)
}

func (t *commitObservingTransport) SetHeartbeatHandler(cb func(rpc raft.RPC)) {
	// Heartbeats bypass the consumer channel by design (and carry no
	// commit index), so the handler goes straight to the inner transport.
	t.inner.SetHeartbeatHandler(cb)
}

func (t *commitObservingTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	return t.inner.TimeoutNow(id, target, args, resp)
}

// raft.WithPreVote. The production transports (TCP and TLS
// NetworkTransport) both support pre-vote; a wrapped transport that does
// not answers with an error, which Raft treats as a refused pre-vote.

func (t *commitObservingTransport) RequestPreVote(id raft.ServerID, target raft.ServerAddress, args *raft.RequestPreVoteRequest, resp *raft.RequestPreVoteResponse) error {
	if pv, ok := t.inner.(raft.WithPreVote); ok {
		return pv.RequestPreVote(id, target, args, resp)
	}
	return errPreVoteUnsupported
}

// raft.WithClose.

func (t *commitObservingTransport) Close() error {
	select {
	case <-t.done:
	default:
		close(t.done)
	}
	if c, ok := t.inner.(raft.WithClose); ok {
		return c.Close()
	}
	return nil
}

var (
	_ raft.Transport   = (*commitObservingTransport)(nil)
	_ raft.WithPreVote = (*commitObservingTransport)(nil)
	_ raft.WithClose   = (*commitObservingTransport)(nil)
)
