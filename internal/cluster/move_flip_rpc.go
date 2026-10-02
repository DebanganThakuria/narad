package cluster

// The two leader round trips a move worker makes after a flip whose
// reply was an error: the flip itself, whose refusal by the state
// machine proves the proposal can never commit, and the assignment read
// that resolves the outcome, which counts as proof that the flip cannot
// happen only when a barriered leader answered it.
//
// Both ride the existing wire ops. The additions are on the answers only
// (a 409 and a 404 from handleCompleteMove, a leader_read field in
// handleGetAssignment's JSON), so an older peer's answers decode as
// before and are simply trusted less: its 503 leaves a flip's outcome
// unknown, and its unmarked assignment can confirm a flip but never
// authorize undoing one.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// assignmentReply is handleGetAssignment's 200 body: the assignment's
// own JSON fields, plus LeaderRead.
type assignmentReply struct {
	metastore.Assignment
	// LeaderRead reports that the answering node was the Raft leader and
	// read behind a barrier, so the answer includes every committed
	// entry. Releases before it never set it.
	LeaderRead bool `json:"leader_read,omitempty"`
}

// assignmentMissing is handleGetAssignment's 404 body.
type assignmentMissing struct {
	Error      string `json:"error"`
	LeaderRead bool   `json:"leader_read,omitempty"`
}

// flipRefusedError is a forwarded flip the leader's state machine
// applied and refused: the compare-and-set found another owner or
// target (409), or the partition has no assignment (404). That proposal
// can never commit.
type flipRefusedError struct {
	status int
	msg    string
}

func (e *flipRefusedError) Error() string {
	return fmt.Sprintf("complete move refused (status %d): %s", e.status, e.msg)
}

// flipNotProposedError is a flip that never left this node (no leader
// was known to forward it to).
type flipNotProposedError struct{ cause error }

func (e *flipNotProposedError) Error() string { return "flip not proposed: " + e.cause.Error() }
func (e *flipNotProposedError) Unwrap() error { return e.cause }

// flipSettled reports whether err, returned by a flip proposal, proves
// the proposal cannot commit later: the state machine refused it (the
// compare-and-set's errs.ErrInvalidArgument or a missing assignment's
// errs.ErrNotFound when proposed locally, a 409 or 404 when forwarded),
// or it was never sent. Any other error (a timeout, lost leadership, a
// 503) leaves the proposal possibly in the leader's log.
func flipSettled(err error) bool {
	var refused *flipRefusedError
	var notProposed *flipNotProposedError
	return errors.As(err, &refused) || errors.As(err, &notProposed) ||
		errors.Is(err, errs.ErrInvalidArgument) || errors.Is(err, errs.ErrNotFound)
}

// leaderAssignmentReader is the optional peer capability that reads an
// assignment from the leader and reports whether the answer was a
// barriered leader read (*PeerClient implements it). Without it (test
// fakes, older code) an answer is never a leader read.
type leaderAssignmentReader interface {
	leaderAssignment(ctx context.Context, addr, topicName string, partition int) (a metastore.Assignment, found, leaderRead bool, err error)
}

var _ leaderAssignmentReader = (*PeerClient)(nil)

// leaderAssignment reads the assignment of topicName/partition from the
// node at addr. found is false when that node reports no assignment;
// leaderRead reports that it answered as the barriered leader. Any other
// answer (a follower's 421, a 503) is an error.
func (c *PeerClient) leaderAssignment(ctx context.Context, addr, topicName string, partition int) (metastore.Assignment, bool, bool, error) {
	payload, err := nodewire.EncodeGetAssignmentRequest(nodewire.GetAssignmentRequest{Topic: topicName, Partition: partition})
	res, err := c.send(ctx, addr, "get_assignment", laneControl, payload, err)
	if err != nil {
		return metastore.Assignment{}, false, false, err
	}
	switch res.Status {
	case http.StatusOK:
		var reply assignmentReply
		if err := json.Unmarshal(res.Body, &reply); err != nil {
			return metastore.Assignment{}, false, false, err
		}
		return reply.Assignment, true, reply.LeaderRead, nil
	case http.StatusNotFound:
		var missing assignmentMissing
		_ = json.Unmarshal(res.Body, &missing)
		return metastore.Assignment{}, false, missing.LeaderRead, nil
	}
	return metastore.Assignment{}, false, false, fmt.Errorf("get assignment returned status %d", res.Status)
}
