package node

// The applied-index probe carries no fields: the operation byte is the
// whole request. A follower sends it to the leader right after a
// forwarded control-plane write succeeded, and the leader answers with
// the highest log index its FSM has applied (a JSON body, see
// cluster.RPCServer.handleAppliedIndex). A leader that predates the
// operation answers "unsupported rpc operation", which the follower
// treats as "no wait possible" rather than as a failed write.

// EncodeAppliedIndexRequest encodes the payload-free applied-index probe.
func EncodeAppliedIndexRequest() []byte {
	return opWriter(OpAppliedIndex, 0).finish()
}

// DecodeAppliedIndexRequest verifies an applied-index probe payload.
func DecodeAppliedIndexRequest(payload []byte) error {
	r, err := opReader(payload, OpAppliedIndex)
	if err != nil {
		return err
	}
	return r.done()
}
