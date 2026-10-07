package node

// The remote-replication RPCs. OpRemoteWrite carries every mutation of
// the remotes registry and of remote child links to the leader, where
// it is proposed through Raft and audited; OpRemoteCheck asks one
// member to run the target checks from its own network position, to
// report what its credential cache holds, or to count its ingress
// backlog for one topic. Both carry a sub-operation name and an opaque
// JSON body whose shape belongs to the sub-operation's owner, so a new
// sub-operation never changes this encoding.

// OpRemoteWrite sub-operations. Stable on the wire.
const (
	RemoteSubCreate      = "remote.create"
	RemoteSubUpdate      = "remote.update"
	RemoteSubDelete      = "remote.delete"
	RemoteSubReencrypt   = "remote.reencrypt"
	RemoteSubAttach      = "child.attach"
	RemoteSubPause       = "child.pause"
	RemoteSubResume      = "child.resume"
	RemoteSubSkip        = "child.skip"
	RemoteSubDetach      = "child.delete"
	RemoteSubTopicDelete = "topic.delete"
)

// OpRemoteCheck modes. Stable on the wire.
const (
	RemoteCheckRun       = "check"     // the ch. 4.7 checks, from this member
	RemoteCheckStatus    = "status"    // cache contents and posture; no outbound call
	RemoteCheckUnshipped = "unshipped" // this member's ingress backlog for one topic
)

// RemoteWriteRequest is the OpRemoteWrite payload. Actor and RequestID
// are attribution for the leader's audit line, never authorization:
// the ingress node authorized the request before forwarding it.
type RemoteWriteRequest struct {
	SubOp     string
	Actor     string
	RequestID string
	Body      []byte // the sub-op's JSON payload, defined by the sub-op's owner
}

// RemoteCheckRequest is the OpRemoteCheck payload.
type RemoteCheckRequest struct {
	Mode      string
	RequestID string
	Body      []byte // JSON: remote.CheckRequest, or the unshipped query
}

// EncodeRemoteWriteRequest encodes an OpRemoteWrite payload: the op
// byte, then the sub-op, actor, request ID and body, each
// length-prefixed.
func EncodeRemoteWriteRequest(req RemoteWriteRequest) ([]byte, error) {
	w := opWriter(OpRemoteWrite, fieldLen(req.SubOp)+fieldLen(req.Actor)+fieldLen(req.RequestID)+fieldLenBytes(req.Body))
	for _, s := range []string{req.SubOp, req.Actor, req.RequestID} {
		if err := w.string(s); err != nil {
			return nil, err
		}
	}
	if err := w.bytes(req.Body); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeRemoteWriteRequest decodes an OpRemoteWrite payload. Body
// aliases payload.
func DecodeRemoteWriteRequest(payload []byte) (RemoteWriteRequest, error) {
	r, err := opReader(payload, OpRemoteWrite)
	if err != nil {
		return RemoteWriteRequest{}, err
	}
	var req RemoteWriteRequest
	if req.SubOp, err = r.string(); err != nil {
		return RemoteWriteRequest{}, err
	}
	if req.Actor, err = r.string(); err != nil {
		return RemoteWriteRequest{}, err
	}
	if req.RequestID, err = r.string(); err != nil {
		return RemoteWriteRequest{}, err
	}
	if req.Body, err = r.bytes(); err != nil {
		return RemoteWriteRequest{}, err
	}
	if err := r.done(); err != nil {
		return RemoteWriteRequest{}, err
	}
	return req, nil
}

// EncodeRemoteCheckRequest encodes an OpRemoteCheck payload: the op
// byte, then the mode, request ID and body, each length-prefixed.
func EncodeRemoteCheckRequest(req RemoteCheckRequest) ([]byte, error) {
	w := opWriter(OpRemoteCheck, fieldLen(req.Mode)+fieldLen(req.RequestID)+fieldLenBytes(req.Body))
	if err := w.string(req.Mode); err != nil {
		return nil, err
	}
	if err := w.string(req.RequestID); err != nil {
		return nil, err
	}
	if err := w.bytes(req.Body); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeRemoteCheckRequest decodes an OpRemoteCheck payload. Body
// aliases payload.
func DecodeRemoteCheckRequest(payload []byte) (RemoteCheckRequest, error) {
	r, err := opReader(payload, OpRemoteCheck)
	if err != nil {
		return RemoteCheckRequest{}, err
	}
	var req RemoteCheckRequest
	if req.Mode, err = r.string(); err != nil {
		return RemoteCheckRequest{}, err
	}
	if req.RequestID, err = r.string(); err != nil {
		return RemoteCheckRequest{}, err
	}
	if req.Body, err = r.bytes(); err != nil {
		return RemoteCheckRequest{}, err
	}
	if err := r.done(); err != nil {
		return RemoteCheckRequest{}, err
	}
	return req, nil
}
