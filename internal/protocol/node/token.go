package node

import "fmt"

// maxTokenBatch bounds how many entries one delta frame may carry, so a
// peer cannot make this node allocate an unbounded slice by lying about
// the count. Registrations reserve nothing, so batches are allowed to be
// large: the cap is about memory safety, not flow control.
const maxTokenBatch = 4096

// EncodeTokenDelta encodes an OpTokenRegister payload: the adds and
// drops for one peer, batched into a single frame. Everything is a
// fixed field order written straight into one right-sized buffer — no
// reflection and no intermediate objects, because this runs whenever a
// consumer arrives or is served.
func EncodeTokenDelta(delta TokenDelta) ([]byte, error) {
	if len(delta.Add) > maxTokenBatch || len(delta.Drop) > maxTokenBatch {
		return nil, fmt.Errorf("token delta too large: %d adds, %d drops (max %d each)",
			len(delta.Add), len(delta.Drop), maxTokenBatch)
	}
	size := fieldLen(delta.From) + 4 + 4
	for _, a := range delta.Add {
		size += fieldLen(a.Topic) + 8 + 4
	}
	for _, t := range delta.Drop {
		size += fieldLen(t)
	}
	w := opWriter(OpTokenRegister, size)
	if err := w.string(delta.From); err != nil {
		return nil, err
	}
	w.i32(int32(len(delta.Add)))
	for _, a := range delta.Add {
		if err := w.string(a.Topic); err != nil {
			return nil, err
		}
		w.i64(a.TTLNanos)
		w.i32(a.MinRecords)
	}
	w.i32(int32(len(delta.Drop)))
	for _, t := range delta.Drop {
		if err := w.string(t); err != nil {
			return nil, err
		}
	}
	return w.finish(), nil
}

// DecodeTokenDelta decodes an OpTokenRegister payload.
func DecodeTokenDelta(payload []byte) (TokenDelta, error) {
	r, err := opReader(payload, OpTokenRegister)
	if err != nil {
		return TokenDelta{}, err
	}
	from, err := r.string()
	if err != nil {
		return TokenDelta{}, err
	}
	addCount, err := r.i32()
	if err != nil {
		return TokenDelta{}, err
	}
	if addCount < 0 || addCount > maxTokenBatch {
		return TokenDelta{}, fmt.Errorf("invalid token add count %d", addCount)
	}
	delta := TokenDelta{From: from, Add: make([]TokenRegistration, 0, addCount)}
	for range addCount {
		topic, err := r.string()
		if err != nil {
			return TokenDelta{}, err
		}
		ttl, err := r.i64()
		if err != nil {
			return TokenDelta{}, err
		}
		minRecords, err := r.i32()
		if err != nil {
			return TokenDelta{}, err
		}
		delta.Add = append(delta.Add, TokenRegistration{
			Topic: topic, TTLNanos: ttl, MinRecords: minRecords,
		})
	}
	dropCount, err := r.i32()
	if err != nil {
		return TokenDelta{}, err
	}
	if dropCount < 0 || dropCount > maxTokenBatch {
		return TokenDelta{}, fmt.Errorf("invalid token drop count %d", dropCount)
	}
	if dropCount > 0 {
		delta.Drop = make([]string, 0, dropCount)
	}
	for range dropCount {
		topic, err := r.string()
		if err != nil {
			return TokenDelta{}, err
		}
		delta.Drop = append(delta.Drop, topic)
	}
	if err := r.done(); err != nil {
		return TokenDelta{}, err
	}
	return delta, nil
}

// EncodeTokenNotifyRequest encodes an OpTokenNotify payload: the frame
// an owner sends to spend one of a peer's tokens.
func EncodeTokenNotifyRequest(req TokenNotifyRequest) ([]byte, error) {
	w := opWriter(OpTokenNotify, fieldLen(req.From)+fieldLen(req.Topic)+4)
	if err := w.string(req.From); err != nil {
		return nil, err
	}
	if err := w.string(req.Topic); err != nil {
		return nil, err
	}
	w.i32(req.Available)
	return w.finish(), nil
}

// DecodeTokenNotifyRequest decodes an OpTokenNotify payload.
func DecodeTokenNotifyRequest(payload []byte) (TokenNotifyRequest, error) {
	r, err := opReader(payload, OpTokenNotify)
	if err != nil {
		return TokenNotifyRequest{}, err
	}
	from, err := r.string()
	if err != nil {
		return TokenNotifyRequest{}, err
	}
	topic, err := r.string()
	if err != nil {
		return TokenNotifyRequest{}, err
	}
	available, err := r.i32()
	if err != nil {
		return TokenNotifyRequest{}, err
	}
	if err := r.done(); err != nil {
		return TokenNotifyRequest{}, err
	}
	return TokenNotifyRequest{From: from, Topic: topic, Available: available}, nil
}

// EncodeTokenNotifyReply encodes the verdict body: one byte, because
// the reply exists only to say claim-or-pass and it is sent on every
// notification.
func EncodeTokenNotifyReply(reply TokenNotifyReply) []byte {
	var b byte
	if reply.Claiming {
		b = 1
	}
	return []byte{b}
}

// DecodeTokenNotifyReply decodes the verdict body. An empty or
// unrecognised body reads as a pass, which is the safe direction: the
// owner offers the record to someone else instead of waiting out a
// deadline for a claim that is not coming.
func DecodeTokenNotifyReply(body []byte) TokenNotifyReply {
	return TokenNotifyReply{Claiming: len(body) == 1 && body[0] == 1}
}
