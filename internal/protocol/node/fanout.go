package node

// EncodeChildLinkRequest encodes a fan-out attach/detach payload under
// the given operation. Actor is an optional trailing field, written
// only when set.
func EncodeChildLinkRequest(op Operation, req ChildLinkRequest) ([]byte, error) {
	size := fieldLen(req.Parent) + fieldLen(req.Child) + 8
	if req.Actor != "" {
		size += fieldLen(req.Actor)
	}
	w := opWriter(op, size)
	if err := w.string(req.Parent); err != nil {
		return nil, err
	}
	if err := w.string(req.Child); err != nil {
		return nil, err
	}
	w.i64(req.DelayMs)
	if req.Actor != "" {
		if err := w.string(req.Actor); err != nil {
			return nil, err
		}
	}
	return w.finish(), nil
}

// DecodeChildLinkRequest decodes a fan-out attach/detach payload,
// verifying it carries the given operation.
func DecodeChildLinkRequest(payload []byte, op Operation) (ChildLinkRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return ChildLinkRequest{}, err
	}
	parent, err := r.string()
	if err != nil {
		return ChildLinkRequest{}, err
	}
	child, err := r.string()
	if err != nil {
		return ChildLinkRequest{}, err
	}
	delayMs, err := r.i64()
	if err != nil {
		return ChildLinkRequest{}, err
	}
	var actor string
	if r.remaining() > 0 {
		if actor, err = r.string(); err != nil {
			return ChildLinkRequest{}, err
		}
	}
	if err := r.done(); err != nil {
		return ChildLinkRequest{}, err
	}
	return ChildLinkRequest{Parent: parent, Child: child, DelayMs: delayMs, Actor: actor}, nil
}
