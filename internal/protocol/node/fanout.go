package node

// EncodeChildLinkRequest encodes a fan-out attach/detach payload under
// the given operation.
func EncodeChildLinkRequest(op Operation, req ChildLinkRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Parent)+fieldLen(req.Child)+8)
	if err := w.string(req.Parent); err != nil {
		return nil, err
	}
	if err := w.string(req.Child); err != nil {
		return nil, err
	}
	w.i64(req.DelayMs)
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
	if err := r.done(); err != nil {
		return ChildLinkRequest{}, err
	}
	return ChildLinkRequest{Parent: parent, Child: child, DelayMs: delayMs}, nil
}
