package node

// EncodeUserRequest encodes a user write payload under the given
// operation (create, update, or delete user).
func EncodeUserRequest(op Operation, req UserRequest) ([]byte, error) {
	w := opWriter(op, fieldLen(req.Username)+fieldLenBytes(req.Body))
	if err := w.string(req.Username); err != nil {
		return nil, err
	}
	if err := w.bytes(req.Body); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeUserRequest decodes a user write payload, verifying it carries
// the given operation.
func DecodeUserRequest(payload []byte, op Operation) (UserRequest, error) {
	r, err := opReader(payload, op)
	if err != nil {
		return UserRequest{}, err
	}
	username, err := r.string()
	if err != nil {
		return UserRequest{}, err
	}
	body, err := r.bytes()
	if err != nil {
		return UserRequest{}, err
	}
	if err := r.done(); err != nil {
		return UserRequest{}, err
	}
	return UserRequest{Username: username, Body: body}, nil
}
