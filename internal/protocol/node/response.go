package node

import (
	"fmt"
	"math"
)

// Response is the reply to any node RPC: an HTTP-style status code plus
// an optional body.
type Response struct {
	Status      int
	ContentType string
	Body        []byte
}

// EncodeResponse encodes a Response payload. The status must fit in a
// uint16.
func EncodeResponse(res Response) ([]byte, error) {
	if res.Status < 0 || res.Status > math.MaxUint16 {
		return nil, fmt.Errorf("invalid response status %d", res.Status)
	}
	w := newWriter(2 + fieldLen(res.ContentType) + fieldLenBytes(res.Body))
	w.u16(uint16(res.Status))
	if err := w.string(res.ContentType); err != nil {
		return nil, err
	}
	if err := w.bytes(res.Body); err != nil {
		return nil, err
	}
	return w.finish(), nil
}

// DecodeResponse decodes a Response payload.
func DecodeResponse(payload []byte) (Response, error) {
	r := newReader(payload)
	status, err := r.u16()
	if err != nil {
		return Response{}, err
	}
	contentType, err := r.string()
	if err != nil {
		return Response{}, err
	}
	body, err := r.bytes()
	if err != nil {
		return Response{}, err
	}
	if err := r.done(); err != nil {
		return Response{}, err
	}
	return Response{Status: int(status), ContentType: contentType, Body: body}, nil
}
