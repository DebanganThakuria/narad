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

// contentTypeOctetStream is the Response.ContentType for raw byte bodies
// (segment chunks, token deltas).
const contentTypeOctetStream = "application/octet-stream"

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

// AppendResponse appends the encoding of res to dst and returns the
// extended slice, so a caller can encode into a buffer it reuses. On
// error dst is returned unchanged. The encoding is identical to
// EncodeResponse's.
func AppendResponse(dst []byte, res Response) ([]byte, error) {
	if res.Status < 0 || res.Status > math.MaxUint16 {
		return dst, fmt.Errorf("invalid response status %d", res.Status)
	}
	w := writer{buf: dst}
	w.u16(uint16(res.Status))
	if err := w.string(res.ContentType); err != nil {
		return dst, err
	}
	if err := w.bytes(res.Body); err != nil {
		return dst, err
	}
	return w.finish(), nil
}

// DecodeResponse decodes a Response payload. Body aliases payload.
func DecodeResponse(payload []byte) (Response, error) {
	r := reader{payload: payload}
	status, err := r.u16()
	if err != nil {
		return Response{}, err
	}
	contentType, err := r.bytes()
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
	return Response{Status: int(status), ContentType: contentTypeString(contentType), Body: body}, nil
}

// contentTypeString converts a decoded content type to a string. The
// values servers actually send map to their constants, so decoding a
// reply does not allocate a copy of the same few bytes every time.
func contentTypeString(b []byte) string {
	switch string(b) {
	case "":
		return ""
	case ContentTypeJSON:
		return ContentTypeJSON
	case contentTypeOctetStream:
		return contentTypeOctetStream
	}
	return string(b)
}
