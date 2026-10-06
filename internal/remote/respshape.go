package remote

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
)

// Response shape, shared by the checks and the sink's classifier.
// Answers from a remote are read through size-capped readers and only a
// few facts are taken from them: the status, whether the body has
// Narad's error shape, and fields of typed structs. The remote's error
// text is never logged and never returned, because a schema-validation
// error can echo payload values.
const (
	// MaxProduceAnswerBytes caps a produce answer.
	MaxProduceAnswerBytes = 64 << 10
	// MaxReadAnswerBytes caps a topic or children read.
	MaxReadAnswerBytes = 1 << 20
)

// ReadBody reads at most limit bytes of resp's body and always closes
// it. A body longer than limit is truncated, not an error: callers only
// ever parse a prefix they can bound.
func ReadBody(resp *http.Response, limit int64) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	// Drain a little more so the connection can be reused, without
	// reading an unbounded body.
	_, _ = io.CopyN(io.Discard, resp.Body, 4<<10)
	return body, err
}

// NaradError reports whether resp carries Narad's error shape: an
// application/json body that is an object with an "error" string
// (handlers/respond.go). msg is that string; callers classify on it and
// never log or return it.
func NaradError(resp *http.Response, body []byte) (msg string, ok bool) {
	if resp == nil || !isJSON(resp.Header.Get("Content-Type")) {
		return "", false
	}
	var shape struct {
		Error *string `json:"error"`
	}
	if err := json.Unmarshal(body, &shape); err != nil || shape.Error == nil {
		return "", false
	}
	return *shape.Error, true
}

// goNotFoundBody is the exact body of Go's http.NotFound, which a
// ServeMux answers for a route it does not have.
const goNotFoundBody = "404 page not found\n"

// IsGoNotFound reports Go's exact not-found answer: status 404, body
// "404 page not found\n" and Content-Type "text/plain; charset=utf-8".
// A Narad cluster registers no custom not-found handler, so that is
// what it answers for a route it lacks, while its handlers answer
// every 404 of their own in Narad's JSON shape.
func IsGoNotFound(resp *http.Response, body []byte) bool {
	return resp != nil &&
		resp.StatusCode == http.StatusNotFound &&
		resp.Header.Get("Content-Type") == "text/plain; charset=utf-8" &&
		bytes.Equal(body, []byte(goNotFoundBody))
}

func isJSON(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "application/json"
}
