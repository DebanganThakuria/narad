package messaging

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
)

// captureWriter is an http.ResponseWriter that keeps what a handler or
// router wrote instead of sending it. The batch forms of consume and
// ack run the single-record machinery (routing, long-polls, error
// mapping) against one and then fold the outcome into their own
// response, so a record in a batch is served by exactly the code that
// serves it alone.
type captureWriter struct {
	header http.Header
	status int
	body   []byte
}

func (c *captureWriter) Header() http.Header {
	if c.header == nil {
		c.header = make(http.Header)
	}
	return c.header
}

func (c *captureWriter) WriteHeader(status int) {
	if c.status == 0 {
		c.status = status
	}
}

func (c *captureWriter) Write(p []byte) (int, error) {
	if c.status == 0 {
		c.status = http.StatusOK
	}
	c.body = append(c.body, p...)
	return len(p), nil
}

// code is the status the capture would have sent: net/http answers 200
// for a handler that wrote nothing.
func (c *captureWriter) code() int {
	if c.status == 0 {
		return http.StatusOK
	}
	return c.status
}

// message reports whether the capture is one delivered record: a 200
// whose JSON body is a single message, which is what every consume path
// writes when it has one.
func (c *captureWriter) message() ([]byte, bool) {
	if c.code() != http.StatusOK || len(c.body) == 0 {
		return nil, false
	}
	if ct := c.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return nil, false
	}
	return bytes.TrimRight(c.body, "\n"), true
}

// replay sends the captured response on w unchanged.
func (c *captureWriter) replay(w http.ResponseWriter) {
	h := w.Header()
	for k, v := range c.header {
		h[k] = v
	}
	w.WriteHeader(c.code())
	if len(c.body) > 0 {
		_, _ = w.Write(c.body)
	}
}

// outcome is the status and, for a failure, the error message of the
// captured response: the "error" field of a JSON body, else its text.
func (c *captureWriter) outcome() (int, string) {
	status := c.code()
	if status < http.StatusMultipleChoices {
		return status, ""
	}
	if strings.HasPrefix(c.header.Get("Content-Type"), "application/json") {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(c.body, &e) == nil && e.Error != "" {
			return status, e.Error
		}
	}
	return status, strings.TrimSpace(string(c.body))
}
