package cluster

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
)

const headerRequestID = "X-Request-ID"

func (rt *Router) forwardProbe(r *http.Request, addr string, body []byte) httptestResponseRecorder {
	probe := httptestResponseRecorder{header: make(http.Header)}
	rt.forward(&probe, r, addr, body)
	if probe.code == 0 {
		probe.code = http.StatusOK
	}
	return probe
}

type httptestResponseRecorder struct {
	header http.Header
	body   []byte
	code   int
}

func (r *httptestResponseRecorder) Header() http.Header {
	return r.header
}

func (r *httptestResponseRecorder) Write(body []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	r.body = append(r.body, body...)
	return len(body), nil
}

func (r *httptestResponseRecorder) WriteHeader(statusCode int) {
	r.code = statusCode
}

func copyHeader(dst, src http.Header) {
	for key := range dst {
		dst.Del(key)
	}
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

// forward proxies r to http://addr, optionally replacing the body.
func (rt *Router) forward(w http.ResponseWriter, r *http.Request, addr string, body []byte) {
	if body != nil {
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
	}
	target, _ := url.Parse("http://" + addr)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Del(headerRequestID)
		return nil
	}
	proxy.ServeHTTP(w, r)
}
