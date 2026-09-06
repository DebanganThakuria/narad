package httpserver

import (
	"mime"
	"net/http"
	"strconv"
)

// ClientHeader is a request header whose mere presence marks a request
// as coming from an API client rather than a browser form or fetch: a
// browser only sends custom headers cross-origin after a CORS preflight,
// which Narad never approves. Its value is free-form (a client name).
const ClientHeader = "X-Narad-Client"

// apiContentTypes are the media types state-changing requests may
// carry. Both need a CORS preflight when sent cross-origin by a browser
// (they are not "simple" request content types), so requiring one of
// them, or ClientHeader, means a page on another origin cannot ride an
// operator's cached Basic credentials into a POST here.
var apiContentTypes = map[string]bool{
	"application/json":         true,
	"application/octet-stream": true,
}

// RequireAPIContentType is the cross-site request forgery guard for
// Basic-auth sessions. Browsers attach cached Basic credentials to
// cross-origin requests, and a POST/PUT/PATCH whose body is text/plain,
// form-urlencoded or multipart needs no preflight, so a malicious page
// could create topics, produce, ack, or decommission a member on behalf
// of an operator who used the API from that browser. Handlers never
// checked Content-Type. State-changing methods must now carry
// application/json or application/octet-stream, or ClientHeader;
// anything else is answered 415. DELETE (no body) and the read methods
// are always preflighted or harmless and are not checked.
func RequireAPIContentType() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost, http.MethodPut, http.MethodPatch:
			default:
				next.ServeHTTP(w, r)
				return
			}
			if r.Header.Get(ClientHeader) != "" || apiContentType(r.Header.Get("Content-Type")) {
				next.ServeHTTP(w, r)
				return
			}
			writeUnsupportedMediaType(w)
		})
	}
}

// apiContentType reports whether the Content-Type header names one of
// apiContentTypes (parameters such as charset are allowed).
func apiContentType(header string) bool {
	if header == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(header)
	if err != nil {
		return false
	}
	return apiContentTypes[mediaType]
}

func writeUnsupportedMediaType(w http.ResponseWriter) {
	msg := "state-changing requests must send Content-Type: application/json or application/octet-stream, or an " + ClientHeader + " header"
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = strconv.AppendQuote(body, msg)
	body = append(body, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusUnsupportedMediaType)
	_, _ = w.Write(body)
}
