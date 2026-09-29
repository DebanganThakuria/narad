package httpserver

import (
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	httpmessaging "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/messaging"
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
// anything else is answered 415. DELETE (no body) is always preflighted.
//
// A GET consume is not read-only either: it reserves records and hides
// them for their visibility window, and a cross-origin page can send
// one (an image tag) with the cached credentials. It cannot read the
// response, so it can delay delivery but neither see nor lose a record.
// A single consume stays open to plain clients (curl without the
// header), as it always was; a batch consume (?max=N), which reserves up
// to 100 records a request, must carry ClientHeader and is answered 400
// without it. HEAD is checked too: the mux serves it on GET routes, and a
// page can send it without a preflight as well.
func RequireAPIContentType() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost, http.MethodPut, http.MethodPatch:
			case http.MethodGet, http.MethodHead:
				if r.Header.Get(ClientHeader) == "" && batchConsume(r) {
					writeBatchConsumeNeedsClientHeader(w)
					return
				}
				next.ServeHTTP(w, r)
				return
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

// batchConsume reports whether r is a consume with a non-empty max
// parameter (an empty max= is a single consume), read with the consume
// handler's own query walker. A plain consume costs a suffix check and
// two substring checks; the query is walked only when it mentions max or
// is percent-encoded (%6Dax is max too).
func batchConsume(r *http.Request) bool {
	return strings.HasSuffix(r.URL.Path, "/consume") && httpmessaging.BatchConsumeRequested(r.URL.RawQuery)
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
	writeCrossSiteRefusal(w, http.StatusUnsupportedMediaType,
		"state-changing requests must send Content-Type: application/json or application/octet-stream, or an "+ClientHeader+" header")
}

func writeBatchConsumeNeedsClientHeader(w http.ResponseWriter) {
	writeCrossSiteRefusal(w, http.StatusBadRequest,
		"a batch consume (max) must send an "+ClientHeader+" header: it reserves messages, and the header keeps a page on another origin from doing that with a browser's cached credentials")
}

func writeCrossSiteRefusal(w http.ResponseWriter, status int, msg string) {
	body := make([]byte, 0, len(msg)+14)
	body = append(body, `{"error":`...)
	body = topic.AppendJSONQuoted(body, msg)
	body = append(body, "}\n"...)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
