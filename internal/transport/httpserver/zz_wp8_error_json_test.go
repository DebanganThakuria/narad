package httpserver

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// TestWP8EdgeErrorBodiesAreValidJSON pins the edge middleware's own
// error bodies to valid JSON with the message intact.
func TestWP8EdgeErrorBodiesAreValidJSON(t *testing.T) {
	for name, write := range map[string]func(*httptest.ResponseRecorder){
		"unsupported media type": func(w *httptest.ResponseRecorder) { writeUnsupportedMediaType(w) },
		"too many in flight":     func(w *httptest.ResponseRecorder) { writeTooManyInFlight(w, 7) },
	} {
		rec := httptest.NewRecorder()
		write(rec)
		var got struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got.Error == "" {
			t.Fatalf("%s: body %q: err %v", name, rec.Body.String(), err)
		}
	}
}
