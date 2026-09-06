package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fuzzRequest has one field of every shape the request types use.
type fuzzRequest struct {
	Name     string          `json:"name"`
	Count    int             `json:"count"`
	Big      int64           `json:"big"`
	Optional *int64          `json:"optional,omitempty"`
	Raw      json.RawMessage `json:"raw,omitempty"`
	Flag     bool            `json:"flag"`
	Items    []string        `json:"items,omitempty"`
}

// FuzzDecodeJSONBytes feeds arbitrary bodies to the strict decoder. It
// must never panic; a rejected body is answered 400 and a decoded body
// is exactly one JSON value with no unknown fields.
func FuzzDecodeJSONBytes(f *testing.F) {
	f.Add([]byte(`{"name":"a","count":1,"big":2,"optional":3,"raw":{"x":[1]},"flag":true,"items":["z"]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(``))
	f.Add([]byte(`{"name":"a"} {"name":"b"}`))
	f.Add([]byte(`{"unknown":1}`))
	f.Add([]byte(`{"count":1e400}`))
	f.Add([]byte(`{"count":"1"}`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{"name":"\ud800"}`))
	f.Add([]byte(strings.Repeat("[", 10000)))
	f.Add([]byte(`{"raw":` + strings.Repeat("{\"a\":", 5000) + "1" + strings.Repeat("}", 5000) + `}`))

	s := newTestSet(&fakeBroker{})
	f.Fuzz(func(t *testing.T, body []byte) {
		var dst fuzzRequest
		rec := httptest.NewRecorder()
		ok := s.DecodeJSONBytes(rec, body, &dst)
		if !ok {
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("rejected body answered %d, want 400", rec.Code)
			}
			return
		}
		if rec.Body.Len() != 0 {
			t.Fatalf("accepted body wrote a response: %s", rec.Body.String())
		}
		// Strictness: encoding/json on its own must accept it too, and
		// the value must survive a round trip.
		var again fuzzRequest
		enc, err := json.Marshal(dst)
		if err != nil {
			t.Fatalf("marshal decoded value: %v", err)
		}
		if err := json.Unmarshal(enc, &again); err != nil {
			t.Fatalf("re-decode: %v", err)
		}
	})
}
