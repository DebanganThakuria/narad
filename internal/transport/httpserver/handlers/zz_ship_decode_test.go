package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// After a valid value, DecodeJSON reads one token of whatever follows.
// A token that cannot start a value is refused with the tokenizer's own
// error; one that starts a value, even a lone {, is a second value.
// Trailing whitespace is accepted.
func TestShipDecodeJSONTrailingToken(t *testing.T) {
	type req struct {
		Name string `json:"name"`
	}
	for _, tc := range []struct {
		trailing string
		want     string // the "error" of the 400 body; "" means accepted
	}{
		{"]", "invalid json: invalid character ']' looking for beginning of value"},
		{",", "invalid json: invalid character ',' looking for beginning of value"},
		{"x", "invalid json: invalid character 'x' looking for beginning of value"},
		{"}", "invalid json: invalid character '}' looking for beginning of value"},
		{"{", "invalid json: multiple JSON values"},
		{"[1]", "invalid json: multiple JSON values"},
		{" \n\t", ""},
	} {
		s := newTestSet(&fakeBroker{})
		res := httptest.NewRecorder()
		httpReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"orders"}`+tc.trailing))
		var decoded req
		ok := s.DecodeJSON(res, httpReq, &decoded)
		if tc.want == "" {
			if !ok || decoded.Name != "orders" {
				t.Fatalf("trailing %q: DecodeJSON = %v (%d %s), want the value accepted", tc.trailing, ok, res.Code, res.Body)
			}
			continue
		}
		if ok || res.Code != http.StatusBadRequest {
			t.Fatalf("trailing %q: DecodeJSON = %v, status %d; want false, 400", tc.trailing, ok, res.Code)
		}
		var body struct {
			Error string `json:"error"`
		}
		if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil || body.Error != tc.want {
			t.Fatalf("trailing %q: error %q (%v), want %q", tc.trailing, body.Error, err, tc.want)
		}
	}
}
