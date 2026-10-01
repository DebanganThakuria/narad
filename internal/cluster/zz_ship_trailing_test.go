package cluster

import (
	"net/http"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The rolling-upgrade fallbacks key on an older owner's refusal of a
// field it does not know, which is the decoder's trailing-data error
// passed through in a 400. Anything else is a real failure.
func TestIsTrailingFieldRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  nodewire.Response
		want bool
	}{
		{"older owner", errorResponse(http.StatusBadRequest, "invalid consume request: "+nodewire.TrailingPayloadError), true},
		{"other bad request", errorResponse(http.StatusBadRequest, "invalid consume request: unexpected EOF"), false},
		{"unknown op", errorResponse(http.StatusBadRequest, "unsupported rpc operation 42"), false},
		{"not a 400", errorResponse(http.StatusInternalServerError, nodewire.TrailingPayloadError), false},
		{"success", nodewire.Response{Status: http.StatusOK, Body: []byte("trailing")}, false},
	} {
		if got := isTrailingFieldRefusal(tc.res); got != tc.want {
			t.Errorf("%s: isTrailingFieldRefusal = %v, want %v", tc.name, got, tc.want)
		}
	}
}
