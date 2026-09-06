package topics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// The leader-direct delete reads the record's incarnation BEFORE the
// delete removes it and hands that ID to the purge broadcast, so a
// member that has already applied a same-named recreate purges the old
// directory and not the new one.
func TestDeleteHandlerBroadcastsIncarnation(t *testing.T) {
	var deleted bool
	br := &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			if deleted {
				t.Fatal("GetTopic called after DeleteTopic; the incarnation must be read before the record is gone")
			}
			return topic.Topic{Name: name, ID: "0123456789abcdef"}, nil
		},
		deleteTopicFn: func(context.Context, string) error {
			deleted = true
			return nil
		},
	}
	var gotTopic, gotID string
	s := newTestSetWithRouter(br, &fakeRouter{broadcastDeleteTopicFn: func(_ context.Context, topicName, id string) error {
		gotTopic, gotID = topicName, id
		return nil
	}})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Code)
	}
	if gotTopic != "orders" || gotID != "0123456789abcdef" {
		t.Fatalf("broadcast = (%q, %q), want (orders, 0123456789abcdef)", gotTopic, gotID)
	}
}
