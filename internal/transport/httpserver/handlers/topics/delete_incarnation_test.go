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

// racingDeleteBroker is a broker whose topic name is deleted and
// recreated by another client right after the handler first reads it;
// deleted records the incarnation each delete actually removed.
type racingDeleteBroker struct {
	*fakeBroker
	current, recreate string
	deleted           []string
}

func (b *racingDeleteBroker) DeleteTopicID(context.Context, string) (string, error) {
	id := b.current
	b.deleted = append(b.deleted, id)
	b.current = ""
	return id, nil
}

// The leader-direct delete's purge fan-out names the incarnation the
// delete removed, as the broker reports it from under its name lock
// (audit M8). Master read the record before the delete, so a recreate
// in between made it name the old incarnation while the delete removed
// the new one.
func TestLeaderDirectDeleteBroadcastsTheDeletedIncarnation(t *testing.T) {
	br := &racingDeleteBroker{current: "000000000000000a", recreate: "000000000000000b"}
	br.fakeBroker = &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			seen := br.current
			if br.recreate != "" {
				br.current, br.recreate = br.recreate, ""
			}
			return topic.Topic{Name: name, ID: seen}, nil
		},
		deleteTopicFn: func(context.Context, string) error {
			br.deleted = append(br.deleted, br.current)
			br.current = ""
			return nil
		},
	}
	var gotID string
	s := newTestSetWithRouter(br, &fakeRouter{broadcastDeleteTopicFn: func(_ context.Context, _, id string) error {
		gotID = id
		return nil
	}})

	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)

	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", res.Code)
	}
	if len(br.deleted) != 1 || gotID != br.deleted[0] {
		t.Fatalf("the purge fan-out named incarnation %q, but the delete removed %v", gotID, br.deleted)
	}
}
