package topics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// ownedTopics is a broker whose GetTopic answers with a fixed owner per
// topic name and whose mutations record what they were called with.
func ownedTopics(owners map[string]string) *fakeBroker {
	return &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			if owner, ok := owners[name]; ok {
				return topic.Topic{Name: name, Owner: owner, Partitions: 3}, nil
			}
			return topic.Topic{}, errs.ErrTopicNotFound
		},
		getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
			if owner, ok := owners[name]; ok {
				return topic.Details{Topic: topic.Topic{Name: name, Owner: owner, Partitions: 3}}, nil
			}
			return topic.Details{}, errs.ErrTopicNotFound
		},
		attachChildFn: func(context.Context, string, string, int64) error { return nil },
		detachChildFn: func(context.Context, string, string) error { return nil },
		fanoutCursorStatsFn: func(context.Context, string) ([]topic.FanoutCursorStat, error) {
			return nil, nil
		},
	}
}

// Attaching a child rewrites the child's schema history and starts
// pumping the parent's records into it, so owning the parent alone must
// not be enough: alice (parent owner) cannot claim bob's topic, and bob
// (child owner) cannot hook his topic onto alice's parent either.
func TestAttachChildRequiresManageOnBothTopics(t *testing.T) {
	b := ownedTopics(map[string]string{"alices-parent": "alice", "bobs-private": "bob"})
	cases := []struct {
		name string
		id   user.User
		want int
	}{
		{"parent owner only", user.User{Username: "alice"}, http.StatusForbidden},
		{"child owner only", user.User{Username: "bob"}, http.StatusForbidden},
		{"unrelated user", user.User{Username: "carol"}, http.StatusForbidden},
		{"admin", user.User{Username: "root", Root: true}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attached := false
			b.attachChildFn = func(context.Context, string, string, int64) error {
				attached = true
				return nil
			}
			r := withIdentity(attachRequest("alices-parent", `{"child":"bobs-private"}`), tc.id)
			w := httptest.NewRecorder()
			AttachChild(newTestSet(b))(w, r)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.want, w.Body)
			}
			if attached != (tc.want == http.StatusOK) {
				t.Fatalf("attached = %v for status %d", attached, w.Code)
			}
		})
	}

	// The same principal owning both topics may attach.
	both := ownedTopics(map[string]string{"p": "alice", "c": "alice"})
	r := withIdentity(attachRequest("p", `{"child":"c"}`), user.User{Username: "alice"})
	w := httptest.NewRecorder()
	AttachChild(newTestSet(both))(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("owner of both: status = %d, want 200 (body %s)", w.Code, w.Body)
	}
}

// Either side's owner may detach: the parent's owner to stop feeding the
// child, the child's owner to stop receiving. Anyone else is refused.
func TestDetachChildAllowsEitherOwner(t *testing.T) {
	b := ownedTopics(map[string]string{"alices-parent": "alice", "bobs-child": "bob"})
	cases := []struct {
		name string
		id   user.User
		want int
	}{
		{"parent owner", user.User{Username: "alice"}, http.StatusNoContent},
		{"child owner", user.User{Username: "bob"}, http.StatusNoContent},
		{"unrelated user", user.User{Username: "carol"}, http.StatusForbidden},
		{"admin", user.User{Username: "root", Root: true}, http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodDelete, "/v1/topics/alices-parent/children/bobs-child", nil)
			r.SetPathValue("parent", "alices-parent")
			r.SetPathValue("child", "bobs-child")
			r = withIdentity(r, tc.id)
			w := httptest.NewRecorder()
			DetachChild(newTestSet(b))(w, r)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.want, w.Body)
			}
		})
	}
}

// Topic details carry partition owners, sizes and watermarks; only a
// principal with some stake in the topic (any grant, ownership, admin)
// may read them. A user with no grants at all sees 403.
func TestGetTopicRequiresAnyGrant(t *testing.T) {
	b := ownedTopics(map[string]string{"orders": "alice"})
	cases := []struct {
		name string
		id   *user.User
		want int
	}{
		{"security disabled", nil, http.StatusOK},
		{"no grants", &user.User{Username: "nobody"}, http.StatusForbidden},
		{"grant on another topic", &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"logs-*"}}}}, http.StatusForbidden},
		{"consume grant", &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"orders"}}}}, http.StatusOK},
		{"produce wildcard grant", &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"ord*"}}}}, http.StatusOK},
		{"create grant", &user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"orders"}}}}, http.StatusOK},
		{"owner without grants", &user.User{Username: "alice"}, http.StatusOK},
		{"admin", &user.User{Username: "root", Root: true}, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run("get/"+tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
			r.SetPathValue("topic", "orders")
			if tc.id != nil {
				r = withIdentity(r, *tc.id)
			}
			w := httptest.NewRecorder()
			Get(newTestSet(b))(w, r)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.want, w.Body)
			}
		})
		t.Run("children/"+tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/children", nil)
			r.SetPathValue("parent", "orders")
			if tc.id != nil {
				r = withIdentity(r, *tc.id)
			}
			w := httptest.NewRecorder()
			ListChildren(newTestSet(b))(w, r)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", w.Code, tc.want, w.Body)
			}
		})
	}
}

// A missing topic must still answer 404, not 403, so the read check
// cannot be used to probe for names the caller has no grant on either
// way (403 would confirm existence).
func TestGetTopicMissingIs404ForUngrantedUser(t *testing.T) {
	b := ownedTopics(map[string]string{})
	r := httptest.NewRequest(http.MethodGet, "/v1/topics/ghost", nil)
	r.SetPathValue("topic", "ghost")
	r = withIdentity(r, user.User{Username: "nobody"})
	w := httptest.NewRecorder()
	Get(newTestSet(b))(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (body %s)", w.Code, w.Body)
	}
}

// GET /v1/topics lists only the topics the caller holds a grant on or
// owns; admins and dev mode see everything. The page token is passed
// through untouched even when the filtered page is empty.
func TestListTopicsFiltersToReadable(t *testing.T) {
	all := []topic.Topic{
		{Name: "alices-orders", Owner: "alice"},
		{Name: "bobs-logs", Owner: "bob"},
		{Name: "shared-events", Owner: "carol"},
	}
	b := &fakeBroker{listTopicsFn: func(context.Context, metastore.ListOptions) ([]topic.Topic, string, error) {
		return all, "next", nil
	}}
	cases := []struct {
		name string
		id   *user.User
		want []string
	}{
		{"security disabled", nil, []string{"alices-orders", "bobs-logs", "shared-events"}},
		{"admin", &user.User{Username: "root", Root: true}, []string{"alices-orders", "bobs-logs", "shared-events"}},
		{"owner plus grant", &user.User{Username: "alice", Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"shared-*"}}}}, []string{"alices-orders", "shared-events"}},
		{"grant only", &user.User{Username: "dave", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"bobs-logs"}}}}, []string{"bobs-logs"}},
		{"no grants", &user.User{Username: "nobody"}, []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/topics", nil)
			if tc.id != nil {
				r = withIdentity(r, *tc.id)
			}
			w := httptest.NewRecorder()
			List(newTestSet(b))(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body %s)", w.Code, w.Body)
			}
			var resp struct {
				Topics        []topic.Topic `json:"topics"`
				NextPageToken string        `json:"next_page_token"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := make([]string, 0, len(resp.Topics))
			for _, t := range resp.Topics {
				got = append(got, t.Name)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("topics = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("topics = %v, want %v", got, tc.want)
				}
			}
			if resp.NextPageToken != "next" {
				t.Fatalf("next_page_token = %q, want %q (must survive filtering)", resp.NextPageToken, "next")
			}
		})
	}
}
