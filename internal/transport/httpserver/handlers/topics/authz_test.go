package topics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

// withIdentity injects an authenticated user, as the auth middleware
// would.
func withIdentity(r *http.Request, u user.User) *http.Request {
	return r.WithContext(security.WithIdentity(r.Context(), u))
}

func TestCreateRequiresCreateGrantAndAssignsOwner(t *testing.T) {
	var gotOpts brokertopics.CreateOpts
	s := newTestSet(&fakeBroker{createTopicFn: func(_ context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
		gotOpts = opts
		return topic.Topic{Name: opts.Name, Owner: opts.Owner}, nil
	}})

	// Denied: no create grant for this name.
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"orders","partitions":3}`))
	req = withIdentity(req, user.User{Username: "bob", Grants: []user.Grant{
		{Action: user.ActionCreate, Patterns: []string{"logs-*"}},
	}})
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("ungranted create: status = %d, want 403", res.Code)
	}

	// Allowed: matching grant; creator becomes owner even if the client
	// tries to spoof one.
	req = httptest.NewRequest(http.MethodPost, "/v1/topics",
		bytes.NewBufferString(`{"name":"orders","partitions":3,"owner":"mallory"}`))
	req = withIdentity(req, user.User{Username: "alice", Grants: []user.Grant{
		{Action: user.ActionCreate, Patterns: []string{"orders*"}},
	}})
	res = httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("granted create: status = %d, body = %s", res.Code, res.Body)
	}
	if gotOpts.Owner != "alice" {
		t.Fatalf("owner = %q, want alice (client-supplied owner must be discarded)", gotOpts.Owner)
	}
}

func TestCreateForwardCarriesAuthenticatedOwner(t *testing.T) {
	s := newTestSetWithRouter(&fakeBroker{createTopicFn: func(context.Context, brokertopics.CreateOpts) (topic.Topic, error) {
		return topic.Topic{}, errors.New("unexpected local create")
	}}, &fakeRouter{routeCreateTopicFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, body []byte) bool {
		var req createRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatalf("decode forwarded body: %v", err)
		}
		if req.Owner != "alice" {
			t.Fatalf("forwarded owner = %q, want alice", req.Owner)
		}
		w.WriteHeader(http.StatusCreated)
		return true
	}})

	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"orders","partitions":3}`))
	req = withIdentity(req, user.User{Username: "alice", Grants: []user.Grant{
		{Action: user.ActionCreate, Patterns: []string{"*"}},
	}})
	res := httptest.NewRecorder()
	Create(s).ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body)
	}
}

func TestAlterAndDeleteEnforceOwnerOrAdmin(t *testing.T) {
	ownedByAlice := func() *fakeBroker {
		return &fakeBroker{
			getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
				return topic.Topic{Name: name, Partitions: 3, Owner: "alice"}, nil
			},
			updateTopicRetentionFn: func(_ context.Context, name string, _ int64) (topic.Topic, error) {
				return topic.Topic{Name: name, Owner: "alice"}, nil
			},
			deleteTopicFn: func(context.Context, string) error { return nil },
		}
	}

	cases := []struct {
		name string
		id   user.User
		want int
	}{
		{"owner allowed", user.User{Username: "alice"}, http.StatusOK},
		{"admin allowed", user.User{Username: "root", Root: true}, http.StatusOK},
		{"other denied", user.User{Username: "bob"}, http.StatusForbidden},
	}
	for _, c := range cases {
		t.Run("alter/"+c.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders",
				bytes.NewBufferString(`{"retention_ms":5}`))
			req.SetPathValue("topic", "orders")
			req = withIdentity(req, c.id)
			res := httptest.NewRecorder()
			Alter(newTestSet(ownedByAlice())).ServeHTTP(res, req)
			if res.Code != c.want {
				t.Fatalf("status = %d, want %d (body %s)", res.Code, c.want, res.Body)
			}
		})
		t.Run("delete/"+c.name, func(t *testing.T) {
			want := c.want
			if want == http.StatusOK {
				want = http.StatusNoContent
			}
			req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)
			req.SetPathValue("topic", "orders")
			req = withIdentity(req, c.id)
			res := httptest.NewRecorder()
			Delete(newTestSet(ownedByAlice())).ServeHTTP(res, req)
			if res.Code != want {
				t.Fatalf("status = %d, want %d (body %s)", res.Code, want, res.Body)
			}
		})
	}
}

func TestManageMissingTopicFallsThroughTo404(t *testing.T) {
	s := newTestSet(&fakeBroker{
		getTopicFn: func(context.Context, string) (topic.Topic, error) {
			return topic.Topic{}, errs.ErrTopicNotFound
		},
		deleteTopicFn: func(context.Context, string) error { return errs.ErrTopicNotFound },
	})
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/ghost", nil)
	req.SetPathValue("topic", "ghost")
	req = withIdentity(req, user.User{Username: "bob"})
	res := httptest.NewRecorder()
	Delete(s).ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (ownership check must not mask missing topics)", res.Code)
	}
}
