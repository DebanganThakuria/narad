package topics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
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

// manageRequests are the owner-or-admin requests that name an existing
// topic: alter and delete of it, a create-as-child under it, an attach
// with it on either side, and a detach.
func manageRequests(ghost, own string) map[string]func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
	return map[string]func() (*http.Request, func(*handlers.Set) http.HandlerFunc){
		"delete": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			r := httptest.NewRequest(http.MethodDelete, "/v1/topics/"+ghost, nil)
			r.SetPathValue("topic", ghost)
			return r, Delete
		},
		"alter": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			r := httptest.NewRequest(http.MethodPatch, "/v1/topics/"+ghost, bytes.NewBufferString(`{"retention_ms":3600000}`))
			r.SetPathValue("topic", ghost)
			return r, Alter
		},
		"create as child": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			return httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewBufferString(`{"name":"bob-copy","parent":"`+ghost+`"}`)), Create
		},
		"attach under it": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			r := httptest.NewRequest(http.MethodPost, "/v1/topics/"+ghost+"/children", bytes.NewBufferString(`{"child":"`+own+`"}`))
			r.SetPathValue("parent", ghost)
			return r, AttachChild
		},
		"attach it": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			r := httptest.NewRequest(http.MethodPost, "/v1/topics/"+own+"/children", bytes.NewBufferString(`{"child":"`+ghost+`"}`))
			r.SetPathValue("parent", own)
			return r, AttachChild
		},
		"detach": func() (*http.Request, func(*handlers.Set) http.HandlerFunc) {
			r := httptest.NewRequest(http.MethodDelete, "/v1/topics/"+ghost+"/children/"+ghost+"-2", nil)
			r.SetPathValue("parent", ghost)
			r.SetPathValue("child", ghost+"-2")
			return r, DetachChild
		},
	}
}

// refusingRouter fails the test if any manage request is forwarded.
func refusingRouter(t *testing.T) *fakeRouter {
	forwarded := func(op string) { t.Errorf("%s was forwarded to the leader", op) }
	return &fakeRouter{
		routeCreateTopicFn: func(context.Context, http.ResponseWriter, *http.Request, []byte) bool {
			forwarded("create")
			return true
		},
		routeAlterTopicFn: func(context.Context, http.ResponseWriter, *http.Request, string, []byte) bool {
			forwarded("alter")
			return true
		},
		routeDeleteTopicFn: func(context.Context, http.ResponseWriter, *http.Request, string) bool {
			forwarded("delete")
			return true
		},
		routeAttachChildFn: func(context.Context, http.ResponseWriter, *http.Request, string, string, int64) bool {
			forwarded("attach")
			return true
		},
		routeDetachChildFn: func(context.Context, http.ResponseWriter, *http.Request, string, string) bool {
			forwarded("detach")
			return true
		},
	}
}

// bobOwns serves bob's topic and reports every other name missing.
func bobOwns(name string) *fakeBroker {
	return &fakeBroker{getTopicFn: func(_ context.Context, n string) (topic.Topic, error) {
		if n == name {
			return topic.Topic{Name: n, Partitions: 3, Owner: "bob"}, nil
		}
		return topic.Topic{}, errs.ErrTopicNotFound
	}}
}

var bob = user.User{Username: "bob", Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}}}

// A non-admin's manage request naming a topic this node's replica does
// not have is answered 404 once the node has caught up with the leader,
// never forwarded as allowed (audit H1: master's canManageTopic counted
// a missing topic as manageable, so a request racing a create, or read
// from a lagging follower, reached the leader unchecked).
func TestManageOfTopicMissingLocallyIsNotForwarded(t *testing.T) {
	for name, build := range manageRequests("ghost", "bob-topic") {
		t.Run(name, func(t *testing.T) {
			router := refusingRouter(t)
			s := newTestSetWithRouter(bobOwns("bob-topic"), router)
			req, handler := build()
			res := httptest.NewRecorder()
			handler(s).ServeHTTP(res, withIdentity(req, bob))
			if res.Code != http.StatusNotFound {
				t.Fatalf("status %d body %s, want 404", res.Code, res.Body)
			}
			if router.syncs == 0 {
				t.Fatal("answered 404 without catching up with the leader first")
			}
		})
	}

	// The topic exists on the leader and reaches this replica during the
	// sync: the request is checked against it and forwarded.
	synced := false
	b := &fakeBroker{getTopicFn: func(_ context.Context, n string) (topic.Topic, error) {
		if synced {
			return topic.Topic{Name: n, Partitions: 3, Owner: "bob"}, nil
		}
		return topic.Topic{}, errs.ErrTopicNotFound
	}}
	forwarded := false
	router := &fakeRouter{
		syncWithLeaderFn: func(context.Context) error { synced = true; return nil },
		routeDeleteTopicFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string) bool {
			forwarded = true
			w.WriteHeader(http.StatusNoContent)
			return true
		},
	}
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/fresh", nil)
	req.SetPathValue("topic", "fresh")
	res := httptest.NewRecorder()
	Delete(newTestSetWithRouter(b, router)).ServeHTTP(res, withIdentity(req, bob))
	if res.Code != http.StatusNoContent || !forwarded {
		t.Fatalf("delete of bob's freshly synced topic: status %d forwarded %v, want 204 forwarded", res.Code, forwarded)
	}
}

// When the node cannot reach the leader to confirm a topic it does not
// have, the request is a 503 to retry, not a forward and not a 404.
func TestManageWithLeaderUnreachableAnswers503(t *testing.T) {
	for name, build := range manageRequests("ghost", "bob-topic") {
		t.Run(name, func(t *testing.T) {
			router := refusingRouter(t)
			router.syncWithLeaderFn = func(context.Context) error {
				return fmt.Errorf("%w: no leader", errs.ErrUnavailable)
			}
			s := newTestSetWithRouter(bobOwns("bob-topic"), router)
			req, handler := build()
			res := httptest.NewRecorder()
			handler(s).ServeHTTP(res, withIdentity(req, bob))
			if res.Code != http.StatusServiceUnavailable {
				t.Fatalf("status %d body %s, want 503", res.Code, res.Body)
			}
		})
	}
}
