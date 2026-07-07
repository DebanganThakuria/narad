package topics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/security"
)

func attachRequest(parent, body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/topics/"+parent+"/children", strings.NewReader(body))
	r.SetPathValue("parent", parent)
	return r
}

func TestAttachChildHandler(t *testing.T) {
	var gotParent, gotChild string
	b := &fakeBroker{
		attachChildFn: func(_ context.Context, parent, child string) error {
			gotParent, gotChild = parent, child
			return nil
		},
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{Name: name, Role: topic.RoleParent, Children: []string{"audit"}}, nil
		},
	}
	w := httptest.NewRecorder()
	AttachChild(newTestSet(b))(w, attachRequest("orders", `{"child":"audit"}`))

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", w.Code, w.Body)
	}
	if gotParent != "orders" || gotChild != "audit" {
		t.Fatalf("AttachChild called with (%q, %q), want (orders, audit)", gotParent, gotChild)
	}
	var resp topic.Topic
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Role != topic.RoleParent || len(resp.Children) != 1 {
		t.Fatalf("response topic = %+v, want parent with one child", resp)
	}
}

func TestAttachChildHandlerRejectsBadRequests(t *testing.T) {
	b := &fakeBroker{}
	cases := []struct {
		name string
		body string
	}{
		{"missing child", `{}`},
		{"unknown field", `{"child":"a","backfill":true}`},
		{"bad json", `{`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			AttachChild(newTestSet(b))(w, attachRequest("orders", tc.body))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestAttachChildHandlerMapsFanoutConflicts(t *testing.T) {
	for _, sentinel := range []error{
		errs.ErrFanoutRoleConflict,
		errs.ErrFanoutChildLimit,
		errs.ErrFanoutSchemaMismatch,
		errs.ErrAlreadyExists,
	} {
		b := &fakeBroker{attachChildFn: func(context.Context, string, string) error {
			return fmt.Errorf("%w: details", sentinel)
		}}
		w := httptest.NewRecorder()
		AttachChild(newTestSet(b))(w, attachRequest("orders", `{"child":"audit"}`))
		if w.Code != http.StatusConflict {
			t.Fatalf("status for %v = %d, want 409", sentinel, w.Code)
		}
	}

	b := &fakeBroker{attachChildFn: func(context.Context, string, string) error {
		return errs.ErrTopicNotFound
	}}
	w := httptest.NewRecorder()
	AttachChild(newTestSet(b))(w, attachRequest("orders", `{"child":"ghost"}`))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status for missing topic = %d, want 404", w.Code)
	}
}

func TestAttachDetachForwardToLeader(t *testing.T) {
	b := &fakeBroker{
		attachChildFn: func(context.Context, string, string) error {
			t.Fatal("local AttachChild called despite router forwarding")
			return nil
		},
		detachChildFn: func(context.Context, string, string) error {
			t.Fatal("local DetachChild called despite router forwarding")
			return nil
		},
	}
	forwarded := 0
	router := &fakeRouter{
		routeAttachChildFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, parent, child string) bool {
			forwarded++
			w.WriteHeader(http.StatusOK)
			return true
		},
		routeDetachChildFn: func(_ context.Context, w http.ResponseWriter, _ *http.Request, parent, child string) bool {
			forwarded++
			w.WriteHeader(http.StatusNoContent)
			return true
		},
	}
	set := newTestSetWithRouter(b, router)

	w := httptest.NewRecorder()
	AttachChild(set)(w, attachRequest("orders", `{"child":"audit"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("attach status = %d, want 200 from forward", w.Code)
	}

	r := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders/children/audit", nil)
	r.SetPathValue("parent", "orders")
	r.SetPathValue("child", "audit")
	w = httptest.NewRecorder()
	DetachChild(set)(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("detach status = %d, want 204 from forward", w.Code)
	}
	if forwarded != 2 {
		t.Fatalf("forwarded = %d, want 2", forwarded)
	}
}

func TestDetachChildHandler(t *testing.T) {
	var gotParent, gotChild string
	b := &fakeBroker{detachChildFn: func(_ context.Context, parent, child string) error {
		gotParent, gotChild = parent, child
		return nil
	}}
	r := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders/children/audit", nil)
	r.SetPathValue("parent", "orders")
	r.SetPathValue("child", "audit")
	w := httptest.NewRecorder()
	DetachChild(newTestSet(b))(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", w.Code)
	}
	if gotParent != "orders" || gotChild != "audit" {
		t.Fatalf("DetachChild called with (%q, %q)", gotParent, gotChild)
	}

	b = &fakeBroker{detachChildFn: func(context.Context, string, string) error {
		return fmt.Errorf("%w: not attached", errs.ErrNotFound)
	}}
	w = httptest.NewRecorder()
	DetachChild(newTestSet(b))(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status for unattached = %d, want 404", w.Code)
	}
}

func TestListChildrenHandlerAggregatesLag(t *testing.T) {
	b := &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{
				Name: name, Partitions: 2, Role: topic.RoleParent,
				Children: []string{"audit", "slow", "fresh"},
			}, nil
		},
		fanoutCursorStatsFn: func(context.Context, string) ([]topic.FanoutCursorStat, error) {
			return []topic.FanoutCursorStat{
				{Child: "audit", Partition: 0, NextOffset: 10, HighWatermark: 10},
				{Child: "audit", Partition: 1, NextOffset: 4, HighWatermark: 7},
				{Child: "slow", Partition: 0, NextOffset: 0, HighWatermark: 10},
				// "slow" partition 1 has not reported: lag is a lower bound.
				// "fresh" has no cursors at all yet.
			}, nil
		},
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/children", nil)
	r.SetPathValue("parent", "orders")
	w := httptest.NewRecorder()
	ListChildren(newTestSet(b))(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", w.Code, w.Body)
	}

	var resp childrenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Parent != "orders" || len(resp.Children) != 3 {
		t.Fatalf("response = %+v, want 3 children of orders", resp)
	}
	want := map[string]childStatus{
		"audit": {Name: "audit", LagMessages: 3, LagComplete: true},
		"slow":  {Name: "slow", LagMessages: 10, LagComplete: false},
		"fresh": {Name: "fresh", LagMessages: 0, LagComplete: false},
	}
	for _, got := range resp.Children {
		if got != want[got.Name] {
			t.Fatalf("child %q = %+v, want %+v", got.Name, got, want[got.Name])
		}
	}
}

func TestListChildrenHandlerStandaloneTopicIsEmpty(t *testing.T) {
	b := &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{Name: name, Partitions: 3, Role: topic.RoleStandalone}, nil
		},
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/topics/plain/children", nil)
	r.SetPathValue("parent", "plain")
	w := httptest.NewRecorder()
	ListChildren(newTestSet(b))(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp childrenResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Children) != 0 {
		t.Fatalf("children = %+v, want empty", resp.Children)
	}
}

// Attach and detach are owner-or-admin operations on the parent, the
// same rule as alter/delete.
func TestAttachDetachEnforceOwnerOrAdmin(t *testing.T) {
	parentOwnedByAlice := &fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{Name: name, Owner: "alice"}, nil
		},
	}
	identities := []struct {
		name       string
		id         user.User
		wantStatus int
	}{
		{"owner", user.User{Username: "alice"}, http.StatusOK},
		{"admin", user.User{Username: "root", Root: true}, http.StatusOK},
		{"other user", user.User{Username: "bob"}, http.StatusForbidden},
	}
	for _, tc := range identities {
		t.Run(tc.name, func(t *testing.T) {
			r := attachRequest("orders", `{"child":"audit"}`)
			r = r.WithContext(security.WithIdentity(r.Context(), tc.id))
			w := httptest.NewRecorder()
			AttachChild(newTestSet(parentOwnedByAlice))(w, r)
			if w.Code != tc.wantStatus {
				t.Fatalf("attach as %s: status = %d, want %d", tc.name, w.Code, tc.wantStatus)
			}

			dr := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders/children/audit", nil)
			dr.SetPathValue("parent", "orders")
			dr.SetPathValue("child", "audit")
			dr = dr.WithContext(security.WithIdentity(dr.Context(), tc.id))
			dw := httptest.NewRecorder()
			DetachChild(newTestSet(parentOwnedByAlice))(dw, dr)
			wantDetach := tc.wantStatus
			if wantDetach == http.StatusOK {
				wantDetach = http.StatusNoContent
			}
			if dw.Code != wantDetach {
				t.Fatalf("detach as %s: status = %d, want %d", tc.name, dw.Code, wantDetach)
			}
		})
	}
}
