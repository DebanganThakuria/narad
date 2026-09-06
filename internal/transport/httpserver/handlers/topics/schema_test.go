package topics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

func schemaHistoryRequest(topicName string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/"+topicName+"/schema", nil)
	req.SetPathValue("topic", topicName)
	return req
}

func TestSchemaHistoryHandler(t *testing.T) {
	history := topic.SchemaHistory{Topic: "orders", Version: 2, Versions: []topic.SchemaVersion{
		{Version: 1, Schema: json.RawMessage(`{"type":"object"}`)},
		{Version: 2, Schema: json.RawMessage(`{"type":"object","title":"v2"}`)},
	}}
	s := newTestSet(&fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			if name != "orders" {
				return topic.Topic{}, errs.ErrTopicNotFound
			}
			return topic.Topic{Name: name, Owner: "alice"}, nil
		},
		topicSchemaHistoryFn: func(_ context.Context, name string) (topic.SchemaHistory, error) {
			if name != "orders" {
				return topic.SchemaHistory{}, errs.ErrTopicNotFound
			}
			return history, nil
		},
	})

	res := httptest.NewRecorder()
	SchemaHistory(s).ServeHTTP(res, schemaHistoryRequest("orders"))
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", res.Code, res.Body)
	}
	var got topic.SchemaHistory
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != 2 || len(got.Versions) != 2 || string(got.Versions[1].Schema) != `{"type":"object","title":"v2"}` {
		t.Fatalf("history = %+v", got)
	}

	res = httptest.NewRecorder()
	SchemaHistory(s).ServeHTTP(res, schemaHistoryRequest("missing"))
	if res.Code != http.StatusNotFound {
		t.Fatalf("missing topic status = %d, want 404", res.Code)
	}

	res = httptest.NewRecorder()
	SchemaHistory(s).ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/topics//schema", nil))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("empty topic status = %d, want 400", res.Code)
	}
}

// Reading the schema follows the topic read rule: any grant on the
// topic, ownership, or admin. Changing it follows the manage rule:
// owner or admin only, so a produce grant can read but not write.
func TestSchemaReadAndWriteAuthorization(t *testing.T) {
	var updates int
	s := newTestSet(&fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{Name: name, Partitions: 3, Owner: "alice"}, nil
		},
		getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
			return topic.Details{Topic: topic.Topic{Name: name, Partitions: 3, Owner: "alice"}, Partitions: make([]topic.PartitionStats, 3)}, nil
		},
		topicSchemaHistoryFn: func(_ context.Context, name string) (topic.SchemaHistory, error) {
			return topic.SchemaHistory{Topic: name}, nil
		},
		updateTopicSchemaFn: func(_ context.Context, name string, _ []byte, _ int) (topic.Topic, error) {
			updates++
			return topic.Topic{Name: name}, nil
		},
	})
	grant := func(action user.Action) user.User {
		return user.User{Username: "bob", Grants: []user.Grant{{Action: action, Patterns: []string{"orders"}}}}
	}
	identities := []struct {
		name      string
		user      user.User
		wantRead  int
		wantWrite int
	}{
		{"owner", user.User{Username: "alice"}, http.StatusOK, http.StatusOK},
		{"admin", user.User{Username: "root", Grants: []user.Grant{{Action: user.ActionAdmin, Patterns: []string{"*"}}}}, http.StatusOK, http.StatusOK},
		{"produce grant", grant(user.ActionProduce), http.StatusOK, http.StatusForbidden},
		{"consume grant", grant(user.ActionConsume), http.StatusOK, http.StatusForbidden},
		{"create grant", grant(user.ActionCreate), http.StatusOK, http.StatusForbidden},
		{"no grant", user.User{Username: "mallory"}, http.StatusForbidden, http.StatusForbidden},
	}
	for _, tc := range identities {
		t.Run(tc.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			SchemaHistory(s).ServeHTTP(res, withIdentity(schemaHistoryRequest("orders"), tc.user))
			if res.Code != tc.wantRead {
				t.Fatalf("GET schema status = %d, want %d (%s)", res.Code, tc.wantRead, res.Body)
			}
			res = httptest.NewRecorder()
			GetDetails := Get(s)
			GetDetails.ServeHTTP(res, withIdentity(func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
				r.SetPathValue("topic", "orders")
				return r
			}(), tc.user))
			if (res.Code == http.StatusForbidden) != (tc.wantRead == http.StatusForbidden) {
				t.Fatalf("GET topic status = %d, want the same read rule as GET schema (%d)", res.Code, tc.wantRead)
			}

			before := updates
			req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", strings.NewReader(`{"schema":{"type":"object"}}`))
			req.SetPathValue("topic", "orders")
			res = httptest.NewRecorder()
			Alter(s).ServeHTTP(res, withIdentity(req, tc.user))
			if res.Code != tc.wantWrite {
				t.Fatalf("PATCH schema status = %d, want %d (%s)", res.Code, tc.wantWrite, res.Body)
			}
			if wrote := updates > before; wrote != (tc.wantWrite == http.StatusOK) {
				t.Fatalf("broker update called = %v for status %d", wrote, res.Code)
			}
		})
	}
}

// GET /v1/topics/{topic} carries the current schema and its version
// exactly as the broker reports them, and omits the schema when the
// topic has none.
func TestGetHandlerCarriesSchemaAndVersion(t *testing.T) {
	details := map[string]topic.Details{
		"with":    {Topic: topic.Topic{Name: "with", Partitions: 1}, SchemaVersion: 3, Schema: json.RawMessage(`{"type":"object"}`), Partitions: []topic.PartitionStats{{Index: 0}}},
		"without": {Topic: topic.Topic{Name: "without", Partitions: 1}, Partitions: []topic.PartitionStats{{Index: 0}}},
	}
	s := newTestSet(&fakeBroker{getTopicDetailsFn: func(_ context.Context, name string) (topic.Details, error) {
		return details[name], nil
	}})
	for name, want := range details {
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/"+name, nil)
		req.SetPathValue("topic", name)
		res := httptest.NewRecorder()
		Get(s).ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", name, res.Code)
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(res.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		if string(raw["schema_version"]) != fmt.Sprint(want.SchemaVersion) {
			t.Fatalf("%s: schema_version = %s, want %d", name, raw["schema_version"], want.SchemaVersion)
		}
		if got, ok := raw["schema"]; ok != (want.Schema != nil) || (ok && !bytes.Equal(got, want.Schema)) {
			t.Fatalf("%s: schema = %s (present=%v), want %s", name, got, ok, want.Schema)
		}
	}
}

func TestAlterSchemaBaseVersionAndConflictMapping(t *testing.T) {
	var gotBase int
	var updateErr error
	s := newTestSet(&fakeBroker{
		getTopicFn: func(_ context.Context, name string) (topic.Topic, error) {
			return topic.Topic{Name: name, Partitions: 3}, nil
		},
		updateTopicSchemaFn: func(_ context.Context, name string, _ []byte, base int) (topic.Topic, error) {
			gotBase = base
			return topic.Topic{Name: name}, updateErr
		},
	})
	patch := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", strings.NewReader(body))
		req.SetPathValue("topic", "orders")
		res := httptest.NewRecorder()
		Alter(s).ServeHTTP(res, req)
		return res
	}

	if res := patch(`{"schema":{"type":"object"},"schema_base_version":4}`); res.Code != http.StatusOK || gotBase != 4 {
		t.Fatalf("base version passthrough: status %d base %d", res.Code, gotBase)
	}
	if res := patch(`{"schema":{"type":"object"}}`); res.Code != http.StatusOK || gotBase != 0 {
		t.Fatalf("no base version: status %d base %d", res.Code, gotBase)
	}
	if res := patch(`{"schema":{"type":"object"},"schema_base_version":-1}`); res.Code != http.StatusBadRequest {
		t.Fatalf("negative base version status = %d, want 400", res.Code)
	}
	if res := patch(`{"schema_base_version":1}`); res.Code != http.StatusBadRequest {
		t.Fatalf("base version without schema status = %d, want 400", res.Code)
	}
	if res := patch(`{"schema":null}`); res.Code != http.StatusBadRequest {
		// A null schema is decoded as the literal null, which is not a
		// schema; it must never reach the broker as "no schema field".
		t.Fatalf("null schema status = %d, want 400 (body %s)", res.Code, res.Body)
	}

	for _, tc := range []struct {
		err  error
		want int
	}{
		{fmt.Errorf("%w: base 1 vs current 2", errs.ErrSchemaVersionConflict), http.StatusConflict},
		{fmt.Errorf("%w: 1000 versions", errs.ErrSchemaHistoryFull), http.StatusConflict},
		{fmt.Errorf("%w: %w: type narrowed", errs.ErrInvalidArgument, errs.ErrSchemaIncompatible), http.StatusBadRequest},
		{fmt.Errorf("%w: attached", errs.ErrFanoutSchemaManaged), http.StatusConflict},
		{fmt.Errorf("%w: raced", errs.ErrAlreadyExists), http.StatusConflict},
		{errs.ErrTopicNotFound, http.StatusNotFound},
		{errors.New("disk on fire"), http.StatusInternalServerError},
	} {
		updateErr = tc.err
		if res := patch(`{"schema":{"type":"object"}}`); res.Code != tc.want {
			t.Fatalf("error %v: status = %d, want %d", tc.err, res.Code, tc.want)
		}
	}
	updateErr = nil

	// A schema body over the JSON body limit is 413, not 400.
	huge := `{"schema":{"description":"` + strings.Repeat("x", int(handlers.MaxJSONBodyBytes)) + `"}}`
	if res := patch(huge); res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized schema body status = %d, want 413", res.Code)
	}
}
