package httpserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	brokermsg "github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/security"
)

// zzWP18RouterBroker records single and batch accepts. With hold set, a
// batch accept signals started and waits for hold to close.
type zzWP18RouterBroker struct {
	*fakeBroker
	mu      sync.Mutex
	singles [][]byte
	batches [][]brokermsg.ProduceMessage
	hold    chan struct{}
	started chan struct{}
}

func (b *zzWP18RouterBroker) AcceptProduce(_ context.Context, _, _ string, payload []byte, _ ...int) (ingress.AcceptedProduce, error) {
	b.mu.Lock()
	b.singles = append(b.singles, append([]byte(nil), payload...))
	b.mu.Unlock()
	return ingress.AcceptedProduce{}, nil
}

func (b *zzWP18RouterBroker) AcceptProduceBatch(_ context.Context, _ string, msgs []brokermsg.ProduceMessage) ([]ingress.AcceptedProduce, error) {
	if b.hold != nil {
		b.started <- struct{}{}
		<-b.hold
	}
	b.mu.Lock()
	b.batches = append(b.batches, msgs)
	b.mu.Unlock()
	return make([]ingress.AcceptedProduce, len(msgs)), nil
}

func zzWP18Do(t *testing.T, h http.Handler, method, target, body string, header map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	return res
}

var zzWP18Client = map[string]string{"X-Narad-Client": "test"}

// TestZZWP18ProduceBatchRoute checks the batch produce route through the
// real router and middleware, and that a single produce whose body looks
// like a batch is still one message with exactly that payload.
func TestZZWP18ProduceBatchRoute(t *testing.T) {
	br := &zzWP18RouterBroker{fakeBroker: &fakeBroker{}}
	h := NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, DefaultRouterOptions())
	batch := `{"messages":[{"key":"a","payload":{"n":1}},{"key":"a","payload":{"n":2}}]}`

	res := zzWP18Do(t, h, http.MethodPost, "/v1/topics/orders/produce/batch", batch, zzWP18Client)
	if res.Code != http.StatusAccepted || res.Body.String() != "{\"accepted\":2}\n" {
		t.Fatalf("batch produce = %d %q, want 202 {\"accepted\":2}", res.Code, res.Body)
	}
	res = zzWP18Do(t, h, http.MethodPost, "/v1/topics/orders/produce", batch, zzWP18Client)
	if res.Code != http.StatusAccepted {
		t.Fatalf("single produce = %d, want 202", res.Code)
	}
	if len(br.batches) != 1 || len(br.batches[0]) != 2 || len(br.singles) != 1 || string(br.singles[0]) != batch {
		t.Fatalf("batches %d, singles %q: want one batch of 2 and the single body stored verbatim", len(br.batches), br.singles)
	}

	// The cross-site guard covers the batch route like any POST.
	res = zzWP18Do(t, h, http.MethodPost, "/v1/topics/orders/produce/batch", batch, map[string]string{"Content-Type": "text/plain"})
	if res.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("batch produce as text/plain = %d, want 415", res.Code)
	}
	if res = zzWP18Do(t, h, http.MethodGet, "/v1/topics/orders/produce/batch", "", nil); res.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET batch produce = %d, want 405", res.Code)
	}
}

// TestZZWP18ProduceBatchAuthenticated checks the batch route sits behind
// authentication and the produce grant.
func TestZZWP18ProduceBatchAuthenticated(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	auth := security.New(staticUserStore{users: map[string]user.User{
		"writer": {Username: "writer", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"orders"}}}},
		"reader": {Username: "reader", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionConsume, Patterns: []string{"orders"}}}},
	}}, newTestLogger())
	br := &zzWP18RouterBroker{fakeBroker: &fakeBroker{}}
	h := NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, auth, DefaultRouterOptions())
	body := `{"messages":[{"payload":1}]}`
	post := func(name string) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce/batch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if name != "" {
			req.SetBasicAuth(name, "pw")
		}
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res.Code
	}
	if code := post(""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous batch produce = %d, want 401", code)
	}
	if code := post("reader"); code != http.StatusForbidden {
		t.Fatalf("batch produce without the produce grant = %d, want 403", code)
	}
	if code := post("writer"); code != http.StatusAccepted {
		t.Fatalf("batch produce with the produce grant = %d, want 202", code)
	}
	if len(br.batches) != 1 {
		t.Fatalf("broker saw %d batches, want 1", len(br.batches))
	}
}

// TestZZWP18ProduceInFlightCapCountsBatch checks the produce in-flight
// cap: a batch takes its message count of the identity's budget (a lone
// batch larger than the cap is still served), and a single produce takes
// one.
func TestZZWP18ProduceInFlightCapCountsBatch(t *testing.T) {
	br := &zzWP18RouterBroker{fakeBroker: &fakeBroker{}, hold: make(chan struct{}), started: make(chan struct{}, 4)}
	srv := httptest.NewServer(NewRouterWithOptions(newTestSet(br), newTestLogger(), nil, nil, nil, RouterOptions{ProduceInFlightPerIdentity: 4}))
	t.Cleanup(srv.Close)
	post := func(path, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return 0, ""
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(out)
	}
	three := `{"messages":[{"payload":1},{"payload":2},{"payload":3}]}`

	var wg sync.WaitGroup
	wg.Go(func() { post("/v1/topics/orders/produce/batch", three) })
	<-br.started

	if code, body := post("/v1/topics/orders/produce/batch", `{"messages":[{"payload":1},{"payload":2}]}`); code != http.StatusTooManyRequests ||
		!strings.Contains(body, "in-flight produce requests") {
		t.Fatalf("batch of 2 beside a batch of 3 at cap 4 = %d %s, want 429 naming produce", code, body)
	}
	if code, _ := post("/v1/topics/orders/produce", "x"); code != http.StatusAccepted {
		t.Fatalf("single produce beside a batch of 3 at cap 4 = %d, want 202", code)
	}
	close(br.hold)
	wg.Wait()

	many := `{"messages":[{"payload":1},{"payload":2},{"payload":3},{"payload":4},{"payload":5},{"payload":6}]}`
	if code, _ := post("/v1/topics/orders/produce/batch", many); code != http.StatusAccepted {
		t.Fatalf("a lone batch larger than the cap = %d, want 202", code)
	}
	select {
	case <-br.started:
	case <-time.After(time.Second):
		t.Fatal("the lone batch never reached the broker")
	}
}
