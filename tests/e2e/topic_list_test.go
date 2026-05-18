package e2e

import (
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// listResponse mirrors the JSON shape returned by GET /v1/topics.
type listResponse struct {
	Topics        []topic.Topic `json:"topics"`
	NextPageToken string        `json:"next_page_token"`
}

func TestListTopics_EmptyInitially(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)

	resp := getJSON(t, env.Server.URL+"/v1/topics")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var body listResponse
	decodeJSON(t, resp, &body)

	if len(body.Topics) != 0 {
		t.Errorf("topics: got %d want 0", len(body.Topics))
	}
	if body.NextPageToken != "" {
		t.Errorf("next_page_token: got %q want empty", body.NextPageToken)
	}
}

// TestListTopics_ReturnsLexicographicOrder seeds out-of-order names
// and verifies the returned slice is sorted.
func TestListTopics_ReturnsLexicographicOrder(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for _, n := range []string{"zebra", "alpha", "mango", "banana"} {
		mustCreateTopic(t, env, createTopicReq{Name: n})
	}

	resp := getJSON(t, env.Server.URL+"/v1/topics")
	var body listResponse
	decodeJSON(t, resp, &body)

	names := make([]string, len(body.Topics))
	for i, tp := range body.Topics {
		names[i] = tp.Name
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("topics not sorted: %v", names)
	}
}

// TestListTopics_PaginationWalk walks every page and reassembles the
// full set, asserting nothing was duplicated or lost.
func TestListTopics_PaginationWalk(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	want := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, n := range want {
		mustCreateTopic(t, env, createTopicReq{Name: n})
	}

	const pageSize = 3
	var got []string
	token := ""
	for page := range 10 { // safety bound
		_ = page
		url := fmt.Sprintf("%s/v1/topics?limit=%d", env.Server.URL, pageSize)
		if token != "" {
			url += "&page_token=" + token
		}
		resp := getJSON(t, url)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("page %d status: %d body=%s", page, resp.StatusCode, readBody(resp))
		}
		var body listResponse
		decodeJSON(t, resp, &body)

		for _, tp := range body.Topics {
			got = append(got, tp.Name)
		}
		if body.NextPageToken == "" {
			break
		}
		token = body.NextPageToken
	}

	if !equalStringSlices(got, want) {
		t.Errorf("walk: got %v want %v", got, want)
	}
}

// TestListTopics_PaginationCursorPointsToLastReturned verifies the
// keyset semantics: next_page_token equals the name of the last topic
// in the current page, so passing it back fetches strictly-greater
// names.
func TestListTopics_PaginationCursorPointsToLastReturned(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for _, n := range []string{"a", "b", "c", "d"} {
		mustCreateTopic(t, env, createTopicReq{Name: n})
	}

	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=2")
	var body listResponse
	decodeJSON(t, resp, &body)
	if body.NextPageToken != "b" {
		t.Errorf("next_page_token: got %q want %q (last name of page 1)", body.NextPageToken, "b")
	}
}

func TestListTopics_LastPageHasEmptyToken(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for _, n := range []string{"a", "b"} {
		mustCreateTopic(t, env, createTopicReq{Name: n})
	}

	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=10") // larger than total
	var body listResponse
	decodeJSON(t, resp, &body)
	if body.NextPageToken != "" {
		t.Errorf("next_page_token on last page: got %q want empty", body.NextPageToken)
	}
}

// TestListTopics_PageTokenReturningNoRows verifies that asking for
// names strictly greater than the largest existing name returns an
// empty page with no error.
func TestListTopics_PageTokenReturningNoRows(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "alpha"})

	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=10&page_token=zzzz")
	var body listResponse
	decodeJSON(t, resp, &body)
	if len(body.Topics) != 0 {
		t.Errorf("topics: got %d want 0", len(body.Topics))
	}
	if body.NextPageToken != "" {
		t.Errorf("next_page_token: got %q want empty", body.NextPageToken)
	}
}

func TestListTopics_RejectsLimitZero(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=0")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestListTopics_RejectsNegativeLimit(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=-5")
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestListTopics_RejectsNonIntegerLimit(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=abc")
	expectStatus(t, resp, http.StatusBadRequest)
}

// TestListTopics_ClampsLargeLimit confirms that a limit way above the
// 1000 cap is silently clamped (not rejected). Only 7 topics exist —
// they all come back in one page, and that's enough to know the cap
// didn't trip a 4xx.
func TestListTopics_ClampsLargeLimit(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		mustCreateTopic(t, env, createTopicReq{Name: n})
	}

	resp := getJSON(t, env.Server.URL+"/v1/topics?limit=999999")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var body listResponse
	decodeJSON(t, resp, &body)
	if len(body.Topics) != 7 {
		t.Errorf("topics: got %d want 7", len(body.Topics))
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
