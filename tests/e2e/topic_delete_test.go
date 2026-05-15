package e2e

import (
	"net/http"
	"testing"
)

func TestDeleteTopic_DeletesExisting(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "doomed"})

	resp := jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/doomed", nil)
	expectStatus(t, resp, http.StatusNoContent)

	// Subsequent GET returns 404.
	resp = getJSON(t, env.Server.URL+"/v1/topics/doomed")
	expectStatus(t, resp, http.StatusNotFound)
}

func TestDeleteTopic_NotFound(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	resp := jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/never-existed", nil)
	expectStatus(t, resp, http.StatusNotFound)
}

// TestDeleteTopic_AllowsRecreate verifies that the namespace is
// reusable after a delete: producing into a never-deleted topic, then
// deleting it, then creating a same-named topic and producing again
// should all work cleanly.
func TestDeleteTopic_AllowsRecreate(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "recycle", Partitions: 1})
	mustProduce(t, env, "recycle", "k", map[string]int{"v": 1})

	// Delete.
	resp := jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/recycle", nil)
	expectStatus(t, resp, http.StatusNoContent)

	// Recreate the same name and confirm a fresh produce starts at offset 0.
	mustCreateTopic(t, env, createTopicReq{Name: "recycle", Partitions: 1})
	got := mustProduce(t, env, "recycle", "k", map[string]int{"v": 99})
	if got.Offset != 0 {
		t.Errorf("first produce after recreate: offset %d want 0", got.Offset)
	}
}

// TestDeleteTopic_ScopedToOneTopic verifies that deleting one topic
// doesn't disturb others.
func TestDeleteTopic_ScopedToOneTopic(t *testing.T) {
	t.Parallel()
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "keep"})
	mustCreateTopic(t, env, createTopicReq{Name: "drop"})

	resp := jsonReq(t, http.MethodDelete, env.Server.URL+"/v1/topics/drop", nil)
	expectStatus(t, resp, http.StatusNoContent)

	// "keep" still resolves.
	resp = getJSON(t, env.Server.URL+"/v1/topics/keep")
	expectStatus(t, resp, http.StatusOK)
}
