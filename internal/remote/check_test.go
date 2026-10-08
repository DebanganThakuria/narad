package remote

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func pass(node, target string, cv uint64) NodeReport {
	return NodeReport{Node: node, Result: ResultPass, CredentialVersion: cv, TargetID: target}
}

func TestVerdictAgreesWhenEveryMemberPasses(t *testing.T) {
	id, err := Verdict([]NodeReport{pass("n0", "t1", 3), pass("n1", "t1", 3), pass("n2", "t1", 3)}, 3)
	if err != nil || id != "t1" {
		t.Fatalf("Verdict = %q, %v", id, err)
	}
}

func TestVerdictFailures(t *testing.T) {
	fail := func(node, class string) NodeReport {
		return NodeReport{Node: node, Result: ResultFail, Class: class, CredentialVersion: 3}
	}
	cases := []struct {
		name    string
		reports []NodeReport
		status  int
		class   string
	}{
		{"none", nil, http.StatusPreconditionFailed, ClassUnreachable},
		{"stale", []NodeReport{pass("n0", "t", 3), pass("n1", "t", 2)}, http.StatusPreconditionFailed, ClassStale},
		{"disagree", []NodeReport{pass("n0", "t1", 3), pass("n1", "t2", 3)}, http.StatusPreconditionFailed, ClassTargetDisagreement},
		{"old release wins over auth", []NodeReport{fail("n0", topic.RemoteStateAuthFailed), fail("n1", ClassOldRelease)}, http.StatusPreconditionFailed, ClassOldRelease},
		{"auth", []NodeReport{pass("n0", "t", 3), fail("n1", topic.RemoteStateAuthFailed)}, http.StatusConflict, topic.RemoteStateAuthFailed},
		{"no batch produce", []NodeReport{fail("n0", topic.RemoteStateNoBatchProduce)}, http.StatusConflict, topic.RemoteStateNoBatchProduce},
		{"throttled", []NodeReport{fail("n0", topic.RemoteStateThrottled), fail("n1", topic.RemoteStateForbidden)}, http.StatusTooManyRequests, topic.RemoteStateThrottled},
		{"redirect", []NodeReport{fail("n0", topic.RemoteStateRedirectRefused)}, http.StatusBadGateway, topic.RemoteStateRedirectRefused},
		{"unavailable", []NodeReport{fail("n0", topic.RemoteStateUnavailable)}, http.StatusServiceUnavailable, topic.RemoteStateUnavailable},
		{"insecure", []NodeReport{fail("n0", topic.RemoteStateNodeInsecure)}, http.StatusPreconditionFailed, topic.RemoteStateNodeInsecure},
	}
	for _, c := range cases {
		_, err := Verdict(c.reports, 3)
		var ce *CheckError
		if !errors.As(err, &ce) {
			t.Fatalf("%s: err = %v, want *CheckError", c.name, err)
		}
		if ce.Status != c.status || ce.Class != c.class || len(ce.Reports) != len(c.reports) {
			t.Fatalf("%s: got %d %s (%d reports), want %d %s", c.name, ce.Status, ce.Class, len(ce.Reports), c.status, c.class)
		}
	}
}

// A blind report carries nothing the target answered, so it has no
// target_serves_ids at all, not a false that reads as the target's
// answer. An unblinded report keeps the field, true or false.
func TestBlindReportsOmitTargetServesIDs(t *testing.T) {
	serves, servesNot := true, false
	raw, err := json.Marshal(Blind([]NodeReport{{Node: "n0", Result: ResultPass, TargetServesIDs: &serves}}))
	if err != nil {
		t.Fatal(err)
	}
	var blind []map[string]any
	if err := json.Unmarshal(raw, &blind); err != nil {
		t.Fatal(err)
	}
	if v, ok := blind[0]["target_serves_ids"]; ok {
		t.Fatalf("blind report carries target_serves_ids %v: %s", v, raw)
	}
	for _, want := range []*bool{&serves, &servesNot} {
		raw, err := json.Marshal(NodeReport{Node: "n0", Result: ResultPass, TargetServesIDs: want})
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got["target_serves_ids"] != *want {
			t.Fatalf("unblinded report: target_serves_ids = %v, want %v: %s", got["target_serves_ids"], *want, raw)
		}
	}
}

// A member on an older release sends target_serves_ids as a plain bool;
// a report from a check that stopped before the children listing has
// none, which reads as not serving IDs.
func TestNodeReportDecodesAnOlderMembersAnswer(t *testing.T) {
	for _, c := range []struct {
		raw  string
		set  bool
		want bool
	}{
		{`{"node":"n0","result":"pass","target_serves_ids":true}`, true, true},
		{`{"node":"n0","result":"pass","target_serves_ids":false}`, true, false},
		{`{"node":"n0","result":"fail","class":"auth_failed"}`, false, false},
	} {
		var r NodeReport
		if err := json.Unmarshal([]byte(c.raw), &r); err != nil {
			t.Fatalf("%s: %v", c.raw, err)
		}
		if (r.TargetServesIDs != nil) != c.set || r.ServesIDs() != c.want {
			t.Fatalf("%s: field set %v, ServesIDs %v; want set %v, ServesIDs %v", c.raw, r.TargetServesIDs != nil, r.ServesIDs(), c.set, c.want)
		}
	}
}
