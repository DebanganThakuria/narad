package remote

import (
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
