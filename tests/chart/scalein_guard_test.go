package chart_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// guardJob renders the chart and returns the scale-in guard's Job.
func guardJob(t *testing.T, args ...string) map[string]any {
	t.Helper()
	return only(t, render(t, append(append([]string{}, fenced...), args...)...), "Job")
}

func guardPodSpec(job map[string]any) any { return at(job, "spec", "template", "spec") }

func guardScript(t *testing.T, job map[string]any) string {
	t.Helper()
	script, _ := at(guardPodSpec(job), "containers", 0, "args", 0).(string)
	if !strings.Contains(script, "scale-in guard") {
		t.Fatalf("the Job carries no guard script in its first arg:\n%q", script)
	}
	return script
}

// A lowered replicaCount, or a helm rollback to a revision with fewer
// replicas, deletes the highest-ordinal pods. The guard used to be a
// template-time lookup, which helm rollback never renders; it is now a
// hook Job that runs before both, sized by the revision that rendered it.
func TestScaleInGuardRunsBeforeUpgradeAndRollback(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=5")

	ann, _ := at(job, "metadata", "annotations").(map[string]any)
	if got := ann["helm.sh/hook"]; got != "pre-upgrade,pre-rollback" {
		t.Fatalf("helm.sh/hook = %v, want pre-upgrade,pre-rollback", got)
	}
	if got, _ := ann["helm.sh/hook-delete-policy"].(string); !strings.Contains(got, "before-hook-creation") {
		t.Fatalf("hook-delete-policy = %q, want before-hook-creation (a failed Job is kept for its log until the next run)", got)
	}
	if got := at(job, "spec", "backoffLimit"); got != "0" {
		t.Fatalf("backoffLimit = %v, want 0", got)
	}
	if at(job, "spec", "activeDeadlineSeconds") == nil {
		t.Fatal("the Job has no activeDeadlineSeconds, so a stuck guard holds the upgrade forever")
	}

	spec := guardPodSpec(job)
	env, from := containerEnv(spec)
	if env["TARGET_REPLICAS"] != "5" {
		t.Fatalf("TARGET_REPLICAS = %q, want the revision's replicaCount 5", env["TARGET_REPLICAS"])
	}
	// The hook pod must not be selected by the Services, the PDB or the
	// NetworkPolicy, which all select on the release's selector labels.
	labels, _ := at(job, "spec", "template", "metadata", "labels").(map[string]any)
	selected := true
	for k, v := range releaseSelector {
		if labels[k] != v {
			selected = false
		}
	}
	if selected {
		t.Fatalf("the hook pod carries the release's selector labels %v", labels)
	}
	if got := at(spec, "automountServiceAccountToken"); got != "false" {
		t.Fatalf("automountServiceAccountToken = %v, want false", got)
	}
	if got := at(spec, "containers", 0, "securityContext", "readOnlyRootFilesystem"); got != "true" {
		t.Fatalf("readOnlyRootFilesystem = %v, want true", got)
	}
	if got := at(spec, "securityContext", "runAsNonRoot"); got != "true" {
		t.Fatalf("runAsNonRoot = %v, want true", got)
	}

	// Admin credentials come from the security Secret, never a literal.
	if env["NARAD_USER"] != "admin" {
		t.Fatalf("NARAD_USER = %q, want admin", env["NARAD_USER"])
	}
	wantRef := map[string]any{"secretKeyRef": map[string]any{"name": "narad-security", "key": "admin-password", "optional": "true"}}
	if got := from["NARAD_PASS"]; !reflect.DeepEqual(got, wantRef) {
		t.Fatalf("NARAD_PASS comes from %v, want %v", got, wantRef)
	}

	// Kubernetes expands $(VAR) and turns $$ into $ in container args.
	script := guardScript(t, job)
	if strings.Contains(script, "$$") || regexp.MustCompile(`\$\([A-Za-z_][A-Za-z0-9_]*\)`).MatchString(script) {
		t.Fatal("the guard script contains $$ or $(VAR), which Kubernetes rewrites in container args")
	}

	t.Run("no credentials with security off", func(t *testing.T) {
		_, from := containerEnv(guardPodSpec(guardJob(t, "--set", "security.enabled=false", "--set", "security.allowInsecureCluster=true")))
		if _, ok := from["NARAD_PASS"]; ok {
			t.Fatal("the guard reads the admin password with security off")
		}
	})
	t.Run("scaleInGuard.enabled=false renders no Job", func(t *testing.T) {
		if n := len(ofKind(render(t, append(append([]string{}, fenced...), "--set", "scaleInGuard.enabled=false")...), "Job")); n != 0 {
			t.Fatalf("rendered %d Jobs with the guard off", n)
		}
	})
	t.Run("the NetworkPolicy admits the guard to a narrowed API", func(t *testing.T) {
		docs := render(t,
			"--set", "networkPolicy.enabled=true",
			"--set", "networkPolicy.apiFrom[0].namespaceSelector.matchLabels.team=payments")
		guard := podSelectorPeer(labels)
		for _, from := range ingressAdmits(t, only(t, docs, "NetworkPolicy"))["7942/TCP"] {
			found := false
			for _, peer := range from {
				if reflect.DeepEqual(peer, guard) {
					found = true
				}
			}
			if !found {
				t.Fatalf("API admitted from %v, which leaves out the guard pod %v", from, guard)
			}
		}
	})
}

type guardMember struct {
	ID              string `json:"id"`
	Addr            string `json:"addr"`
	Status          string `json:"status"`
	Draining        bool   `json:"draining"`
	OwnedPartitions int    `json:"owned_partitions"`
	OutboundMoves   int    `json:"outbound_moves"`
	Voter           *bool  `json:"voter,omitempty"`
}

func member(ordinal, owned int) guardMember {
	id := "narad-" + strconv.Itoa(ordinal)
	return guardMember{ID: id, Addr: id + ":7942", Status: "alive", OwnedPartitions: owned}
}

type guardWorld struct {
	pods     []int         // ordinals whose pod resolves in DNS
	members  []guardMember // what every reachable server answers
	answers  map[string][]guardMember
	fail     []string // servers that answer an error
	failAll  string   // every server answers this error
	password string   // NARAD_PASS
	// dnsDown makes every lookup fail, as getent does on SERVFAIL or a
	// timeout; dnsDownAfterAPI does so from the first API call on.
	dnsDown, dnsDownAfterAPI bool
}

type guardRun struct {
	exit   int
	output string
	calls  []string // the server of each `narad cluster members`
}

const serviceURL = "http://narad.narad.svc.cluster.local:7942"

func podURL(ordinal int) string {
	return "http://narad-" + strconv.Itoa(ordinal) + ".narad-headless.narad.svc.cluster.local:7942"
}

// runGuard runs the rendered guard script with the host's sh, with
// getent, timeout and narad replaced by stubs that act out w.
func runGuard(t *testing.T, job map[string]any, w guardWorld) guardRun {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	answers := filepath.Join(dir, "answers")
	for _, d := range []string{bin, answers} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	stub := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	stub("getent", `[ -z "$FAKE_DNS_DOWN" ] || exit 2
[ ! -e "$FAKE_DNS_FLAG" ] || exit 2
for p in $FAKE_PODS; do case "$2" in "$p".*) exit 0 ;; esac; done
exit 2
`)
	stub("timeout", `shift
exec "$@"
`)
	stub("narad", `echo "$NARAD_ADDR" >> "$FAKE_CALLS"
[ -z "$FAKE_DNS_DOWN_AFTER_API" ] || : > "$FAKE_DNS_FLAG"
[ "$*" = "cluster members" ] || { echo "unexpected narad $*" >&2; exit 64; }
if [ -n "$FAKE_FAIL_ALL" ]; then echo "Error: $FAKE_FAIL_ALL" >&2; exit 1; fi
for a in $FAKE_FAIL; do
  if [ "$a" = "$NARAD_ADDR" ]; then echo "Error: http 503: no ready pod" >&2; exit 1; fi
done
key=$(printf '%s' "$NARAD_ADDR" | tr '/:' '__')
if [ -f "$FAKE_ANSWERS/$key" ]; then cat "$FAKE_ANSWERS/$key"; else cat "$FAKE_ANSWERS/default"; fi
`)
	writeAnswer := func(name string, members []guardMember) {
		t.Helper()
		if members == nil {
			members = []guardMember{}
		}
		body, err := json.MarshalIndent(map[string]any{"members": members}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(answers, name), append(body, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeAnswer("default", w.members)
	for server, members := range w.answers {
		writeAnswer(strings.NewReplacer("/", "_", ":", "_").Replace(server), members)
	}

	var pods []string
	for _, p := range w.pods {
		pods = append(pods, "narad-"+strconv.Itoa(p))
	}
	calls := filepath.Join(dir, "calls")
	env, _ := containerEnv(guardPodSpec(job))
	cmd := exec.Command(sh, "-c", guardScript(t, job))
	cmd.Env = []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR=" + dir,
		"FAKE_PODS=" + strings.Join(pods, " "),
		"FAKE_ANSWERS=" + answers,
		"FAKE_FAIL=" + strings.Join(w.fail, " "),
		"FAKE_FAIL_ALL=" + w.failAll,
		"FAKE_CALLS=" + calls,
		"NARAD_PASS=" + w.password,
		"FAKE_DNS_FLAG=" + filepath.Join(dir, "dns-down"),
	}
	if w.dnsDown {
		cmd.Env = append(cmd.Env, "FAKE_DNS_DOWN=1")
	}
	if w.dnsDownAfterAPI {
		cmd.Env = append(cmd.Env, "FAKE_DNS_DOWN_AFTER_API=1")
	}
	for k, v := range env {
		if k == "HOME" {
			v = dir
		}
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	run := guardRun{}
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("run the guard: %v", err)
		}
		run.exit = ee.ExitCode()
	}
	run.output = out.String()
	if b, err := os.ReadFile(calls); err == nil {
		run.calls = strings.Fields(string(b))
	}
	return run
}

func wantExit(t *testing.T, run guardRun, code int, says ...string) {
	t.Helper()
	if run.exit != code {
		t.Fatalf("guard exited %d, want %d; output:\n%s", run.exit, code, run.output)
	}
	for _, s := range says {
		if !strings.Contains(run.output, s) {
			t.Errorf("guard output lacks %q:\n%s", s, run.output)
		}
	}
}

// A rollback from 5 replicas to 3 deletes narad-3 and narad-4. While
// they are still members (Raft voters until decommission removes them),
// the guard refuses, whatever they own.
func TestScaleInGuardRefusesDeletingPodsThatAreStillMembers(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3")
	all := []guardMember{member(0, 3), member(1, 3), member(2, 3), member(3, 2), member(4, 0)}

	run := runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3, 4}, members: all, password: "pw"})
	wantExit(t, run, 1, "REFUSED", "narad-3 (owns 2 partitions)", "narad-4 (owns 0 partitions)",
		"narad cluster decommission", "--no-hooks")
	if len(run.calls) != 1 || run.calls[0] != serviceURL {
		t.Fatalf("asked %v, want the Service only", run.calls)
	}

	t.Run("a member any remaining pod lists blocks, when the Service is down", func(t *testing.T) {
		run := runGuard(t, job, guardWorld{
			pods:     []int{0, 1, 2, 3},
			fail:     []string{serviceURL},
			password: "pw",
			members:  []guardMember{member(0, 4), member(1, 4), member(2, 4)},
			answers: map[string][]guardMember{
				// A stale replica still lists narad-3.
				podURL(1): {member(0, 4), member(1, 4), member(2, 4), member(3, 0)},
			},
		})
		wantExit(t, run, 1, "REFUSED", "narad-3 (owns 0 partitions)")
		for _, c := range run.calls {
			if c == podURL(3) {
				t.Fatalf("asked the pod being deleted (%v); its view may be stale", run.calls)
			}
		}
	})
}

// Decommission removes a node from the member list once it owns
// nothing; deleting its pod afterwards is the documented scale-in.
func TestScaleInGuardAllowsRemovingPodsThatLeftTheCluster(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3")
	run := runGuard(t, job, guardWorld{
		pods:     []int{0, 1, 2, 3, 4},
		members:  []guardMember{member(0, 5), member(1, 5), member(2, 5)},
		password: "pw",
	})
	wantExit(t, run, 0, "allowed")

	no := false
	run = runGuard(t, job, guardWorld{
		pods:     []int{0, 1, 2, 3},
		members:  []guardMember{member(0, 5), member(1, 5), member(2, 5), {ID: "narad-3", Status: "alive", Voter: &no}},
		password: "pw",
	})
	wantExit(t, run, 0, "allowed")
}

// An upgrade or rollback that deletes no pod is never blocked, and never
// waits on the API: an emergency rollback of a broken release must not
// depend on that release's API answering.
func TestScaleInGuardAllowsWhenNoPodIsDeleted(t *testing.T) {
	for _, c := range []struct {
		name     string
		replicas string
		pods     []int
	}{
		{"same size", "3", []int{0, 1, 2}},
		{"scale-out", "5", []int{0, 1, 2}},
	} {
		t.Run(c.name, func(t *testing.T) {
			job := guardJob(t, "--set", "replicaCount="+c.replicas)
			run := runGuard(t, job, guardWorld{pods: c.pods, failAll: "http 503: no leader"})
			wantExit(t, run, 0, "deletes none")
			if len(run.calls) != 0 {
				t.Fatalf("called the API %v although nothing is deleted", run.calls)
			}
		})
	}
}

// Without the member list the guard cannot tell a decommissioned pod
// from a voter, so it refuses and says why and how to go ahead.
func TestScaleInGuardRefusesWhenMembersCannotBeRead(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3")

	run := runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3}, failAll: "http 401: authentication required"})
	wantExit(t, run, 1, "REFUSED: could not read", "401", "no 'admin-password' key", "--no-hooks")
	want := []string{serviceURL, podURL(0), podURL(1), podURL(2)}
	if !reflect.DeepEqual(run.calls, want) {
		t.Fatalf("asked %v, want the Service and then each remaining pod %v", run.calls, want)
	}

	run = runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3}, failAll: "http 401: authentication required", password: "old"})
	wantExit(t, run, 1, "REFUSED: could not read", "root admin's current password")
}

// A member whose pod does not exist is not deleted by this change; the
// guard says so and lets the change through.
func TestScaleInGuardOnlyNotesMembersWithoutPods(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3")
	run := runGuard(t, job, guardWorld{
		pods:     []int{0, 1, 2, 3},
		members:  []guardMember{member(0, 5), member(1, 5), member(2, 5), member(7, 0)},
		password: "pw",
	})
	wantExit(t, run, 0, "narad-7", "allowed")
}

// getent fails the same way for a pod that does not exist and for a DNS
// error, so a failed lookup counts as a missing pod only while a pod
// that stays resolves. A rollback from 5 to 3 replicas during a CoreDNS
// outage used to see no pod at or above ordinal 3 and let the
// StatefulSet delete two voters that were never decommissioned.
func TestScaleInGuardRefusesWhenDNSFails(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3")
	all := []guardMember{member(0, 3), member(1, 3), member(2, 3), member(3, 2), member(4, 0)}

	t.Run("DNS fails from the start", func(t *testing.T) {
		run := runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3, 4}, members: all, password: "pw", dnsDown: true})
		wantExit(t, run, 1, "REFUSED: DNS lookups fail; cannot tell which pods exist", "narad-2 narad-0", "--no-hooks")
		if len(run.calls) != 0 {
			t.Fatalf("called the API %v although it could not tell which pods exist", run.calls)
		}
	})
	t.Run("DNS fails after the member list is read", func(t *testing.T) {
		run := runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3, 4}, members: all, password: "pw", dnsDownAfterAPI: true})
		wantExit(t, run, 1, "REFUSED: DNS lookups fail", "--no-hooks")
		if strings.Contains(run.output, "allowed") {
			t.Fatalf("the guard allowed the change:\n%s", run.output)
		}
	})
	t.Run("a scale-out finds a pod that stays at ordinal 0", func(t *testing.T) {
		run := runGuard(t, guardJob(t, "--set", "replicaCount=5"), guardWorld{pods: []int{0, 1, 2}, failAll: "http 503: no leader"})
		wantExit(t, run, 0, "deletes none")
	})
}

// A member list with no members is not a member list: a running cluster
// lists at least the node that answers. With security off, a pod whose
// replica is empty answers {"members":[]}, and the guard used to read
// that as "no pod being deleted is a member" and allow the change.
func TestScaleInGuardRefusesAnEmptyMemberList(t *testing.T) {
	job := guardJob(t, "--set", "replicaCount=3", "--set", "security.enabled=false", "--set", "security.allowInsecureCluster=true")

	t.Run("every server answers no members", func(t *testing.T) {
		run := runGuard(t, job, guardWorld{pods: []int{0, 1, 2, 3, 4}})
		wantExit(t, run, 1, "REFUSED: could not read", "answered a member list with no members", "--no-hooks")
		want := []string{serviceURL, podURL(0), podURL(1), podURL(2)}
		if !reflect.DeepEqual(run.calls, want) {
			t.Fatalf("asked %v, want the Service and then each remaining pod %v", run.calls, want)
		}
	})
	t.Run("a pod that stays lists the members", func(t *testing.T) {
		run := runGuard(t, job, guardWorld{
			pods: []int{0, 1, 2, 3},
			answers: map[string][]guardMember{
				podURL(2): {member(0, 4), member(1, 4), member(2, 4), member(3, 1)},
			},
		})
		wantExit(t, run, 1, "REFUSED: these pods are still cluster members", "narad-3 (owns 1 partitions)")
	})
}
