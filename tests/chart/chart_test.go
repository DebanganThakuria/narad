// Package chart_test renders charts/narad with the helm binary and checks
// the objects it produces. Every test skips when helm is not on PATH.
package chart_test

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// releaseSelector is what the chart's selector labels render to for the
// release name the tests use.
var releaseSelector = map[string]any{
	"app.kubernetes.io/name":     "narad",
	"app.kubernetes.io/instance": "narad",
}

// fenced turns the chart's NetworkPolicy off and acknowledges plaintext
// Raft as fenced by something outside the chart instead.
var fenced = []string{"--set", "networkPolicy.enabled=false", "--set", "security.allowPlaintextRaft=true"}

func chartDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "charts", "narad")
}

func helmBinary(t *testing.T) string {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	return helm
}

// helmTemplate runs `helm template` for a release called narad in the
// namespace narad. It returns stdout, or stderr with the error.
func helmTemplate(t *testing.T, args ...string) (string, error) {
	t.Helper()
	full := append([]string{"template", "narad", chartDir(t), "--namespace", "narad"}, args...)
	cmd := exec.Command(helmBinary(t), full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return stderr.String(), err
	}
	return stdout.String(), nil
}

// render renders the chart and parses every document.
func render(t *testing.T, args ...string) []map[string]any {
	t.Helper()
	out, err := helmTemplate(t, args...)
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, out)
	}
	var docs []map[string]any
	for _, text := range splitDocs(out) {
		v := parseYAML(text)
		m, ok := v.(map[string]any)
		if !ok {
			if v == nil {
				continue
			}
			t.Fatalf("rendered document is not a mapping:\n%s", text)
		}
		docs = append(docs, m)
	}
	return docs
}

func ofKind(docs []map[string]any, kind string) []map[string]any {
	var out []map[string]any
	for _, d := range docs {
		if d["kind"] == kind {
			out = append(out, d)
		}
	}
	return out
}

func only(t *testing.T, docs []map[string]any, kind string) map[string]any {
	t.Helper()
	found := ofKind(docs, kind)
	if len(found) != 1 {
		t.Fatalf("rendered %d objects of kind %s, want 1", len(found), kind)
	}
	return found[0]
}

// at walks a parsed document by map keys and list indexes.
func at(v any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				return nil
			}
			v = l[k]
		}
	}
	return v
}

func list(v any) []any {
	l, _ := v.([]any)
	return l
}

// containerEnv returns the literal env values of a pod spec's first
// container, and the names of those taken from elsewhere.
func containerEnv(podSpec any) (map[string]string, map[string]any) {
	values := map[string]string{}
	from := map[string]any{}
	for _, e := range list(at(podSpec, "containers", 0, "env")) {
		name, _ := at(e, "name").(string)
		if v, ok := at(e, "value").(string); ok {
			values[name] = v
		} else {
			from[name] = at(e, "valueFrom")
		}
	}
	return values, from
}

func statefulSetEnv(t *testing.T, docs []map[string]any) map[string]string {
	t.Helper()
	values, _ := containerEnv(at(only(t, docs, "StatefulSet"), "spec", "template", "spec"))
	if _, ok := values["NARAD_SECURITY_ENABLED"]; !ok {
		t.Fatalf("parsed no NARAD_SECURITY_ENABLED from the StatefulSet's env %v; the parse is broken", values)
	}
	return values
}

// ingressAdmits lists, for each "port/PROTOCOL" a NetworkPolicy admits,
// the from list of every rule that admits it (nil: from anywhere).
func ingressAdmits(t *testing.T, np map[string]any) map[string][][]any {
	t.Helper()
	admits := map[string][][]any{}
	for i, rule := range list(at(np, "spec", "ingress")) {
		ports := list(at(rule, "ports"))
		if len(ports) == 0 {
			t.Fatalf("ingress rule %d names no ports, so it admits every port:\n%v", i, rule)
		}
		for _, p := range ports {
			proto, _ := at(p, "protocol").(string)
			if proto == "" {
				proto = "TCP"
			}
			port, _ := at(p, "port").(string)
			admits[port+"/"+proto] = append(admits[port+"/"+proto], list(at(rule, "from")))
		}
	}
	return admits
}

func podSelectorPeer(labels map[string]any) any {
	return map[string]any{"podSelector": map[string]any{"matchLabels": labels}}
}

// The chart fences Raft and the node RPC plane with its own
// NetworkPolicy unless told not to: Raft has no authentication of its
// own, and the default install runs it in plaintext. Turning it off
// renders none.
func TestNetworkPolicyIsOnByDefault(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--set", "security.clusterTLS.enabled=true"},
	} {
		if n := len(ofKind(render(t, args...), "NetworkPolicy")); n != 1 {
			t.Fatalf("helm template %v rendered %d NetworkPolicies, want 1", args, n)
		}
	}
	for _, args := range [][]string{
		fenced,
		{"--set", "networkPolicy.enabled=false", "--set", "security.clusterTLS.enabled=true"},
	} {
		if n := len(ofKind(render(t, args...), "NetworkPolicy")); n != 0 {
			t.Fatalf("helm template %v rendered %d NetworkPolicies, want none", args, n)
		}
	}
}

// Raft has no authentication of its own and the node RPC plane trusts
// the cluster secret, so the policy admits both from the release's own
// pods only, while the API and the metrics port stay open to clients.
func TestNetworkPolicyFencesRaftAndNodeRPCToTheReleasePods(t *testing.T) {
	docs := render(t)
	np := only(t, docs, "NetworkPolicy")

	if got := at(np, "spec", "podSelector", "matchLabels"); !reflect.DeepEqual(got, releaseSelector) {
		t.Fatalf("NetworkPolicy selects %v, want the release's pods %v", got, releaseSelector)
	}
	if got := list(at(np, "spec", "policyTypes")); !reflect.DeepEqual(got, []any{"Ingress"}) {
		t.Fatalf("policyTypes = %v, want [Ingress] only", got)
	}
	if at(np, "spec", "egress") != nil {
		t.Fatal("NetworkPolicy restricts egress; the chart fences ingress only")
	}

	admits := ingressAdmits(t, np)
	peers := []any{podSelectorPeer(releaseSelector)}
	for _, port := range []string{"7943/TCP", "7942/UDP"} {
		froms := admits[port]
		if len(froms) == 0 {
			t.Fatalf("%s is not admitted at all; the pods could not reach each other", port)
		}
		for _, from := range froms {
			if !reflect.DeepEqual(from, peers) {
				t.Fatalf("%s is admitted from %v, want only the release's pods %v", port, from, peers)
			}
		}
	}
	for _, port := range []string{"7942/TCP", "9100/TCP"} {
		if froms := admits[port]; len(froms) != 1 || froms[0] != nil {
			t.Fatalf("%s is admitted from %v, want from anywhere", port, froms)
		}
	}
	if froms, ok := admits["6060/TCP"]; ok {
		t.Fatalf("pprof port admitted from %v with pprof off", froms)
	}

	t.Run("pprof stays closed unless pprofFrom names a source", func(t *testing.T) {
		admits := ingressAdmits(t, only(t, render(t,
			"--set", "networkPolicy.enabled=true",
			"--set", "narad.pprof.enabled=true"), "NetworkPolicy"))
		if froms, ok := admits["6060/TCP"]; ok {
			t.Fatalf("pprof port admitted from %v with no pprofFrom", froms)
		}
		admits = ingressAdmits(t, only(t, render(t,
			"--set", "networkPolicy.enabled=true",
			"--set", "narad.pprof.enabled=true",
			"--set", "networkPolicy.pprofFrom[0].podSelector.matchLabels.role=debug"), "NetworkPolicy"))
		want := [][]any{{podSelectorPeer(map[string]any{"role": "debug"})}}
		if got := admits["6060/TCP"]; !reflect.DeepEqual(got, want) {
			t.Fatalf("pprof port admitted from %v, want %v", got, want)
		}
	})

	t.Run("apiFrom and metricsFrom narrow those ports and leave Raft fenced", func(t *testing.T) {
		admits := ingressAdmits(t, only(t, render(t,
			"--set", "networkPolicy.enabled=true",
			"--set", "networkPolicy.apiFrom[0].namespaceSelector.matchLabels.team=payments",
			"--set", "networkPolicy.metricsFrom[0].namespaceSelector.matchLabels.team=monitoring"), "NetworkPolicy"))
		api := map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]any{"team": "payments"}}}
		for _, from := range admits["7942/TCP"] {
			if len(from) == 0 || !reflect.DeepEqual(from[0], api) {
				t.Fatalf("API admitted from %v, want apiFrom first", from)
			}
		}
		want := [][]any{{map[string]any{"namespaceSelector": map[string]any{"matchLabels": map[string]any{"team": "monitoring"}}}}}
		if got := admits["9100/TCP"]; !reflect.DeepEqual(got, want) {
			t.Fatalf("metrics admitted from %v, want %v", got, want)
		}
		for _, from := range admits["7943/TCP"] {
			if !reflect.DeepEqual(from, peers) {
				t.Fatalf("Raft admitted from %v once apiFrom is set", from)
			}
		}
	})

	t.Run("extraIngress is appended as given", func(t *testing.T) {
		admits := ingressAdmits(t, only(t, render(t,
			"--set", "networkPolicy.enabled=true",
			"--set", "networkPolicy.extraIngress[0].ports[0].port=8080",
			"--set", "networkPolicy.extraIngress[0].ports[0].protocol=TCP"), "NetworkPolicy"))
		if froms := admits["8080/TCP"]; len(froms) != 1 || froms[0] != nil {
			t.Fatalf("extraIngress rule admits 8080/TCP from %v, want one open rule", froms)
		}
	})
}

// NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT tells the broker the Raft port is
// fenced. The chart used to set it by default while shipping nothing
// that fenced the port; it now says so only when something does (its own
// NetworkPolicy by default), and a secured install with the policy off,
// no Raft TLS and no other fence fails to render and names the three
// ways out.
func TestPlaintextRaftIsAcknowledgedOnlyWhenFenced(t *testing.T) {
	out, err := helmTemplate(t, "--set", "networkPolicy.enabled=false")
	if err == nil {
		t.Fatalf("rendered with the NetworkPolicy off (plaintext Raft acknowledged: %v); want a refusal, since nothing fences the Raft port",
			strings.Contains(out, "NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT"))
	}
	for _, want := range []string{
		"networkPolicy.enabled is false",
		"security.clusterTLS.enabled=true",
		"networkPolicy.enabled=true",
		"security.allowPlaintextRaft=true",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the refusal does not name %q:\n%s", want, out)
		}
	}

	cases := []struct {
		name string
		args []string
		ack  bool
	}{
		{"the default values: the chart's NetworkPolicy fences it", nil, true},
		{"the operator fences it", fenced, true},
		{"Raft runs over TLS", []string{"--set", "security.clusterTLS.enabled=true"}, false},
		{"Raft runs over TLS behind the policy", []string{"--set", "security.clusterTLS.enabled=true", "--set", "networkPolicy.enabled=true"}, false},
		{"Raft runs over TLS and the operator also fences it", []string{"--set", "security.clusterTLS.enabled=true", "--set", "security.allowPlaintextRaft=true"}, false},
		{"security is off", []string{"--set", "security.enabled=false", "--set", "security.allowInsecureCluster=true"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := statefulSetEnv(t, render(t, c.args...))
			got, ok := env["NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT"]
			if ok != c.ack || (ok && got != "true") {
				t.Fatalf("NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT = %q (set: %v), want set: %v", got, ok, c.ack)
			}
		})
	}
}

// helm upgrade --reuse-values renders with the previous chart's values,
// where every key this chart added is missing and the old defaults are
// set. Setting a key to null drops it the same way. The render must
// still work and fall back to this chart's defaults, except that a
// missing networkPolicy key reads as off: the upgrade must not add a
// policy the operator never chose.
func TestChartRendersWithValuesFromAnOlderRelease(t *testing.T) {
	docs := render(t,
		"--set", "networkPolicy=null",
		"--set", "scaleInGuard=null",
		"--set", "allowScaleInTo=null",
		// The older chart's defaults, carried by --reuse-values.
		"--set", "security.allowPlaintextRaft=true",
		"--set", "allowScaleIn=false")
	if n := len(ofKind(docs, "NetworkPolicy")); n != 0 {
		t.Errorf("rendered %d NetworkPolicies without the networkPolicy key, want none", n)
	}
	if n := len(ofKind(docs, "Job")); n != 1 {
		t.Errorf("rendered %d scale-in guard Jobs without the scaleInGuard key, want 1 (on by default)", n)
	}
	if env := statefulSetEnv(t, docs); env["NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT"] != "true" {
		t.Error("the stored allowPlaintextRaft: true no longer renders the acknowledgement")
	}
}

// A release installed with v3.1.0's chart and upgraded with
// --reuse-values carries none of the keys this chart added for remotes.
// The StatefulSet must still render every secretKeyRef with a key: an
// empty one is refused by the API server and fails the upgrade.
func TestChartRendersSecretKeysWithValuesFromV310(t *testing.T) {
	docs := render(t, append([]string{
		"--set", "security.clusterSecretPreviousKey=null",
		"--set", "remotes=null",
	}, fenced...)...)
	_, from := containerEnv(at(only(t, docs, "StatefulSet"), "spec", "template", "spec"))
	for name, ref := range from {
		key, _ := at(ref, "secretKeyRef", "key").(string)
		if at(ref, "secretKeyRef") != nil && key == "" {
			t.Errorf("%s renders a secretKeyRef with no key", name)
		}
	}
	if key, _ := at(from["NARAD_CLUSTER_SECRET_PREVIOUS"], "secretKeyRef", "key").(string); key != "cluster-secret-previous" {
		t.Errorf("NARAD_CLUSTER_SECRET_PREVIOUS key = %q, want the default cluster-secret-previous", key)
	}
}

func TestChartPassesHelmLint(t *testing.T) {
	helm := helmBinary(t)
	for _, args := range [][]string{
		{},
		fenced,
		{"--set", "security.clusterTLS.enabled=true", "--set", "narad.pprof.enabled=true"},
	} {
		cmd := exec.Command(helm, append([]string{"lint", chartDir(t), "--namespace", "narad"}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("helm lint %v: %v\n%s", args, err, out)
		}
	}
}

// splitDocs splits helm's output into its YAML documents.
func splitDocs(out string) []string {
	var docs []string
	var cur []string
	for _, line := range strings.Split(out, "\n") {
		if line == "---" {
			docs = append(docs, strings.Join(cur, "\n"))
			cur = nil
			continue
		}
		cur = append(cur, line)
	}
	return append(docs, strings.Join(cur, "\n"))
}

// parseYAML parses the block-style YAML subset helm templates and toYaml
// produce: mappings, sequences (also at their key's indentation), plain,
// quoted and block scalars, and the empty {} and []. Scalars stay
// strings. The module has no YAML library, and the tests need structure
// rather than substrings.
func parseYAML(text string) any {
	p := &yamlParser{lines: strings.Split(text, "\n")}
	ind, _, ok := p.peek()
	if !ok {
		return nil
	}
	return p.node(ind)
}

type yamlParser struct {
	lines []string
	i     int
}

// peek returns the next line that is neither blank nor a comment.
func (p *yamlParser) peek() (int, string, bool) {
	for p.i < len(p.lines) {
		line := p.lines[p.i]
		text := strings.TrimLeft(line, " ")
		if text == "" || strings.HasPrefix(text, "#") {
			p.i++
			continue
		}
		return len(line) - len(text), text, true
	}
	return 0, "", false
}

func isItem(text string) bool { return text == "-" || strings.HasPrefix(text, "- ") }

func (p *yamlParser) node(indent int) any {
	_, text, ok := p.peek()
	if !ok {
		return nil
	}
	if isItem(text) {
		return p.sequence(indent)
	}
	return p.mapping(indent)
}

func (p *yamlParser) sequence(indent int) []any {
	out := []any{}
	for {
		ind, text, ok := p.peek()
		if !ok || ind != indent || !isItem(text) {
			return out
		}
		rest := strings.TrimPrefix(strings.TrimPrefix(text, "-"), " ")
		switch {
		case rest == "":
			p.i++
			next, _, _ := p.peek()
			out = append(out, p.node(next))
		case isBlockIndicator(rest):
			p.i++
			out = append(out, p.block(indent))
		case isKey(rest):
			// "- key: value" opens a mapping whose keys sit two columns in.
			p.lines[p.i] = strings.Repeat(" ", indent+2) + rest
			out = append(out, p.mapping(indent+2))
		default:
			p.i++
			out = append(out, scalar(rest))
		}
	}
}

func (p *yamlParser) mapping(indent int) map[string]any {
	m := map[string]any{}
	for {
		ind, text, ok := p.peek()
		if !ok || ind != indent || isItem(text) {
			return m
		}
		key, val := splitKey(text)
		p.i++
		switch {
		case val == "":
			next, ntext, nok := p.peek()
			if nok && (next > indent || (next == indent && isItem(ntext))) {
				m[key] = p.node(next)
			} else {
				m[key] = nil
			}
		case isBlockIndicator(val):
			m[key] = p.block(indent)
		default:
			m[key] = scalar(val)
		}
	}
}

// block reads a block scalar whose lines are indented deeper than parent.
func (p *yamlParser) block(parent int) string {
	var lines []string
	strip := -1
	for p.i < len(p.lines) {
		line := p.lines[p.i]
		text := strings.TrimLeft(line, " ")
		ind := len(line) - len(text)
		if text != "" && ind <= parent {
			break
		}
		if text != "" && strip < 0 {
			strip = ind
		}
		if text == "" || ind < strip {
			lines = append(lines, "")
		} else {
			lines = append(lines, line[strip:])
		}
		p.i++
	}
	return strings.TrimRight(strings.Join(lines, "\n"), "\n") + "\n"
}

func isBlockIndicator(v string) bool {
	switch v {
	case "|", "|-", "|+", ">", ">-", ">+":
		return true
	}
	return false
}

func isKey(text string) bool {
	if strings.HasPrefix(text, `"`) || strings.HasPrefix(text, "'") {
		return false
	}
	return strings.Contains(text, ": ") || strings.HasSuffix(text, ":")
}

func splitKey(text string) (string, string) {
	if i := strings.Index(text, ": "); i >= 0 {
		return text[:i], strings.TrimSpace(text[i+2:])
	}
	return strings.TrimSuffix(text, ":"), ""
}

func scalar(v string) any {
	switch {
	case v == "{}":
		return map[string]any{}
	case v == "[]":
		return []any{}
	case strings.HasPrefix(v, `"`):
		if s, err := strconv.Unquote(v); err == nil {
			return s
		}
	case strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") && len(v) >= 2:
		return strings.ReplaceAll(v[1:len(v)-1], "''", "'")
	}
	return v
}

// remotes.maxHeldBytes reaches the pods whatever its value: 0 (hold
// nothing, re-read instead) is a setting, not an absent one.
func TestRemotesMaxHeldBytesRendersZero(t *testing.T) {
	for _, c := range []struct {
		args []string
		want string
	}{
		{nil, "268435456"},
		{[]string{"--set", "remotes.maxHeldBytes=0"}, "0"},
		{[]string{"--set", "remotes.maxHeldBytes=1048576"}, "1048576"},
	} {
		env := statefulSetEnv(t, render(t, c.args...))
		if got, ok := env["NARAD_REMOTES_MAX_HELD_BYTES"]; !ok || got != c.want {
			t.Errorf("with %v: NARAD_REMOTES_MAX_HELD_BYTES = %q (set: %v), want %q", c.args, got, ok, c.want)
		}
	}
}

// With security off, the render refuses every remotes value the binary
// counts as configured (any value off its default), so the operator
// sees the refusal at helm upgrade instead of every pod crash-looping
// at config validation. The defaults, spelled out or absent, render.
func TestRemotesSettingsWithSecurityOffFailTheRender(t *testing.T) {
	insecure := []string{"--set", "security.enabled=false", "--set", "security.allowInsecureCluster=true"}
	for _, c := range []struct {
		args   []string
		refuse bool
	}{
		{nil, false},
		{[]string{"--set", "remotes=null"}, false},
		{[]string{"--set", "remotes.allowedPorts={443}", "--set", "remotes.maxHeldBytes=268435456"}, false},
		{[]string{"--set", "remotes.allowedPorts=null", "--set", "remotes.maxHeldBytes=null"}, false},
		{[]string{"--set", "remotes.allowedPorts={8443}"}, true},
		{[]string{"--set", "remotes.allowedPorts={443,8443}"}, true},
		{[]string{"--set", "remotes.maxHeldBytes=0"}, true},
		{[]string{"--set", "remotes.maxHeldBytes=1048576"}, true},
		{[]string{"--set", "remotes.allowedHosts={b.example.com}"}, true},
		{[]string{"--set", "remotes.allowAddresses={127.0.0.0/8}"}, true},
		{[]string{"--set", "remotes.apiHopEncrypted=true"}, true},
	} {
		out, err := helmTemplate(t, append(append([]string{}, insecure...), c.args...)...)
		refused := err != nil && strings.Contains(out, "remotes settings require security.enabled")
		if refused != c.refuse || (err != nil && !refused) {
			t.Errorf("security off with %v: render error %v, want refused %v\n%s", c.args, err, c.refuse, out)
		}
	}
}
