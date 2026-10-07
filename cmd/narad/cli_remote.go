package main

// `narad remote`: the remotes registry (other Narad clusters this one
// replicates to). The remote's password is read from stdin only, never
// from argv (visible in ps and shell history), and is refused, not just
// warned about, over a plain-http context to a host that is not this
// machine: it is a credential for another cluster.

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// remoteLimitFlags are the limits a create or change may name.
type remoteLimitFlags struct {
	maxInFlight     int
	requestTimeout  time.Duration
	idleConnTimeout time.Duration
	connMaxAge      time.Duration
	checkInterval   time.Duration
	compression     string
}

func (f *remoteLimitFlags) register(cmd *cobra.Command) {
	cmd.Flags().IntVar(&f.maxInFlight, "max-in-flight", 0, "requests in flight to the remote per node (1-256, default 16)")
	cmd.Flags().DurationVar(&f.requestTimeout, "request-timeout", 0, "per-request timeout (5s-120s, default 30s)")
	cmd.Flags().DurationVar(&f.idleConnTimeout, "idle-conn-timeout", 0, "idle connection timeout (default 30s)")
	cmd.Flags().DurationVar(&f.connMaxAge, "conn-max-age", 0, "replace the connections, busy ones included, this often (default 5m)")
	cmd.Flags().DurationVar(&f.checkInterval, "check-interval", 0, "runtime target check interval (default 60s)")
	cmd.Flags().StringVar(&f.compression, "compression", "", "none (default) or zstd")
}

// body returns the limits the flags set, or nil.
func (f *remoteLimitFlags) body(cmd *cobra.Command) map[string]any {
	out := map[string]any{}
	set := func(flag, key string, v any) {
		if cmd.Flags().Changed(flag) {
			out[key] = v
		}
	}
	set("max-in-flight", "max_in_flight", f.maxInFlight)
	set("request-timeout", "request_timeout_ms", f.requestTimeout.Milliseconds())
	set("idle-conn-timeout", "idle_conn_timeout_ms", f.idleConnTimeout.Milliseconds())
	set("conn-max-age", "conn_max_age_ms", f.connMaxAge.Milliseconds())
	set("check-interval", "check_interval_ms", f.checkInterval.Milliseconds())
	set("compression", "compression", f.compression)
	if len(out) == 0 {
		return nil
	}
	return out
}

// remotePasswordConnection resolves the connection for a command that
// sends a remote password. The caller's own --password-stdin reads the
// same stdin, so the two are refused together; and a remote password
// never goes over plain http to a host that is not loopback.
func remotePasswordConnection() (*httpClient, error) {
	if flagPasswordStdin {
		return nil, errors.New("--password-stdin and --remote-password-stdin both read stdin: give your own password with NARAD_PASS or a context (narad ctx add)")
	}
	c, err := cliConnection()
	if err != nil {
		return nil, err
	}
	if plaintextOffBox(c.Server) {
		return nil, errors.New("refusing to send a remote password over plain http to a host that is not this machine: use an https:// server URL")
	}
	return newContextHTTPClient(c), nil
}

// plaintextOffBox reports an http:// server URL whose host is not
// loopback.
func plaintextOffBox(server string) bool {
	u, err := url.Parse(server)
	if err != nil || !strings.EqualFold(u.Scheme, "http") {
		return false
	}
	host := u.Hostname()
	if host == "localhost" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return false
	}
	return true
}

// readRemotePassword reads the remote password from the first line of
// stdin.
func readRemotePassword() (string, error) {
	pw, err := readPasswordStdin()
	if err != nil {
		return "", errors.New("--remote-password-stdin: no password on stdin")
	}
	return pw, nil
}

func readCAFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("--ca-file: %w", err)
	}
	return string(b), nil
}

func remotePath(name string, rest ...string) string {
	p := "/v1/remotes/" + url.PathEscape(name)
	for _, r := range rest {
		p += "/" + r
	}
	return p
}

// newRemoteCmd is the `narad remote` command tree.
func newRemoteCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "remote",
		Short: "manage remotes: other Narad clusters this one replicates to (admin)",
	}
	root.AddCommand(newRemoteAddCmd(), newRemoteSetCmd(), newRemoteRmCmd(), newRemoteLsCmd(), newRemoteTestCmd(), newRemoteReencryptCmd())
	return root
}

func newRemoteAddCmd() *cobra.Command {
	var serverURL, username, caFile string
	var passwordStdin bool
	var limits remoteLimitFlags
	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "register a remote; the password is read from stdin",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if serverURL == "" || username == "" {
				return errors.New("--url and --username are required")
			}
			if !passwordStdin {
				return errors.New("--remote-password-stdin is required: the remote password is read from stdin only")
			}
			client, err := remotePasswordConnection()
			if err != nil {
				return err
			}
			body := map[string]any{"name": args[0], "url": serverURL, "username": username}
			if caFile != "" {
				if body["ca_pem"], err = readCAFile(caFile); err != nil {
					return err
				}
			}
			if l := limits.body(cmd); l != nil {
				body["limits"] = l
			}
			pw, err := readRemotePassword()
			if err != nil {
				return err
			}
			body["password"] = pw
			return client.postAndPrint("/v1/remotes", body)
		},
	}
	cmd.Flags().StringVar(&serverURL, "url", "", "the remote cluster's https URL (required)")
	cmd.Flags().StringVar(&username, "username", "", "the replicator's username on the remote (required)")
	cmd.Flags().StringVar(&caFile, "ca-file", "", "PEM bundle that alone verifies the remote (default: system roots)")
	cmd.Flags().BoolVar(&passwordStdin, "remote-password-stdin", false, "read the remote password from the first line of stdin (the only way to pass it)")
	limits.register(cmd)
	return cmd
}

func newRemoteSetCmd() *cobra.Command {
	var serverURL, username, caFile string
	var noCA, passwordStdin bool
	var limits remoteLimitFlags
	cmd := &cobra.Command{
		Use:   "set <name>",
		Short: "change a remote; a new url, username or CA needs the password again",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bound := cmd.Flags().Changed("url") || cmd.Flags().Changed("username") || caFile != "" || noCA
			if bound && !passwordStdin {
				return errors.New("--url, --username, --ca-file and --no-ca need --remote-password-stdin: the stored password is bound to the URL, the username and the CA it was entered with")
			}
			if caFile != "" && noCA {
				return errors.New("--ca-file and --no-ca are mutually exclusive")
			}
			var client *httpClient
			var err error
			if passwordStdin {
				client, err = remotePasswordConnection()
			} else {
				client = cliClient()
			}
			if err != nil {
				return err
			}
			body := map[string]any{}
			if cmd.Flags().Changed("url") {
				body["url"] = serverURL
			}
			if cmd.Flags().Changed("username") {
				body["username"] = username
			}
			if caFile != "" {
				if body["ca_pem"], err = readCAFile(caFile); err != nil {
					return err
				}
			}
			if noCA {
				body["ca_pem"] = ""
			}
			if l := limits.body(cmd); l != nil {
				body["limits"] = l
			}
			if passwordStdin {
				pw, err := readRemotePassword()
				if err != nil {
					return err
				}
				body["password"] = pw
			}
			if len(body) == 0 {
				return errors.New("nothing to change: name a field to set")
			}
			return client.patchAndPrint(remotePath(args[0]), body)
		},
	}
	cmd.Flags().StringVar(&serverURL, "url", "", "new https URL (needs --remote-password-stdin)")
	cmd.Flags().StringVar(&username, "username", "", "new replicator username (needs --remote-password-stdin)")
	cmd.Flags().StringVar(&caFile, "ca-file", "", "new PEM bundle (needs --remote-password-stdin)")
	cmd.Flags().BoolVar(&noCA, "no-ca", false, "drop the CA and use the system roots (needs --remote-password-stdin)")
	cmd.Flags().BoolVar(&passwordStdin, "remote-password-stdin", false, "read the new remote password from the first line of stdin")
	limits.register(cmd)
	return cmd
}

func newRemoteRmCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "rm <name>",
		Short: "delete a remote (--force: even while remote children use it; they hold in remote_missing)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			path := remotePath(args[0])
			if force {
				path += "?force=true"
			}
			return cliClient().deleteRequest(path)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "delete while remote children use it")
	return cmd
}

func newRemoteLsCmd() *cobra.Command {
	var noNodes bool
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "list remotes, their links, and what each node's cache holds",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			path := "/v1/remotes"
			if noNodes {
				path += "?nodes=false"
			}
			resp, err := cliClient().do(http.MethodGet, path, nil)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if err := printResponse(&http.Response{StatusCode: resp.StatusCode, Body: io.NopCloser(strings.NewReader(string(raw)))}); err != nil {
				return err
			}
			if warn := silentMembersWarning(raw); warn != "" {
				fmt.Fprintln(os.Stderr, warn)
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&noNodes, "no-nodes", false, "skip asking every node for its cache status")
	return cmd
}

// silentMembersWarning names the members a remote listing could not
// ask. Such a member may still hold a deleted remote's credential, so
// the listing alone does not prove every node let go of it.
func silentMembersWarning(body []byte) string {
	var ans struct {
		NotAnswering []string `json:"not_answering"`
	}
	if json.Unmarshal(body, &ans) != nil || len(ans.NotAnswering) == 0 {
		return ""
	}
	return fmt.Sprintf("warning: %s did not answer; a node that did not answer may still hold a deleted remote's credential", strings.Join(ans.NotAnswering, ", "))
}

func newRemoteTestCmd() *cobra.Command {
	var topicName, source string
	cmd := &cobra.Command{
		Use:   "test <name>",
		Short: "run the attach checks against a remote topic; exits 0 only if every node that ran them passes (every node with remotes.allowed_hosts set, only the answering node without)",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if topicName == "" {
				return errors.New("--topic is required")
			}
			body := map[string]any{"topic": topicName}
			if source != "" {
				body["source"] = source
			}
			resp, err := cliClient().do(http.MethodPost, remotePath(args[0], "test"), body)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			var ans struct {
				Result string `json:"result"`
				Class  string `json:"class"`
			}
			_ = json.Unmarshal(raw, &ans)
			if err := printResponse(&http.Response{StatusCode: resp.StatusCode, Body: io.NopCloser(strings.NewReader(string(raw)))}); err != nil {
				return err
			}
			if ans.Result != "pass" {
				return fmt.Errorf("remote test failed: %s", ans.Class)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&topicName, "topic", "", "topic on the remote (required)")
	cmd.Flags().StringVar(&source, "source", "", "parent topic on this cluster, for the source, schema and loop checks")
	return cmd
}

func newRemoteReencryptCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reencrypt",
		Short: "after a cluster secret rotation, re-seal every remote password under the new key; exits non-zero if any remote failed to re-seal (keep the previous secret until it exits 0)",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := cliClient().do(http.MethodPost, "/v1/cluster/reencrypt-remotes", map[string]any{})
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if err := printResponse(&http.Response{StatusCode: resp.StatusCode, Body: io.NopCloser(strings.NewReader(string(raw)))}); err != nil {
				return err
			}
			var ans struct {
				Failed []struct {
					Name   string `json:"name"`
					Reason string `json:"reason"`
				} `json:"failed"`
			}
			_ = json.Unmarshal(raw, &ans)
			if len(ans.Failed) == 0 {
				return nil
			}
			failed := make([]string, 0, len(ans.Failed))
			for _, f := range ans.Failed {
				failed = append(failed, f.Name+" ("+f.Reason+")")
			}
			return fmt.Errorf("%d remotes failed to re-seal: %s; keep the previous cluster secret until reencrypt exits 0", len(failed), strings.Join(failed, ", "))
		},
	}
}
