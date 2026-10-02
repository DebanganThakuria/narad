//go:build cluster

package cluster

// A single broker with security on and no cluster secret answers node
// RPC (QUIC on the API port over UDP) only from itself: it generates a
// per-process secret, and a process that cannot prove a secret is
// refused.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	ncluster "github.com/debanganthakuria/narad/internal/cluster"
	"github.com/debanganthakuria/narad/internal/domain/user"
)

// singleNode runs one `narad serve` with no peers and the given extra
// env, waits until it serves the API (with security on, until the root
// admin authenticates), and returns its API address and the path of its
// log. The process is stopped at cleanup.
func singleNode(t *testing.T, extra map[string]string) (api, logPath string) {
	t.Helper()
	httpPorts, raftPorts := allocPorts(t)
	dir := t.TempDir()
	api = fmt.Sprintf("127.0.0.1:%d", httpPorts[0])
	logPath = filepath.Join(dir, "narad.log")
	env := map[string]string{
		"NARAD_HTTP_ADDR":        api,
		"NARAD_CLUSTER_ADDR":     fmt.Sprintf("127.0.0.1:%d", raftPorts[0]),
		"NARAD_NODE_ID":          "narad-single",
		"NARAD_DATA_DIR":         filepath.Join(dir, "data"),
		"NARAD_SECURITY_ENABLED": "true",
		"NARAD_ADMIN_PASSWORD":   adminPassword,
		"NARAD_LOG_FORMAT":       "text",
		"NARAD_LOG_LEVEL":        "info",
		"GORACE":                 "halt_on_error=0",
	}
	for k, v := range extra {
		env[k] = v
	}
	secured := env["NARAD_SECURITY_ENABLED"] == "true"
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(naradBin, "serve")
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "TMPDIR=" + os.TempDir()}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); logFile.Close(); close(exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		if body, err := os.ReadFile(logPath); err == nil {
			for _, bad := range []string{"panic:", "fatal error:", "WARNING: DATA RACE"} {
				if strings.Contains(string(body), bad) {
					t.Errorf("node log contains %q:\n%s", bad, body)
				}
			}
		}
	})
	deadline := time.Now().Add(60 * time.Second)
	for {
		status := usersStatus(api, "admin", adminPassword)
		if status == http.StatusOK || (!secured && status > 0) {
			return api, logPath
		}
		select {
		case <-exited:
			body, _ := os.ReadFile(logPath)
			t.Fatalf("narad exited during startup:\n%s", body)
		default:
		}
		if time.Now().After(deadline) {
			body, _ := os.ReadFile(logPath)
			t.Fatalf("the API never came up; log:\n%s", body)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// usersStatus is the status of GET /v1/users with the given Basic
// credentials, or -1 when the request did not complete.
func usersStatus(api, username, password string) int {
	req, _ := http.NewRequest(http.MethodGet, "http://"+api+"/v1/users", nil)
	req.SetBasicAuth(username, password)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return -1
	}
	resp.Body.Close()
	return resp.StatusCode
}

// createUserOverNodeRPC sends one OpCreateUser frame for an admin
// "rpc-probe-user" without the cluster secret, and reports the
// reply and whether that user can then list users.
func createUserOverNodeRPC(t *testing.T, api string) (rpcStatus int, rpcErr error, after int) {
	t.Helper()
	if before := usersStatus(api, "rpc-probe-user", "pw"); before != http.StatusUnauthorized {
		t.Fatalf("precondition: the user's GET /v1/users = %d, want 401", before)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(user.User{Username: "rpc-probe-user", PasswordHash: hash, Grants: []user.Grant{{Action: user.ActionAdmin}}})
	if err != nil {
		t.Fatal(err)
	}
	pc := ncluster.NewPeerClient(5*time.Second, "")
	defer pc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := pc.CreateUser(ctx, api, body)
	if err == nil {
		rpcStatus = res.Status
	}
	time.Sleep(300 * time.Millisecond)
	return rpcStatus, err, usersStatus(api, "rpc-probe-user", "pw")
}

func TestSingleNodeNodeRPCRefusesUnauthenticatedPeers(t *testing.T) {
	t.Run("no cluster secret", func(t *testing.T) {
		api, logPath := singleNode(t, nil)
		status, err, after := createUserOverNodeRPC(t, api)
		t.Logf("unauthenticated OpCreateUser: status=%d err=%v; the user's GET /v1/users afterwards = %d", status, err, after)
		if err == nil && status == http.StatusCreated {
			t.Errorf("an unauthenticated node RPC peer created a user (status %d)", status)
		}
		if after != http.StatusUnauthorized {
			t.Errorf("the user's GET /v1/users = %d after the unauthenticated node RPC call, want 401", after)
		}
		body, _ := os.ReadFile(logPath)
		if !strings.Contains(string(body), "node RPC is closed to other processes") {
			t.Errorf("startup did not say node RPC is closed to other processes; log:\n%s", body)
		}
	})

	// Control: an explicit secret was already enough before the change.
	t.Run("explicit cluster secret", func(t *testing.T) {
		api, _ := singleNode(t, map[string]string{"NARAD_CLUSTER_SECRET": "an-explicit-single-node-secret"})
		status, err, after := createUserOverNodeRPC(t, api)
		if (err == nil && status == http.StatusCreated) || after != http.StatusUnauthorized {
			t.Fatalf("with an explicit secret: status=%d err=%v after=%d, want the call refused and the user 401", status, err, after)
		}
	})

	// Security off is an explicit opt-in to an open node RPC plane: the
	// node still starts, and it says so.
	t.Run("security off", func(t *testing.T) {
		_, logPath := singleNode(t, map[string]string{"NARAD_SECURITY_ENABLED": "false"})
		deadline := time.Now().Add(10 * time.Second)
		for {
			body, _ := os.ReadFile(logPath)
			if strings.Contains(string(body), "node RPC plane is unauthenticated") {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("security off: no warning naming the unauthenticated node RPC plane; log:\n%s", body)
			}
			time.Sleep(200 * time.Millisecond)
		}
	})
}
