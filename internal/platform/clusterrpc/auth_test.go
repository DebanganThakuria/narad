package clusterrpc

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

func TestVerifyAuthFrame(t *testing.T) {
	const secret = "s3cr3t"

	if !verifyAuthFrame(secret, authFrame(secret)) {
		t.Fatal("valid token rejected")
	}
	if verifyAuthFrame(secret, authFrame("wrong")) {
		t.Fatal("token from wrong secret accepted")
	}
	if verifyAuthFrame(secret, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeRequest, Payload: authToken(secret)}) {
		t.Fatal("non-auth frame type accepted")
	}
	if verifyAuthFrame(secret, clusterwire.StreamFrame{Type: clusterwire.StreamFrameAuth}) {
		t.Fatal("empty token accepted")
	}
}

func TestAuthTokenIsStableAndSecretSpecific(t *testing.T) {
	if string(authToken("a")) == string(authToken("b")) {
		t.Fatal("different secrets produced the same token")
	}
	if string(authToken("a")) != string(authToken("a")) {
		t.Fatal("token is not deterministic")
	}
	// The raw secret must not appear in the token.
	if string(authToken("supersecret")) == "supersecret" {
		t.Fatal("token leaked the raw secret")
	}
}
