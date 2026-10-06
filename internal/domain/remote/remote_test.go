package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

const canary = "canary-7f3k9q-password"

func TestSecretRedactsEverywhere(t *testing.T) {
	s := NewSecret([]byte(canary))
	req := struct {
		Name     string
		Password Secret
		Ptr      *Secret
	}{Name: "b", Password: s, Ptr: &s}

	var out []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%T"} {
		out = append(out, fmt.Sprintf(verb, s), fmt.Sprintf(verb, req), fmt.Sprintf(verb, &s))
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "secret", s, "req", req)
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "secret", s, "req", req)
	out = append(out, buf.String())
	j, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	txt, _ := s.MarshalText()
	out = append(out, string(j), string(txt), fmt.Errorf("wrapped: %v", s).Error())
	func() {
		defer func() { out = append(out, fmt.Sprint(recover())) }()
		panic(s)
	}()
	for _, o := range out {
		if strings.Contains(o, canary) {
			t.Fatalf("secret leaked: %s", o)
		}
	}
	if !strings.Contains(string(j), Redacted) {
		t.Fatalf("JSON = %s, want %s", j, Redacted)
	}
	if string(s.Bytes()) != canary || s.Len() != len(canary) {
		t.Fatal("Bytes lost the value")
	}
}

func TestSecretUnmarshalErrorQuotesNothing(t *testing.T) {
	var v struct {
		Password Secret `json:"password"`
	}
	if err := json.Unmarshal([]byte(`{"password":"`+canary+`"}`), &v); err != nil || string(v.Password.Bytes()) != canary {
		t.Fatalf("decode string: %v", err)
	}
	for _, bad := range []string{`{"password":123}`, `{"password":{"x":"` + canary + `"}}`, `{"password":["` + canary + `"]}`, `{"password":true}`} {
		err := json.Unmarshal([]byte(bad), &v)
		if err == nil || !errors.Is(err, ErrSecretInvalid) {
			t.Fatalf("decode %s: err = %v, want ErrSecretInvalid", bad, err)
		}
		if strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), "123") {
			t.Fatalf("error quotes its input: %v", err)
		}
	}
}

func TestSecretWipe(t *testing.T) {
	b := []byte(canary)
	NewSecret(b).Wipe()
	if !bytes.Equal(b, make([]byte, len(canary))) {
		t.Fatal("Wipe left bytes behind")
	}
}

func TestLimitsDefaultsAndPatch(t *testing.T) {
	if (Limits{}).WithDefaults() != DefaultLimits() {
		t.Fatal("zero limits did not take every default")
	}
	l := Limits{MaxInFlight: 48}.WithDefaults()
	if l.MaxInFlight != 48 || l.RequestTimeoutMs != DefaultRequestTimeoutMs || l.Compression != CompressionNone {
		t.Fatalf("WithDefaults = %+v", l)
	}
	n, z := 64, CompressionZstd
	got := l.Apply(LimitsPatch{MaxInFlight: &n, Compression: &z})
	if got.MaxInFlight != 64 || got.Compression != CompressionZstd || got.RequestTimeoutMs != l.RequestTimeoutMs {
		t.Fatalf("Apply = %+v", got)
	}
}

func TestEnvelopePrintsNoCiphertext(t *testing.T) {
	e := Envelope{V: 1, KV: "9d3e0c1f5a7b2e48", CT: []byte(canary)}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("x", "env", e)
	for _, s := range []string{fmt.Sprintf("%v %+v %s", e, e, e), buf.String(), fmt.Sprintf("%+v", Record{Credential: e})} {
		if strings.Contains(s, canary) || strings.Contains(s, "9d3e0c1f5a7b2e48") {
			t.Fatalf("envelope leaked: %s", s)
		}
	}
}
