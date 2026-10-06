package remote

import (
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
)

func TestPostureAllowsRemotes(t *testing.T) {
	cases := map[Posture]bool{
		{SecurityEnabled: true}:                          true,
		{SecurityEnabled: true, RaftTLS: false}:          true, // Q23: reported, not gated
		{SecurityEnabled: false}:                         false,
		{SecurityEnabled: true, LegacyClusterAuth: true}: false,
	}
	for p, want := range cases {
		if got := PostureAllowsRemotes(p); got != want {
			t.Fatalf("PostureAllowsRemotes(%+v) = %v", p, got)
		}
	}
	if HopAllowed(Posture{SecurityEnabled: true}) || !HopAllowed(Posture{APIHopEncrypted: true}) {
		t.Fatal("HopAllowed follows remotes.api_hop_encrypted")
	}
}

func TestStartupCheck(t *testing.T) {
	weak := func(string) error { return errs.ErrRemoteSecretWeak }
	strong := func(string) error { return nil }
	secured := Posture{SecurityEnabled: true, RaftTLS: true}

	// Nothing is checked on a node without remotes.
	if _, err := StartupCheck(StartupInputs{Posture: Posture{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := StartupCheck(StartupInputs{HoldsRemotes: true, Posture: Posture{}, ClusterSecret: "s"}); !errors.Is(err, ErrStartupSecurityOff) {
		t.Fatalf("security off with remotes: %v", err)
	}
	if _, err := StartupCheck(StartupInputs{HoldsRemotes: true, Posture: secured, ClusterSecret: " "}); !errors.Is(err, ErrStartupNoSecret) {
		t.Fatalf("no secret with remotes: %v", err)
	}
	rep, err := StartupCheck(StartupInputs{HoldsRemotes: true, Posture: secured, ClusterSecret: "s", SecretCheck: weak})
	if err != nil || !rep.WeakSecret || rep.PlaintextRaft {
		t.Fatalf("weak secret starts and reports: %+v %v", rep, err)
	}
	rep, err = StartupCheck(StartupInputs{HoldsRemotes: true, Posture: Posture{SecurityEnabled: true}, ClusterSecret: "s", SecretCheck: strong})
	if err != nil || !rep.PlaintextRaft || rep.WeakSecret {
		t.Fatalf("plaintext raft starts and reports: %+v %v", rep, err)
	}
}

func TestWriteLimiterSlidingMinute(t *testing.T) {
	l := NewWriteLimiter()
	now := time.Unix(1_000_000, 0)
	l.now = func() time.Time { return now }
	for i := range WritesPerMinute {
		if !l.Allow() {
			t.Fatalf("write %d refused", i+1)
		}
	}
	if l.Allow() {
		t.Fatal("11th write in a minute allowed")
	}
	now = now.Add(time.Minute + time.Millisecond)
	if !l.Allow() {
		t.Fatal("a write after the minute refused")
	}
}

func TestCheckLimiterOneAtATimeAndOnePerInterval(t *testing.T) {
	l := NewCheckLimiter()
	now := time.Unix(1_000_000, 0)
	l.now = func() time.Time { return now }
	release, ok := l.Acquire("b")
	if !ok {
		t.Fatal("first check refused")
	}
	if _, ok := l.Acquire("b"); ok {
		t.Fatal("a concurrent check of the same remote allowed")
	}
	if r2, ok := l.Acquire("c"); !ok {
		t.Fatal("another remote's check refused")
	} else {
		r2()
	}
	release()
	if _, ok := l.Acquire("b"); ok {
		t.Fatal("a check within 5 s allowed")
	}
	now = now.Add(CheckInterval)
	if _, ok := l.Acquire("b"); !ok {
		t.Fatal("a check after 5 s refused")
	}
}
