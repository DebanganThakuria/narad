package remote

import (
	"errors"
	"testing"
)

func TestStaticLookupPublishesEveryChange(t *testing.T) {
	srv, ca := tlsServer(t, nil)
	e := staticEntryFor(t, srv.URL, ca, "u", "p")
	l := NewStaticLookup()
	if _, err := l.Get("b"); !errors.Is(err, ErrRemoteMissing) {
		t.Fatalf("Get on empty lookup: %v", err)
	}
	for _, change := range []func(){
		func() { l.Set(e) },
		func() { l.Fail("b", ErrCredentialUnreadable) },
		func() { l.Delete("b") },
	} {
		ch := l.Changed()
		change()
		select {
		case <-ch:
		default:
			t.Fatal("a change did not close the Changed channel")
		}
	}
	l.Set(e)
	if got, err := l.Get("b"); err != nil || got != e {
		t.Fatalf("Get = %v, %v", got, err)
	}
	l.Fail("b", ErrNodeInsecure)
	if _, err := l.Get("b"); !errors.Is(err, ErrNodeInsecure) {
		t.Fatalf("Get after Fail = %v", err)
	}
	l.Set(e)
	if _, err := l.Get("b"); err != nil {
		t.Fatalf("Set did not clear the failure: %v", err)
	}
}
