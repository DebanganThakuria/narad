package schema

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/debanganthakuria/narad/internal/errs"
)

// wp4bSource is an in-memory schema history that counts its reads.
type wp4bSource struct {
	history   map[string][][]byte // topic -> versions 1..n
	gets      int
	latests   int
	latestErr error
}

func (s *wp4bSource) GetSchema(_ context.Context, topic string, version int) ([]byte, error) {
	s.gets++
	h := s.history[topic]
	if version < 1 || version > len(h) {
		return nil, errs.ErrNotFound
	}
	return h[version-1], nil
}

// wp4bLatestSource adds the one-read latest lookup the metastore has.
type wp4bLatestSource struct{ *wp4bSource }

func (s wp4bLatestSource) LatestSchema(_ context.Context, topic string) (int, []byte, error) {
	s.latests++
	if s.latestErr != nil {
		return 0, nil, s.latestErr
	}
	h := s.history[topic]
	if len(h) == 0 {
		return 0, nil, nil
	}
	return len(h), h[len(h)-1], nil
}

// wp4bMaxLengthHistory is n versions whose field "s" allows strings up
// to 10+version characters, so a payload tells which version validated
// it.
func wp4bMaxLengthHistory(n int) [][]byte {
	var h [][]byte
	for v := 1; v <= n; v++ {
		h = append(h, fmt.Appendf(nil, `{"type":"object","properties":{"s":{"type":"string","maxLength":%d}}}`, 10+v))
	}
	return h
}

func wp4bString(n int) []byte {
	b := []byte(`{"s":"`)
	for range n {
		b = append(b, 'x')
	}
	return append(b, `"}`...)
}

// TestWP4BHydrateReadsOnlyTheLatestVersion: the registry keeps only the
// latest version, so a hydrate from the metastore reads only that one.
// It used to read every version, one metastore read and one copy each.
func TestWP4BHydrateReadsOnlyTheLatestVersion(t *testing.T) {
	src := wp4bLatestSource{&wp4bSource{history: map[string][][]byte{"t": wp4bMaxLengthHistory(50)}}}
	reg := NewJSONSchema()
	has, err := Hydrate(context.Background(), src, reg, "t")
	if err != nil || !has {
		t.Fatalf("Hydrate = %v, %v; want true, nil", has, err)
	}
	if src.gets != 0 || src.latests != 1 {
		t.Fatalf("reads: GetSchema=%d LatestSchema=%d; want 0 and 1", src.gets, src.latests)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(60)); err != nil {
		t.Fatalf("payload valid under v50 rejected: %v", err)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(61)); err == nil {
		t.Fatal("payload over v50's maxLength accepted")
	}
}

// A source without the latest lookup still hydrates, by walking the
// history (the fallback test fakes and embedded sources take).
func TestWP4BHydrateWithoutLatestSourceWalksHistory(t *testing.T) {
	src := &wp4bSource{history: map[string][][]byte{"t": wp4bMaxLengthHistory(5)}}
	reg := NewJSONSchema()
	has, err := Hydrate(context.Background(), src, reg, "t")
	if err != nil || !has {
		t.Fatalf("Hydrate = %v, %v; want true, nil", has, err)
	}
	if src.gets != 6 {
		t.Fatalf("GetSchema calls = %d, want 6 (five versions and the miss)", src.gets)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(15)); err != nil {
		t.Fatalf("payload valid under v5 rejected: %v", err)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(16)); err == nil {
		t.Fatal("payload over v5's maxLength accepted")
	}
}

// A topic whose schemas are gone (deleted, or recreated without one)
// is dropped from the registry, and a failed read leaves it untouched.
func TestWP4BHydrateLatestSourceEmptyAndError(t *testing.T) {
	inner := &wp4bSource{history: map[string][][]byte{"t": wp4bMaxLengthHistory(2)}}
	src := wp4bLatestSource{inner}
	reg := NewJSONSchema()
	if _, err := Hydrate(context.Background(), src, reg, "t"); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	inner.latestErr = boom
	if _, err := Hydrate(context.Background(), src, reg, "t"); !errors.Is(err, boom) {
		t.Fatalf("Hydrate with a failing read = %v, want it wrapped", err)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(13)); err == nil {
		t.Fatal("a failed hydrate changed the loaded schema")
	}

	inner.latestErr = nil
	delete(inner.history, "t")
	has, err := Hydrate(context.Background(), src, reg, "t")
	if err != nil || has {
		t.Fatalf("Hydrate of a topic without schemas = %v, %v; want false, nil", has, err)
	}
	if err := reg.Validate(context.Background(), "t", wp4bString(99)); !errors.Is(err, ErrSchemaNotFound) {
		t.Fatalf("Validate after the schemas went = %v, want ErrSchemaNotFound", err)
	}
}
