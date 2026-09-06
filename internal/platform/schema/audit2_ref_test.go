package schema

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Audit finding 1.3: the compiler used to run with jsonschema's default
// FileLoader, so a $ref with a file:// URL in a client-supplied schema
// was resolved against the broker's filesystem (the file's constraints
// became the topic's schema, a read primitive) and a failed load echoed
// the path and the OS error back to the client (an existence oracle).
// The compiler now refuses every external load and the error names
// neither the path nor the URL.
func TestAudit2SchemaRefLoadsLocalFiles(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret.json")
	if err := os.WriteFile(secret, []byte(`{"type":"integer"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	r := NewJSONSchema()
	ref := []byte(`{"$ref":"file://` + secret + `"}`)
	err := r.Load(context.Background(), "t", 1, ref)
	if err == nil {
		t.Fatal("Load() with a file:// $ref succeeded: the broker read a local file on behalf of the client")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dir) || strings.Contains(err.Error(), "file://") {
		t.Errorf("error echoes the client's path back: %v", err)
	}
	if !errors.Is(err, errExternalRef) {
		t.Errorf("error = %v, want the external $ref refusal", err)
	}
	// Nothing was registered.
	if err := r.Validate(context.Background(), "t", []byte(`"not an integer"`)); !errors.Is(err, ErrSchemaNotFound) {
		t.Errorf("Validate() after refused Load error = %v, want %v", err, ErrSchemaNotFound)
	}

	// Existence oracle: a missing file produces exactly the same error
	// as an existing one, with no path in it.
	missing := filepath.Join(dir, "does-not-exist.json")
	err2 := r.ValidateDefinition(context.Background(), "u", []byte(`{"$ref":"file://`+missing+`"}`))
	if err2 == nil {
		t.Fatal("ValidateDefinition() with a missing file $ref succeeded")
	}
	if strings.Contains(err2.Error(), missing) || strings.Contains(err2.Error(), "no such file") {
		t.Errorf("error leaks the path or the OS error: %v", err2)
	}
	if err.Error() != err2.Error() {
		t.Errorf("existing and missing file $ref produce different errors (%q vs %q): existence oracle", err, err2)
	}

	// A relative $ref used to be resolved against the working directory;
	// now it resolves against the opaque narad:// resource and is refused
	// the same way.
	err3 := r.ValidateDefinition(context.Background(), "u", []byte(`{"$ref":"go.mod"}`))
	if err3 == nil || !errors.Is(err3, errExternalRef) {
		t.Errorf("relative $ref error = %v, want the external $ref refusal", err3)
	}
	// http(s) too.
	err4 := r.ValidateDefinition(context.Background(), "u", []byte(`{"properties":{"a":{"$ref":"https://example.invalid/x.json"}}}`))
	if err4 == nil || !errors.Is(err4, errExternalRef) || strings.Contains(err4.Error(), "example.invalid") {
		t.Errorf("https $ref error = %v, want the external $ref refusal without the URL", err4)
	}
}
