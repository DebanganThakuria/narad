package user

import (
	"encoding/json"
	"strings"
	"testing"
)

// FuzzValidateUsername checks the username validator never panics and
// that every name it accepts is one safe segment: no separators, no
// dot-names, no control bytes, bounded length.
func FuzzValidateUsername(f *testing.F) {
	for _, s := range []string{"admin", "", ".", "..", "a/b", "a\\b", strings.Repeat("a", 64), strings.Repeat("a", 65), "a\x00", "ünïcode", "a b"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if err := ValidateUsername(name); err != nil {
			return
		}
		if name == "" || len(name) > 64 || name == "." || name == ".." ||
			strings.ContainsAny(name, "/\\\x00 \t\r\n") {
			t.Fatalf("ValidateUsername accepted %q", name)
		}
		for i := 0; i < len(name); i++ {
			if name[i] >= 0x80 {
				t.Fatalf("ValidateUsername accepted non-ASCII %q", name)
			}
		}
	})
}

// FuzzValidateGrants decodes arbitrary JSON as a grant list and runs the
// validator: it must never panic, and every accepted grant must carry a
// known action with patterns of the documented shape.
func FuzzValidateGrants(f *testing.F) {
	f.Add([]byte(`[{"action":"produce","patterns":["orders*"]}]`))
	f.Add([]byte(`[{"action":"admin"}]`))
	f.Add([]byte(`[{"action":"admin","patterns":["x"]}]`))
	f.Add([]byte(`[{"action":"consume","patterns":[]}]`))
	f.Add([]byte(`[{"action":"consume","patterns":["*"]}]`))
	f.Add([]byte(`[{"action":"consume","patterns":["a*b"]}]`))
	f.Add([]byte(`[{"action":"nope"}]`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))

	f.Fuzz(func(t *testing.T, body []byte) {
		var grants []Grant
		if err := json.Unmarshal(body, &grants); err != nil {
			return
		}
		if err := ValidateGrants(grants); err != nil {
			return
		}
		for _, g := range grants {
			switch g.Action {
			case ActionAdmin:
				if len(g.Patterns) != 0 {
					t.Fatalf("admin grant with patterns accepted: %+v", g)
				}
			case ActionProduce, ActionConsume, ActionCreate:
				if len(g.Patterns) == 0 {
					t.Fatalf("scoped grant without patterns accepted: %+v", g)
				}
				for _, p := range g.Patterns {
					if p == "" || len(p) > 256 || strings.Count(p, "*") > 1 ||
						(strings.Contains(p, "*") && !strings.HasSuffix(p, "*")) ||
						strings.ContainsAny(p, "/\\\x00 ") {
						t.Fatalf("pattern %q accepted in %+v", p, g)
					}
				}
			default:
				t.Fatalf("unknown action accepted: %+v", g)
			}
		}
	})
}
