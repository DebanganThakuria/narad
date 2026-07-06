package user

import "testing"

func TestMatchPattern(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"orders", "orders", true},
		{"orders", "orders-eu", false},
		{"orders-*", "orders-eu", true},
		{"orders-*", "orders-", true},
		{"orders-*", "orders", false},
		{"*", "anything", true},
		{"*", "", true},
		{"", "", true},
		{"", "x", false},
	}
	for _, c := range cases {
		if got := MatchPattern(c.pattern, c.name); got != c.want {
			t.Errorf("MatchPattern(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

func TestAllowed(t *testing.T) {
	u := User{Grants: []Grant{
		{Action: ActionProduce, Patterns: []string{"orders-*", "audit"}},
		{Action: ActionConsume, Patterns: []string{"orders-eu"}},
	}}

	cases := []struct {
		action Action
		topic  string
		want   bool
	}{
		{ActionProduce, "orders-eu", true},
		{ActionProduce, "audit", true},
		{ActionProduce, "audit-2", false},
		{ActionConsume, "orders-eu", true},
		{ActionConsume, "orders-us", false},
		{ActionCreate, "orders-eu", false},
	}
	for _, c := range cases {
		if got := u.Allowed(c.action, c.topic); got != c.want {
			t.Errorf("Allowed(%q, %q) = %v, want %v", c.action, c.topic, got, c.want)
		}
	}
}

func TestAdminAllowsEverything(t *testing.T) {
	for _, u := range []User{
		{Root: true},
		{Grants: []Grant{{Action: ActionAdmin}}},
	} {
		for _, action := range []Action{ActionProduce, ActionConsume, ActionCreate, ActionAdmin} {
			if !u.Allowed(action, "any-topic") {
				t.Errorf("admin user %+v denied %q", u, action)
			}
		}
		if !u.IsAdmin() {
			t.Errorf("IsAdmin() = false for %+v", u)
		}
	}
}

func TestCoversEnforcesNoEscalation(t *testing.T) {
	granted := []Grant{
		{Action: ActionProduce, Patterns: []string{"orders-*"}},
		{Action: ActionConsume, Patterns: []string{"orders-eu"}},
	}

	cases := []struct {
		name      string
		requested []Grant
		want      bool
	}{
		{"identical", granted, true},
		{"narrower wildcard", []Grant{{Action: ActionProduce, Patterns: []string{"orders-eu-*"}}}, true},
		{"literal under wildcard", []Grant{{Action: ActionProduce, Patterns: []string{"orders-x"}}}, true},
		{"broader wildcard", []Grant{{Action: ActionProduce, Patterns: []string{"ord*"}}}, false},
		{"star from scoped", []Grant{{Action: ActionProduce, Patterns: []string{"*"}}}, false},
		{"action not held", []Grant{{Action: ActionCreate, Patterns: []string{"orders-x"}}}, false},
		{"literal exact", []Grant{{Action: ActionConsume, Patterns: []string{"orders-eu"}}}, true},
		{"literal covers nothing wider", []Grant{{Action: ActionConsume, Patterns: []string{"orders-eu-*"}}}, false},
		{"admin request", []Grant{{Action: ActionAdmin}}, false},
	}
	for _, c := range cases {
		if got := Covers(granted, c.requested); got != c.want {
			t.Errorf("%s: Covers = %v, want %v", c.name, got, c.want)
		}
	}

	if !Covers([]Grant{{Action: ActionAdmin}}, []Grant{{Action: ActionAdmin}}) {
		t.Error("admin should cover granting admin")
	}
	if !Covers([]Grant{{Action: ActionAdmin}}, granted) {
		t.Error("admin should cover any scoped grant")
	}
}

func TestCanDelegate(t *testing.T) {
	root := User{Root: true}
	admin := User{Grants: []Grant{{Action: ActionAdmin}}}
	scoped := User{Grants: []Grant{{Action: ActionProduce, Patterns: []string{"orders-*"}}}}

	adminGrant := []Grant{{Action: ActionAdmin}}
	produceGrant := []Grant{{Action: ActionProduce, Patterns: []string{"orders-eu"}}}
	broaderGrant := []Grant{{Action: ActionProduce, Patterns: []string{"ord*"}}}

	cases := []struct {
		name      string
		granter   User
		requested []Grant
		want      bool
	}{
		{"root confers admin", root, adminGrant, true},
		{"admin cannot confer admin", admin, adminGrant, false},
		{"root confers scoped", root, produceGrant, true},
		{"admin confers scoped", admin, produceGrant, true},
		{"scoped confers subset", scoped, produceGrant, true},
		{"scoped cannot broaden", scoped, broaderGrant, false},
		{"scoped cannot confer admin", scoped, adminGrant, false},
	}
	for _, c := range cases {
		if got := c.granter.CanDelegate(c.requested); got != c.want {
			t.Errorf("%s: CanDelegate = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestValidateUsername(t *testing.T) {
	for _, ok := range []string{"admin", "svc.orders-1", "A_b-c.9"} {
		if err := ValidateUsername(ok); err != nil {
			t.Errorf("ValidateUsername(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "with space", "colon:name", "über", string(make([]byte, 65))} {
		if err := ValidateUsername(bad); err == nil {
			t.Errorf("ValidateUsername(%q) = nil, want error", bad)
		}
	}
}

func TestValidateGrants(t *testing.T) {
	valid := [][]Grant{
		{{Action: ActionProduce, Patterns: []string{"orders"}}},
		{{Action: ActionConsume, Patterns: []string{"orders-*", "*"}}},
		{{Action: ActionAdmin}},
		nil,
	}
	for i, g := range valid {
		if err := ValidateGrants(g); err != nil {
			t.Errorf("valid[%d]: %v", i, err)
		}
	}

	invalid := [][]Grant{
		{{Action: "publish", Patterns: []string{"x"}}},
		{{Action: ActionProduce}},
		{{Action: ActionProduce, Patterns: []string{""}}},
		{{Action: ActionProduce, Patterns: []string{"mid*dle"}}},
		{{Action: ActionProduce, Patterns: []string{"sp ace"}}},
		{{Action: ActionAdmin, Patterns: []string{"x"}}},
	}
	for i, g := range invalid {
		if err := ValidateGrants(g); err == nil {
			t.Errorf("invalid[%d]: expected error", i)
		}
	}
}
