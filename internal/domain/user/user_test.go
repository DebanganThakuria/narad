package user

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

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

func TestAllowedAnyMatchesAnyActionOrAdmin(t *testing.T) {
	u := User{Grants: []Grant{
		{Action: ActionProduce, Patterns: []string{"orders-*"}},
		{Action: ActionConsume, Patterns: []string{"logs"}},
	}}
	for name, want := range map[string]bool{
		"orders-eu": true,  // produce grant, wildcard
		"logs":      true,  // consume grant, literal
		"logs-2":    false, // literal does not prefix-match
		"payments":  false,
	} {
		if got := u.AllowedAny(name); got != want {
			t.Fatalf("AllowedAny(%q) = %v, want %v", name, got, want)
		}
	}
	if (User{}).AllowedAny("orders-eu") {
		t.Fatal("a user with no grants must not see any topic")
	}
	if !(User{Root: true}).AllowedAny("anything") {
		t.Fatal("root must see every topic")
	}
	if !(User{Grants: []Grant{{Action: ActionAdmin}}}).AllowedAny("anything") {
		t.Fatal("admin must see every topic")
	}
}

func TestValidatePassword(t *testing.T) {
	if err := ValidatePassword(""); err == nil {
		t.Fatal("empty password accepted")
	}
	if err := ValidatePassword(strings.Repeat("a", MaxPasswordBytes)); err != nil {
		t.Fatalf("72-byte password rejected: %v", err)
	}
	if err := ValidatePassword(strings.Repeat("a", MaxPasswordBytes+1)); err == nil {
		t.Fatal("73-byte password accepted")
	}
	// Bytes, not runes: 24 three-byte runes are 72 bytes, 25 are 75.
	if err := ValidatePassword(strings.Repeat("€", 24)); err != nil {
		t.Fatalf("72-byte multibyte password rejected: %v", err)
	}
	if err := ValidatePassword(strings.Repeat("€", 25)); err == nil {
		t.Fatal("75-byte multibyte password accepted")
	}
}

func TestValidatePasswordHashAcceptsOnlyBcrypt(t *testing.T) {
	good, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidatePasswordHash(good); err != nil {
		t.Fatalf("a real bcrypt hash was refused: %v", err)
	}
	for _, bad := range [][]byte{
		nil,
		{},
		[]byte("eA=="),
		[]byte("hunter2"),
		[]byte("$2a$99$" + strings.Repeat("a", 53)), // cost out of range
	} {
		if err := ValidatePasswordHash(bad); err == nil {
			t.Errorf("ValidatePasswordHash(%q) accepted a value that is not a bcrypt hash", bad)
		}
	}
}

func TestValidateNewUserRefusesBadNamesGrantsAndHashes(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	ok := User{Username: "svc", PasswordHash: hash, Grants: []Grant{{Action: ActionProduce, Patterns: []string{"a*"}}}}
	if err := ValidateNewUser(ok); err != nil {
		t.Fatalf("a valid user was refused: %v", err)
	}
	for name, u := range map[string]User{
		"path the mux cleans": {Username: "svc/../ghost", PasswordHash: hash},
		"dot-dot":             {Username: "..", PasswordHash: hash},
		"space":               {Username: "a b", PasswordHash: hash},
		"empty name":          {Username: "", PasswordHash: hash},
		"unknown action":      {Username: "svc", PasswordHash: hash, Grants: []Grant{{Action: "nope"}}},
		"no hash":             {Username: "svc"},
		"not bcrypt":          {Username: "svc", PasswordHash: []byte("hunter2")},
	} {
		if err := ValidateNewUser(u); err == nil {
			t.Errorf("%s: ValidateNewUser accepted %+v", name, u)
		}
	}
}
