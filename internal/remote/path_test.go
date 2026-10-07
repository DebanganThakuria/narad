package remote

import "testing"

func TestTopicPathRefusesEverythingButOneSafeSegment(t *testing.T) {
	for _, name := range []string{"../users", ".", "..", "a/b", "%2e%2e", "", "a b", "a?b", "a#b", "a%2fb"} {
		if p, err := TopicPath(name, "produce", "batch"); err == nil {
			t.Fatalf("TopicPath(%q) = %q, want an error", name, p)
		}
	}
	for _, suffix := range [][]string{{""}, {"."}, {".."}} {
		if _, err := TopicPath("orders", suffix...); err == nil {
			t.Fatalf("TopicPath(orders, %q) = nil error", suffix)
		}
	}
}

func TestTopicPathBuildsEscapedPaths(t *testing.T) {
	cases := map[string][]string{
		"/v1/topics/orders":                 {"orders"},
		"/v1/topics/orders.eu-1_x/children": {"orders.eu-1_x", "children"},
		"/v1/topics/o/produce/batch":        {"o", "produce", "batch"},
	}
	for want, args := range cases {
		got, err := TopicPath(args[0], args[1:]...)
		if err != nil || got != want {
			t.Fatalf("TopicPath(%q) = %q, %v, want %q", args, got, err, want)
		}
	}
	if UsersPath() != "/v1/users" {
		t.Fatalf("UsersPath() = %q", UsersPath())
	}
}
