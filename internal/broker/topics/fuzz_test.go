package topics

import (
	"strings"
	"testing"
)

// FuzzValidateTopicName checks the topic-name validator never panics and
// that every name it accepts is one safe path segment: topic names
// become directory names under the data dir.
func FuzzValidateTopicName(f *testing.F) {
	for _, s := range []string{"orders", "", ".", "..", "a/b", "a\\b", strings.Repeat("a", 255), strings.Repeat("a", 256), "a\x00", "ünïcode", "a b", "...", ".hidden"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, name string) {
		if err := validateTopicName(name); err != nil {
			return
		}
		if name == "" || len(name) > 255 || name == "." || name == ".." ||
			strings.ContainsAny(name, "/\\\x00 \t\r\n") {
			t.Fatalf("validateTopicName accepted %q", name)
		}
		for i := 0; i < len(name); i++ {
			if name[i] >= 0x80 {
				t.Fatalf("validateTopicName accepted non-ASCII %q", name)
			}
		}
	})
}
