package config

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// rollbackTargetStorageKeys are the storage keys the config loader of the
// newest release (v3.0.1) accepts. That loader rejects every other
// storage key as an internal setting, and config.Load's error stops the
// process, so a node rolled back with a newer key still in its config
// file (the chart's narad.config) crash-loops. When a release is cut,
// replace this set with that release's configurableStorageKeys.
var rollbackTargetStorageKeys = []string{
	"data_dir",
	"codec",
	"compression_level",
	"idle_log_eviction_ms",
	"cold_retention_walk_ms",
}

// rollbackDocs are the places an operator reads before rolling back. Each
// must tell them to remove every storage key the rollback target rejects.
var rollbackDocs = []struct {
	path    string // from the repository root
	heading string // the section that must carry the instruction
}{
	{"docs/operate/upgrade.md", "## Roll back {#roll-back}"},
	{"CHANGELOG.md", "### Upgrade and rollback notes"}, // the first one is the unreleased section
	{"docs/reference/configuration.md", "## Config file {#config-file}"},
}

var wordRemove = regexp.MustCompile(`(?i)\bremove\b`)

// Every storage key this loader accepts and the rollback target's loader
// rejects is named, together with an instruction to remove it, in the
// rollback notes. Without one, the documented rollback (the same helm
// upgrade with the older image tag) keeps the key in the file and the
// first rolled-back pod fails to start.
func TestRollbackNotesTellOperatorsToRemoveNewStorageKeys(t *testing.T) {
	for key := range configurableStorageKeys {
		if slices.Contains(rollbackTargetStorageKeys, key) {
			continue
		}
		for _, doc := range rollbackDocs {
			if !blockSaysRemove(t, doc.path, doc.heading, key) {
				t.Errorf("%s, section %q: no paragraph or list item names storage key %q and says to remove it before a rollback; the rollback target's loader rejects it",
					doc.path, doc.heading, key)
			}
		}
	}
}

// blockSaysRemove reports whether one paragraph or list item of the
// section under heading names key and contains the word "remove".
func blockSaysRemove(t *testing.T, path, heading, key string) bool {
	t.Helper()
	for _, block := range markdownBlocks(sectionOf(t, path, heading)) {
		if (strings.Contains(block, "storage."+key) || strings.Contains(block, "`"+key+"`")) && wordRemove.MatchString(block) {
			return true
		}
	}
	return false
}

// sectionOf returns the lines of the first section headed exactly
// heading, up to the next heading of the same or a higher level.
func sectionOf(t *testing.T, path, heading string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", filepath.FromSlash(path)))
	if err != nil {
		t.Fatal(err)
	}
	level := strings.IndexFunc(heading, func(r rune) bool { return r != '#' })
	lines := strings.Split(string(raw), "\n")
	start := slices.Index(lines, heading)
	if start < 0 {
		t.Fatalf("%s has no heading %q", path, heading)
	}
	inFence := false
	for i := start + 1; i < len(lines); i++ {
		line := lines[i]
		if strings.HasPrefix(line, "```") {
			inFence = !inFence
		}
		if inFence || !strings.HasPrefix(line, "#") {
			continue
		}
		if l := strings.IndexFunc(line, func(r rune) bool { return r != '#' }); l > 0 && l <= level {
			return lines[start+1 : i]
		}
	}
	return lines[start+1:]
}

// markdownBlocks splits lines into paragraphs and list items: a blank
// line or a line starting a list item begins a new block.
func markdownBlocks(lines []string) []string {
	var blocks []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			blocks = append(blocks, strings.Join(cur, " "))
			cur = nil
		}
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "":
			flush()
			continue
		case strings.HasPrefix(line, "- "), strings.HasPrefix(line, "* "):
			flush()
		}
		cur = append(cur, trimmed)
	}
	flush()
	return blocks
}
