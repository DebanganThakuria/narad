package main

import (
	"fmt"
	"runtime/debug"
)

// version is set at build time via -ldflags '-X main.version=...'. When
// unset (`go run`, `go build` without ldflags) we fall back to the Go
// build info, which still surfaces the VCS commit if available.
var version = ""

func runVersion(_ []string) error {
	fmt.Println(versionString())
	return nil
}

// versionString renders the human-readable version reported by both
// `narad version` and the serve startup log.
func versionString() string {
	if version != "" {
		return "narad " + version
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		var rev, dirty string
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				if s.Value == "true" {
					dirty = "+dirty"
				}
			}
		}
		if rev != "" {
			if len(rev) > 12 {
				rev = rev[:12]
			}
			return "narad dev (" + rev + dirty + ")"
		}
	}
	return "narad dev"
}
