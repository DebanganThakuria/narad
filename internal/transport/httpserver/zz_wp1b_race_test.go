//go:build race

package httpserver

// wp1bRaceEnabled lets allocation-count tests stand down under the race
// detector, which adds allocations of its own.
const wp1bRaceEnabled = true
