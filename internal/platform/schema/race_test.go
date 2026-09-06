//go:build race

package schema

// raceEnabled lets the worst-case bound tests stand down under the
// race detector, where their timings mean nothing and their 256 KiB
// and 1 MiB inputs would take minutes.
const raceEnabled = true
