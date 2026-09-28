//go:build race

package messaging

// zzWP18RaceEnabled lets allocation-count tests stand down under the
// race detector, whose instrumentation allocates on its own.
const zzWP18RaceEnabled = true
