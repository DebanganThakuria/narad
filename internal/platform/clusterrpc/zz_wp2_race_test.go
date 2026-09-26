//go:build race

package clusterrpc

// zzWP2Race lets the allocation-count tests stand down under the race
// detector, which adds allocations of its own.
const zzWP2Race = true
