package config

// DebugConfig governs operator-only debug listeners. These are kept
// separate from the public API so they can bind to loopback and stay
// disabled by default in production.
//
// PProfAddr controls Go's net/http/pprof endpoints. An empty string
// disables the listener entirely. Recommended value when enabled is
// "127.0.0.1:6060" — pprof leaks goroutine stacks and heap layout, and
// /debug/pprof/profile?seconds=N can be abused to pin a CPU, so do not
// expose it on a public interface.
type DebugConfig struct {
	PProfAddr string `json:"pprof_addr"`
}
