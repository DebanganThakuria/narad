package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a JSON-friendly time.Duration. It marshals and unmarshals
// as a string parseable by time.ParseDuration ("10s", "500ms", "1h30m").
// A bare JSON number is REJECTED: it used to be read as nanoseconds, so
// an operator who wrote "read_timeout": 30 instead of "30s" got a
// server whose every request timed out after 30 ns, and validation
// (which only checks > 0) let it start. The defaults round-trip as
// strings, so nothing legitimate relied on the numeric form.
//
// Callers that need a stdlib time.Duration use Duration.D() or convert
// directly: time.Duration(d).
type Duration time.Duration

// D returns the value as a stdlib time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// String renders the duration in time.Duration's canonical form.
func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON encodes the duration as a string ("10s").
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts a duration string ("10s") and nothing else.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("config: duration must be a string with a unit (e.g. \"10s\", \"500ms\"), got %s", string(b))
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("config: parse duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}
