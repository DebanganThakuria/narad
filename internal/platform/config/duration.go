package config

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a JSON-friendly time.Duration. It marshals/unmarshals as a
// string parseable by time.ParseDuration (e.g. "10s", "500ms", "1h30m"),
// and falls back to a numeric nanosecond value if the JSON contains a
// number — so existing zero-value defaults still round-trip.
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

// UnmarshalJSON accepts either a duration string ("10s") or a numeric
// nanosecond value.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("config: parse duration %q: %w", s, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("config: duration must be a string (e.g. \"10s\") or a number of nanoseconds")
	}
	*d = Duration(n)
	return nil
}
