// Package config defines Narad's runtime configuration and the rules
// for loading it.
//
// Precedence, lowest to highest:
//
//	defaults -> JSON config file -> environment variables -> CLI flags
//
// Load applies the first three layers; callers overlay CLI flags
// themselves and then call (*Config).Validate before using the result.
package config
