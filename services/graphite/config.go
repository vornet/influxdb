package graphite

import (
	"github.com/influxdb/influxdb/toml"
)

const (
	// DefaultBindAddress is the default binding interface if none is specified.
	DefaultBindAddress = ":2003"

	// DefaultDatabase is the default database if none is specified.
	DefaultDatabase = "graphite"

	// DefaultNameSeparator represents the default field separator.
	DefaultNameSeparator = "."

	// DefaultProtocol is the default IP protocol used by the Graphite input.
	DefaultProtocol = "tcp"

	// DefaultConsistencyLevel is the default write consistency for the Graphite input.
	DefaultConsistencyLevel = "one"
)

// Config represents the configuration for Graphite endpoints.
type Config struct {
	BindAddress      string        `toml:"bind-address"`
	Database         string        `toml:"database"`
	Enabled          bool          `toml:"enabled"`
	Protocol         string        `toml:"protocol"`
	NameSeparator    string        `toml:"name-separator"`
	NameSchema       string        `toml:"name-schema"`
	BatchSize        int           `toml:"batch-size"`
	BatchTimeout     toml.Duration `toml:"batch-timeout"`
	ConsistencyLevel string        `toml:"consistency-level"`
}

// NewConfig returns a new Config with defaults.
func NewConfig() Config {
	return Config{
		BindAddress:      DefaultBindAddress,
		Database:         DefaultDatabase,
		Protocol:         DefaultProtocol,
		NameSeparator:    DefaultNameSeparator,
		ConsistencyLevel: DefaultConsistencyLevel,
	}
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	if d.BindAddress == "" {
		d.BindAddress = DefaultBindAddress
	}
	if d.Database == "" {
		d.Database = DefaultDatabase
	}
	if d.Protocol == "" {
		d.Protocol = DefaultProtocol
	}
	if d.NameSeparator == "" {
		d.NameSeparator = DefaultNameSeparator
	}
	if d.ConsistencyLevel == "" {
		d.ConsistencyLevel = DefaultConsistencyLevel
	}
	return &d
}
