// Package config provides Aether runtime configuration.
//
// Configuration is loaded from ~/.aether/config.yaml, environment
// variables, and CLI flags (in that priority order).
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	DefaultChunkSize  = 2 << 20 // 2 MB
	DefaultWorkers    = 5
	DefaultRelayURL   = "http://localhost:8080"
	DefaultRelayPort  = 8080
	DefaultCompression = true
	DefaultEncryption  = false
)

// Config holds all Aether runtime settings.
type Config struct {
	ChunkSize   uint32 `json:"chunk_size"`
	Workers     int    `json:"workers"`
	RelayURL    string `json:"relay_url"`
	RelayPort   int    `json:"relay_port"`
	Compression bool   `json:"compression"`
	Encryption  bool   `json:"encryption"`
}

// Default returns a Config with all default values.
func Default() *Config {
	return &Config{
		ChunkSize:   DefaultChunkSize,
		Workers:     DefaultWorkers,
		RelayURL:    DefaultRelayURL,
		RelayPort:   DefaultRelayPort,
		Compression: DefaultCompression,
		Encryption:  DefaultEncryption,
	}
}

// configDir returns ~/.aether/
func configDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".aether"
	}
	return filepath.Join(home, ".aether")
}

// configPath returns ~/.aether/config.json
func configPath() string {
	return filepath.Join(configDir(), "config.json")
}

// Load reads config from ~/.aether/config.json.
// Returns Default() if file doesn't exist.
func Load() *Config {
	cfg := Default()

	data, err := os.ReadFile(configPath())
	if err != nil {
		return cfg
	}

	json.Unmarshal(data, cfg)
	return cfg
}

// Save writes the current config to ~/.aether/config.json.
func Save(cfg *Config) error {
	dir := configDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(configPath(), data, 0o644)
}
