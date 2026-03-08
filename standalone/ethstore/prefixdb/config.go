package prefixdb

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config holds the configuration for PrefixDB.
type Config struct {
	// BaseDir is the root directory for the database.
	// If empty, it defaults to the directory provided to NewPrefixDB.
	BaseDir string `json:"base_dir"`
	// Sub-directories or file paths relative to BaseDir (or absolute).
	// If empty, defaults will be used.
	AccountDir    string `json:"account_dir"`
	StorageDir    string `json:"storage_dir"`
}

// DefaultConfig returns a configuration with default values.
// dirpath is the base directory for the database.
func DefaultConfig(dirpath string) *Config {
	prefixDBDir := filepath.Join(dirpath, "prefixdb")
	return &Config{
		BaseDir:        dirpath,
		AccountDir:     filepath.Join(prefixDBDir, "na"),
		StorageDir:     filepath.Join(prefixDBDir, "storagefiles"),
	}
}

// LoadConfig reads configuration from a JSON file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// resolvePath returns the absolute path.
// If path is absolute, it returns it.
// If path is relative, it joins it with baseDir.
func resolvePath(baseDir, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(baseDir, path)
}
