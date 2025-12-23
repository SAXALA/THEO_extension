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
	TrieDir       string `json:"trie_dir"`
	PebblePath    string `json:"pebble_path"`
	StorageDir    string `json:"storage_dir"`
	HotStorageDir string `json:"hot_storage_dir"`
	SlotIndexFile string `json:"slot_index_file"`
	MemcacheAddr  string `json:"memcache_addr"`

	// Cache sizes and other parameters
	MaxCacheSize   int `json:"max_cache_size"`
	WriteBatchSize int `json:"write_batch_size"`
	SlotCacheSize  int `json:"slot_cache_size"`
}

// DefaultConfig returns a configuration with default values.
// dirpath is the base directory for the database.
func DefaultConfig(dirpath string) *Config {
	prefixDBDir := filepath.Join(dirpath, "prefixdb")
	return &Config{
		BaseDir:        dirpath,
		AccountDir:     filepath.Join(prefixDBDir, "na"),
		TrieDir:        filepath.Join(prefixDBDir, "trie"),
		PebblePath:     filepath.Join(prefixDBDir, "accountHash_key_pebble"),
		StorageDir:     filepath.Join(prefixDBDir, "storagefiles"),
		HotStorageDir:  filepath.Join(prefixDBDir, "storagefiles", "hotstorage"),
		SlotIndexFile:  filepath.Join(prefixDBDir, "slotIndex"),
		MemcacheAddr:   "127.0.0.1:11211",
		MaxCacheSize:   65535,
		WriteBatchSize: 4096,
		SlotCacheSize:  512,
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
