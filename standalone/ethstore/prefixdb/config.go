package prefixdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/allegro/bigcache/v3"
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
	StorageDir    string `json:"storage_dir"`
	HotStorageDir string `json:"hot_storage_dir"`
	SlotIndexFile string `json:"slot_index_file"`

	// Cache sizes and other parameters
	NodeCacheSize int             `json:"node_cache_size"`
	// DeprecatedMaxCacheSize keeps backward compatibility for older config files.
	DeprecatedMaxCacheSize int             `json:"max_cache_size,omitempty"`
	WriteBatchSize          int             `json:"write_batch_size"`
	BigCacheConfig          bigcache.Config `json:"bigcache_config"`
}

var nodeCacheSizeOverride atomic.Int64

// SetNodeCacheSizeOverride sets a process-wide NodeCache size override for PrefixDB.
// Use size <= 0 to clear override and fallback to config/default value.
func SetNodeCacheSizeOverride(size int) {
	nodeCacheSizeOverride.Store(int64(size))
}

func effectiveNodeCacheSize(configValue int) int {
	if override := int(nodeCacheSizeOverride.Load()); override > 0 {
		return override
	}
	return configValue
}

// DefaultConfig returns a configuration with default values.
// dirpath is the base directory for the database.
func DefaultConfig(dirpath string) *Config {
	prefixDBDir := filepath.Join(dirpath, "prefixdb")
	return &Config{
		BaseDir:        dirpath,
		AccountDir:     filepath.Join(prefixDBDir, "na"),
		TrieDir:        filepath.Join(prefixDBDir, "trie"),
		StorageDir:     filepath.Join(prefixDBDir, "storagefiles"),
		HotStorageDir:  filepath.Join(prefixDBDir, "storagefiles", "hotstorage"),
		NodeCacheSize:  1 << 18,
		WriteBatchSize: 4096,
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
	if cfg.NodeCacheSize <= 0 && cfg.DeprecatedMaxCacheSize > 0 {
		cfg.NodeCacheSize = cfg.DeprecatedMaxCacheSize
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
