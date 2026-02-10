package prefixdb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

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
	PebblePath    string `json:"pebble_path"`
	StorageDir    string `json:"storage_dir"`
	HotStorageDir string `json:"hot_storage_dir"`
	SlotIndexFile string `json:"slot_index_file"`
	MemcacheAddr  string `json:"memcache_addr"`

	// Cache sizes and other parameters
	MaxCacheSize   int             `json:"max_cache_size"`
	WriteBatchSize int             `json:"write_batch_size"`
	BigCacheConfig bigcache.Config `json:"bigcache_config"`
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
		MemcacheAddr:   "127.0.0.1:11211",
		MaxCacheSize:   1 << 20, // 1M entries
		WriteBatchSize: 4096,
		BigCacheConfig: bigcache.Config{
			Shards:             1024,            // 分片数，必须是 2 的幂。越大并发冲突越小。
			LifeWindow:         1 * time.Minute, // 对象的过期时间
			CleanWindow:        1 * time.Minute, // 多久清理一次过期数据
			MaxEntriesInWindow: 1000 * 10 * 60,  // 预期缓存条目数（影响内存预分配）
			MaxEntrySize:       150,             // 预期单个 Entry 的平均字节数（影响内存预分配）
			Verbose:            true,            // 是否打印内存分配信息
			HardMaxCacheSize:   1024,            // 最大占用内存 (MB)
		},
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
