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
	AccountDir string `json:"account_dir"`
	StorageDir string `json:"storage_dir"`
	// NodeFileGCUnsortedRatioThreshold triggers file-node GC when
	// unsorted_count / sorted_count reaches this ratio.
	// Values <= 0 fall back to the default 1.0.
	NodeFileGCUnsortedRatioThreshold float64 `json:"node_file_gc_unsorted_ratio_threshold"`
	// GCWorkers controls the shared worker pool used by node-file GC and storage GC.
	// Values <= 0 use an automatic worker count.
	GCWorkers int `json:"gc_workers"`
	// NodeFileGCWorkers is a deprecated compatibility alias for GCWorkers.
	// Values are used only when GCWorkers is not set.
	NodeFileGCWorkers int `json:"node_file_gc_workers"`
	// StorageGCThreshold triggers segmented storage GC when
	// chunk_file_size >= target_chunk_size * threshold.
	// Values <= 0 fall back to the default 2.0.
	StorageGCThreshold float64 `json:"storage_gc_threshold"`
	// NodeFileSortedCompression enables zstd compression for the sorted portion
	// of node files. Disabled by default.
	NodeFileSortedCompression bool `json:"node_file_sorted_compression"`
	// SegmentIndexCompression enables zstd compression for segment index files.
	// Disabled by default.
	SegmentIndexCompression bool `json:"segment_index_compression"`
}

// DefaultConfig returns a configuration with default values.
// dirpath is the base directory for the database.
func DefaultConfig(dirpath string) *Config {
	prefixDBDir := filepath.Join(dirpath, "prefixdb")
	return &Config{
		BaseDir:                          dirpath,
		AccountDir:                       filepath.Join(prefixDBDir, "na"),
		StorageDir:                       filepath.Join(prefixDBDir, "storagefiles"),
		NodeFileGCUnsortedRatioThreshold: 1.0,
		StorageGCThreshold:               2.0,
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
