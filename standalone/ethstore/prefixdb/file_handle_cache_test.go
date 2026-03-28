package prefixdb

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

type evictionLatencyStats struct {
	totalElapsed time.Duration
	p50          time.Duration
	p95          time.Duration
	p99          time.Duration
	max          time.Duration
	ops          int
}

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func runEvictionLatencyStress(syncClose bool, workers int, opsPerWorker int, capacity int, closeDelay time.Duration) (evictionLatencyStats, error) {
	stats := evictionLatencyStats{}
	if workers <= 0 || opsPerWorker <= 0 {
		return stats, errors.New("invalid stress parameters")
	}

	closeWorkers := workers / 2
	if closeWorkers < 1 {
		closeWorkers = 1
	}

	var closeWG sync.WaitGroup
	closeCh := make(chan struct{}, 1024)
	var closerWG sync.WaitGroup
	if !syncClose {
		for i := 0; i < closeWorkers; i++ {
			closerWG.Add(1)
			go func() {
				defer closerWG.Done()
				for range closeCh {
					time.Sleep(closeDelay)
					closeWG.Done()
				}
			}()
		}
	}

	cache, err := lru.NewWithEvict(capacity, func(key interface{}, value interface{}) {
		if syncClose {
			time.Sleep(closeDelay)
			return
		}
		closeWG.Add(1)
		select {
		case closeCh <- struct{}{}:
		default:
			go func() {
				closeCh <- struct{}{}
			}()
		}
	})
	if err != nil {
		if !syncClose {
			close(closeCh)
			closerWG.Wait()
		}
		return stats, err
	}

	latencyBatches := make(chan []time.Duration, workers)
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			latencies := make([]time.Duration, 0, opsPerWorker)
			base := workerID * opsPerWorker
			for i := 0; i < opsPerWorker; i++ {
				begin := time.Now()
				cache.Add(base+i, base+i)
				latencies = append(latencies, time.Since(begin))
			}
			latencyBatches <- latencies
		}()
	}

	wg.Wait()
	close(latencyBatches)

	allLatencies := make([]time.Duration, 0, workers*opsPerWorker)
	for batch := range latencyBatches {
		allLatencies = append(allLatencies, batch...)
	}

	if !syncClose {
		closeWG.Wait()
		close(closeCh)
		closerWG.Wait()
	}

	stats.totalElapsed = time.Since(start)
	stats.ops = len(allLatencies)
	if len(allLatencies) == 0 {
		return stats, nil
	}
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	stats.p50 = percentileDuration(allLatencies, 0.50)
	stats.p95 = percentileDuration(allLatencies, 0.95)
	stats.p99 = percentileDuration(allLatencies, 0.99)
	stats.max = allLatencies[len(allLatencies)-1]
	return stats, nil
}

func TestFileHandleCacheEvictionClosesInBackground(t *testing.T) {
	cache, err := newFileHandleCache(128)
	if err != nil {
		t.Fatalf("newFileHandleCache failed: %v", err)
	}

	dir := t.TempDir()
	files := make([]string, 129)
	for i := range files {
		p := filepath.Join(dir, "f_"+string(rune('a'+(i%26)))+"_"+string(rune('0'+(i%10))))
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatalf("write temp file failed: %v", err)
		}
		files[i] = p
	}

	first, err := cache.Open(files[0], os.O_RDONLY)
	if err != nil {
		t.Fatalf("open first failed: %v", err)
	}

	for i := 1; i < len(files); i++ {
		f, err := cache.Open(files[i], os.O_RDONLY)
		if err != nil {
			t.Fatalf("open %d failed: %v", i, err)
		}
		_ = f
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, statErr := first.Stat()
		if statErr != nil {
			if errors.Is(statErr, os.ErrClosed) {
				break
			}
			t.Fatalf("unexpected stat error: %v", statErr)
		}
		if time.Now().After(deadline) {
			t.Fatal("evicted file handle was not closed by background worker in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cache.Purge()
}

func TestFileHandleCachePurgeWaitsForClose(t *testing.T) {
	cache, err := newFileHandleCache(128)
	if err != nil {
		t.Fatalf("newFileHandleCache failed: %v", err)
	}

	path := filepath.Join(t.TempDir(), "purge.node")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write temp file failed: %v", err)
	}

	f, err := cache.Open(path, os.O_RDONLY)
	if err != nil {
		t.Fatalf("cache open failed: %v", err)
	}

	cache.Purge()

	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("expected file to be closed after Purge, got err=%v", err)
	}
}

func TestFileHandleCacheEvictionLatencyPercentilesStress(t *testing.T) {
	workers := 8
	opsPerWorker := 600
	capacity := 128
	closeDelay := 250 * time.Microsecond
	if testing.Short() {
		workers = 4
		opsPerWorker = 200
		closeDelay = 150 * time.Microsecond
	}

	syncStats, err := runEvictionLatencyStress(true, workers, opsPerWorker, capacity, closeDelay)
	if err != nil {
		t.Fatalf("sync-close stress failed: %v", err)
	}
	asyncStats, err := runEvictionLatencyStress(false, workers, opsPerWorker, capacity, closeDelay)
	if err != nil {
		t.Fatalf("async-close stress failed: %v", err)
	}

	t.Logf("eviction stress config: workers=%d ops/worker=%d capacity=%d closeDelay=%s totalOps=%d",
		workers, opsPerWorker, capacity, closeDelay, syncStats.ops)
	t.Logf("before(sync close)  p50=%s p95=%s p99=%s max=%s total=%s",
		syncStats.p50, syncStats.p95, syncStats.p99, syncStats.max, syncStats.totalElapsed)
	t.Logf("after(async close)  p50=%s p95=%s p99=%s max=%s total=%s",
		asyncStats.p50, asyncStats.p95, asyncStats.p99, asyncStats.max, asyncStats.totalElapsed)

	if syncStats.p95 > 0 {
		improve := (float64(syncStats.p95-asyncStats.p95) / float64(syncStats.p95)) * 100
		t.Logf("online path p95 improvement: %.2f%%", improve)
	}
	if asyncStats.p95 >= syncStats.p95 {
		t.Fatalf("expected async-close p95 < sync-close p95, got async=%s sync=%s", asyncStats.p95, syncStats.p95)
	}
}
