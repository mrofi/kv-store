package store

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkCacheGetUnbounded measures pure concurrent-read throughput on a
// single hot key — the case most exposed to the RWMutex→Mutex trade-off
// LRU eviction required (Get now takes an exclusive lock to update
// recency order, where the old implementation only needed a read lock).
func BenchmarkCacheGetUnbounded(b *testing.B) {
	c := NewCache(0)
	c.Set("hotkey", Entry{Value: []byte("bar"), UpdatedAt: time.Now()})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("hotkey")
		}
	})
}

// BenchmarkCacheSetUnbounded measures concurrent-write throughput with
// unique keys and no eviction pressure (maxEntries=0).
func BenchmarkCacheSetUnbounded(b *testing.B) {
	c := NewCache(0)
	entry := Entry{Value: []byte("bar"), UpdatedAt: time.Now()}
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			c.Set(fmt.Sprintf("key-%d", n), entry)
		}
	})
}

// BenchmarkCacheMixedReadWrite measures a realistic combined workload: a
// 1000-key working set with roughly 10% writes, 90% reads.
func BenchmarkCacheMixedReadWrite(b *testing.B) {
	c := NewCache(0)
	entry := Entry{Value: []byte("bar"), UpdatedAt: time.Now()}
	for i := 0; i < 1000; i++ {
		c.Set(fmt.Sprintf("key-%d", i), entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			i++
			if i%10 == 0 {
				c.Set(fmt.Sprintf("key-%d", i%1000), entry)
			} else {
				c.Get(fmt.Sprintf("key-%d", i%1000))
			}
		}
	})
}

// BenchmarkCacheSetWithEviction measures write throughput once the cache
// is at capacity and every Set forces an LRU eviction.
func BenchmarkCacheSetWithEviction(b *testing.B) {
	c := NewCache(1000)
	entry := Entry{Value: []byte("bar"), UpdatedAt: time.Now()}
	var counter int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&counter, 1)
			c.Set(fmt.Sprintf("key-%d", n), entry)
		}
	})
}
