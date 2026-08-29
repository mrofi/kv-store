package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	c := NewCache(0)
	entry := Entry{Value: []byte("bar"), VersionID: "v1", UpdatedAt: time.Now()}
	c.Set("foo", entry)

	got, ok := c.Get("foo")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(got.Value) != "bar" || got.VersionID != "v1" {
		t.Fatalf("got %+v, want value=bar version=v1", got)
	}
}

func TestCacheMiss(t *testing.T) {
	c := NewCache(0)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected cache miss")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache(0)
	c.Set("foo", Entry{Value: []byte("bar")})
	c.Delete("foo")
	if _, ok := c.Get("foo"); ok {
		t.Fatal("expected cache miss after delete")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCache(0)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			c.Set("foo", Entry{Value: []byte("bar")})
		}()
		go func() {
			defer wg.Done()
			c.Get("foo")
		}()
	}
	wg.Wait()
}

func TestCacheUnboundedWhenMaxEntriesZero(t *testing.T) {
	c := NewCache(0)
	for i := 0; i < 100; i++ {
		c.Set(fmt.Sprintf("key-%d", i), Entry{Value: []byte("v")})
	}
	// The very first key inserted should still be present, since
	// maxEntries=0 means no eviction ever happens.
	if _, ok := c.Get("key-0"); !ok {
		t.Fatal("expected the first-ever inserted key to still be present when maxEntries=0")
	}
}

func TestCacheEvictsLeastRecentlyUsedWhenOverCapacity(t *testing.T) {
	c := NewCache(2)
	c.Set("a", Entry{Value: []byte("a")})
	c.Set("b", Entry{Value: []byte("b")})
	c.Set("c", Entry{Value: []byte("c")}) // over capacity: evicts "a" (least recently used)

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected \"a\" to have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("expected \"b\" to still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("expected \"c\" to still be present")
	}
}

func TestCacheGetRefreshesRecency(t *testing.T) {
	c := NewCache(2)
	c.Set("a", Entry{Value: []byte("a")})
	c.Set("b", Entry{Value: []byte("b")})
	c.Get("a")                              // "a" is now the most recently used
	c.Set("c", Entry{Value: []byte("c")}) // over capacity: evicts "b", not "a"

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected \"b\" to have been evicted since \"a\" was accessed more recently")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected \"a\" to still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("expected \"c\" to still be present")
	}
}

func TestCacheSetOnExistingKeyRefreshesRecency(t *testing.T) {
	c := NewCache(2)
	c.Set("a", Entry{Value: []byte("a1")})
	c.Set("b", Entry{Value: []byte("b")})
	c.Set("a", Entry{Value: []byte("a2")}) // updating "a" refreshes its recency
	c.Set("c", Entry{Value: []byte("c")}) // over capacity: evicts "b", not "a"

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected \"b\" to have been evicted")
	}
	got, ok := c.Get("a")
	if !ok {
		t.Fatal("expected \"a\" to still be present")
	}
	if string(got.Value) != "a2" {
		t.Fatalf("got value %q, want updated value a2", got.Value)
	}
}

func TestCacheDeleteThenRefillDoesNotResurrectDeletedKey(t *testing.T) {
	c := NewCache(2)
	c.Set("a", Entry{Value: []byte("a")})
	c.Delete("a")
	c.Set("b", Entry{Value: []byte("b")})
	c.Set("c", Entry{Value: []byte("c")})

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected \"a\" to remain deleted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("expected \"b\" to still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("expected \"c\" to still be present")
	}
}

func TestCacheSetIfNewerAcceptsWhenNewerReturnsTrue(t *testing.T) {
	c := NewCache(0)
	c.Set("foo", Entry{Value: []byte("old")})

	c.SetIfNewer("foo", Entry{Value: []byte("new")}, func(existing Entry, exists bool) bool {
		return true
	})

	got, ok := c.Get("foo")
	if !ok || string(got.Value) != "new" {
		t.Fatalf("got %+v, ok=%v, want value=new", got, ok)
	}
}

func TestCacheSetIfNewerRejectsWhenNewerReturnsFalse(t *testing.T) {
	c := NewCache(0)
	c.Set("foo", Entry{Value: []byte("old")})

	c.SetIfNewer("foo", Entry{Value: []byte("new")}, func(existing Entry, exists bool) bool {
		return false
	})

	got, ok := c.Get("foo")
	if !ok || string(got.Value) != "old" {
		t.Fatalf("got %+v, ok=%v, want value=old (unchanged)", got, ok)
	}
}

func TestCacheSetIfNewerPassesExistsFalseOnMiss(t *testing.T) {
	c := NewCache(0)

	var sawExists bool
	c.SetIfNewer("foo", Entry{Value: []byte("bar")}, func(existing Entry, exists bool) bool {
		sawExists = exists
		return !exists
	})

	if sawExists {
		t.Fatal("expected exists=false when nothing was cached yet")
	}
	got, ok := c.Get("foo")
	if !ok || string(got.Value) != "bar" {
		t.Fatalf("got %+v, ok=%v, want value=bar", got, ok)
	}
}

func TestCacheSetIfNewerRespectsEvictionOnNewKey(t *testing.T) {
	c := NewCache(2)
	c.Set("a", Entry{Value: []byte("a")})
	c.Set("b", Entry{Value: []byte("b")})

	c.SetIfNewer("c", Entry{Value: []byte("c")}, func(existing Entry, exists bool) bool {
		return true
	})

	if _, ok := c.Get("a"); ok {
		t.Fatal("expected \"a\" to have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("expected \"b\" to still be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("expected \"c\" to still be present")
	}
}

func TestCacheSetIfNewerConcurrentCallsConvergeDeterministically(t *testing.T) {
	c := NewCache(0)
	base := time.Now()
	const n = 100

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			candidate := Entry{
				Value:     []byte(fmt.Sprintf("v%d", i)),
				UpdatedAt: base.Add(time.Duration(i) * time.Millisecond),
			}
			c.SetIfNewer("foo", candidate, func(existing Entry, exists bool) bool {
				return !exists || candidate.UpdatedAt.After(existing.UpdatedAt)
			})
		}(i)
	}
	wg.Wait()

	got, ok := c.Get("foo")
	if !ok {
		t.Fatal("expected a cached value")
	}
	want := fmt.Sprintf("v%d", n-1)
	if string(got.Value) != want {
		t.Fatalf("cached value = %q, want %q (the entry with the latest UpdatedAt must always win, regardless of goroutine scheduling)", got.Value, want)
	}
}
