package store

import (
	"sync"
	"testing"
	"time"
)

func TestCacheSetGet(t *testing.T) {
	c := NewCache()
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
	c := NewCache()
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected cache miss")
	}
}

func TestCacheDelete(t *testing.T) {
	c := NewCache()
	c.Set("foo", Entry{Value: []byte("bar")})
	c.Delete("foo")
	if _, ok := c.Get("foo"); ok {
		t.Fatal("expected cache miss after delete")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := NewCache()
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
