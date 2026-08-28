package store

import (
	"sync"
	"time"
)

type Entry struct {
	Value     []byte
	VersionID string
	ETag      string
	UpdatedAt time.Time

	// CachedAt is when this instance last confirmed the entry fresh
	// (via a write, a peer push, or an S3 Head match) — distinct from
	// UpdatedAt, which is S3's own object modification time. Only
	// kv.Service reads or writes this field; S3Store implementations
	// leave it zero.
	CachedAt time.Time
}

type Cache struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func NewCache() *Cache {
	return &Cache{data: make(map[string]Entry)}
}

func (c *Cache) Get(key string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.data[key]
	return e, ok
}

func (c *Cache) Set(key string, entry Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = entry
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.data, key)
}
