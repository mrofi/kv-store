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
