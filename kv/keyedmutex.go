package kv

import (
	"hash/fnv"
	"sync"
)

// keyedStripes bounds memory use to a fixed set of locks rather than one
// per key ever seen — two unrelated keys hashing to the same stripe will
// serialize against each other unnecessarily, but that's a throughput
// cost, not a correctness one, and 256 stripes makes collisions rare in
// practice.
const keyedStripes = 256

// keyedMutex serializes operations on the same key within one instance.
// Put and Delete on the same key each read-then-write both S3 and the
// local cache; without this, a Put and a Delete racing on one key can
// interleave so that S3's delete lands before Put's own S3 write, but
// Put's cache write lands after Delete's cache eviction — leaving the
// cache holding a value S3 no longer has, until the next Head check (or
// indefinitely within the CACHE_TTL window).
type keyedMutex struct {
	stripes [keyedStripes]sync.Mutex
}

// Lock acquires the stripe for key and returns a function to release it.
func (m *keyedMutex) Lock(key string) func() {
	stripe := &m.stripes[hashKey(key)%keyedStripes]
	stripe.Lock()
	return stripe.Unlock
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}
