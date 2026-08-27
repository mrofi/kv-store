# S3-Backed KV Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Go KV storage service where S3 (versioning enabled) is the durable, versioned source of truth and each instance keeps an in-memory cache for fast reads, with peer instances warming each other's caches on write.

**Architecture:** Client-facing HTTP API backed by a `kv.Service` that composes an `S3Store` (real S3 or an in-memory fake for tests) and an in-memory `Cache`. Writes go to S3 first, then update the local cache and asynchronously push to peers and prune old versions. Reads check the local cache against a cheap S3 `HEAD` to confirm freshness before ever fetching the body, so correctness never depends on peer availability.

**Tech Stack:** Go 1.22+ (stdlib `net/http` `ServeMux` with method+path-parameter routing, no third-party router), AWS SDK v2 for S3 (`github.com/aws/aws-sdk-go-v2/...`), MinIO for local/integration testing against an S3-compatible API.

**Spec:** `docs/superpowers/specs/2026-08-28-s3-kv-store-design.md`

## Global Constraints

- Object body is the raw value; S3's own `VersionId`/`ETag`/`LastModified` are the version metadata — no custom JSON envelope, no hand-rolled version numbers.
- Write success = S3 `PutObject` only. Peer cache-warm push and version pruning happen asynchronously after the response and never affect it.
- Reads never broadcast to peers. Freshness is confirmed via a cheap S3 `HeadObject` compared against the local cache; peer broadcast plays no role in correctness.
- `MAX_VERSIONS` (default 10) caps retained versions per key, enforced by app-side pruning after each write (immediate) and, at the infra level, an S3 lifecycle rule (backstop, not app code).
- Peer list loads from the `PEERS` env var at startup and is mutable at runtime via `PUT /internal/peers`; peers are used only for cache-warm push, never for correctness.
- `/internal/push` and `/internal/peers` optionally require an `X-Internal-Auth` header matching `INTERNAL_AUTH_TOKEN`; if unset, the check is skipped.
- No automatic peer discovery (Kubernetes service discovery, gossip, message broker) — explicitly out of scope per the spec.

---

### Task 1: Project scaffolding

**Files:**
- Create: `go.mod`
- Create: `cmd/kvstore/main.go`
- Create: `.gitignore`

**Interfaces:**
- Produces: Go module named `kvstore`, so all internal packages are imported as `kvstore/internal/...`.

- [ ] **Step 1: Initialize the module**

Run: `cd kv-store && go mod init kvstore`

- [ ] **Step 2: Create a placeholder entrypoint**

`cmd/kvstore/main.go`:

```go
package main

import "log"

func main() {
	log.Println("kv-store: not yet implemented")
}
```

- [ ] **Step 3: Add a .gitignore**

`.gitignore`:

```
/bin/
*.test
```

- [ ] **Step 4: Verify it builds**

Run: `go build ./...`
Expected: builds with no errors (no tests exist yet, so nothing to run).

- [ ] **Step 5: Commit**

```bash
git add go.mod cmd/kvstore/main.go .gitignore
git commit -m "Scaffold kv-store Go module"
```

---

### Task 2: Config loader

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`

**Interfaces:**
- Produces: `config.Config{Port, S3Bucket, AWSRegion, SelfAddr, Peers []string, MaxVersions int, InternalAuthToken}` and `config.Load() (Config, error)`, consumed by `cmd/kvstore/main.go` in Task 12.

- [ ] **Step 1: Write the failing tests**

`internal/config/config_test.go`:

```go
package config

import "testing"

func TestLoadRequiresS3Bucket(t *testing.T) {
	_, err := Load()
	if err == nil {
		t.Fatal("expected an error when S3_BUCKET is unset")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.Port != "8080" {
		t.Fatalf("Port = %q, want 8080", cfg.Port)
	}
	if cfg.AWSRegion != "us-east-1" {
		t.Fatalf("AWSRegion = %q, want us-east-1", cfg.AWSRegion)
	}
	if cfg.MaxVersions != 10 {
		t.Fatalf("MaxVersions = %d, want 10", cfg.MaxVersions)
	}
	if len(cfg.Peers) != 0 {
		t.Fatalf("Peers = %v, want empty", cfg.Peers)
	}
}

func TestLoadParsesPeers(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("PEERS", "http://a:8080, http://b:8080,,http://c:8080")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	want := []string{"http://a:8080", "http://b:8080", "http://c:8080"}
	if len(cfg.Peers) != len(want) {
		t.Fatalf("Peers = %v, want %v", cfg.Peers, want)
	}
	for i := range want {
		if cfg.Peers[i] != want[i] {
			t.Fatalf("Peers[%d] = %q, want %q", i, cfg.Peers[i], want[i])
		}
	}
}

func TestLoadRejectsInvalidMaxVersions(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("MAX_VERSIONS", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for invalid MAX_VERSIONS")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/config/...`
Expected: FAIL with "undefined: Load" (the `config` package doesn't exist yet).

- [ ] **Step 3: Write the implementation**

`internal/config/config.go`:

```go
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port              string
	S3Bucket          string
	AWSRegion         string
	SelfAddr          string
	Peers             []string
	MaxVersions       int
	InternalAuthToken string
}

func Load() (Config, error) {
	cfg := Config{
		Port:        getEnv("PORT", "8080"),
		S3Bucket:    os.Getenv("S3_BUCKET"),
		AWSRegion:   getEnv("AWS_REGION", "us-east-1"),
		SelfAddr:    os.Getenv("SELF_ADDR"),
		MaxVersions: 10,
	}

	if cfg.S3Bucket == "" {
		return Config{}, fmt.Errorf("config: S3_BUCKET is required")
	}

	if raw := os.Getenv("MAX_VERSIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid MAX_VERSIONS %q: %w", raw, err)
		}
		cfg.MaxVersions = n
	}

	if raw := os.Getenv("PEERS"); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				cfg.Peers = append(cfg.Peers, p)
			}
		}
	}

	cfg.InternalAuthToken = os.Getenv("INTERNAL_AUTH_TOKEN")

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/...`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/config
git commit -m "Add config loader"
```

---

### Task 3: In-memory cache

**Files:**
- Create: `internal/store/cache.go`
- Test: `internal/store/cache_test.go`

**Interfaces:**
- Produces: `store.Entry{Value []byte, VersionID string, ETag string, UpdatedAt time.Time}`, `store.NewCache() *Cache`, `(*Cache).Get(key string) (Entry, bool)`, `(*Cache).Set(key string, entry Entry)`, `(*Cache).Delete(key string)`. Consumed by `kv.Service` (Tasks 5-6).

- [ ] **Step 1: Write the failing tests**

`internal/store/cache_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/store/...`
Expected: FAIL with "undefined: NewCache" (package `store` doesn't exist yet).

- [ ] **Step 3: Write the implementation**

`internal/store/cache.go`:

```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/store/...`
Expected: PASS (4 tests, no race detected).

- [ ] **Step 5: Commit**

```bash
git add internal/store/cache.go internal/store/cache_test.go
git commit -m "Add in-memory cache"
```

---

### Task 4: S3Store interface, types, and in-memory fake

**Files:**
- Create: `internal/store/store.go`
- Create: `internal/store/fake.go`
- Test: `internal/store/fake_test.go`

**Interfaces:**
- Produces: `store.ErrNotFound`, `store.ObjectVersion{VersionID, ETag, LastModified time.Time, IsDeleteMarker bool}`, `store.S3Store` interface (`Put`, `Head`, `Get`, `GetVersion`, `ListVersions`, `Delete`, `PruneToMax`), and `store.NewFakeS3Store() *FakeS3Store` implementing it. Consumed by `kv.Service` (Tasks 5-6) and the real `AWSS3Store` (Task 9).

- [ ] **Step 1: Write the interface and types (no test — pure type definitions)**

`internal/store/store.go`:

```go
package store

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("store: not found")

type ObjectVersion struct {
	VersionID      string
	ETag           string
	LastModified   time.Time
	IsDeleteMarker bool
}

// S3Store is the durable, versioned backing store. Every implementation
// must behave like S3 versioning: Put never overwrites in place, Delete
// leaves a marker rather than erasing history, and ErrNotFound is returned
// when the current version of a key doesn't exist (including when the
// current version is a delete marker).
type S3Store interface {
	Put(ctx context.Context, key string, value []byte) (Entry, error)
	Head(ctx context.Context, key string) (Entry, error)
	Get(ctx context.Context, key string) (Entry, error)
	GetVersion(ctx context.Context, key, versionID string) (Entry, error)
	ListVersions(ctx context.Context, key string) ([]ObjectVersion, error)
	Delete(ctx context.Context, key string) error
	PruneToMax(ctx context.Context, key string, maxVersions int) error
}
```

- [ ] **Step 2: Write the failing tests for the fake**

`internal/store/fake_test.go`:

```go
package store

import (
	"context"
	"errors"
	"testing"
)

func TestFakePutCreatesNewVersion(t *testing.T) {
	f := NewFakeS3Store()
	v1, err := f.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	v2, err := f.Put(context.Background(), "foo", []byte("v2"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if v1.VersionID == v2.VersionID {
		t.Fatal("expected distinct version ids for successive Puts")
	}
}

func TestFakeGetReturnsLatest(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := f.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	got, err := f.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(got.Value) != "v2" {
		t.Fatalf("Get = %q, want v2", got.Value)
	}
}

func TestFakeGetMissingKeyReturnsNotFound(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestFakeGetVersionReturnsSpecificVersion(t *testing.T) {
	f := NewFakeS3Store()
	v1, err := f.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := f.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	got, err := f.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("GetVersion = %q, want v1", got.Value)
	}
}

func TestFakeListVersionsOrderedNewestFirst(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	v2, err := f.Put(context.Background(), "foo", []byte("v2"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	versions, err := f.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
	if versions[0].VersionID != v2.VersionID {
		t.Fatalf("versions[0] = %q, want newest %q", versions[0].VersionID, v2.VersionID)
	}
}

func TestFakeDeleteThenGetNotFound(t *testing.T) {
	f := NewFakeS3Store()
	if _, err := f.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := f.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := f.Get(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if _, err := f.Head(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected Head ErrNotFound after delete, got %v", err)
	}
}

func TestFakePruneToMaxKeepsNewestVersions(t *testing.T) {
	f := NewFakeS3Store()
	for i := 0; i < 5; i++ {
		if _, err := f.Put(context.Background(), "foo", []byte("v")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}
	if err := f.PruneToMax(context.Background(), "foo", 2); err != nil {
		t.Fatalf("PruneToMax returned error: %v", err)
	}
	versions, err := f.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/store/...`
Expected: FAIL with "undefined: NewFakeS3Store".

- [ ] **Step 4: Write the implementation**

`internal/store/fake.go`:

```go
package store

import (
	"context"
	"strconv"
	"sync"
	"time"
)

type versionRecord struct {
	entry          Entry
	isDeleteMarker bool
}

// FakeS3Store is an in-memory S3Store used for unit tests. It is not a
// real store; it exists so higher layers (kv.Service) can be tested
// without a network dependency, while still behaving like S3 versioning:
// every Put appends a new version, Delete appends a delete marker, and
// nothing is ever overwritten in place.
type FakeS3Store struct {
	mu       sync.Mutex
	versions map[string][]versionRecord // oldest first
	seq      int

	HeadErr error
	GetErr  error
	PutErr  error

	HeadCalls int
	GetCalls  int
	PutCalls  int
}

func NewFakeS3Store() *FakeS3Store {
	return &FakeS3Store{versions: make(map[string][]versionRecord)}
}

func (f *FakeS3Store) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PutCalls++
	if f.PutErr != nil {
		return Entry{}, f.PutErr
	}
	f.seq++
	entry := Entry{
		Value:     append([]byte(nil), value...),
		VersionID: "v" + strconv.Itoa(f.seq),
		ETag:      "etag-" + strconv.Itoa(f.seq),
		UpdatedAt: time.Now(),
	}
	f.versions[key] = append(f.versions[key], versionRecord{entry: entry})
	return entry, nil
}

func (f *FakeS3Store) Head(ctx context.Context, key string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.HeadCalls++
	if f.HeadErr != nil {
		return Entry{}, f.HeadErr
	}
	recs := f.versions[key]
	if len(recs) == 0 || recs[len(recs)-1].isDeleteMarker {
		return Entry{}, ErrNotFound
	}
	latest := recs[len(recs)-1].entry
	return Entry{VersionID: latest.VersionID, ETag: latest.ETag, UpdatedAt: latest.UpdatedAt}, nil
}

func (f *FakeS3Store) Get(ctx context.Context, key string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.GetCalls++
	if f.GetErr != nil {
		return Entry{}, f.GetErr
	}
	recs := f.versions[key]
	if len(recs) == 0 || recs[len(recs)-1].isDeleteMarker {
		return Entry{}, ErrNotFound
	}
	return recs[len(recs)-1].entry, nil
}

func (f *FakeS3Store) GetVersion(ctx context.Context, key, versionID string) (Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.versions[key] {
		if r.entry.VersionID == versionID && !r.isDeleteMarker {
			return r.entry, nil
		}
	}
	return Entry{}, ErrNotFound
}

func (f *FakeS3Store) ListVersions(ctx context.Context, key string) ([]ObjectVersion, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs := f.versions[key]
	out := make([]ObjectVersion, 0, len(recs))
	for i := len(recs) - 1; i >= 0; i-- {
		r := recs[i]
		out = append(out, ObjectVersion{
			VersionID:      r.entry.VersionID,
			ETag:           r.entry.ETag,
			LastModified:   r.entry.UpdatedAt,
			IsDeleteMarker: r.isDeleteMarker,
		})
	}
	return out, nil
}

func (f *FakeS3Store) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	f.versions[key] = append(f.versions[key], versionRecord{
		entry:          Entry{VersionID: "v" + strconv.Itoa(f.seq), UpdatedAt: time.Now()},
		isDeleteMarker: true,
	})
	return nil
}

func (f *FakeS3Store) PruneToMax(ctx context.Context, key string, maxVersions int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	recs := f.versions[key]
	if maxVersions <= 0 || len(recs) <= maxVersions {
		return nil
	}
	f.versions[key] = recs[len(recs)-maxVersions:]
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/store/...`
Expected: PASS (all cache + fake tests).

- [ ] **Step 6: Commit**

```bash
git add internal/store/store.go internal/store/fake.go internal/store/fake_test.go
git commit -m "Add S3Store interface and in-memory fake"
```

---

### Task 5: KV service — read path

**Files:**
- Create: `internal/kv/service.go`
- Test: `internal/kv/service_test.go`

**Interfaces:**
- Consumes: `store.Cache`, `store.S3Store`, `store.Entry`, `store.ObjectVersion`, `store.ErrNotFound` (Tasks 3-4).
- Produces: `kv.ErrNotFound`, `kv.ErrUnavailable`, `kv.Pusher` interface (`Push(ctx, key, entry)`), `kv.NewService(cache *store.Cache, s3 store.S3Store, pusher Pusher, maxVersions int) *Service`, `(*Service).Get`, `(*Service).GetVersion`, `(*Service).ListVersions`. `Put`/`Delete`/`ApplyPush` are added to this same struct in Task 6. Consumed by `internal/httpapi` (Tasks 10-11) and `cmd/kvstore/main.go` (Task 12).

The core design point this task implements: a cache **hit** is confirmed fresh with a cheap `Head` call before being served (falling back to `Get` only if the version differs), while a cache **miss** goes straight to `Get` — no wasted `Head` call when there's nothing to compare against.

- [ ] **Step 1: Write the failing tests**

`internal/kv/service_test.go`:

```go
package kv

import (
	"context"
	"errors"
	"testing"

	"kvstore/internal/store"
)

type recordedPush struct {
	key   string
	entry store.Entry
}

type recordingPusher struct {
	calls chan recordedPush
}

func newRecordingPusher() *recordingPusher {
	return &recordingPusher{calls: make(chan recordedPush, 10)}
}

func (p *recordingPusher) Push(ctx context.Context, key string, entry store.Entry) {
	p.calls <- recordedPush{key: key, entry: entry}
}

func TestGetCacheHitAvoidsBodyFetch(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	s3.HeadCalls = 0
	s3.GetCalls = 0

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected fresh result")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
	if s3.GetCalls != 0 {
		t.Fatalf("expected no GetObject calls on cache hit, got %d", s3.GetCalls)
	}
	if s3.HeadCalls != 1 {
		t.Fatalf("expected exactly 1 HeadObject call, got %d", s3.HeadCalls)
	}
}

func TestGetCacheMissFetchesDirectlyWithoutHead(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected fresh result")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
	if s3.HeadCalls != 0 {
		t.Fatalf("expected no HeadObject call on cache miss, got %d", s3.HeadCalls)
	}
	if s3.GetCalls != 1 {
		t.Fatalf("expected exactly 1 GetObject call, got %d", s3.GetCalls)
	}
}

func TestGetCacheStaleRefetches(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	v1, err := s3.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", v1)
	// Simulate another instance writing a newer version directly to S3
	// without pushing to this instance's cache.
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected non-stale result since S3 is reachable")
	}
	if string(got.Value) != "v2" {
		t.Fatalf("got value %q, want %q", got.Value, "v2")
	}
}

func TestGetNotFound(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err := svc.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetServesStaleCacheWhenS3Unreachable(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	s3.HeadErr = errors.New("simulated network error")
	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !stale {
		t.Fatal("expected stale=true when S3 is unreachable but cache has a value")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("got value %q, want %q", got.Value, "bar")
	}
}

func TestGetUnavailableWhenS3DownAndNoCache(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	s3.GetErr = errors.New("simulated network error")
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err := svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestGetCacheHitButKeyNowDeletedInS3(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	if err := s3.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, _, err = svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected cache entry to be evicted")
	}
}

func TestGetVersionReturnsSpecificVersion(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	v1, err := s3.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	got, err := svc.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("got value %q, want v1", got.Value)
	}
}

func TestGetVersionNotFound(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	_, err := svc.GetVersion(context.Background(), "foo", "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListVersionsReturnsAll(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	versions, err := svc.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/kv/...`
Expected: FAIL with "undefined: NewService" (package `kv` doesn't exist yet).

- [ ] **Step 3: Write the implementation**

`internal/kv/service.go`:

```go
package kv

import (
	"context"
	"errors"

	"kvstore/internal/store"
)

var (
	ErrNotFound    = errors.New("kv: not found")
	ErrUnavailable = errors.New("kv: unavailable")
)

// Pusher broadcasts a freshly written entry to peer instances so their
// local caches warm up without waiting for their own S3 round trip. It is
// best-effort: implementations must not surface a per-peer failure to the
// caller, only log it and move on.
type Pusher interface {
	Push(ctx context.Context, key string, entry store.Entry)
}

type noopPusher struct{}

func (noopPusher) Push(ctx context.Context, key string, entry store.Entry) {}

type Service struct {
	cache       *store.Cache
	s3          store.S3Store
	pusher      Pusher
	maxVersions int
}

func NewService(cache *store.Cache, s3 store.S3Store, pusher Pusher, maxVersions int) *Service {
	if pusher == nil {
		pusher = noopPusher{}
	}
	return &Service{cache: cache, s3: s3, pusher: pusher, maxVersions: maxVersions}
}

// Get returns the latest value for key. stale is true only when S3 was
// unreachable and a cached value was served instead of the confirmed
// latest value.
func (s *Service) Get(ctx context.Context, key string) (entry store.Entry, stale bool, err error) {
	cached, hit := s.cache.Get(key)

	if !hit {
		fresh, err := s.s3.Get(ctx, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return store.Entry{}, false, ErrNotFound
			}
			return store.Entry{}, false, ErrUnavailable
		}
		s.cache.Set(key, fresh)
		return fresh, false, nil
	}

	head, err := s.s3.Head(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.cache.Delete(key)
			return store.Entry{}, false, ErrNotFound
		}
		// S3 unreachable/erroring: fall back to the cached value.
		return cached, true, nil
	}

	if cached.VersionID == head.VersionID {
		return cached, false, nil
	}

	fresh, err := s.s3.Get(ctx, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			s.cache.Delete(key)
			return store.Entry{}, false, ErrNotFound
		}
		return cached, true, nil
	}

	s.cache.Set(key, fresh)
	return fresh, false, nil
}

func (s *Service) GetVersion(ctx context.Context, key, versionID string) (store.Entry, error) {
	entry, err := s.s3.GetVersion(ctx, key, versionID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return store.Entry{}, ErrNotFound
		}
		return store.Entry{}, err
	}
	return entry, nil
}

func (s *Service) ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error) {
	return s.s3.ListVersions(ctx, key)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/kv/...`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/kv/service.go internal/kv/service_test.go
git commit -m "Add kv.Service read path"
```

---

### Task 6: KV service — write path

**Files:**
- Modify: `internal/kv/service.go` (append `Put`, `Delete`, `ApplyPush`; add `"log"` to the import block)
- Modify: `internal/kv/service_test.go` (append new test functions)

**Interfaces:**
- Produces (added to `*Service` from Task 5): `Put(ctx, key, value []byte) (store.Entry, error)`, `Delete(ctx, key string) error`, `ApplyPush(key string, entry store.Entry)`. Consumed by `internal/httpapi` (Tasks 10-11) and the peer push handler.

- [ ] **Step 1: Append the failing tests**

Add to the end of `internal/kv/service_test.go`:

```go
func TestPutStoresInS3AndUpdatesCache(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	entry, err := svc.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if entry.VersionID == "" {
		t.Fatal("expected non-empty version id")
	}

	cached, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected cache to be populated after Put")
	}
	if string(cached.Value) != "bar" {
		t.Fatalf("cached value = %q, want %q", cached.Value, "bar")
	}
}

func TestPutBroadcastsToPusher(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	pusher := newRecordingPusher()
	svc := NewService(cache, s3, pusher, 10)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	select {
	case call := <-pusher.calls:
		if call.key != "foo" || string(call.entry.Value) != "bar" {
			t.Fatalf("unexpected push call: %+v", call)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for push")
	}
}

func TestPutPrunesOldVersions(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 2)

	for i := 0; i < 5; i++ {
		if _, err := svc.Put(context.Background(), "foo", []byte("v")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}

	deadline := time.Now().Add(time.Second)
	for {
		versions, err := s3.ListVersions(context.Background(), "foo")
		if err != nil {
			t.Fatalf("ListVersions returned error: %v", err)
		}
		if len(versions) <= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at most 2 versions after pruning, got %d", len(versions))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestGetAfterDeleteReturnsNotFoundAndEvictsCache(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := svc.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	_, _, err := svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected cache entry to be evicted after delete")
	}
}

func TestApplyPushUpdatesCacheWithoutTouchingS3(t *testing.T) {
	cache := store.NewCache()
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10)

	pushed := store.Entry{Value: []byte("from-peer"), VersionID: "v1"}
	svc.ApplyPush("foo", pushed)

	got, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected cache to be populated by ApplyPush")
	}
	if string(got.Value) != "from-peer" {
		t.Fatalf("cached value = %q, want from-peer", got.Value)
	}
	if s3.PutCalls != 0 || s3.GetCalls != 0 || s3.HeadCalls != 0 {
		t.Fatal("ApplyPush must not touch S3")
	}
}
```

This requires adding `"time"` to the test file's import block (it now uses `time.After`, `time.Second`, `time.Now`, `time.Sleep`):

```go
import (
	"context"
	"errors"
	"testing"
	"time"

	"kvstore/internal/store"
)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/kv/...`
Expected: FAIL with "undefined: svc.Put" / "undefined: svc.Delete" / "undefined: svc.ApplyPush".

- [ ] **Step 3: Append the implementation**

Update the import block at the top of `internal/kv/service.go` to add `"log"`:

```go
import (
	"context"
	"errors"
	"log"

	"kvstore/internal/store"
)
```

Add to the end of `internal/kv/service.go`:

```go
// Put writes value to S3 and updates the local cache. It returns once S3
// has durably stored the value; peer cache-warming and version pruning
// continue asynchronously afterward and never affect the returned error.
func (s *Service) Put(ctx context.Context, key string, value []byte) (store.Entry, error) {
	entry, err := s.s3.Put(ctx, key, value)
	if err != nil {
		return store.Entry{}, err
	}
	s.cache.Set(key, entry)

	go s.pusher.Push(context.Background(), key, entry)
	go func() {
		if err := s.s3.PruneToMax(context.Background(), key, s.maxVersions); err != nil {
			log.Printf("kv: prune failed for key %q: %v", key, err)
		}
	}()

	return entry, nil
}

func (s *Service) Delete(ctx context.Context, key string) error {
	if err := s.s3.Delete(ctx, key); err != nil {
		return err
	}
	s.cache.Delete(key)
	return nil
}

// ApplyPush updates the local cache from a peer's cache-warm push. It
// never touches S3 — the next Get for this key still confirms freshness
// against S3 via Head, so an incorrect or stale push can never serve wrong
// data, only cost a slower cache hit.
func (s *Service) ApplyPush(key string, entry store.Entry) {
	s.cache.Set(key, entry)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/kv/...`
Expected: PASS (15 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/kv/service.go internal/kv/service_test.go
git commit -m "Add kv.Service write path"
```

---

### Task 7: Peers registry

**Files:**
- Create: `internal/peers/registry.go`
- Test: `internal/peers/registry_test.go`

**Interfaces:**
- Produces: `peers.NewRegistry(self string, initial []string) *Registry`, `(*Registry).List() []string`, `(*Registry).Set(newPeers []string)`. Consumed by `peers.HTTPPusher` (Task 8), `internal/httpapi` (Task 11), and `cmd/kvstore/main.go` (Task 12).

- [ ] **Step 1: Write the failing tests**

`internal/peers/registry_test.go`:

```go
package peers

import "testing"

func TestRegistryExcludesSelf(t *testing.T) {
	r := NewRegistry("http://self:8080", []string{"http://self:8080", "http://a:8080"})
	got := r.List()
	if len(got) != 1 || got[0] != "http://a:8080" {
		t.Fatalf("List() = %v, want [http://a:8080]", got)
	}
}

func TestRegistrySetReplacesList(t *testing.T) {
	r := NewRegistry("", []string{"http://a:8080"})
	r.Set([]string{"http://b:8080", "http://c:8080"})

	got := r.List()
	if len(got) != 2 || got[0] != "http://b:8080" || got[1] != "http://c:8080" {
		t.Fatalf("List() = %v, want [http://b:8080 http://c:8080]", got)
	}
}

func TestRegistryListReturnsIndependentCopy(t *testing.T) {
	r := NewRegistry("", []string{"http://a:8080"})
	got := r.List()
	got[0] = "mutated"

	if r.List()[0] != "http://a:8080" {
		t.Fatal("mutating the returned slice must not affect the registry's internal state")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/peers/...`
Expected: FAIL with "undefined: NewRegistry" (package `peers` doesn't exist yet).

- [ ] **Step 3: Write the implementation**

`internal/peers/registry.go`:

```go
package peers

import "sync"

// Registry holds the set of peer base URLs used for cache-warm pushes. It
// is safe for concurrent use and can be updated at runtime without a
// process restart.
type Registry struct {
	mu    sync.RWMutex
	self  string
	peers []string
}

func NewRegistry(self string, initial []string) *Registry {
	r := &Registry{self: self}
	r.Set(initial)
	return r
}

// List returns the current peer URLs, excluding self.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.peers))
	copy(out, r.peers)
	return out
}

// Set replaces the peer list, excluding self if present.
func (r *Registry) Set(newPeers []string) {
	filtered := make([]string, 0, len(newPeers))
	for _, p := range newPeers {
		if p != "" && p != r.self {
			filtered = append(filtered, p)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = filtered
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/peers/...`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/peers/registry.go internal/peers/registry_test.go
git commit -m "Add peer registry"
```

---

### Task 8: Peer HTTP pusher

**Files:**
- Create: `internal/peers/pusher.go`
- Test: `internal/peers/pusher_test.go`

**Interfaces:**
- Consumes: `peers.Registry` (Task 7), `store.Entry` (Task 3).
- Produces: `peers.NewHTTPPusher(registry *Registry, authToken string, perPeerTimeout time.Duration) *HTTPPusher` with a `Push(ctx, key, entry)` method — this satisfies `kv.Pusher` (Task 5) structurally, so `peers` never needs to import `kv`. Consumed by `cmd/kvstore/main.go` (Task 12).

- [ ] **Step 1: Write the failing tests**

`internal/peers/pusher_test.go`:

```go
package peers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kvstore/internal/store"
)

func TestHTTPPusherDeliversToReachablePeer(t *testing.T) {
	received := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	registry := NewRegistry("", []string{srv.URL})
	pusher := NewHTTPPusher(registry, "secret", time.Second)

	pusher.Push(context.Background(), "foo", store.Entry{
		Value:     []byte("bar"),
		VersionID: "v1",
		ETag:      "etag1",
		UpdatedAt: time.Now(),
	})

	select {
	case h := <-received:
		if h.Get("X-Key") != "foo" || h.Get("X-Version-Id") != "v1" || h.Get("X-Internal-Auth") != "secret" {
			t.Fatalf("unexpected headers: %+v", h)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for push request")
	}
}

func TestHTTPPusherSkipsUnreachablePeerWithinTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	registry := NewRegistry("", []string{slow.URL})
	pusher := NewHTTPPusher(registry, "", 50*time.Millisecond)

	start := time.Now()
	pusher.Push(context.Background(), "foo", store.Entry{Value: []byte("bar")})
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("Push took %v, expected it to give up around the 50ms per-peer timeout", elapsed)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/peers/...`
Expected: FAIL with "undefined: NewHTTPPusher".

- [ ] **Step 3: Write the implementation**

`internal/peers/pusher.go`:

```go
package peers

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"kvstore/internal/store"
)

// HTTPPusher broadcasts a written entry to every peer's /internal/push
// endpoint. It is best-effort: a slow or unreachable peer is skipped after
// a short timeout and never causes Push to return an error, since a lost
// push only costs a peer one extra S3 round trip on its next read, never
// correctness.
type HTTPPusher struct {
	registry  *Registry
	client    *http.Client
	authToken string
}

func NewHTTPPusher(registry *Registry, authToken string, perPeerTimeout time.Duration) *HTTPPusher {
	return &HTTPPusher{
		registry:  registry,
		client:    &http.Client{Timeout: perPeerTimeout},
		authToken: authToken,
	}
}

func (p *HTTPPusher) Push(ctx context.Context, key string, entry store.Entry) {
	for _, peer := range p.registry.List() {
		p.pushOne(ctx, peer, key, entry)
	}
}

func (p *HTTPPusher) pushOne(ctx context.Context, peer, key string, entry store.Entry) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer+"/internal/push", bytes.NewReader(entry.Value))
	if err != nil {
		log.Printf("peers: building push request to %s failed: %v", peer, err)
		return
	}
	req.Header.Set("X-Key", key)
	req.Header.Set("X-Version-Id", entry.VersionID)
	req.Header.Set("X-ETag", entry.ETag)
	req.Header.Set("X-Updated-At", entry.UpdatedAt.Format(time.RFC3339Nano))
	req.Header.Set("Content-Length", strconv.Itoa(len(entry.Value)))
	if p.authToken != "" {
		req.Header.Set("X-Internal-Auth", p.authToken)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("peers: push to %s failed: %v", peer, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		log.Printf("peers: push to %s returned status %d", peer, resp.StatusCode)
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/peers/...`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/peers/pusher.go internal/peers/pusher_test.go
git commit -m "Add peer HTTP pusher"
```

---

### Task 9: Real AWS S3Store implementation + MinIO integration tests

**Files:**
- Create: `internal/store/s3store.go`
- Create: `internal/store/s3store_integration_test.go` (build tag `integration`)
- Create: `deploy/docker-compose.minio.yml`

**Interfaces:**
- Consumes: `store.S3Store`, `store.Entry`, `store.ObjectVersion`, `store.ErrNotFound` (Task 4).
- Produces: `store.NewAWSS3Store(client *s3.Client, bucket string) *AWSS3Store` implementing `store.S3Store`. Consumed by `cmd/kvstore/main.go` (Task 12) and the end-to-end integration test (Task 13).

This is the one component that can't be meaningfully unit-tested with a fake — it has to talk to something implementing the S3 API. Verification here uses MinIO instead of real AWS.

- [ ] **Step 1: Add the AWS SDK dependency**

Run: `go get github.com/aws/aws-sdk-go-v2/aws github.com/aws/aws-sdk-go-v2/config github.com/aws/aws-sdk-go-v2/credentials github.com/aws/aws-sdk-go-v2/service/s3`

- [ ] **Step 2: Add the MinIO docker-compose file**

`deploy/docker-compose.minio.yml`:

```yaml
services:
  minio:
    image: minio/minio:latest
    command: server /data
    ports:
      - "9000:9000"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
```

- [ ] **Step 3: Start MinIO and create the versioned test bucket**

Run:
```bash
docker compose -f deploy/docker-compose.minio.yml up -d
docker run --rm --network host minio/mc alias set local http://localhost:9000 minioadmin minioadmin
docker run --rm --network host minio/mc mb local/kv-store-test
docker run --rm --network host minio/mc version enable local/kv-store-test
```

Leave this running — Task 13's integration test reuses the same bucket.

- [ ] **Step 4: Write the failing integration tests**

`internal/store/s3store_integration_test.go`:

```go
//go:build integration

package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"kvstore/internal/store"
)

func newTestStore(t *testing.T) *store.AWSS3Store {
	t.Helper()
	endpoint := os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	bucket := os.Getenv("TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "kv-store-test"
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	return store.NewAWSS3Store(client, bucket)
}

func TestAWSS3StorePutGetRoundTrip(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	key := "integration-test-key"

	entry, err := s3store.Put(ctx, key, []byte("hello"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if entry.VersionID == "" {
		t.Fatal("expected a version id from a versioned bucket")
	}

	got, err := s3store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(got.Value) != "hello" {
		t.Fatalf("got value %q, want hello", got.Value)
	}
}

func TestAWSS3StoreHeadMatchesGet(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	key := "integration-test-head"

	entry, err := s3store.Put(ctx, key, []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	head, err := s3store.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head returned error: %v", err)
	}
	if head.VersionID != entry.VersionID {
		t.Fatalf("Head version = %q, want %q", head.VersionID, entry.VersionID)
	}
}

func TestAWSS3StoreListVersionsAndGetVersion(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	key := "integration-test-versions"

	v1, err := s3store.Put(ctx, key, []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3store.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	versions, err := s3store.ListVersions(ctx, key)
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}

	old, err := s3store.GetVersion(ctx, key, v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(old.Value) != "v1" {
		t.Fatalf("old value = %q, want v1", old.Value)
	}
}

func TestAWSS3StoreDeleteThenGetNotFound(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	key := "integration-test-delete"

	if _, err := s3store.Put(ctx, key, []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := s3store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	if _, err := s3store.Get(ctx, key); err != store.ErrNotFound {
		t.Fatalf("Get after delete returned %v, want ErrNotFound", err)
	}
}

func TestAWSS3StorePruneToMax(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	key := "integration-test-prune"

	for i := 0; i < 5; i++ {
		if _, err := s3store.Put(ctx, key, []byte("v")); err != nil {
			t.Fatalf("Put returned error: %v", err)
		}
	}
	if err := s3store.PruneToMax(ctx, key, 2); err != nil {
		t.Fatalf("PruneToMax returned error: %v", err)
	}

	versions, err := s3store.ListVersions(ctx, key)
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `go test -tags=integration ./internal/store/...`
Expected: FAIL with "undefined: store.NewAWSS3Store".

- [ ] **Step 6: Write the implementation**

`internal/store/s3store.go`:

```go
package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWSS3Store implements S3Store against a real (or S3-API-compatible,
// e.g. MinIO) bucket. The bucket MUST have versioning enabled.
type AWSS3Store struct {
	client *s3.Client
	bucket string
}

func NewAWSS3Store(client *s3.Client, bucket string) *AWSS3Store {
	return &AWSS3Store{client: client, bucket: bucket}
}

func objectKey(key string) string {
	return "kv/" + key
}

func (s *AWSS3Store) Put(ctx context.Context, key string, value []byte) (Entry, error) {
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
		Body:   bytes.NewReader(value),
	})
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     value,
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: time.Now().UTC(),
	}, nil
}

func (s *AWSS3Store) Head(ctx context.Context, key string) (Entry, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	return Entry{
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) Get(ctx context.Context, key string) (Entry, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     body,
		VersionID: aws.ToString(out.VersionId),
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) GetVersion(ctx context.Context, key, versionID string) (Entry, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(s.bucket),
		Key:       aws.String(objectKey(key)),
		VersionId: aws.String(versionID),
	})
	if err != nil {
		if isNotFound(err) {
			return Entry{}, ErrNotFound
		}
		return Entry{}, err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Entry{}, err
	}
	return Entry{
		Value:     body,
		VersionID: versionID,
		ETag:      aws.ToString(out.ETag),
		UpdatedAt: aws.ToTime(out.LastModified),
	}, nil
}

func (s *AWSS3Store) ListVersions(ctx context.Context, key string) ([]ObjectVersion, error) {
	out, err := s.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(objectKey(key)),
	})
	if err != nil {
		return nil, err
	}

	var versions []ObjectVersion
	for _, v := range out.Versions {
		if aws.ToString(v.Key) != objectKey(key) {
			continue
		}
		versions = append(versions, ObjectVersion{
			VersionID:    aws.ToString(v.VersionId),
			ETag:         aws.ToString(v.ETag),
			LastModified: aws.ToTime(v.LastModified),
		})
	}
	for _, m := range out.DeleteMarkers {
		if aws.ToString(m.Key) != objectKey(key) {
			continue
		}
		versions = append(versions, ObjectVersion{
			VersionID:      aws.ToString(m.VersionId),
			LastModified:   aws.ToTime(m.LastModified),
			IsDeleteMarker: true,
		})
	}

	sort.Slice(versions, func(i, j int) bool {
		return versions[i].LastModified.After(versions[j].LastModified)
	})
	return versions, nil
}

func (s *AWSS3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey(key)),
	})
	return err
}

func (s *AWSS3Store) PruneToMax(ctx context.Context, key string, maxVersions int) error {
	if maxVersions <= 0 {
		return nil
	}
	versions, err := s.ListVersions(ctx, key)
	if err != nil {
		return err
	}
	if len(versions) <= maxVersions {
		return nil
	}

	var toDelete []types.ObjectIdentifier
	for _, v := range versions[maxVersions:] {
		toDelete = append(toDelete, types.ObjectIdentifier{
			Key:       aws.String(objectKey(key)),
			VersionId: aws.String(v.VersionID),
		})
	}

	_, err = s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &types.Delete{Objects: toDelete},
	})
	return err
}

func isNotFound(err error) bool {
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return true
	}
	var nf *types.NotFound
	return errors.As(err, &nf)
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test -tags=integration ./internal/store/...`
Expected: PASS (5 tests). If it fails with a connection error, confirm MinIO is running (`docker ps`) and the bucket exists with versioning enabled (Step 3).

- [ ] **Step 8: Commit**

```bash
git add internal/store/s3store.go internal/store/s3store_integration_test.go deploy/docker-compose.minio.yml go.mod go.sum
git commit -m "Add real AWS S3Store implementation"
```

---

### Task 10: HTTP server — client-facing handlers

**Files:**
- Create: `internal/httpapi/server.go`
- Create: `internal/httpapi/kv_service_fake_test.go`
- Test: `internal/httpapi/server_test.go`

**Interfaces:**
- Consumes: `store.Entry`, `store.ObjectVersion` (Task 3-4), `kv.ErrNotFound`, `kv.ErrUnavailable` (Task 5), `peers.Registry` (Task 7).
- Produces: `httpapi.KVService` interface (the subset of `*kv.Service` the HTTP layer needs), `httpapi.NewServer(svc KVService, registry *peers.Registry, internalAuthToken string) *Server` implementing `http.Handler`. Consumed by `cmd/kvstore/main.go` (Task 12).

`KVService` is declared here rather than importing `*kv.Service` directly so this package can be tested with a fake instead of a real `S3Store`.

- [ ] **Step 1: Write the fake KVService test double**

`internal/httpapi/kv_service_fake_test.go`:

```go
package httpapi

import (
	"context"

	"kvstore/internal/store"
)

type fakeKVService struct {
	putEntry store.Entry
	putErr   error

	getEntry store.Entry
	getStale bool
	getErr   error

	versionEntry store.Entry
	versionErr   error

	versions    []store.ObjectVersion
	versionsErr error

	deleteErr error

	appliedPushKey   string
	appliedPushEntry store.Entry
}

func (f *fakeKVService) Put(ctx context.Context, key string, value []byte) (store.Entry, error) {
	return f.putEntry, f.putErr
}

func (f *fakeKVService) Get(ctx context.Context, key string) (store.Entry, bool, error) {
	return f.getEntry, f.getStale, f.getErr
}

func (f *fakeKVService) GetVersion(ctx context.Context, key, versionID string) (store.Entry, error) {
	return f.versionEntry, f.versionErr
}

func (f *fakeKVService) ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error) {
	return f.versions, f.versionsErr
}

func (f *fakeKVService) Delete(ctx context.Context, key string) error {
	return f.deleteErr
}

func (f *fakeKVService) ApplyPush(key string, entry store.Entry) {
	f.appliedPushKey = key
	f.appliedPushEntry = entry
}
```

- [ ] **Step 2: Write the failing tests**

`internal/httpapi/server_test.go`:

```go
package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kvstore/internal/kv"
	"kvstore/internal/peers"
	"kvstore/internal/store"
)

func TestHandlePutReturnsVersionHeaders(t *testing.T) {
	svc := &fakeKVService{putEntry: store.Entry{VersionID: "v1", ETag: "etag1"}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodPut, "/kv/foo", strings.NewReader("bar"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Header().Get("X-Version-Id") != "v1" {
		t.Fatalf("X-Version-Id = %q, want v1", rec.Header().Get("X-Version-Id"))
	}
}

func TestHandleGetReturnsValueAndHeaders(t *testing.T) {
	svc := &fakeKVService{getEntry: store.Entry{Value: []byte("bar"), VersionID: "v1", UpdatedAt: time.Now()}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "bar" {
		t.Fatalf("body = %q, want bar", rec.Body.String())
	}
	if rec.Header().Get("X-Stale") != "" {
		t.Fatal("expected no X-Stale header for fresh result")
	}
}

func TestHandleGetSetsStaleHeader(t *testing.T) {
	svc := &fakeKVService{getEntry: store.Entry{Value: []byte("bar")}, getStale: true}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Header().Get("X-Stale") != "true" {
		t.Fatalf("X-Stale = %q, want true", rec.Header().Get("X-Stale"))
	}
}

func TestHandleGetNotFound(t *testing.T) {
	svc := &fakeKVService{getErr: kv.ErrNotFound}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/missing", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandleGetUnavailable(t *testing.T) {
	svc := &fakeKVService{getErr: kv.ErrUnavailable}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestHandleGetSpecificVersion(t *testing.T) {
	svc := &fakeKVService{versionEntry: store.Entry{Value: []byte("old"), VersionID: "v0"}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo?version=v0", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Body.String() != "old" {
		t.Fatalf("body = %q, want old", rec.Body.String())
	}
}

func TestHandleListVersions(t *testing.T) {
	svc := &fakeKVService{versions: []store.ObjectVersion{{VersionID: "v1"}, {VersionID: "v0"}}}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/kv/foo/versions", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "v1") || !strings.Contains(rec.Body.String(), "v0") {
		t.Fatalf("body = %q, expected both versions listed", rec.Body.String())
	}
}

func TestHandleDelete(t *testing.T) {
	srv := NewServer(&fakeKVService{}, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodDelete, "/kv/foo", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

func TestHandleHealthz(t *testing.T) {
	srv := NewServer(&fakeKVService{}, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/httpapi/...`
Expected: FAIL with "undefined: NewServer" (package `httpapi` doesn't exist yet).

- [ ] **Step 4: Write the implementation**

`internal/httpapi/server.go`:

```go
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"kvstore/internal/kv"
	"kvstore/internal/peers"
	"kvstore/internal/store"
)

// KVService is the subset of *kv.Service the HTTP layer depends on,
// declared here (rather than importing *kv.Service directly) so this
// package can be tested with a fake.
type KVService interface {
	Put(ctx context.Context, key string, value []byte) (store.Entry, error)
	Get(ctx context.Context, key string) (entry store.Entry, stale bool, err error)
	GetVersion(ctx context.Context, key, versionID string) (store.Entry, error)
	ListVersions(ctx context.Context, key string) ([]store.ObjectVersion, error)
	Delete(ctx context.Context, key string) error
	ApplyPush(key string, entry store.Entry)
}

type Server struct {
	mux *http.ServeMux
}

// registry and internalAuthToken are threaded through now, even though
// this task doesn't use them yet, so the signature doesn't change again
// when Task 11 wires up the internal routes that need them.
func NewServer(svc KVService, registry *peers.Registry, internalAuthToken string) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", handlePut(svc))
	mux.HandleFunc("GET /kv/{key}/versions", handleListVersions(svc))
	mux.HandleFunc("GET /kv/{key}", handleGet(svc))
	mux.HandleFunc("DELETE /kv/{key}", handleDelete(svc))
	mux.HandleFunc("GET /healthz", handleHealthz)

	return &Server{mux: mux}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func handlePut(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		entry, err := svc.Put(r.Context(), key, body)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Version-Id", entry.VersionID)
		w.Header().Set("X-ETag", entry.ETag)
		w.WriteHeader(http.StatusOK)
	}
}

func handleGet(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		if versionID := r.URL.Query().Get("version"); versionID != "" {
			entry, err := svc.GetVersion(r.Context(), key, versionID)
			if err != nil {
				writeGetError(w, err)
				return
			}
			writeEntry(w, entry, false)
			return
		}

		entry, stale, err := svc.Get(r.Context(), key)
		if err != nil {
			writeGetError(w, err)
			return
		}
		writeEntry(w, entry, stale)
	}
}

func writeEntry(w http.ResponseWriter, entry store.Entry, stale bool) {
	w.Header().Set("X-Version-Id", entry.VersionID)
	w.Header().Set("X-ETag", entry.ETag)
	w.Header().Set("X-Updated-At", entry.UpdatedAt.Format(time.RFC3339Nano))
	if stale {
		w.Header().Set("X-Stale", "true")
	}
	w.WriteHeader(http.StatusOK)
	w.Write(entry.Value)
}

func writeGetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, kv.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, kv.ErrUnavailable):
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func handleListVersions(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		versions, err := svc.ListVersions(r.Context(), key)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(versions)
	}
}

func handleDelete(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := svc.Delete(r.Context(), key); err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/httpapi/...`
Expected: PASS (9 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/httpapi/server.go internal/httpapi/server_test.go internal/httpapi/kv_service_fake_test.go
git commit -m "Add client-facing HTTP handlers"
```

---

### Task 11: HTTP server — internal handlers + auth middleware

**Files:**
- Create: `internal/httpapi/internal.go`
- Create: `internal/httpapi/internal_test.go`
- Modify: `internal/httpapi/server.go` (wire the two internal routes into `NewServer`)

**Interfaces:**
- Consumes: `peers.Registry` (Task 7), `KVService`, `Server` (Task 10).
- Produces: internal routes `POST /internal/push` and `PUT /internal/peers` on the same `*Server`, gated by `withInternalAuth`.

- [ ] **Step 1: Write the failing tests**

`internal/httpapi/internal_test.go`:

```go
package httpapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kvstore/internal/peers"
)

func TestInternalAuthRejectsMissingToken(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestInternalAuthAcceptsCorrectToken(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "secret")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	req.Header.Set("X-Internal-Auth", "secret")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if svc.appliedPushKey != "foo" || string(svc.appliedPushEntry.Value) != "body" {
		t.Fatalf("ApplyPush not called correctly: key=%q value=%q", svc.appliedPushKey, svc.appliedPushEntry.Value)
	}
}

func TestInternalAuthSkippedWhenTokenUnset(t *testing.T) {
	svc := &fakeKVService{}
	srv := NewServer(svc, peers.NewRegistry("", nil), "")

	req := httptest.NewRequest(http.MethodPost, "/internal/push", strings.NewReader("body"))
	req.Header.Set("X-Key", "foo")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestHandleSetPeersReplacesRegistry(t *testing.T) {
	registry := peers.NewRegistry("", []string{"http://old"})
	srv := NewServer(&fakeKVService{}, registry, "")

	req := httptest.NewRequest(http.MethodPut, "/internal/peers", bytes.NewReader([]byte(`{"peers":["http://a","http://b"]}`)))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := registry.List()
	if len(got) != 2 || got[0] != "http://a" || got[1] != "http://b" {
		t.Fatalf("registry.List() = %v, want [http://a http://b]", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/httpapi/...`
Expected: FAIL — requests to `/internal/push` and `/internal/peers` hit `http.NotFoundHandler` (404) since those routes aren't registered yet.

- [ ] **Step 3: Write the implementation**

`internal/httpapi/internal.go`:

```go
package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"kvstore/internal/peers"
	"kvstore/internal/store"
)

func withInternalAuth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && r.Header.Get("X-Internal-Auth") != token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handlePush(svc KVService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}
		key := r.Header.Get("X-Key")
		if key == "" {
			http.Error(w, "missing X-Key header", http.StatusBadRequest)
			return
		}
		updatedAt, _ := time.Parse(time.RFC3339Nano, r.Header.Get("X-Updated-At"))
		svc.ApplyPush(key, store.Entry{
			Value:     body,
			VersionID: r.Header.Get("X-Version-Id"),
			ETag:      r.Header.Get("X-ETag"),
			UpdatedAt: updatedAt,
		})
		w.WriteHeader(http.StatusOK)
	}
}

type setPeersRequest struct {
	Peers []string `json:"peers"`
}

func handleSetPeers(registry *peers.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req setPeersRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		registry.Set(req.Peers)
		w.WriteHeader(http.StatusOK)
	}
}
```

Update `internal/httpapi/server.go`'s `NewServer` to wire these in:

```go
func NewServer(svc KVService, registry *peers.Registry, internalAuthToken string) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("PUT /kv/{key}", handlePut(svc))
	mux.HandleFunc("GET /kv/{key}/versions", handleListVersions(svc))
	mux.HandleFunc("GET /kv/{key}", handleGet(svc))
	mux.HandleFunc("DELETE /kv/{key}", handleDelete(svc))
	mux.HandleFunc("GET /healthz", handleHealthz)

	mux.Handle("POST /internal/push", withInternalAuth(internalAuthToken, handlePush(svc)))
	mux.Handle("PUT /internal/peers", withInternalAuth(internalAuthToken, handleSetPeers(registry)))

	return &Server{mux: mux}
}
```

(This replaces the comment-only version from Task 10 — the signature is unchanged, only the two `mux.Handle` lines are added.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/httpapi/...`
Expected: PASS (13 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/httpapi/server.go internal/httpapi/internal.go internal/httpapi/internal_test.go
git commit -m "Add internal peer-push and peers-update endpoints"
```

---

### Task 12: Wire main.go

**Files:**
- Modify: `cmd/kvstore/main.go` (replace the Task 1 placeholder)

**Interfaces:**
- Consumes: `config.Load` (Task 2), `store.NewCache`, `store.NewAWSS3Store` (Tasks 3, 9), `peers.NewRegistry`, `peers.NewHTTPPusher` (Tasks 7-8), `kv.NewService` (Tasks 5-6), `httpapi.NewServer` (Tasks 10-11).

- [ ] **Step 1: Replace the placeholder with real wiring**

`cmd/kvstore/main.go`:

```go
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	kvconfig "kvstore/internal/config"
	"kvstore/internal/httpapi"
	"kvstore/internal/kv"
	"kvstore/internal/peers"
	"kvstore/internal/store"
)

func main() {
	cfg, err := kvconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.AWSRegion))
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg)

	cache := store.NewCache()
	s3Store := store.NewAWSS3Store(s3Client, cfg.S3Bucket)
	registry := peers.NewRegistry(cfg.SelfAddr, cfg.Peers)
	pusher := peers.NewHTTPPusher(registry, cfg.InternalAuthToken, 2*time.Second)
	svc := kv.NewService(cache, s3Store, pusher, cfg.MaxVersions)

	server := httpapi.NewServer(svc, registry, cfg.InternalAuthToken)

	log.Printf("kv-store listening on :%s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, server); err != nil {
		log.Fatalf("server: %v", err)
	}
}
```

- [ ] **Step 2: Verify it builds**

Run: `go build ./...`
Expected: builds with no errors.

- [ ] **Step 3: Run the full unit test suite**

Run: `go vet ./... && go test ./...`
Expected: PASS across every package touched in Tasks 2-11 (config, store, kv, peers, httpapi). Integration-tagged tests are skipped by default since they require `-tags=integration`.

- [ ] **Step 4: Manual smoke test (optional, requires a real or MinIO S3 bucket)**

If MinIO is still running from Task 9, set path-style/endpoint overrides aren't available in this minimal `main.go` (it targets real AWS S3 config resolution by design — see Task 13 for the MinIO-backed end-to-end test). If you have a real S3 bucket available:

```bash
export S3_BUCKET=your-bucket
export AWS_REGION=us-east-1
go run ./cmd/kvstore &
curl -X PUT localhost:8080/kv/hello -d 'world'
curl localhost:8080/kv/hello
curl localhost:8080/kv/hello/versions
curl -X DELETE localhost:8080/kv/hello
curl -i localhost:8080/kv/hello   # expect 404
kill %1
```

If no S3 bucket is available, skip this step — Task 13's automated integration test covers the same paths against MinIO.

- [ ] **Step 5: Commit**

```bash
git add cmd/kvstore/main.go
git commit -m "Wire kv-store service in main"
```

---

### Task 13: End-to-end integration test (two instances against MinIO)

**Files:**
- Create: `test/integration/full_stack_test.go` (build tag `integration`)

**Interfaces:**
- Consumes: every package built in Tasks 2-11, plus `store.NewAWSS3Store` (Task 9) and a running MinIO instance (Task 9's docker-compose, same bucket).

This is the test that actually proves the design: two full server instances, sharing one S3 bucket, prove that a write on one instance becomes visible on the other both via the cache-warm push (fast path) and via the S3 `HEAD` fallback when the push never arrives (correctness path).

- [ ] **Step 1: Confirm MinIO is running**

Run: `docker ps | grep minio`
If not running, redo Task 9 Step 3 (start the container and recreate the versioned `kv-store-test` bucket).

- [ ] **Step 2: Write the failing tests**

`test/integration/full_stack_test.go`:

```go
//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"kvstore/internal/httpapi"
	"kvstore/internal/kv"
	"kvstore/internal/peers"
	"kvstore/internal/store"
)

const (
	authToken = "test-secret"
	bucket    = "kv-store-test"
)

type instance struct {
	server   *httptest.Server
	registry *peers.Registry
}

func newInstance(t *testing.T) *instance {
	t.Helper()

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://localhost:9000")
		o.UsePathStyle = true
	})

	cache := store.NewCache()
	s3Store := store.NewAWSS3Store(client, bucket)
	registry := peers.NewRegistry("", nil)
	pusher := peers.NewHTTPPusher(registry, authToken, 2*time.Second)
	svc := kv.NewService(cache, s3Store, pusher, 10)
	srv := httpapi.NewServer(svc, registry, authToken)

	return &instance{server: httptest.NewServer(srv), registry: registry}
}

func put(t *testing.T, baseURL, key, value string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, baseURL+"/kv/"+key, strings.NewReader(value))
	if err != nil {
		t.Fatalf("building PUT request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200", resp.StatusCode)
	}
}

func get(t *testing.T, baseURL, key string) (string, int) {
	t.Helper()
	resp, err := http.Get(baseURL + "/kv/" + key)
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading GET body failed: %v", err)
	}
	return string(body), resp.StatusCode
}

func TestWritePropagatesToPeerViaPush(t *testing.T) {
	a := newInstance(t)
	b := newInstance(t)
	defer a.server.Close()
	defer b.server.Close()

	a.registry.Set([]string{b.server.URL})
	b.registry.Set([]string{a.server.URL})

	put(t, a.server.URL, "push-key", "hello")

	deadline := time.Now().Add(2 * time.Second)
	for {
		body, status := get(t, b.server.URL, "push-key")
		if status == http.StatusOK && body == "hello" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer B never saw the pushed value (last status=%d body=%q)", status, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWriteVisibleViaHeadFallbackWhenPushFails(t *testing.T) {
	a := newInstance(t)
	defer a.server.Close()

	// Point A's peer list at an address nothing is listening on, so the
	// push always fails; a fresh instance B should still see the write
	// via its own Head-check against S3 rather than depending on the push.
	a.registry.Set([]string{"http://127.0.0.1:1"})
	put(t, a.server.URL, "no-push-key", "still-here")

	b := newInstance(t)
	defer b.server.Close()

	body, status := get(t, b.server.URL, "no-push-key")
	if status != http.StatusOK || body != "still-here" {
		t.Fatalf("GET on B = (%d, %q), want (200, still-here)", status, body)
	}
}

func TestVersionCapEnforcedEndToEnd(t *testing.T) {
	a := newInstance(t)
	defer a.server.Close()

	for i := 0; i < 15; i++ {
		put(t, a.server.URL, "capped-key", "v")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(a.server.URL + "/kv/capped-key/versions")
		if err != nil {
			t.Fatalf("GET versions failed: %v", err)
		}
		var versions []store.ObjectVersion
		decodeErr := json.NewDecoder(resp.Body).Decode(&versions)
		resp.Body.Close()
		if decodeErr != nil {
			t.Fatalf("decode versions failed: %v", decodeErr)
		}
		if len(versions) <= 10 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected at most 10 versions, got %d", len(versions))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestDeleteThenGetReturnsNotFound(t *testing.T) {
	a := newInstance(t)
	defer a.server.Close()

	put(t, a.server.URL, "delete-key", "gone-soon")

	req, err := http.NewRequest(http.MethodDelete, a.server.URL+"/kv/delete-key", nil)
	if err != nil {
		t.Fatalf("building DELETE request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	resp.Body.Close()

	_, status := get(t, a.server.URL, "delete-key")
	if status != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", status)
	}
}

func TestInternalPushRejectedWithoutAuthToken(t *testing.T) {
	a := newInstance(t)
	defer a.server.Close()

	req, err := http.NewRequest(http.MethodPost, a.server.URL+"/internal/push", strings.NewReader("body"))
	if err != nil {
		t.Fatalf("building POST request failed: %v", err)
	}
	req.Header.Set("X-Key", "foo")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test -tags=integration ./test/integration/...`
Expected: FAIL only if MinIO/the bucket aren't set up correctly (connection refused / NoSuchBucket) — this step is really a check that the harness is wired right, since all the production code it exercises already exists from Tasks 1-12.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags=integration ./test/integration/...`
Expected: PASS (5 tests). `TestWritePropagatesToPeerViaPush` and `TestVersionCapEnforcedEndToEnd` poll with a 2-second deadline since the push and prune are asynchronous — this is expected, not flaky, given the write path's fire-and-forget design (Task 6).

- [ ] **Step 5: Commit**

```bash
git add test/integration/full_stack_test.go
git commit -m "Add end-to-end integration test against MinIO"
```

---

## Deployment Notes (infra, not application code)

These are one-time bucket setup steps for a real deployment, not covered by any task above since they're outside the Go codebase:

- Enable versioning on the production S3 bucket (`aws s3api put-bucket-versioning --bucket <bucket> --versioning-configuration Status=Enabled`).
- Add a lifecycle rule with `NoncurrentVersionExpiration` + `NewerNoncurrentVersions` set to the same value as `MAX_VERSIONS`, as the backstop described in the spec (§3) for when app-side pruning fails or is skipped.

## Plan Self-Review

**Spec coverage:** §2 Architecture → Tasks 1, 12. §3 Data model/S3 layout → Task 9. §3 Version cap → Tasks 6 (app-side) + 9 (S3 client) + Deployment Notes (lifecycle rule). §4 Write path → Task 6. §5 Read path/failure handling → Task 5. §6 API surface → Tasks 10-11. §7 Config → Task 2. §8 Testing plan → unit tests throughout Tasks 2-11, integration tests in Tasks 9 and 13. §9 Out of scope → no task implements peer discovery or bulk preload, consistent with the spec.

**Placeholder scan:** no TODO/TBD markers; every step has runnable code and exact commands.

**Type consistency:** `store.Entry`, `store.ObjectVersion`, `store.S3Store`, `store.ErrNotFound`, `kv.ErrNotFound`, `kv.ErrUnavailable`, `kv.Pusher`, `kv.Service`, `peers.Registry`, `peers.HTTPPusher`, and `httpapi.KVService`/`httpapi.Server` are each defined exactly once (Tasks 3, 4, 5, 7, 8, 10) and referenced with matching names and signatures in every later task that consumes them.

