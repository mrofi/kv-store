package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mrofi/kv-store/store"
)

type recordedPush struct {
	key   string
	entry store.Entry
}

type recordingPusher struct {
	calls       chan recordedPush
	deleteCalls chan string
}

func newRecordingPusher() *recordingPusher {
	return &recordingPusher{
		calls:       make(chan recordedPush, 10),
		deleteCalls: make(chan string, 10),
	}
}

func (p *recordingPusher) Push(ctx context.Context, key string, entry store.Entry) {
	p.calls <- recordedPush{key: key, entry: entry}
}

func (p *recordingPusher) PushDelete(ctx context.Context, key string) {
	p.deleteCalls <- key
}

func TestGetCacheHitAvoidsBodyFetch(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
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
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	_, _, err := svc.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestGetServesStaleCacheWhenS3Unreachable(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	s3.GetErr = errors.New("simulated network error")
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	_, _, err := svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expected ErrUnavailable, got %v", err)
	}
}

func TestGetCacheHitButKeyNowDeletedInS3(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	cache.Set("foo", entry)
	if err := s3.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	_, _, err = svc.Get(context.Background(), "foo")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected cache entry to be evicted")
	}
}

func TestGetVersionReturnsSpecificVersion(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	v1, err := s3.Put(context.Background(), "foo", []byte("v1"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	got, err := svc.GetVersion(context.Background(), "foo", v1.VersionID)
	if err != nil {
		t.Fatalf("GetVersion returned error: %v", err)
	}
	if string(got.Value) != "v1" {
		t.Fatalf("got value %q, want v1", got.Value)
	}
}

func TestGetVersionNotFound(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	_, err := svc.GetVersion(context.Background(), "foo", "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListVersionsReturnsAll(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	if _, err := s3.Put(context.Background(), "foo", []byte("v1")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := s3.Put(context.Background(), "foo", []byte("v2")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	versions, err := svc.ListVersions(context.Background(), "foo")
	if err != nil {
		t.Fatalf("ListVersions returned error: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("len(versions) = %d, want 2", len(versions))
	}
}

func TestPutStoresInS3AndUpdatesCache(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	pusher := newRecordingPusher()
	svc := NewService(cache, s3, pusher, 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 2, 0)

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

// TestPutPruneUsesTimeoutContext guards against a leaked goroutine: the
// prune runs detached from the request (Put already returned), so if it
// were given context.Background() a hung or slow S3 endpoint would keep
// that goroutine (and whatever it's holding) alive forever. A bounded
// timeout guarantees it eventually gives up.
func TestPutPruneUsesTimeoutContext(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		if ctx := s3.LastPruneCtx(); ctx != nil {
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("expected PruneToMax to be called with a context carrying a deadline, got one with none")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the async prune to run")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestConcurrentPutAndDeleteSameKeyDoesNotResurrectDeletedValue guards
// against a same-instance race: without serializing Put and Delete per
// key, this interleaving is possible — Put's S3 write completes, then a
// concurrent Delete's S3 delete and cache eviction both complete, then
// Put's own (now-stale) cache write lands last and resurrects a value in
// the cache that S3 no longer has.
//
// The PutHook pauses Put after its S3 write but before its cache write,
// giving Delete a window to run. The select below tolerates either
// outcome of that window: pre-fix, Delete races ahead and finishes before
// the timeout; post-fix, Delete blocks on Put's per-key lock until the
// timeout fires and Put is released. Either way, the final assertion is
// what actually matters: the cache must never disagree with S3's truth
// once both operations have completed.
func TestConcurrentPutAndDeleteSameKeyDoesNotResurrectDeletedValue(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	putEnteredS3Write := make(chan struct{})
	releasePut := make(chan struct{})
	var once sync.Once
	s3.PutHook = func() {
		once.Do(func() { close(putEnteredS3Write) })
		<-releasePut
	}

	deleteDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if _, err := svc.Put(context.Background(), "foo", []byte("v1")); err != nil {
			t.Errorf("Put returned error: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		<-putEnteredS3Write
		if err := svc.Delete(context.Background(), "foo"); err != nil {
			t.Errorf("Delete returned error: %v", err)
		}
		close(deleteDone)
	}()

	select {
	case <-deleteDone:
	case <-time.After(50 * time.Millisecond):
	}
	close(releasePut)
	wg.Wait()

	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected cache to have no entry for foo after a Delete that raced a Put (cache resurrected a value S3 no longer has)")
	}
	if _, _, err := svc.Get(context.Background(), "foo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound from S3 after the race, got %v", err)
	}
}

func TestGetAfterDeleteReturnsNotFoundAndEvictsCache(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

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

func TestGetWithinTTLSkipsS3Entirely(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	entry.CachedAt = time.Now()
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

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
	if s3.HeadCalls != 0 || s3.GetCalls != 0 {
		t.Fatalf("expected zero S3 calls within TTL window, got head=%d get=%d", s3.HeadCalls, s3.GetCalls)
	}
}

func TestGetPastTTLFallsBackToHeadCheck(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	entry.CachedAt = time.Now().Add(-time.Hour)
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Second)

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
	if s3.HeadCalls != 1 {
		t.Fatalf("expected exactly 1 HeadObject call once TTL has expired, got %d", s3.HeadCalls)
	}
}

func TestGetTTLDisabledAlwaysChecksHead(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	entry.CachedAt = time.Now()
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	if _, _, err := svc.Get(context.Background(), "foo"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if s3.HeadCalls != 1 {
		t.Fatalf("expected TTL=0 to always check Head, got %d calls", s3.HeadCalls)
	}
}

func TestHeadMatchRefreshesCachedAtExtendingTTLWindow(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	entry, err := s3.Put(context.Background(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	entry.CachedAt = time.Now().Add(-time.Hour)
	cache.Set("foo", entry)
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	// First Get is past the TTL window, so it falls through to a Head
	// check; since the version still matches, CachedAt should refresh.
	if _, _, err := svc.Get(context.Background(), "foo"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if s3.HeadCalls != 1 {
		t.Fatalf("expected the first Get to perform exactly 1 Head check, got %d", s3.HeadCalls)
	}

	s3.HeadCalls = 0

	// A second Get should now be within the freshly-extended TTL window
	// and skip S3 entirely.
	if _, _, err := svc.Get(context.Background(), "foo"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if s3.HeadCalls != 0 {
		t.Fatalf("expected the second Get to skip Head after CachedAt refresh, got %d calls", s3.HeadCalls)
	}
}

func TestPutStampsCachedAtEnablingTTLFastPath(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if _, _, err := svc.Get(context.Background(), "foo"); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if s3.HeadCalls != 0 || s3.GetCalls != 0 {
		t.Fatalf("expected Put to enable the TTL fast path, got head=%d get=%d", s3.HeadCalls, s3.GetCalls)
	}
}

func TestApplyPushStampsCachedAtEnablingTTLFastPath(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	svc.ApplyPush("foo", store.Entry{Value: []byte("from-peer"), VersionID: "v1"})

	got, stale, err := svc.Get(context.Background(), "foo")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if stale {
		t.Fatal("expected fresh result")
	}
	if string(got.Value) != "from-peer" {
		t.Fatalf("got value %q, want from-peer", got.Value)
	}
	if s3.HeadCalls != 0 || s3.GetCalls != 0 {
		t.Fatalf("expected ApplyPush to enable the TTL fast path, got head=%d get=%d", s3.HeadCalls, s3.GetCalls)
	}
}

func TestApplyPushRejectsOutOfOrderPush(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	now := time.Now()
	svc.ApplyPush("foo", store.Entry{Value: []byte("newer"), UpdatedAt: now})
	// An older push arrives after, e.g. delayed/reordered on the network.
	svc.ApplyPush("foo", store.Entry{Value: []byte("older"), UpdatedAt: now.Add(-time.Minute)})

	got, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected cache to still hold a value")
	}
	if string(got.Value) != "newer" {
		t.Fatalf("cached value = %q, want newer (the out-of-order push should have been ignored)", got.Value)
	}
}

func TestApplyPushAcceptsNewerPush(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	now := time.Now()
	svc.ApplyPush("foo", store.Entry{Value: []byte("older"), UpdatedAt: now})
	svc.ApplyPush("foo", store.Entry{Value: []byte("newer"), UpdatedAt: now.Add(time.Minute)})

	got, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected cache to hold a value")
	}
	if string(got.Value) != "newer" {
		t.Fatalf("cached value = %q, want newer", got.Value)
	}
}

func TestApplyPushAcceptsFirstPushWhenNothingCached(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, time.Hour)

	svc.ApplyPush("foo", store.Entry{Value: []byte("bar")})

	got, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected cache to hold the pushed value")
	}
	if string(got.Value) != "bar" {
		t.Fatalf("cached value = %q, want bar", got.Value)
	}
}

func TestDeleteNotifiesPusher(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	pusher := newRecordingPusher()
	svc := NewService(cache, s3, pusher, 10, 0)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	<-pusher.calls // drain the Put's push so it doesn't get confused with delete below

	if err := svc.Delete(context.Background(), "foo"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	select {
	case key := <-pusher.deleteCalls:
		if key != "foo" {
			t.Fatalf("PushDelete called with key %q, want foo", key)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PushDelete")
	}
}

func TestApplyDeleteEvictsCache(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	svc.ApplyPush("foo", store.Entry{Value: []byte("bar")})
	if _, ok := cache.Get("foo"); !ok {
		t.Fatal("expected cache to hold the pushed value before ApplyDelete")
	}

	svc.ApplyDelete("foo")

	if _, ok := cache.Get("foo"); ok {
		t.Fatal("expected ApplyDelete to evict the cache entry")
	}
}

// TestWaitBlocksUntilAsyncPushAndPruneComplete guards against a process
// exiting (or a caller assuming shutdown is done) while Put's detached
// push/prune goroutines are still running — httpServer.Shutdown only
// waits for in-flight HTTP requests, which Put has already returned from
// by the time these goroutines are still working.
func TestWaitBlocksUntilAsyncPushAndPruneComplete(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()

	releasePush := make(chan struct{})
	blockingPusher := &blockingPusherWrapper{inner: newRecordingPusher(), release: releasePush}
	svc := NewService(cache, s3, blockingPusher, 10, 0)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	waitDone := make(chan struct{})
	go func() {
		if err := svc.Wait(context.Background()); err != nil {
			t.Errorf("Wait returned error: %v", err)
		}
		close(waitDone)
	}()

	select {
	case <-waitDone:
		t.Fatal("expected Wait to block while the async push is still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(releasePush)

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Wait to return after the async push completed")
	}
}

func TestWaitReturnsErrorWhenContextExpiresBeforeAsyncWorkCompletes(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	releasePush := make(chan struct{})
	defer close(releasePush)
	blockingPusher := &blockingPusherWrapper{inner: newRecordingPusher(), release: releasePush}
	svc := NewService(cache, s3, blockingPusher, 10, 0)

	if _, err := svc.Put(context.Background(), "foo", []byte("bar")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := svc.Wait(ctx); err == nil {
		t.Fatal("expected Wait to return an error when its context expires before async work completes")
	}
}

type blockingPusherWrapper struct {
	inner   Pusher
	release chan struct{}
}

func (p *blockingPusherWrapper) Push(ctx context.Context, key string, entry store.Entry) {
	<-p.release
	p.inner.Push(ctx, key, entry)
}

func (p *blockingPusherWrapper) PushDelete(ctx context.Context, key string) {
	<-p.release
	p.inner.PushDelete(ctx, key)
}

func TestPingReturnsNilWhenS3Reachable(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	// The probe key doesn't exist, which is expected and fine — it still
	// means S3 itself answered the request.
	if err := svc.Ping(context.Background()); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}
}

func TestPingReturnsErrorWhenS3Unreachable(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	s3.HeadErr = errors.New("simulated network error")
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	if err := svc.Ping(context.Background()); err == nil {
		t.Fatal("expected Ping to return an error when S3 is unreachable")
	}
}

// TestApplyPushConcurrentPushesConvergeToNewest guards against a
// check-then-act race: ApplyPush must decide "is this newer than what's
// cached" and apply it atomically, not via a separate Get then Set —
// otherwise concurrent pushes for the same key can race so that an older
// push wins depending on goroutine scheduling, regardless of UpdatedAt.
func TestApplyPushConcurrentPushesConvergeToNewest(t *testing.T) {
	cache := store.NewCache(0)
	s3 := store.NewFakeS3Store()
	svc := NewService(cache, s3, newRecordingPusher(), 10, 0)

	base := time.Now()
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc.ApplyPush("foo", store.Entry{
				Value:     []byte(fmt.Sprintf("v%d", i)),
				UpdatedAt: base.Add(time.Duration(i) * time.Millisecond),
			})
		}(i)
	}
	wg.Wait()

	got, ok := cache.Get("foo")
	if !ok {
		t.Fatal("expected a cached value")
	}
	want := fmt.Sprintf("v%d", n-1)
	if string(got.Value) != want {
		t.Fatalf("cached value = %q, want %q (the push with the latest UpdatedAt must always win)", got.Value, want)
	}
}
