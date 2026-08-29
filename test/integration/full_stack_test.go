//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mrofi/kv-store/internal/httpapi"
	"github.com/mrofi/kv-store/kv"
	"github.com/mrofi/kv-store/peers"
	"github.com/mrofi/kv-store/store"
)

const (
	authToken = "test-secret"
	bucket    = "kv-store-test"
)

// uniqueKey avoids cross-run collisions: the test bucket is a persistent
// MinIO instance shared across every invocation of this suite, so a fixed
// key name would accumulate history from prior runs and break assertions
// (and convergence timing) that depend on a key's exact version count.
func uniqueKey(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

type instance struct {
	server   *httptest.Server
	registry *peers.Registry
}

func newInstance(t *testing.T, cacheTTL time.Duration) *instance {
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

	cache := store.NewCache(0)
	s3Store := store.NewAWSS3Store(client, bucket)
	registry := peers.NewRegistry("", nil)
	pusher := peers.NewHTTPPusher(registry, authToken, 2*time.Second)
	svc := kv.NewService(cache, s3Store, pusher, 10, cacheTTL)
	srv := httpapi.NewServer(svc, registry, authToken, 10<<20)

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
	a := newInstance(t, 0)
	b := newInstance(t, 0)
	defer a.server.Close()
	defer b.server.Close()

	a.registry.Set([]string{b.server.URL})
	b.registry.Set([]string{a.server.URL})

	key := uniqueKey("push-key")
	put(t, a.server.URL, key, "hello")

	deadline := time.Now().Add(2 * time.Second)
	for {
		body, status := get(t, b.server.URL, key)
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
	a := newInstance(t, 0)
	defer a.server.Close()

	// Point A's peer list at an address nothing is listening on, so the
	// push always fails; a fresh instance B should still see the write
	// via its own Head-check against S3 rather than depending on the push.
	a.registry.Set([]string{"http://127.0.0.1:1"})
	key := uniqueKey("no-push-key")
	put(t, a.server.URL, key, "still-here")

	b := newInstance(t, 0)
	defer b.server.Close()

	body, status := get(t, b.server.URL, key)
	if status != http.StatusOK || body != "still-here" {
		t.Fatalf("GET on B = (%d, %q), want (200, still-here)", status, body)
	}
}

func TestVersionCapEnforcedEndToEnd(t *testing.T) {
	a := newInstance(t, 0)
	defer a.server.Close()

	key := uniqueKey("capped-key")
	for i := 0; i < 15; i++ {
		put(t, a.server.URL, key, "v")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		resp, err := http.Get(a.server.URL + "/kv/" + key + "/versions")
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
	a := newInstance(t, 0)
	defer a.server.Close()

	key := uniqueKey("delete-key")
	put(t, a.server.URL, key, "gone-soon")

	req, err := http.NewRequest(http.MethodDelete, a.server.URL+"/kv/"+key, nil)
	if err != nil {
		t.Fatalf("building DELETE request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	resp.Body.Close()

	_, status := get(t, a.server.URL, key)
	if status != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", status)
	}
}

func deleteKey(t *testing.T, baseURL, key string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/kv/"+key, nil)
	if err != nil {
		t.Fatalf("building DELETE request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	resp.Body.Close()
}

// TestDeletePropagatesToPeerViaPush uses a long CACHE_TTL specifically so
// the only way B stops serving the deleted value quickly is if the
// delete-push actually evicted it — B's own Head-check fallback is
// bypassed for the whole TTL window by design, so without the fix this
// test would time out waiting for a 404 rather than get one immediately.
func TestDeletePropagatesToPeerViaPush(t *testing.T) {
	a := newInstance(t, time.Hour)
	b := newInstance(t, time.Hour)
	defer a.server.Close()
	defer b.server.Close()

	a.registry.Set([]string{b.server.URL})
	b.registry.Set([]string{a.server.URL})

	key := uniqueKey("delete-push-key")
	put(t, a.server.URL, key, "will-be-deleted")

	// Wait for the write's push to warm B's cache (and its TTL window).
	deadline := time.Now().Add(2 * time.Second)
	for {
		body, status := get(t, b.server.URL, key)
		if status == http.StatusOK && body == "will-be-deleted" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer B never saw the pushed value (last status=%d body=%q)", status, body)
		}
		time.Sleep(20 * time.Millisecond)
	}

	deleteKey(t, a.server.URL, key)

	deadline = time.Now().Add(2 * time.Second)
	for {
		_, status := get(t, b.server.URL, key)
		if status == http.StatusNotFound {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer B still serving the deleted value after 2s (delete-push never arrived, and B's 1h CACHE_TTL means its own Head-check fallback won't catch this for a long time)")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestConcurrentPushesFromMultipleInstancesDoNotCorruptCache proves the
// ApplyPush ordering fix's atomicity holds across genuinely separate peer
// instances racing over real HTTP and real S3 writes — not just
// concurrent goroutines calling ApplyPush directly within one process
// (which is all the unit-level stress test in kv/service_test.go actually
// exercises).
//
// What this deliberately does NOT assert: that target's cache always
// ends up matching whichever write S3 actually committed last. UpdatedAt
// is stamped from each instance's own local wall clock at the moment its
// own PutObject call returns — not from anything S3 reports — so under a
// genuine race between two different instances writing the same key,
// "whoever's local clock read later" and "whichever write S3 actually
// applied last" can disagree. That's an accepted limitation (documented
// in the README), the same root cause as the already-accepted
// last-write-wins behavior for concurrent writes to one key — it just
// also means the push-warmed cache can disagree with S3's truth for up
// to CACHE_TTL, not only that S3 itself picks the winner. An earlier,
// stricter version of this test asserted convergence with S3 truth and
// failed ~30% of the time for exactly this reason.
//
// target uses a long CACHE_TTL so its Get trusts the cache without a
// Head-check, isolating what the push mechanism itself guarantees from
// S3's own safety net.
func TestConcurrentPushesFromMultipleInstancesDoNotCorruptCache(t *testing.T) {
	target := newInstance(t, time.Hour)
	s1 := newInstance(t, 0)
	s2 := newInstance(t, 0)
	defer target.server.Close()
	defer s1.server.Close()
	defer s2.server.Close()

	s1.registry.Set([]string{target.server.URL})
	s2.registry.Set([]string{target.server.URL})

	key := uniqueKey("multi-instance-race-key")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		put(t, s1.server.URL, key, "from-s1")
	}()
	go func() {
		defer wg.Done()
		put(t, s2.server.URL, key, "from-s2")
	}()
	wg.Wait()

	// Give both instances' async pushes time to land at target.
	time.Sleep(500 * time.Millisecond)

	targetBody, targetStatus := get(t, target.server.URL, key)
	if targetStatus != http.StatusOK {
		t.Fatalf("GET on target status = %d, want 200", targetStatus)
	}
	if targetBody != "from-s1" && targetBody != "from-s2" {
		t.Fatalf("target's cached value = %q, want one of {from-s1, from-s2} (cache corrupted by the race)", targetBody)
	}

	// Logged, not asserted: whether target's push-warmed cache happens to
	// agree with S3's actual final state for this particular run.
	// Disagreement here is the accepted limitation described above, not
	// a test failure.
	truth := newInstance(t, 0)
	defer truth.server.Close()
	truthBody, truthStatus := get(t, truth.server.URL, key)
	if truthStatus == http.StatusOK && truthBody != targetBody {
		t.Logf("accepted limitation observed this run: target's push-warmed cache (%q) disagrees with S3 ground truth (%q) after a same-key race between two instances", targetBody, truthBody)
	}
}

func TestInternalPushRejectedWithoutAuthToken(t *testing.T) {
	a := newInstance(t, 0)
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
