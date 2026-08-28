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
	svc := kv.NewService(cache, s3Store, pusher, 10, 0)
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
	a := newInstance(t)
	defer a.server.Close()

	// Point A's peer list at an address nothing is listening on, so the
	// push always fails; a fresh instance B should still see the write
	// via its own Head-check against S3 rather than depending on the push.
	a.registry.Set([]string{"http://127.0.0.1:1"})
	key := uniqueKey("no-push-key")
	put(t, a.server.URL, key, "still-here")

	b := newInstance(t)
	defer b.server.Close()

	body, status := get(t, b.server.URL, key)
	if status != http.StatusOK || body != "still-here" {
		t.Fatalf("GET on B = (%d, %q), want (200, still-here)", status, body)
	}
}

func TestVersionCapEnforcedEndToEnd(t *testing.T) {
	a := newInstance(t)
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
	a := newInstance(t)
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
