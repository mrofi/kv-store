//go:build integration

package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/mrofi/kv-store/store"
)

// uniqueKey avoids cross-run collisions: the test bucket is a persistent
// MinIO instance shared across every invocation of this suite, so a fixed
// key name would accumulate versions from prior runs and break assertions
// that check an exact version count.
func uniqueKey(base string) string {
	return fmt.Sprintf("%s-%d", base, time.Now().UnixNano())
}

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
	key := uniqueKey("integration-test-key")

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
	key := uniqueKey("integration-test-head")

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
	key := uniqueKey("integration-test-versions")

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
	key := uniqueKey("integration-test-delete")

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
	key := uniqueKey("integration-test-prune")

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

func TestEncryptedS3StoreRoundTripAgainstRealS3(t *testing.T) {
	inner := newTestStore(t)
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	encStore, err := store.NewEncryptedS3Store(inner, key)
	if err != nil {
		t.Fatalf("NewEncryptedS3Store returned error: %v", err)
	}

	ctx := context.Background()
	testKey := uniqueKey("integration-test-encrypted")

	if _, err := encStore.Put(ctx, testKey, []byte("top-secret-value")); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	raw, err := inner.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("inner Get returned error: %v", err)
	}
	if string(raw.Value) == "top-secret-value" {
		t.Fatal("expected the real S3-compatible store to hold ciphertext, not plaintext")
	}

	got, err := encStore.Get(ctx, testKey)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(got.Value) != "top-secret-value" {
		t.Fatalf("got value %q, want top-secret-value", got.Value)
	}
}

func TestAWSS3StoreListByPrefix(t *testing.T) {
	s3store := newTestStore(t)
	ctx := context.Background()
	prefix := uniqueKey("list-test") + "."

	for _, k := range []string{"b", "a", "gone"} {
		if _, err := s3store.Put(ctx, prefix+k, []byte("v")); err != nil {
			t.Fatalf("Put(%q) failed: %v", k, err)
		}
	}
	if err := s3store.Delete(ctx, prefix+"gone"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	got, err := s3store.List(ctx, prefix, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(got) != 2 || got[0] != prefix+"a" || got[1] != prefix+"b" {
		t.Fatalf("List = %v, want [%sa %sb] (deleted key omitted, internal kv/ prefix stripped)", got, prefix, prefix)
	}

	got, err = s3store.List(ctx, prefix, 1)
	if err != nil || len(got) != 1 || got[0] != prefix+"a" {
		t.Fatalf("List(limit 1) = %v, %v; want [%sa]", got, err, prefix)
	}
}
