package config

import (
	"encoding/base64"
	"testing"
	"time"
)

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
	if cfg.CacheTTL != 0 {
		t.Fatalf("CacheTTL = %v, want 0 (disabled by default)", cfg.CacheTTL)
	}
	if cfg.EncryptionKey != nil {
		t.Fatalf("EncryptionKey = %v, want nil (disabled by default)", cfg.EncryptionKey)
	}
	if cfg.CacheMaxEntries != 0 {
		t.Fatalf("CacheMaxEntries = %d, want 0 (unbounded by default)", cfg.CacheMaxEntries)
	}
	if cfg.S3Endpoint != "" {
		t.Fatalf("S3Endpoint = %q, want empty (real AWS S3 by default)", cfg.S3Endpoint)
	}
	if cfg.S3ForcePathStyle {
		t.Fatal("S3ForcePathStyle = true, want false by default")
	}
	if cfg.S3AccessKeyID != "" || cfg.S3SecretAccessKey != "" || cfg.S3SessionToken != "" {
		t.Fatal("S3 credential fields should be empty by default")
	}
}

func TestLoadParsesS3Credentials(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("S3_SECRET_ACCESS_KEY", "secret123")
	t.Setenv("S3_SESSION_TOKEN", "token456")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.S3AccessKeyID != "AKIAEXAMPLE" {
		t.Fatalf("S3AccessKeyID = %q, want AKIAEXAMPLE", cfg.S3AccessKeyID)
	}
	if cfg.S3SecretAccessKey != "secret123" {
		t.Fatalf("S3SecretAccessKey = %q, want secret123", cfg.S3SecretAccessKey)
	}
	if cfg.S3SessionToken != "token456" {
		t.Fatalf("S3SessionToken = %q, want token456", cfg.S3SessionToken)
	}
}

func TestLoadAllowsS3SessionTokenAloneWithBothKeysSet(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "AKIAEXAMPLE")
	t.Setenv("S3_SECRET_ACCESS_KEY", "secret123")

	if _, err := Load(); err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
}

func TestLoadRejectsS3AccessKeyIDWithoutSecret(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ACCESS_KEY_ID", "AKIAEXAMPLE")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error when S3_ACCESS_KEY_ID is set without S3_SECRET_ACCESS_KEY")
	}
}

func TestLoadRejectsS3SecretWithoutAccessKeyID(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_SECRET_ACCESS_KEY", "secret123")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error when S3_SECRET_ACCESS_KEY is set without S3_ACCESS_KEY_ID")
	}
}

func TestLoadParsesS3Endpoint(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.S3Endpoint != "http://localhost:9000" {
		t.Fatalf("S3Endpoint = %q, want http://localhost:9000", cfg.S3Endpoint)
	}
}

func TestLoadS3EndpointAutoEnablesPathStyle(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !cfg.S3ForcePathStyle {
		t.Fatal("expected S3ForcePathStyle to default to true when S3_ENDPOINT is set")
	}
}

func TestLoadS3ForcePathStyleExplicitOverride(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("S3_FORCE_PATH_STYLE", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.S3ForcePathStyle {
		t.Fatal("expected explicit S3_FORCE_PATH_STYLE=false to override the auto-default")
	}
}

func TestLoadRejectsInvalidS3ForcePathStyle(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("S3_FORCE_PATH_STYLE", "not-a-bool")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for invalid S3_FORCE_PATH_STYLE")
	}
}

func TestLoadParsesCacheMaxEntries(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("CACHE_MAX_ENTRIES", "50000")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.CacheMaxEntries != 50000 {
		t.Fatalf("CacheMaxEntries = %d, want 50000", cfg.CacheMaxEntries)
	}
}

func TestLoadRejectsInvalidCacheMaxEntries(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("CACHE_MAX_ENTRIES", "not-a-number")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for invalid CACHE_MAX_ENTRIES")
	}
}

func TestLoadParsesEncryptionKey(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	t.Setenv("ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(key))

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if len(cfg.EncryptionKey) != 32 {
		t.Fatalf("len(EncryptionKey) = %d, want 32", len(cfg.EncryptionKey))
	}
	for i := range key {
		if cfg.EncryptionKey[i] != key[i] {
			t.Fatalf("EncryptionKey[%d] = %d, want %d", i, cfg.EncryptionKey[i], key[i])
		}
	}
}

func TestLoadRejectsInvalidBase64EncryptionKey(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("ENCRYPTION_KEY", "not-valid-base64!!!")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for invalid base64 ENCRYPTION_KEY")
	}
}

func TestLoadRejectsWrongLengthEncryptionKey(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("ENCRYPTION_KEY", base64.StdEncoding.EncodeToString([]byte("too-short")))

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for an ENCRYPTION_KEY that isn't 32 bytes")
	}
}

func TestLoadParsesCacheTTL(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("CACHE_TTL", "2s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if cfg.CacheTTL != 2*time.Second {
		t.Fatalf("CacheTTL = %v, want 2s", cfg.CacheTTL)
	}
}

func TestLoadRejectsInvalidCacheTTL(t *testing.T) {
	t.Setenv("S3_BUCKET", "my-bucket")
	t.Setenv("CACHE_TTL", "not-a-duration")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error for invalid CACHE_TTL")
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
