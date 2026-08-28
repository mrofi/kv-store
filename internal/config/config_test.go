package config

import (
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
