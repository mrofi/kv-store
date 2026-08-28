package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port              string
	S3Bucket          string
	AWSRegion         string
	SelfAddr          string
	Peers             []string
	MaxVersions       int
	InternalAuthToken string
	CacheTTL          time.Duration
	EncryptionKey     []byte
	CacheMaxEntries   int
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

	if raw := os.Getenv("CACHE_TTL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid CACHE_TTL %q: %w", raw, err)
		}
		cfg.CacheTTL = d
	}

	if raw := os.Getenv("ENCRYPTION_KEY"); raw != "" {
		key, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid ENCRYPTION_KEY (must be base64): %w", err)
		}
		if len(key) != 32 {
			return Config{}, fmt.Errorf("config: ENCRYPTION_KEY must decode to exactly 32 bytes, got %d", len(key))
		}
		cfg.EncryptionKey = key
	}

	if raw := os.Getenv("CACHE_MAX_ENTRIES"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return Config{}, fmt.Errorf("config: invalid CACHE_MAX_ENTRIES %q: %w", raw, err)
		}
		cfg.CacheMaxEntries = n
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
