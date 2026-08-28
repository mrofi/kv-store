package config

import (
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

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
