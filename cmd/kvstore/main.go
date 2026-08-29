package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	kvconfig "kvstore/internal/config"
	"kvstore/internal/httpapi"
	"kvstore/kv"
	"kvstore/peers"
	"kvstore/store"
)

func main() {
	cfg, err := kvconfig.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// Go's default http.Transport only keeps 2 idle connections per host.
	// Every request beyond that needs a fresh TCP+TLS handshake to S3,
	// which under concurrent load shows up as a long tail of slow
	// requests even though S3 itself responded quickly. Raise the pool
	// so concurrent requests can reuse warm connections instead.
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			MaxConnsPerHost:     100,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	awsCfgOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AWSRegion),
		config.WithHTTPClient(httpClient),
	}
	if cfg.S3AccessKeyID != "" {
		awsCfgOpts = append(awsCfgOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, cfg.S3SessionToken),
		))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), awsCfgOpts...)
	if err != nil {
		log.Fatalf("aws config: %v", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.S3Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		}
		o.UsePathStyle = cfg.S3ForcePathStyle
	})

	cache := store.NewCache(cfg.CacheMaxEntries)
	var s3Store store.S3Store = store.NewAWSS3Store(s3Client, cfg.S3Bucket)
	if cfg.EncryptionKey != nil {
		encStore, err := store.NewEncryptedS3Store(s3Store, cfg.EncryptionKey)
		if err != nil {
			log.Fatalf("encryption: %v", err)
		}
		s3Store = encStore
	}
	registry := peers.NewRegistry(cfg.SelfAddr, cfg.Peers)
	pusher := peers.NewHTTPPusher(registry, cfg.InternalAuthToken, 2*time.Second)
	svc := kv.NewService(cache, s3Store, pusher, cfg.MaxVersions, cfg.CacheTTL)

	server := httpapi.NewServer(svc, registry, cfg.InternalAuthToken, cfg.MaxValueSize)

	// A zero-value http.Server (what http.ListenAndServe uses) has no
	// timeouts at all — a slow or hanging client connection can hold a
	// goroutine and its resources open indefinitely.
	httpServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           server,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	log.Printf("kv-store listening on :%s", cfg.Port)
	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}
