// Package store provides the durable and in-memory storage primitives
// kv-store is built on: the S3Store interface for versioned,
// S3-compatible object storage, and Cache, an in-memory LRU cache used
// to serve reads without a network round trip.
//
// Three S3Store implementations are provided: AWSS3Store talks to real
// S3 or any S3-compatible provider (MinIO, IDrive e2, etc.) via the AWS
// SDK; EncryptedS3Store wraps another S3Store to transparently encrypt
// values at rest with AES-256-GCM; and FakeS3Store is an in-memory
// implementation for tests that don't need a real network dependency.
package store
