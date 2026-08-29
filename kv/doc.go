// Package kv implements the core key-value service: it ties together an
// S3-backed store (see package store) with an in-memory cache and
// optional peer replication (see package peers) into the Put, Get,
// GetVersion, ListVersions, and Delete operations a caller sees.
//
// S3 is always the source of truth. The cache is a latency optimization
// only — every read confirms freshness against S3 (via a Head request)
// unless a short trusted-freshness window is configured via NewService's
// cacheTTL parameter, and every write lands in S3 before it's considered
// durable. Peer pushes (through the Pusher interface) let other
// instances warm their own caches without paying their own S3 round
// trip, but correctness never depends on a push arriving: an instance
// with no pusher, or one whose pushes are all dropped, still serves
// correct data, just with an extra S3 call on the next read.
//
// See the "Consistency model" section of the project README for the
// specific, deliberately accepted trade-offs this design makes around
// concurrent writes to the same key across different instances.
package kv
