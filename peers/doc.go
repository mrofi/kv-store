// Package peers implements cache-warm replication between kv-store
// instances: a Registry tracking the current set of peer base URLs, an
// HTTPPusher that broadcasts writes and deletes to them, and HTTP
// handlers (NewPushHandler, NewDeleteHandler, NewSetPeersHandler) for
// receiving that traffic on the other end.
//
// Replication here is deliberately best-effort. A dropped or
// out-of-order push never produces incorrect data, only a cache miss
// (or, briefly, a stale cache hit within a configured CACHE_TTL window)
// that self-corrects on the next read via package kv's S3 freshness
// check. See that package's, and the project README's, documentation
// for the specific edge cases this trade-off accepts.
package peers
