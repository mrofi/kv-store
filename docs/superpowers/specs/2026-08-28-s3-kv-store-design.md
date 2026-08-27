# S3-Backed KV Store — Design

Status: Approved (pending final spec review)
Author: m.rofi@kata.ai
Date: 2026-08-28

## 1. Purpose & Context

An internal key-value storage service, deployed as multiple identical
instances, that uses S3 as the durable, versioned source of truth and
an in-memory map per instance as a cache for fast reads. Built instead
of an off-the-shelf option (Redis+backup, DynamoDB, etcd) for cost and
operational simplicity: no separate database cluster to run, just app
instances plus an S3 bucket the team already manages.

This is a real internal tool, not a learning exercise or take-home —
it needs to survive real failure modes (a dead peer, a slow/erroring
S3 call), not just the happy path. Scale is not yet known; the design
targets a small-to-medium workload (thousands to low hundreds of
thousands of keys, KB-to-low-MB values, moderate read/write rate) and
notes where it would need to change at larger scale.

## 2. Architecture & Components

Standalone Go service, deployed as N replicas (e.g. 3+ pods/VMs)
behind a load balancer for client traffic. Every instance is identical
and runs:

- **In-memory store** — `map[string]Entry` where
  `Entry{Value []byte, VersionID string, ETag string, UpdatedAt time.Time}`.
  Purely a cache; never treated as a source of truth.
- **S3 client** — talks to one bucket with **native S3 object
  versioning enabled**. S3 is the shared, strongly-consistent, durable,
  versioned store across all instances — the design leans on this
  rather than reinventing versioning or coordination.
- **Client-facing HTTP API** — the public KV interface
  (GET/PUT/DELETE), see §6.
- **Internal push endpoint** — a small HTTP endpoint peers call to
  warm each other's cache after a write. Not required for
  correctness — a latency optimization only.
- **Peer list** — loaded from config at startup, but mutable at
  runtime via an internal admin endpoint (no restart needed to
  add/remove a peer). Guarded by a mutex; used only for the
  cache-warm push, never for correctness.

Because S3 already provides a shared, strongly-consistent, durable,
versioned store, instances don't need to coordinate with each other
for correctness — only to shave latency off cache misses. There's no
consensus protocol, no leader election, and no split-brain scenario to
design around.

## 3. Data Model & S3 Layout

- Each key maps to one S3 object at `kv/{key}`. Key names are
  restricted to an S3-object-key-safe charset.
- The object **body is the raw value** the client sent — no custom
  JSON envelope and no hand-rolled version numbers. S3's own
  `VersionId`, `ETag`, and `LastModified` on that object serve as the
  version metadata.
- Bucket versioning is enabled, so every `PutObject` automatically
  retains the prior version — full history is available via S3's
  version listing with no extra bookkeeping.
- `DELETE` writes an S3 delete marker (via `DeleteObject`), preserving
  history — the key reads as "not found" via GET but old versions
  remain fetchable by explicit version ID.

### Version cap

Unbounded version history grows storage cost indefinitely, so a
`MAX_VERSIONS` config value (default 10) caps how many historical
versions are retained per key. Enforced two ways:

1. **App-side pruning** — after every write, the app lists that key's
   noncurrent versions and deletes any beyond the newest
   `MAX_VERSIONS`. Enforces the cap immediately; runs async after the
   response, failures are logged and not surfaced to the client.
2. **S3 Lifecycle rule** (backstop) — the bucket has a lifecycle rule
   (`NoncurrentVersionExpiration` + `NewerNoncurrentVersions =
   MAX_VERSIONS`) that prunes older noncurrent versions automatically
   on S3's own schedule (can lag up to ~24-48h). Covers the case where
   an app-side prune call fails or is skipped.

## 4. Write Path (CREATE / UPDATE)

1. Client sends `PUT /kv/{key}` with the value as the request body.
2. Instance calls S3 `PutObject`. This is the **only** durability
   requirement for a successful response — the write does not wait on
   peer acknowledgment. S3 returns a new `VersionId`/`ETag`.
3. Instance updates its own local cache entry immediately.
4. Instance responds success to the client.
5. Two things then happen asynchronously, fire-and-forget:
   - **Cache warm**: POST `{key, value, versionId, etag}` to every
     peer's `/internal/push`. Each peer overwrites its cache entry
     unconditionally — no version comparison needed, since a stale
     push self-corrects on that peer's next read via the HEAD check
     (§5). A peer that's down or slow is skipped and logged; no
     retries, no queue — this path only affects latency, not
     correctness.
   - **Prune**: enforce `MAX_VERSIONS` as described in §3.

### Accepted limitation: concurrent writes

Concurrent writes to the same key from different instances are not
coordinated — S3 serializes `PutObject` calls and the last one to
arrive wins (last-write-wins). This is an accepted trade-off given the
chosen consistency bar and scale; it is not a bug to fix later without
a deliberate design change (e.g. conditional/compare-and-swap writes
via S3's `If-Match` support, if stricter semantics are ever needed).

## 5. Read Path (GET) & Failure Handling

1. Client sends `GET /kv/{key}`.
2. **If the key is in the local cache**: issue a cheap S3
   `HeadObject` to get the current `VersionId`/`ETag`.
   - Matches local cache → serve from memory instantly, no body
     transfer.
   - Differs → `GetObject` to fetch the fresh body, update the cache,
     serve it.
3. **If not in the local cache**: `GetObject` directly, populate the
   cache, serve it.
4. If S3 reports the object doesn't exist → `404`.
5. **If S3 itself is unreachable or erroring** (network blip,
   throttling): fall back to serving the local cached value if one
   exists, with an `X-Stale: true` response header so callers can
   detect it; if there's no cached value, return `503`. This favors
   availability over strict correctness only in the narrow window
   where S3 itself is degraded — every other case confirms freshness
   against S3 before answering.

Peer broadcast plays no role in read correctness — it only affects
whether step 2 is a cache hit (fast) or a miss requiring `GetObject`
(slower but still correct). This is the core design choice that avoids
peer-broadcast reads: fanning a GET out to every peer and comparing
timestamps would require solving peer-timeout policy and would need a
monotonic version counter instead of wall-clock time to be correct
across machines with clock skew. Checking S3 directly (strongly
consistent since 2020) sidesteps both problems since it's already the
authoritative store.

There is no cold-start preload: a freshly started instance has an
empty cache and fills it lazily via the cache-aside pattern on first
GET per key, since reads always verify freshness against S3 regardless
of cache state.

## 6. API Surface

### Client-facing

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/kv/{key}` | Create or update (upsert) |
| `GET` | `/kv/{key}` | Get latest value |
| `GET` | `/kv/{key}?version={id}` | Get a specific historical version |
| `GET` | `/kv/{key}/versions` | List version history (id, last-modified, etag) |
| `DELETE` | `/kv/{key}` | Delete (writes a delete marker; history remains) |
| `GET` | `/healthz` | Liveness/readiness |

### Internal (cluster-only, not client-facing)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/internal/push` | Peer cache-warm push after a write |
| `PUT` | `/internal/peers` | Replace the runtime peer list |

### Multi-cluster internal auth

Internal endpoints can cross network boundaries that aren't inherently
trusted (unlike same-cluster pod networking), so calls to
`/internal/push` and `/internal/peers` carry an optional
`X-Internal-Auth: <shared-secret>` header. The receiving instance
validates it against a configured `INTERNAL_AUTH_TOKEN` before
accepting the call. If `INTERNAL_AUTH_TOKEN` is unset, the check is
skipped — fine for a single trusted cluster/network.

## 7. Configuration

| Env var | Purpose |
|---|---|
| `S3_BUCKET` | Target bucket name |
| `AWS_REGION` | S3 region (standard AWS credential resolution / IAM role applies) |
| `PORT` | Client-facing HTTP port |
| `PEERS` | Initial comma-separated peer base URLs (mutable at runtime via `/internal/peers`) |
| `SELF_ADDR` | This instance's own address, to exclude itself from its peer list |
| `MAX_VERSIONS` | Max historical versions retained per key (default 10) |
| `INTERNAL_AUTH_TOKEN` | Optional shared secret for `/internal/*` endpoints; empty disables the check |

## 8. Testing Plan

- **Unit tests**: cache hit/stale-detection comparator logic; S3
  client wrapper against a mocked S3 interface; prune-to-`MAX_VERSIONS`
  logic.
- **Integration tests**: run 2-3 instances against a local
  S3-compatible store (MinIO):
  - Write on instance A is visible on instance B near-instantly via
    push.
  - Write on A is still visible on C after a simulated push failure,
    via the HEAD-check fallback on the next read.
  - Version listing/fetch and delete-marker behavior.
  - Version count stays capped at `MAX_VERSIONS` after repeated writes
    to the same key.
  - `/internal/push` and `/internal/peers` reject calls with a missing
    or wrong `X-Internal-Auth` when `INTERNAL_AUTH_TOKEN` is set.
- **Manual verification**: run the service, exercise all endpoints via
  curl, kill a peer mid-run and confirm writes/reads keep working
  (push loss tolerated, self-corrects via HEAD check).

## 9. Explicitly Out of Scope

- Strong consistency for concurrent writes to the same key
  (last-write-wins is accepted; see §4).
- Automatic peer discovery (Kubernetes service discovery, gossip, or a
  message broker) — peers are configured, not discovered.
- Bulk cache preload on startup — cache fills lazily.
