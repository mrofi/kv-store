# kv-store

A distributed key-value store backed by S3, with in-memory LRU caching,
peer-to-peer cache warming, native versioning, and optional AES-256
encryption — no separate database cluster required.

S3 (or any S3-compatible provider, e.g. MinIO) is the
durable, versioned source of truth. Each instance keeps an in-memory
cache for fast reads, confirmed fresh against S3 on every access unless
a short trusted-freshness window (`CACHE_TTL`) is configured. Writes
land in S3 first, then get pushed asynchronously to peer instances so
their caches warm up without waiting on their own S3 round trip.

## Why

Off-the-shelf options for this (Redis with a backup job, DynamoDB,
etcd) all mean running and paying for another stateful service. This
design leans entirely on S3's own durability, versioning, and strong
read-after-write consistency, so the only infrastructure required is
the application instances themselves plus a bucket.

## Features

- **S3-backed, versioned storage** — every write is a new S3 object
  version; nothing is overwritten in place. History is available via
  the API with zero custom bookkeeping.
- **In-memory LRU cache** — bounded via `CACHE_MAX_ENTRIES`, so memory
  use doesn't grow without limit as key cardinality increases.
- **Optional short-TTL fast path** (`CACHE_TTL`) — skip the S3
  freshness check entirely within a trusted window, trading a bounded
  amount of staleness for a cache hit that costs zero network calls.
- **Peer-to-peer cache warming** — writes get pushed to configured
  peers so they don't have to pay their own S3 round trip on the next
  read. Correctness never depends on this working; it's purely a
  latency optimization.
- **Optional encryption at rest** (`ENCRYPTION_KEY`) — AES-256-GCM,
  applied transparently to whatever is written to S3. The in-memory
  cache and API responses stay plaintext.
- **Version capping** (`MAX_VERSIONS`) — old versions are pruned after
  every write so history doesn't grow unbounded.
- **Usable as a library** — `store` and `kv` are plain, composable Go
  packages with no dependency on the HTTP layer; embed the storage
  engine directly in your own service instead of running this as a
  separate process.

## Running as a standalone service

```bash
export S3_BUCKET=my-bucket
export AWS_REGION=us-east-1
go run ./cmd/kvstore
```

Against a self-hosted MinIO (or any other S3-compatible provider)
instead of real AWS S3, add `S3_ENDPOINT`:

```bash
export S3_BUCKET=my-bucket
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
go run ./cmd/kvstore
```

### Configuration

**S3 connection**

| Env var | Required | Default | Purpose |
|---|---|---|---|
| `S3_BUCKET` | yes | — | Target bucket name (must have versioning enabled) |
| `AWS_REGION` | no | `us-east-1` | S3 region |
| `S3_REGION` | no | *(none)* | Alias for `AWS_REGION`; takes precedence over it when set |
| `S3_ENDPOINT` | no | *(none, real AWS S3)* | Custom S3 endpoint URL, for MinIO or any other S3-compatible provider |
| `S3_FORCE_PATH_STYLE` | no | `true` if `S3_ENDPOINT` is set, else `false` | Use path-style bucket addressing instead of virtual-hosted-style; most non-AWS S3-compatible providers need this |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | no | *(standard AWS SDK credential chain)* | Not a custom var of this project — resolved automatically via env vars, a shared `~/.aws/credentials` file, or an IAM role |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` / `S3_SESSION_TOKEN` | no | *(none)* | Alias for the `AWS_*` vars above, for when a non-AWS provider makes "AWS_"-prefixed names feel out of place. When set, these take precedence over the standard credential chain. `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` must be set together |

**Application settings**

| Env var | Required | Default | Purpose |
|---|---|---|---|
| `PORT` | no | `8080` | Client-facing HTTP port |
| `PEERS` | no | *(none)* | Comma-separated initial peer base URLs, mutable at runtime via `PUT /internal/peers` |
| `SELF_ADDR` | no | *(none)* | This instance's own address, to exclude itself from its peer list |
| `MAX_VERSIONS` | no | `10` | Max historical versions retained per key |
| `MAX_VALUE_SIZE` | no | `10485760` (10 MiB) | Max `PUT` body size in bytes; larger requests get `413 Request Entity Too Large` |
| `INTERNAL_AUTH_TOKEN` | no | *(none)* | Shared secret for `/internal/*` endpoints; unset disables the check |
| `CACHE_TTL` | no | `0` (disabled) | Duration (e.g. `2s`) a cache hit is trusted without re-checking S3 |
| `CACHE_MAX_ENTRIES` | no | `0` (unbounded) | Max entries held in the in-memory cache before LRU eviction kicks in |
| `ENCRYPTION_KEY` | no | *(none)* | Base64-encoded 32-byte AES-256 key; enables encryption of values written to S3 |

Generate an encryption key with `openssl rand -base64 32`.

### API

| Method | Path | Purpose |
|---|---|---|
| `PUT` | `/kv/{key}` | Create or update (upsert) |
| `GET` | `/kv/{key}` | Get latest value |
| `GET` | `/kv/{key}?version={id}` | Get a specific historical version |
| `GET` | `/kv/{key}/versions` | List version history |
| `DELETE` | `/kv/{key}` | Delete (leaves a version-history trail) |
| `GET` | `/healthz` | Liveness — always `200` while the process is running, regardless of S3 |
| `GET` | `/readyz` | Readiness — `200` if S3 is reachable, `503` if not |
| `POST` | `/internal/push` | Peer cache-warm push (cluster-internal) |
| `POST` | `/internal/delete` | Peer delete notification (cluster-internal) |
| `PUT` | `/internal/peers` | Replace the runtime peer list (cluster-internal) |

`GET` responses carry `X-Version-Id`, `X-ETag`, `X-Updated-At`, and
(when the value was served from cache without a fresh S3 confirmation)
`X-Stale: true`. Keys must be non-empty, contain no `/`, and be ≤512
bytes.

## Docker

Every push of a `v*.*.*` tag builds and publishes a multi-arch image
(`linux/amd64`, `linux/arm64`, `linux/arm/v7`) to GitHub Container
Registry via [`.github/workflows/docker.yml`](.github/workflows/docker.yml).
Pull requests trigger a build-only check (no push) so a broken
Dockerfile fails CI before it can reach a tag.

```bash
docker pull ghcr.io/mrofi/kv-store:latest    # most recent release
docker pull ghcr.io/mrofi/kv-store:1.2.3     # a specific version
docker pull ghcr.io/mrofi/kv-store:1.2       # latest patch of a minor version
```

```bash
docker run -p 8080:8080 \
  -e S3_BUCKET=my-bucket \
  -e S3_ACCESS_KEY_ID=... \
  -e S3_SECRET_ACCESS_KEY=... \
  ghcr.io/mrofi/kv-store:latest
```

**Cutting a release**: tag and push — no separate build step needed.

```bash
git tag v1.2.3
git push origin v1.2.3
```

The image runs as a non-root user on a minimal
[distroless](https://github.com/GoogleContainerTools/distroless) base
(no shell, no package manager) — smaller attack surface, and it's just
the statically-linked binary plus CA certificates.

## Using it as a library

`store` and `kv` have no dependency on the HTTP layer — embed the
engine directly in your own service:

```go
cache := store.NewCache(maxEntries)
var s3Store store.S3Store = store.NewAWSS3Store(client, bucket)
// optionally wrap it for encryption at rest:
// s3Store, err = store.NewEncryptedS3Store(s3Store, encryptionKey)

registry := peers.NewRegistry(selfAddr, peerURLs)
pusher := peers.NewHTTPPusher(registry, authToken, timeout)
svc := kv.NewService(cache, s3Store, pusher, maxVersions, cacheTTL)

// mount the peer-facing endpoints on your own mux if you want replication:
mux.Handle("POST /internal/push", peers.NewPushHandler(svc, authToken))
mux.Handle("POST /internal/delete", peers.NewDeleteHandler(svc, authToken))
mux.Handle("PUT /internal/peers", peers.NewSetPeersHandler(registry, authToken))

// use the engine directly:
entry, err := svc.Put(ctx, "my-key", []byte("value"))
entry, stale, err := svc.Get(ctx, "my-key")
```

Pass `nil` as the pusher if you don't need multi-instance replication —
`kv.Service` degrades to a single-instance in-process cache with no
peer dependency.

## Testing

```bash
go test ./...                        # unit tests
go test -tags=integration ./...      # + integration tests (needs a local MinIO)
```

See `deploy/docker-compose.minio.yml` for a local MinIO setup for
integration testing.

## Security model

**Peer authentication is a shared secret, not per-peer identity — this
is an accepted trust boundary, not an oversight.** `INTERNAL_AUTH_TOKEN`
is one token for the whole cluster: every peer presents the same value,
and `/internal/push`, `/internal/delete`, and `/internal/peers` accept
any request bearing it. A peer can't distinguish "this really is peer B"
from "this is anyone who has the token."

**What this protects against**: a network-level attacker who can reach
these endpoints but doesn't hold the token — the common case for a
cluster on a private network/VPC that isn't otherwise exposed.

**What this does *not* protect against**: a genuinely compromised
instance. If an attacker has compromised a node badly enough to read its
environment variables, they already have that node's **S3 credentials**
in the same environment — meaning they can directly read, overwrite, or
delete anything in the bucket, not just push fabricated data to peers'
caches. Cache poisoning via `/internal/push` self-corrects on the next
real S3 check (immediately with `CACHE_TTL` disabled, otherwise within
the TTL window); a compromised S3 credential does not self-correct at
all. Per-peer tokens wouldn't close this gap either — in a full mesh,
every peer ends up holding every other peer's verification secret (it
has to, to verify them), so compromising any one node leaks the
credentials needed to impersonate the others. Closing that gap for real
needs asymmetric signing (each node signs with a private key it never
shares; peers verify with the corresponding public key, which is safe to
leak) — deliberately not built here, since it's real added complexity for
a threat this project's actual blast radius (a compromised node's S3
credentials) already dwarfs.

**The practical mitigation** is standard infrastructure security, not
something this application's peer-auth scheme can substitute for:
- Scope each instance's S3 credentials as tightly as IAM allows (only the
  actions and bucket this service actually needs — not broader account
  access).
- Keep `INTERNAL_AUTH_TOKEN` and S3 credentials out of plain env vars
  where a secrets manager is available, and rotate both if either is
  ever suspected of leaking.
- Run instances on a private network/VPC not reachable from the public
  internet; `/internal/*` endpoints should never be exposed externally.

## Consistency model

**Concurrent writes to the same key from different instances are last-
write-wins, decided entirely by S3** — there's no coordination between
instances, so whichever `PutObject` call physically lands last in S3 is
the version that survives. This is a deliberate trade-off for staying
infrastructure-free (no consensus protocol, no leader election), and is
usually invisible in practice: it only matters when two instances
genuinely write the *same key* within a very tight window.

**A consequence worth knowing about explicitly**: when that race
happens, the peer-push-warmed cache can end up serving a *different*
answer than S3's actual final state for up to the `CACHE_TTL` window.
Each write's ordering signal (`UpdatedAt`) is stamped from the writing
instance's own local wall clock at the moment its `PutObject` call
returns — not anything S3 itself reports (S3's `PutObject` response
carries no timestamp) — so under a genuine two-instance race, "whoever's
local clock read later" and "whichever write S3 actually committed last"
can disagree. `CACHE_TTL=0` (the default) is unaffected, since every read
re-confirms freshness against S3 directly regardless of what's cached.

This was caught empirically: an end-to-end test with two real instances
racing to write one key, pushing to a third with a long `CACHE_TTL`,
disagreed with S3's ground truth in roughly 30-40% of runs — see
`TestConcurrentPushesFromMultipleInstancesDoNotCorruptCache` in
`test/integration/full_stack_test.go`. Closing this for real would mean
an extra `HeadObject` call after every write to get S3's own authoritative
timestamp instead of a local one — a real latency cost on every write, to
narrow an edge case that's already an accepted consequence of skipping
coordination between instances. Not built here; if your workload expects
frequent concurrent writes to the same keys, keep `CACHE_TTL` low or
disabled for those keys.

**A related case: a delete on one instance can be resurrected by a push
from another.** Within a single instance, `Put` and `Delete` on the same
key are serialized against each other, so this can't happen locally. But
across instances there's no shared lock: if instance A's `Put` push for a
key arrives at instance B *after* B has already deleted that same key
(via its own `Delete`, or a peer's delete-push), B's cache has no entry
to compare the incoming push against — so it's accepted unconditionally,
even though it's stale. The deleted value briefly reappears in B's cache
until the next S3 `Head` check corrects it (again, immediately if
`CACHE_TTL=0`, otherwise within the TTL window).

Closing this fully would mean tracking a tombstone (a `key -> deletedAt`
record) so a push arriving after a more recent delete gets rejected the
same way an out-of-order push already does. That's a real mitigation,
not a full fix, since the tombstone's timestamp is still each instance's
own local wall clock — the same root cause as above — so it narrows the
window to ordinary clock skew between instances rather than eliminating
it, at the cost of new state to maintain (bounded retention, another
cleanup path). Given that residual caveat, it's left undone here for the
same reason as the rest of this section: accepted as a bounded,
self-correcting edge case rather than a coordination problem worth
building infrastructure to fully close.

## Deployment notes (S3 bucket setup)

- Enable versioning on the bucket:
  `aws s3api put-bucket-versioning --bucket <bucket> --versioning-configuration Status=Enabled`
- Add a lifecycle rule (`NoncurrentVersionExpiration` +
  `NewerNoncurrentVersions` set to the same value as `MAX_VERSIONS`) as
  a backstop for the app-side version pruning.

## License

MIT, with the [Commons Clause](https://commonsclause.com/) — free to
use, modify, and embed in your own products, but not to resell as a
hosted SaaS/PaaS offering without a separate agreement. See
[LICENSE](./LICENSE).
