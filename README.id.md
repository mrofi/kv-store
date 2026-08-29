# kv-store

🇬🇧 [Read in English](README.md)

Distributed key-value store yang menggunakan S3 sebagai backend,
dilengkapi in-memory LRU cache, peer-to-peer cache warming, versioning
native, dan enkripsi AES-256 opsional — tanpa perlu cluster database
terpisah.

S3 (atau provider apa pun yang kompatibel dengan S3, misalnya MinIO)
menjadi sumber kebenaran (source of truth) yang durable dan
ter-versioning. Setiap instance menyimpan cache in-memory untuk
pembacaan cepat, yang selalu dikonfirmasi kesegarannya (fresh) terhadap
S3 di setiap akses, kecuali jendela waktu tepercaya singkat
(`CACHE_TTL`) dikonfigurasi. Penulisan data masuk ke S3 terlebih dahulu,
baru kemudian di-push secara asinkron ke instance-instance peer supaya
cache mereka ikut ter-warm tanpa harus menunggu round trip ke S3
sendiri.

## Kenapa

Opsi yang sudah jadi (off-the-shelf) untuk kebutuhan ini (Redis dengan
job backup, DynamoDB, etcd) semuanya berarti menjalankan dan membayar
layanan stateful tambahan. Desain ini sepenuhnya bersandar pada
durability, versioning, dan strong read-after-write consistency milik
S3 sendiri, sehingga infrastruktur yang dibutuhkan hanya instance
aplikasi itu sendiri plus satu bucket.

## Fitur

- **Penyimpanan berbasis S3 dengan versioning** — setiap penulisan
  menjadi versi objek S3 baru; tidak ada yang ditimpa di tempat
  (overwritten in place). Histori tersedia lewat API tanpa perlu
  bookkeeping custom.
- **Cache LRU in-memory** — dibatasi lewat `CACHE_MAX_ENTRIES`, sehingga
  penggunaan memory tidak tumbuh tanpa batas seiring bertambahnya
  jumlah key.
- **Fast path TTL singkat (opsional)** (`CACHE_TTL`) — lewati
  pengecekan kesegaran ke S3 sepenuhnya dalam jendela waktu tepercaya,
  menukar staleness yang terbatas dengan cache hit tanpa biaya network
  call sama sekali.
- **Peer-to-peer cache warming** — penulisan data di-push ke peer yang
  dikonfigurasi supaya mereka tidak perlu membayar round trip S3
  sendiri pada pembacaan berikutnya. Correctness tidak pernah
  bergantung pada ini; ini murni optimisasi latency.
- **Enkripsi at-rest (opsional)** (`ENCRYPTION_KEY`) — AES-256-GCM,
  diterapkan secara transparan ke apa pun yang ditulis ke S3. Cache
  in-memory dan respons API tetap plaintext.
- **Pembatasan jumlah versi** (`MAX_VERSIONS`) — versi lama dipangkas
  (pruned) setiap kali ada penulisan baru supaya histori tidak tumbuh
  tanpa batas.
- **Bisa dipakai sebagai library** — `store` dan `kv` adalah package Go
  biasa yang composable, tanpa dependency ke HTTP layer; sematkan
  (embed) storage engine ini langsung ke service Anda sendiri alih-alih
  menjalankannya sebagai proses terpisah.

## Menjalankan sebagai service mandiri

```bash
export S3_BUCKET=my-bucket
export AWS_REGION=us-east-1
go run ./cmd/kvstore
```

Untuk memakai MinIO yang self-hosted (atau provider lain yang
kompatibel dengan S3) alih-alih S3 AWS asli, tambahkan `S3_ENDPOINT`:

```bash
export S3_BUCKET=my-bucket
export S3_ENDPOINT=http://localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
go run ./cmd/kvstore
```

### Konfigurasi

**Koneksi S3**

| Env var | Wajib | Default | Kegunaan |
|---|---|---|---|
| `S3_BUCKET` | ya | — | Nama bucket target (harus mengaktifkan versioning) |
| `AWS_REGION` | tidak | `us-east-1` | Region S3 |
| `S3_REGION` | tidak | *(tidak ada)* | Alias untuk `AWS_REGION`; diprioritaskan di atasnya jika di-set |
| `S3_ENDPOINT` | tidak | *(tidak ada, S3 AWS asli)* | URL endpoint S3 custom, untuk MinIO atau provider lain yang kompatibel S3 |
| `S3_FORCE_PATH_STYLE` | tidak | `true` jika `S3_ENDPOINT` di-set, selain itu `false` | Gunakan path-style bucket addressing alih-alih virtual-hosted-style; kebanyakan provider S3-compatible non-AWS membutuhkan ini |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` | tidak | *(standard AWS SDK credential chain)* | Bukan variabel custom milik project ini — di-resolve otomatis lewat env var, file `~/.aws/credentials` bersama, atau IAM role |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` / `S3_SESSION_TOKEN` | tidak | *(tidak ada)* | Alias untuk variabel `AWS_*` di atas, untuk saat provider non-AWS membuat nama berprefiks "AWS_" terasa kurang pas. Jika di-set, variabel ini diprioritaskan di atas credential chain standar. `S3_ACCESS_KEY_ID` dan `S3_SECRET_ACCESS_KEY` harus di-set bersamaan |

**Pengaturan aplikasi**

| Env var | Wajib | Default | Kegunaan |
|---|---|---|---|
| `PORT` | tidak | `8080` | Port HTTP yang menghadap client |
| `PEERS` | tidak | *(tidak ada)* | Daftar base URL peer awal, dipisah koma, bisa diubah saat runtime lewat `PUT /internal/peers` |
| `SELF_ADDR` | tidak | *(tidak ada)* | Alamat instance ini sendiri, untuk mengecualikan dirinya dari daftar peer-nya |
| `MAX_VERSIONS` | tidak | `10` | Jumlah maksimum versi historis yang disimpan per key |
| `MAX_VALUE_SIZE` | tidak | `10485760` (10 MiB) | Ukuran maksimum body `PUT` dalam byte; request yang lebih besar akan mendapat `413 Request Entity Too Large` |
| `INTERNAL_AUTH_TOKEN` | tidak | *(tidak ada)* | Shared secret untuk endpoint `/internal/*`; jika tidak di-set, pengecekan dinonaktifkan |
| `CACHE_TTL` | tidak | `0` (nonaktif) | Durasi (misalnya `2s`) sebuah cache hit dipercaya tanpa mengecek ulang ke S3 |
| `CACHE_MAX_ENTRIES` | tidak | `0` (tanpa batas) | Jumlah maksimum entry yang ditampung cache in-memory sebelum LRU eviction berjalan |
| `ENCRYPTION_KEY` | tidak | *(tidak ada)* | Key AES-256 32-byte yang di-encode base64; mengaktifkan enkripsi untuk value yang ditulis ke S3 |

Buat encryption key dengan `openssl rand -base64 32`.

### API

| Method | Path | Kegunaan |
|---|---|---|
| `PUT` | `/kv/{key}` | Membuat atau memperbarui (upsert) |
| `GET` | `/kv/{key}` | Mengambil value terbaru |
| `GET` | `/kv/{key}?version={id}` | Mengambil versi historis tertentu |
| `GET` | `/kv/{key}/versions` | Menampilkan daftar histori versi |
| `DELETE` | `/kv/{key}` | Menghapus (tetap meninggalkan jejak histori versi) |
| `GET` | `/healthz` | Liveness — selalu `200` selama proses berjalan, terlepas dari status S3 |
| `GET` | `/readyz` | Readiness — `200` jika S3 bisa dijangkau, `503` jika tidak |
| `POST` | `/internal/push` | Push cache-warm dari peer (internal cluster) |
| `POST` | `/internal/delete` | Notifikasi delete dari peer (internal cluster) |
| `PUT` | `/internal/peers` | Mengganti daftar peer saat runtime (internal cluster) |

Respons `GET` membawa header `X-Version-Id`, `X-ETag`, `X-Updated-At`,
dan (ketika value disajikan dari cache tanpa konfirmasi segar ke S3)
`X-Stale: true`. Key harus tidak kosong, tidak mengandung `/`, dan
panjangnya ≤512 byte.

## Docker

Setiap push tag `v*.*.*` akan membangun dan mempublikasikan image
multi-arch (`linux/amd64`, `linux/arm64`, `linux/arm/v7`) ke GitHub
Container Registry lewat
[`.github/workflows/docker.yml`](.github/workflows/docker.yml). Pull
request memicu pengecekan build-only (tanpa push) supaya Dockerfile
yang rusak gagal di CI sebelum sempat mencapai tag.

```bash
docker pull ghcr.io/mrofi/kv-store:latest    # rilis paling baru
docker pull ghcr.io/mrofi/kv-store:1.2.3     # versi tertentu
docker pull ghcr.io/mrofi/kv-store:1.2       # patch terbaru dari sebuah minor version
```

```bash
docker run -p 8080:8080 \
  -e S3_BUCKET=my-bucket \
  -e S3_ACCESS_KEY_ID=... \
  -e S3_SECRET_ACCESS_KEY=... \
  ghcr.io/mrofi/kv-store:latest
```

**Membuat release**: tag lalu push — tidak perlu langkah build
terpisah.

```bash
git tag v1.2.3
git push origin v1.2.3
```

Image berjalan sebagai non-root user di atas base
[distroless](https://github.com/GoogleContainerTools/distroless)
minimal (tanpa shell, tanpa package manager) — attack surface lebih
kecil, dan isinya cuma binary statically-linked plus CA certificates.

## Memakainya sebagai library

`store` dan `kv` tidak punya dependency ke HTTP layer — sematkan engine
ini langsung ke service Anda sendiri:

```go
cache := store.NewCache(maxEntries)
var s3Store store.S3Store = store.NewAWSS3Store(client, bucket)
// opsional, bungkus untuk enkripsi at-rest:
// s3Store, err = store.NewEncryptedS3Store(s3Store, encryptionKey)

registry := peers.NewRegistry(selfAddr, peerURLs)
pusher := peers.NewHTTPPusher(registry, authToken, timeout)
svc := kv.NewService(cache, s3Store, pusher, maxVersions, cacheTTL)

// pasang endpoint yang menghadap peer di mux Anda sendiri kalau mau replikasi:
mux.Handle("POST /internal/push", peers.NewPushHandler(svc, authToken))
mux.Handle("POST /internal/delete", peers.NewDeleteHandler(svc, authToken))
mux.Handle("PUT /internal/peers", peers.NewSetPeersHandler(registry, authToken))

// pakai engine-nya langsung:
entry, err := svc.Put(ctx, "my-key", []byte("value"))
entry, stale, err := svc.Get(ctx, "my-key")
```

Berikan `nil` sebagai pusher jika Anda tidak butuh replikasi
multi-instance — `kv.Service` akan turun menjadi cache in-process
single-instance tanpa dependency ke peer.

## Testing

```bash
go test ./...                        # unit test
go test -tags=integration ./...      # + integration test (butuh MinIO lokal)
```

Lihat `deploy/docker-compose.minio.yml` untuk setup MinIO lokal untuk
integration testing.

## Model keamanan

**Autentikasi antar-peer memakai shared secret, bukan identitas
per-peer — ini adalah trust boundary yang memang disadari dan
diterima, bukan sebuah kelalaian.** `INTERNAL_AUTH_TOKEN` adalah satu
token untuk seluruh cluster: setiap peer menyodorkan nilai yang sama,
dan `/internal/push`, `/internal/delete`, serta `/internal/peers`
menerima request apa pun yang membawa token tersebut. Sebuah peer tidak
bisa membedakan "ini benar-benar peer B" dari "ini siapa pun yang
memegang token."

**Yang dilindungi oleh mekanisme ini**: penyerang di level jaringan
yang bisa menjangkau endpoint-endpoint ini tapi tidak memegang token —
kasus umum untuk cluster di private network/VPC yang memang tidak
terekspos ke luar.

**Yang *tidak* dilindungi oleh mekanisme ini**: instance yang
benar-benar sudah dikompromikan. Kalau penyerang berhasil
mengompromikan sebuah node sampai bisa membaca environment
variable-nya, mereka otomatis sudah punya **kredensial S3** node
tersebut di environment yang sama — artinya mereka bisa langsung
membaca, menimpa, atau menghapus apa pun di bucket, bukan cuma
mengirim data palsu ke cache peer lain. Cache poisoning lewat
`/internal/push` akan memperbaiki dirinya sendiri pada pengecekan S3
berikutnya (langsung jika `CACHE_TTL` nonaktif, atau dalam jendela TTL
jika aktif); kredensial S3 yang dikompromikan sama sekali tidak bisa
memperbaiki dirinya sendiri. Token per-peer pun tidak menutup celah ini
— dalam topologi full mesh, setiap peer pada akhirnya memegang secret
verifikasi milik semua peer lain (memang harus, untuk bisa
memverifikasi mereka), sehingga mengompromikan satu node saja
membocorkan kredensial yang dibutuhkan untuk menyamar sebagai peer
lain. Untuk benar-benar menutup celah ini dibutuhkan asymmetric
signing (tiap node menandatangani dengan private key yang tidak pernah
dibagikan; peer lain memverifikasi dengan public key pasangannya, yang
aman untuk dibocorkan) — sengaja tidak dibangun di sini, karena itu
kompleksitas tambahan yang nyata untuk sebuah threat yang blast radius
sebenarnya (kredensial S3 sebuah node yang dikompromikan) sudah jauh
lebih besar.

**Mitigasi praktisnya** adalah keamanan infrastruktur standar, dan
bukan sesuatu yang bisa digantikan oleh skema peer-auth aplikasi ini:
- Batasi kredensial S3 tiap instance seketat mungkin lewat IAM (hanya
  action dan bucket yang benar-benar dibutuhkan service ini — bukan
  akses account yang lebih luas).
- Jauhkan `INTERNAL_AUTH_TOKEN` dan kredensial S3 dari plain env var
  jika tersedia secrets manager, dan rotasi keduanya kalau salah
  satunya dicurigai bocor.
- Jalankan instance di private network/VPC yang tidak bisa dijangkau
  dari internet publik; endpoint `/internal/*` seharusnya tidak pernah
  diekspos ke luar.

## Model konsistensi

**Penulisan konkuren ke key yang sama dari instance berbeda bersifat
last-write-wins, dan sepenuhnya ditentukan oleh S3** — tidak ada
koordinasi antar-instance, jadi panggilan `PutObject` mana pun yang
secara fisik mendarat terakhir di S3, itulah versi yang bertahan. Ini
adalah trade-off yang memang disengaja demi tetap infrastructure-free
(tanpa consensus protocol, tanpa leader election), dan biasanya tidak
terasa dalam praktiknya: ini baru jadi masalah kalau dua instance
benar-benar menulis key yang sama dalam jendela waktu yang sangat
sempit.

**Konsekuensi yang perlu diketahui secara eksplisit**: ketika race itu
terjadi, cache yang di-warm lewat peer-push bisa berakhir menyajikan
jawaban yang *berbeda* dari state final S3 yang sebenarnya, sampai
jendela `CACHE_TTL` berakhir. Sinyal urutan tiap penulisan (`UpdatedAt`)
diambil dari local wall clock instance penulis sendiri pada saat
panggilan `PutObject`-nya kembali, bukan dari apa pun yang dilaporkan
S3 sendiri (respons `PutObject` milik S3 tidak membawa timestamp) —
sehingga dalam race dua-instance yang sesungguhnya, "clock lokal siapa
yang membaca waktu lebih akhir" dan "penulisan mana yang benar-benar
di-commit terakhir oleh S3" bisa berbeda. `CACHE_TTL=0` (nilai default)
tidak terpengaruh, karena setiap pembacaan selalu mengonfirmasi ulang
kesegarannya langsung ke S3 terlepas dari apa yang ada di cache.

Ini ditemukan secara empiris: sebuah end-to-end test dengan dua
instance sungguhan yang berlomba menulis satu key, lalu push ke
instance ketiga dengan `CACHE_TTL` yang panjang, berbeda dari ground
truth S3 pada sekitar 30-40% dari total run — lihat
`TestConcurrentPushesFromMultipleInstancesDoNotCorruptCache` di
`test/integration/full_stack_test.go`. Untuk benar-benar menutup ini
dibutuhkan panggilan `HeadObject` tambahan setiap kali ada penulisan,
demi mendapatkan timestamp otoritatif milik S3 sendiri alih-alih
timestamp lokal — sebuah biaya latency nyata di setiap penulisan,
hanya untuk mempersempit edge case yang memang sudah diterima sebagai
konsekuensi dari tidak adanya koordinasi antar-instance. Tidak
dibangun di sini; kalau workload Anda mengharapkan penulisan konkuren
yang sering ke key yang sama, jaga `CACHE_TTL` tetap rendah atau
nonaktifkan untuk key-key tersebut.

**Kasus terkait: delete di satu instance bisa "dihidupkan lagi" oleh
push dari instance lain.** Dalam satu instance yang sama, `Put` dan
`Delete` pada key yang sama sudah diserialisasi satu sama lain,
sehingga ini tidak bisa terjadi secara lokal. Tapi antar-instance tidak
ada shared lock: kalau push `Put` dari instance A untuk sebuah key
sampai di instance B *setelah* B sudah menghapus key yang sama (lewat
`Delete`-nya sendiri, atau delete-push dari peer lain), cache B tidak
punya entry apa pun untuk dibandingkan dengan push yang masuk —
sehingga push itu diterima begitu saja tanpa syarat, walaupun
sebenarnya sudah basi (stale). Value yang sudah dihapus itu sempat
muncul lagi sebentar di cache B sampai pengecekan `Head` ke S3
berikutnya membetulkannya (lagi-lagi, langsung jika `CACHE_TTL=0`, atau
dalam jendela TTL jika aktif).

Menutup ini sepenuhnya berarti harus melacak tombstone (catatan
`key -> deletedAt`) supaya push yang datang setelah delete yang lebih
baru akan ditolak, sama seperti push yang out-of-order sudah ditolak
sekarang. Itu adalah mitigasi nyata, bukan perbaikan penuh, karena
timestamp tombstone-nya tetap berasal dari local wall clock
masing-masing instance, akar masalah yang sama seperti di atas,
sehingga hanya mempersempit jendelanya menjadi sebatas clock skew
biasa antar-instance, bukan menghilangkannya sama sekali, dengan biaya
berupa state baru yang harus dijaga (retention yang dibatasi, jalur
cleanup tambahan). Mengingat caveat yang masih tersisa itu, ini
sengaja tidak dikerjakan di sini dengan alasan yang sama seperti
bagian lain di dokumen ini: diterima sebagai edge case yang terbatas
dan bisa membetulkan dirinya sendiri, bukan sebuah masalah koordinasi
yang layak dibangunkan infrastruktur khusus untuk menutupnya
sepenuhnya.

## Catatan deployment (setup bucket S3)

- Aktifkan versioning pada bucket:
  `aws s3api put-bucket-versioning --bucket <bucket> --versioning-configuration Status=Enabled`
- Tambahkan lifecycle rule (`NoncurrentVersionExpiration` +
  `NewerNoncurrentVersions` di-set ke nilai yang sama dengan
  `MAX_VERSIONS`) sebagai backstop untuk pemangkasan versi di sisi
  aplikasi.

## Lisensi

MIT, dengan [Commons Clause](https://commonsclause.com/) — bebas
dipakai, dimodifikasi, dan disematkan ke produk Anda sendiri, tapi
tidak untuk dijual kembali sebagai layanan SaaS/PaaS hosted tanpa
perjanjian terpisah. Lihat [LICENSE](./LICENSE).
