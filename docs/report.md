# Laporan UAS Sistem Paralel dan Terdistribusi

## Distributed Pub-Sub Log Aggregator

**Nama:** Naufal Tiarana Putra  
**NIM:** 11231071  
**Kelas:** A  
**Program Studi:** Informatika  
**Dosen Pengampu:** Riska Kurniyanto Abdullah, S.T., M.Kom.  
**Repository GitHub:** [Isi URL Repository]  
**Video Demo:** [Isi URL YouTube Unlisted/Public]

---

## Abstrak

Proyek ini membangun *Distributed Pub-Sub Log Aggregator* menggunakan FastAPI,
Redis Streams, PostgreSQL, dan Docker Compose. API menerima event tunggal atau
batch, kemudian menempatkannya pada Redis Stream untuk diproses oleh dua
consumer worker secara paralel. Idempotency dan deduplication dijamin secara
persisten melalui unique constraint `(topic, event_id)` dan pola SQL `INSERT
... ON CONFLICT DO NOTHING` di dalam transaksi PostgreSQL. Worker hanya
memberikan ACK setelah transaksi berhasil. Message yang gagal tetap berada
pada *Pending Entries List*, diambil kembali menggunakan `XAUTOCLAIM`, diberi
retry dengan exponential backoff, dan dipindahkan ke dead-letter stream
setelah batas percobaan tercapai.

Pengujian mencakup 30 unit/integration tests, uji konkurensi 20 pemrosesan
simultan terhadap event yang sama, uji persistence dengan recreate container
PostgreSQL, serta fault injection untuk retry dan dead-letter. Load test k6
mengirim 20.000 event dengan 30% duplikat menggunakan 20 virtual users. Hasil
menunjukkan 100% check berhasil, 0% HTTP failure, 14.000 event unik, dan 6.000
duplikat yang ditolak. Sistem juga menyediakan health, readiness, statistics,
queue lag, pending count, duplicate rate, throughput, audit log, dan named
volumes. Seluruh service berjalan pada jaringan lokal Docker Compose tanpa
ketergantungan layanan eksternal publik.

**Kata kunci:** distributed system, publish-subscribe, idempotency,
deduplication, transaction, concurrency, Redis Streams, PostgreSQL.

---

## 1. Pendahuluan

### 1.1 Latar Belakang

Sistem pengumpulan log menerima event dari banyak producer dan berpotensi
menghadapi pengiriman ulang, kegagalan jaringan, restart consumer, serta
pemrosesan paralel. Pada mekanisme *at-least-once delivery*, event yang sama
dapat diterima lebih dari sekali. Tanpa idempotency, duplikat dapat
menghasilkan data ganda, statistik yang salah, atau side effect yang berulang.
Masalah menjadi lebih sulit saat beberapa worker mencoba menyimpan event yang
sama secara bersamaan karena pengecekan biasa sebelum insert rentan terhadap
race condition.

Sistem terdistribusi juga memiliki karakteristik tidak adanya global clock,
concurrent execution, dan independent failure. Oleh sebab itu, correctness
tidak dapat bergantung pada urutan eksekusi ideal atau asumsi bahwa semua
komponen selalu tersedia. Rancangan harus menempatkan jaminan konsistensi pada
mekanisme atomik, menyediakan recovery, dan mempertahankan data di luar siklus
hidup container (Coulouris et al., 2012).

Proyek ini menerapkan prinsip tersebut dalam bentuk log aggregator
multi-service. Redis Streams memisahkan penerimaan dan pemrosesan event,
sedangkan PostgreSQL menjadi sumber kebenaran persisten. Unique constraint,
transaction, audit log, retry, pending recovery, dead-letter, healthcheck, dan
observability digunakan untuk menghasilkan perilaku yang dapat diuji.

### 1.2 Tujuan

Tujuan proyek adalah:

1. Membangun arsitektur Pub-Sub multi-service menggunakan Docker Compose.
2. Menyediakan API single dan batch event dengan validasi schema.
3. Menjamin event `(topic, event_id)` diproses maksimal satu kali pada storage.
4. Mencegah race condition saat dua worker memproses event yang sama.
5. Mempertahankan data setelah restart atau recreate container.
6. Menyediakan retry, pending recovery, dead-letter, logging, dan metrics.
7. Membuktikan correctness melalui automated tests dan load test 20.000 event.

### 1.3 Batasan

Sistem berjalan pada satu Docker host dan satu instance PostgreSQL serta Redis.
Sistem tidak menyediakan total ordering atau strict per-topic ordering.
Security difokuskan pada isolasi jaringan lokal Compose dan non-root
application image; authentication, TLS, multi-host replication, dan
production-grade secret management berada di luar cakupan proyek.

---

# 2. Bagian Teori

## T1. Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub

Sistem terdistribusi terdiri dari komponen yang berjalan pada proses atau
komputer berbeda dan berkomunikasi melalui jaringan. Karakteristik utamanya
adalah concurrency, tidak adanya global clock yang sempurna, serta kemungkinan
setiap komponen gagal secara independen. Konsekuensinya, keterlambatan respons
tidak selalu dapat dibedakan dari kegagalan, urutan penyelesaian operasi dapat
berbeda dari urutan pengiriman, dan correctness harus tetap dijaga ketika
request diulang. Heterogeneity, openness, scalability, security, failure
handling, dan transparency juga menjadi tantangan desain sistem terdistribusi
(Coulouris et al., 2012).

Pada proyek ini, arsitektur Pub-Sub memisahkan producer dari consumer. API
tidak menunggu proses database selesai; API memvalidasi event, menulisnya ke
Redis Stream, lalu mengembalikan status `202 Accepted`. Worker dapat
ditambahkan secara horizontal melalui consumer group. Trade-off-nya adalah
hasil tidak langsung tersedia setelah publish, event dapat dikirim ulang, dan
urutan selesai tidak selalu sama dengan urutan masuk. Broker juga menjadi
komponen infrastruktur tambahan yang harus dipersistenkan dan dimonitor.

Trade-off tersebut diterima karena kebutuhan utama adalah throughput,
decoupling, dan toleransi gangguan consumer. Ketidakpastian delivery ditangani
dengan idempotent consumer, unique constraint, ACK setelah commit, pending
recovery, dan dead-letter. Dengan demikian, desain tidak menghilangkan
karakteristik sulit sistem terdistribusi, tetapi mengelolanya melalui batas
transaksi dan recovery yang eksplisit.

## T2. Alasan Memilih Publish-Subscribe Dibanding Client-Server

Model client-server cocok ketika client membutuhkan respons langsung dari satu
server yang bertanggung jawab menyelesaikan operasi pada request tersebut.
Keterkaitan temporalnya kuat: client dan server harus aktif pada saat yang
sama, dan latency client mencakup seluruh proses bisnis. Sebaliknya,
publish-subscribe menggunakan indirect communication. Publisher mengirim
event ke broker atau channel tanpa mengetahui consumer tertentu, sedangkan
consumer memproses event secara independen. Pendekatan ini memberikan space,
time, dan synchronization decoupling yang lebih baik (Coulouris et al., 2012).

Log aggregator lebih sesuai memakai Pub-Sub karena producer hanya perlu
memastikan event diterima broker. Producer tidak perlu mengetahui jumlah
worker, alamat worker, atau status PostgreSQL. Ketika worker direstart, API
tetap dapat menerima event selama Redis tersedia. Dua worker dapat berbagi
beban menggunakan consumer group tanpa mengubah publisher. Redis Stream juga
menyimpan message dan status pending, sehingga event dapat dipulihkan jika
consumer gagal sebelum ACK.

Biayanya adalah eventual visibility dan kompleksitas reliability. Respons
`202` berarti event sudah masuk antrean, bukan pasti sudah tersimpan pada
`processed_events`. Sistem memerlukan endpoint `/events` dan `/stats` untuk
melihat hasil asynchronous. Broker, retry policy, idempotency, monitoring
pending message, dan dead-letter juga harus dirancang. Untuk kebutuhan
pengumpulan log dengan burst traffic, duplicate delivery, dan worker paralel,
trade-off tersebut lebih tepat daripada request sinkron client-server yang
langsung mengikat penerimaan event dengan latency database.

## T3. At-Least-Once dan Exactly-Once Delivery serta Peran Idempotent Consumer

*At-most-once delivery* menghindari pengiriman ulang tetapi dapat kehilangan
message ketika kegagalan terjadi. *At-least-once delivery* melakukan retry
hingga message diproses atau melewati kebijakan kegagalan, sehingga peluang
kehilangan lebih kecil tetapi duplicate delivery mungkin terjadi.
*Exactly-once* berarti efek logis hanya terjadi satu kali. Pada sistem nyata,
exactly-once end-to-end sulit dicapai karena broker, consumer, database, dan
ACK memiliki batas kegagalan berbeda. Consumer dapat berhasil commit ke
database lalu crash sebelum ACK; broker akan mengirim ulang message tersebut
(Coulouris et al., 2012).

Proyek ini menggunakan pendekatan at-least-once pada Redis Stream dan
exactly-once effect pada persistent storage melalui idempotent consumer.
Message baru dibaca dengan `XREADGROUP`. Setelah transaksi PostgreSQL sukses,
worker menjalankan `XACK`. Jika worker gagal sebelum ACK, message tetap
pending dan dapat diambil oleh worker lain memakai `XAUTOCLAIM`. Pengiriman
ulang aman karena insert menggunakan unique key `(topic, event_id)`.

Dengan pola ini, event dapat diterima berkali-kali tetapi hanya satu row
`processed_events` yang dibuat. Percobaan berikutnya tercatat sebagai
`duplicate_dropped`, bukan menghasilkan side effect ganda. Jaminan yang tepat
untuk laporan ini bukan exactly-once transport, melainkan at-least-once
delivery dengan idempotent processing. Batas jaminannya juga jelas:
correctness berlaku untuk seluruh side effect yang ditempatkan dalam transaksi
yang sama. Side effect eksternal di luar PostgreSQL memerlukan transactional
outbox atau mekanisme idempotency tambahan.

## T4. Skema Penamaan Topic dan Event ID untuk Deduplication

Nama merupakan dasar komunikasi dan identifikasi pada sistem terdistribusi.
Identifier yang baik harus memiliki ruang lingkup jelas, stabil, mudah
divalidasi, dan memiliki kemungkinan collision yang dapat diterima. Topic
berfungsi sebagai nama kategori event, sedangkan `event_id` mengidentifikasi
kejadian tertentu di dalam kategori tersebut. Pemisahan keduanya memungkinkan
dua domain memakai identifier lokal yang sama tanpa dianggap sebagai event
yang sama (Coulouris et al., 2012).

Sistem menggunakan nama topic hierarkis berbasis dot, misalnya `auth.login`,
`payment.created`, `order.placed`, dan `inventory.updated`. Pydantic membatasi
topic menjadi 1–100 karakter dengan pola `[a-zA-Z0-9_.-]+`. Aturan ini
mencegah nilai kosong dan karakter yang menyulitkan query, log, atau URL.
`event_id` berukuran 1–150 karakter. Pada production, producer sebaiknya
menghasilkan UUID/ULID atau identifier yang menggabungkan source dan sequence
persisten. Load test memakai `k6-{run_id}-{unique_index}`, sehingga setiap run
dapat diverifikasi dengan query prefix.

Deduplication tidak menggunakan `event_id` secara global, tetapi composite key
`(topic, event_id)`. Skema PostgreSQL menetapkan unique constraint
`uq_processed_event` pada pasangan tersebut. Ini collision-resistant selama
producer menjamin keunikan `event_id` dalam satu topic. Timestamp tidak
dipakai sebagai identitas karena dua event berbeda dapat memiliki timestamp
sama dan retry dapat membawa timestamp yang identik. Payload juga tidak
di-hash karena perubahan representasi JSON dapat menghasilkan hash berbeda
untuk kejadian yang secara semantik sama.

## T5. Ordering Praktis dan Dampaknya

Tidak adanya global clock membuat total ordering lintas proses mahal dan
sering tidak diperlukan. Physical timestamp dapat berbeda karena clock skew,
sedangkan urutan message yang diterima satu komponen belum tentu mewakili
urutan kejadian global. Logical clock dan sequence number dapat membentuk
causal atau per-source ordering ketika aplikasi benar-benar membutuhkan
hubungan “terjadi sebelum” (Coulouris et al., 2012).

Proyek ini tidak menjanjikan total ordering. Redis Stream memberikan ID yang
menunjukkan urutan append pada satu stream, tetapi consumer group membagi
message ke dua worker. Worker pertama dan kedua dapat memiliki waktu proses
berbeda, sehingga completion order dan urutan row database dapat berubah.
Kebijakan sistem adalah menerima event out-of-order. Timestamp producer
disimpan sebagai `event_timestamp`, sedangkan waktu penyimpanan dicatat pada
`created_at`. Deduplication tidak bergantung pada urutan, melainkan pada
`(topic, event_id)`.

Dampaknya, endpoint `/events` mengurutkan hasil berdasarkan ID database agar
response deterministik menurut urutan commit, bukan mengklaim urutan kejadian
global. Untuk log aggregation, kebijakan ini memaksimalkan parallelism dan
masih mempertahankan timestamp asli untuk analisis. Jika suatu topic
membutuhkan strict ordering, mitigasinya adalah partitioning deterministik,
misalnya hash topic/source ke satu worker, serta sequence number monotonik per
producer. Konsekuensinya adalah throughput per partition dibatasi oleh satu
consumer dan recovery harus mempertahankan ownership partition tersebut.

## T6. Failure Modes dan Mitigasi

Partial failure berarti satu komponen dapat gagal sementara komponen lain tetap
berjalan. Failure detector pada sistem asynchronous juga tidak sempurna:
timeout hanya menunjukkan respons belum diterima, bukan bukti pasti komponen
mati. Sistem karena itu memerlukan retry, duplicate suppression, persistence,
dan recovery yang tidak bergantung pada asumsi kegagalan tunggal (Coulouris et
al., 2012).

Failure mode pertama adalah worker crash sebelum transaksi. Message tidak
di-ACK sehingga tetap pending. Failure kedua adalah commit sukses tetapi crash
sebelum ACK. Message akan dikirim ulang, tetapi unique constraint menjadikan
retry idempotent. Failure ketiga adalah error sementara database; worker
menambah retry count, menerapkan exponential backoff 0,5; 1; dan seterusnya,
serta membiarkan message pending. `XAUTOCLAIM` memindahkan message idle ke
consumer sehat. Setelah tiga kegagalan, message masuk `events:deadletter` dan
audit log untuk pemeriksaan manual.

PostgreSQL dan Redis memakai named volumes untuk crash/recreate recovery.
Redis mengaktifkan AOF. Compose healthcheck mencegah worker dimulai sebelum
database, broker, dan API siap. API readiness memeriksa `SELECT 1` dan Redis
`PING`. Risiko retry storm dikurangi dengan backoff, walaupun jitter belum
diterapkan. Durable dedup store mencegah duplicate processing setelah restart.
Keterbatasannya, kegagalan total Docker host tidak ditangani melalui
replication; proyek hanya membuktikan recovery pada satu host sesuai cakupan
Compose lokal.

## T7. Eventual Consistency serta Peran Idempotency dan Deduplication

Pada distributed Pub-Sub, penerimaan event dan penyimpanan hasil terjadi pada
waktu berbeda. Setelah API mengembalikan `202 Accepted`, event sudah berada di
Redis Stream tetapi mungkin belum terlihat pada PostgreSQL. Keadaan sementara
ini merupakan bentuk eventual consistency: jika tidak ada kegagalan permanen
dan message terus diproses, state database pada akhirnya mencerminkan seluruh
event yang valid. Model ini meningkatkan availability dan decoupling, tetapi
client tidak boleh menganggap publish response sebagai bukti synchronous
commit (Coulouris et al., 2012).

Idempotency menjaga convergence ketika broker mengirim ulang event.
Deduplication persisten menentukan bahwa semua delivery dengan pasangan
`(topic, event_id)` yang sama menghasilkan satu row event. Unique constraint
menjadi aturan konsistensi global yang berlaku untuk semua worker dan tetap
ada setelah restart. Statistik `unique_processed`, `duplicate_dropped`, serta
`topic_stats` diperbarui secara atomik di transaksi yang sama dengan event dan
audit log, sehingga tidak terjadi state event berhasil tetapi counter gagal.

Endpoint `/stats`, `/metrics`, dan `/events` memberi observability terhadap
proses convergence. `consumer_group_lag`, pending count, serta
`unprocessed_estimate` membantu membedakan state yang belum konvergen dari
kegagalan permanen. Dead-letter menunjukkan message yang tidak dapat mencapai
state valid setelah retry. Dengan demikian, eventual consistency pada proyek
ini bukan berarti konsistensi dibiarkan tanpa batas; terdapat invariant
persisten, mekanisme recovery, serta indikator untuk mengetahui apakah sistem
masih menuju state konvergen.

## T8. Desain Transaksi: ACID, Isolation Level, dan Pencegahan Lost Update

Transaksi mengelompokkan beberapa operasi menjadi satu unit atomik. ACID
mencakup atomicity, consistency, isolation, dan durability. Pada proyek ini,
satu transaksi memuat insert ke `processed_events`, update counter global,
update `topic_stats`, dan insert `audit_logs`. Jika salah satu statement gagal,
seluruh perubahan di-rollback. Hal ini mencegah partial commit, misalnya event
tersimpan tetapi audit dan statistik tidak bertambah (Coulouris et al., 2012).

Isolation level yang dipilih adalah `READ COMMITTED`, yaitu default PostgreSQL.
Setiap statement melihat data yang telah di-commit sebelum statement tersebut
dimulai. Level ini tidak mencegah seluruh non-repeatable read atau phantom
read, tetapi operasi dedup proyek tidak menggunakan pola “SELECT lalu INSERT”.
Keputusan dilakukan atomik oleh `INSERT ... ON CONFLICT DO NOTHING RETURNING
id`, sehingga unique constraint menyelesaikan konflik concurrent insert.
PostgreSQL mendukung perilaku `ON CONFLICT` pada Read Committed untuk
menentukan satu hasil insert atau conflict secara konsisten (PostgreSQL Global
Development Group, n.d.-a).

Lost update pada counter dicegah dengan operasi server-side `value =
stats.value + delta`, bukan membaca nilai ke aplikasi lalu menulis hasil.
Upsert `topic_stats` menggunakan pola yang sama. `SERIALIZABLE` tidak dipilih
karena menambah kemungkinan serialization failure dan retry transaksi,
sedangkan invariant proyek sudah dijaga unique constraint dan atomic update.
Trade-off ini mempertahankan correctness sekaligus memungkinkan dua worker
beroperasi dengan concurrency tinggi.

## T9. Kontrol Konkurensi: Unique Constraint, Upsert, dan Idempotent Write

Race condition terjadi ketika hasil akhir bergantung pada interleaving operasi
concurrent. Pendekatan naive `SELECT COUNT(*)` lalu `INSERT` tidak aman: dua
worker dapat sama-sama membaca “belum ada” dan kemudian menulis duplikat.
Lock eksplisit dapat mencegahnya, tetapi menambah kompleksitas, contention,
dan risiko deadlock. Kontrol konkurensi sebaiknya ditempatkan pada primitive
atomik yang paling dekat dengan data bersama (Coulouris et al., 2012).

Proyek menggunakan unique constraint `(topic, event_id)` sebagai arbiter
database. Kedua worker boleh mencoba insert bersamaan, tetapi PostgreSQL hanya
mengizinkan satu row. Statement `ON CONFLICT DO NOTHING RETURNING id`
memberikan sinyal langsung: row hasil berarti `processed`; tanpa row berarti
`duplicate_dropped`. Tidak ada application-level distributed lock dan tidak
ada celah antara pengecekan dengan penulisan. Dokumentasi PostgreSQL
menempatkan `ON CONFLICT` sebagai mekanisme penanganan alternative action saat
unique conflict terjadi (PostgreSQL Global Development Group, n.d.-b).

Counter global dan per-topic memakai upsert atomik. Seluruh write berada dalam
satu transaksi, sehingga idempotent decision, statistik, dan audit memiliki
batas commit sama. Test concurrency menjalankan 20 coroutine untuk event
identik dan menghasilkan tepat satu `processed`, 19 `duplicate_dropped`, serta
satu row database. Test lain mengirim 10 event unik secara concurrent dan
seluruhnya tersimpan. Hasil ini membuktikan bahwa correctness berasal dari
database constraint dan transaction, bukan kebetulan scheduling worker.

## T10. Orkestrasi Compose, Keamanan Lokal, Persistence, dan Observability

Docker Compose mendefinisikan enam jenis service: PostgreSQL, Redis,
aggregator API, dua aggregator worker, publisher, dan k6. Compose memberikan
service discovery berdasarkan nama service serta satu network lokal
`uas_net`. Hanya API yang mempublikasikan port `8080` ke host; PostgreSQL dan
Redis tidak memiliki host port. Pembatasan ini mengurangi permukaan akses dan
memenuhi syarat seluruh komunikasi internal berlangsung pada jaringan
Compose.

PostgreSQL, Redis, dan API memiliki healthcheck. Dependency menggunakan
`condition: service_healthy`, sehingga worker dan tool tidak hanya menunggu
container berjalan, tetapi menunggu dependency siap menerima request. Docker
Compose menjamin dependency dengan kondisi tersebut sehat sebelum dependent
service dimulai (Docker, Inc., n.d.-a). Application image memakai
`python:3.11-slim` dan user non-root. Password masih berupa environment
development karena sistem hanya ditujukan untuk jaringan lokal demonstrasi.

Persistence memakai named volume `pg_data` dan `redis_data`. Docker volume
mempertahankan data setelah container yang menggunakannya dihapus atau dibuat
ulang (Docker, Inc., n.d.-b). Observability mencakup structured worker logs,
audit table, `/health`, `/ready`, `/stats`, dan `/metrics`. Metrics
menampilkan accepted/processed rate, duplicate rate, stream length, pending,
consumer-group lag, unprocessed estimate, dan dead-letter length. Rancangan
ini membuat deployment dapat diperiksa dari status service hingga correctness
event dan kondisi antrean.

---

# 3. Perancangan Sistem

## 3.1 Arsitektur

```text
Publisher / k6 / curl
          |
          | HTTP POST /publish
          v
  +------------------+
  | Aggregator API   |
  | FastAPI :8080    |
  +------------------+
          |
          | XADD
          v
  +------------------+
  | Redis Stream     |
  | events           |
  +------------------+
       |          |
       |          |
       v          v
 +----------+  +----------+
 | Worker 1 |  | Worker 2 |
 +----------+  +----------+
       \          /
        \        /
         v      v
      +------------+
      | PostgreSQL |
      +------------+

Repeated failure --> Redis Stream events:deadletter
```

API, worker, Redis, dan PostgreSQL memiliki tanggung jawab berbeda. API
menangani transport HTTP dan schema validation. Redis menangani asynchronous
delivery dan consumer group. Worker menangani idempotent processing serta
recovery. PostgreSQL menjadi persistent source of truth untuk event,
statistik, dan audit.

![Status seluruh service](image/01/01-compose-services-health.png)

**Gambar 1.** Seluruh service aktif; API, PostgreSQL, dan Redis berstatus
healthy.

![Health dan readiness](image/01/02-health-ready-endpoints.png)

**Gambar 2.** Endpoint liveness dan readiness membuktikan koneksi API,
PostgreSQL, dan Redis.

![Jaringan internal Compose](image/01/03-compose-internal-network.png)

**Gambar 3.** Service berada pada jaringan lokal Docker Compose.

## 3.2 Model Event

Schema event minimal:

```json
{
  "topic": "auth.login",
  "event_id": "manual-001",
  "timestamp": "2026-06-11T10:00:00Z",
  "source": "manual-curl",
  "payload": {
    "user_id": "u001",
    "action": "login"
  }
}
```

Validasi yang diterapkan:

- `topic`: 1–100 karakter, hanya alfanumerik, `_`, `.`, dan `-`.
- `event_id`: 1–150 karakter.
- `timestamp`: ISO 8601 dan dinormalisasi ke UTC.
- `source`: 1–100 karakter.
- `payload`: JSON object.
- Batch: 1–1000 event.

## 3.3 Endpoint API

| Method | Endpoint | Fungsi |
|---|---|---|
| `GET` | `/health` | Liveness API |
| `GET` | `/ready` | Memeriksa PostgreSQL dan Redis |
| `POST` | `/publish` | Menerima single object atau array event |
| `POST` | `/publish/batch` | Alias batch lama, ditandai deprecated |
| `GET` | `/events` | Daftar event unik, opsional filter topic |
| `GET` | `/stats` | Counter global dan per-topic |
| `GET` | `/metrics` | Throughput dan kondisi antrean |

## 3.4 Model Data

| Tabel | Kunci/Invarian | Fungsi |
|---|---|---|
| `processed_events` | `UNIQUE(topic, event_id)` | Menyimpan event unik |
| `stats` | `PRIMARY KEY(name)` | Counter global |
| `topic_stats` | `PRIMARY KEY(topic)` | Counter unique/duplicate per topic |
| `audit_logs` | `BIGSERIAL id` | Riwayat processed, duplicate, dead-letter |

Payload disimpan sebagai `JSONB`, sedangkan timestamp producer disimpan sebagai
`TIMESTAMPTZ`. Kolom `processed_by` merekam worker yang memenangkan insert.

![Tabel PostgreSQL](image/01/08-postgres-tables.png)

**Gambar 4.** Empat tabel persistent storage pada PostgreSQL.

---

# 4. Implementasi

## 4.1 Single Event Processing

`POST /publish` menerima object `EventIn`, menulis JSON ke Redis Stream dengan
`XADD`, lalu menaikkan counter `received` dan `stream_enqueued`. Respons
`202 Accepted` memuat topic, event ID, stream, dan Redis message ID.

![Publish single event](image/01/04-single-event-publish.png)

**Gambar 5.** API menerima event manual dan mengembalikan Redis message ID.

Worker membaca message melalui `XREADGROUP`, memvalidasi ulang schema, lalu
menjalankan `process_event`. Event yang selesai dapat diambil melalui
`GET /events`.

![Event setelah diproses](image/01/05-single-event-processed.png)

**Gambar 6.** Event unik tersedia pada endpoint `/events` setelah diproses
worker.

## 4.2 Idempotency dan Persistent Deduplication

SQL inti:

```sql
INSERT INTO processed_events (
    topic, event_id, event_timestamp, source, payload, processed_by
)
VALUES (...)
ON CONFLICT (topic, event_id)
DO NOTHING
RETURNING id;
```

Jika `RETURNING id` menghasilkan row, status adalah `processed`. Jika tidak,
status adalah `duplicate_dropped`. Keputusan tersebut dibuat PostgreSQL secara
atomik dan tetap berlaku untuk dua worker concurrent maupun delivery setelah
restart.

![Statistik dedup sederhana](image/01/06-dedup-stats.png)

**Gambar 7.** Tiga delivery event yang sama menghasilkan satu unique dan dua
duplicate.

![Log worker dedup](image/01/07-worker-logs-dedup.png)

**Gambar 8.** Dua worker menerima delivery berbeda, tetapi duplicate ditolak
secara konsisten.

![Satu event unik di database](image/01/09-postgres-unique-event.png)

**Gambar 9.** PostgreSQL hanya menyimpan satu row untuk event identik.

![Audit log dedup](image/01/10-dedup-audit-log.png)

**Gambar 10.** Audit log mencatat satu `processed` dan dua
`duplicate_dropped`.

## 4.3 Transaction Boundary dan Atomic Counter

`process_event` membuka transaksi `READ COMMITTED`. Di dalam transaksi:

1. Worker mencoba insert event unik.
2. Worker menaikkan `unique_processed` atau `duplicate_dropped`.
3. Worker melakukan upsert `topic_stats`.
4. Worker menulis `audit_logs`.

Counter diperbarui dengan ekspresi database:

```sql
value = stats.value + delta
```

Pola tersebut mencegah lost update. Jika salah satu statement gagal, semua
perubahan rollback dan Redis message belum di-ACK.

## 4.4 Batch Processing

Endpoint utama `/publish` menerima object atau array. API menggunakan Redis
pipeline untuk menambahkan seluruh event batch, lalu memperbarui counter
berdasarkan panjang batch. Batch demo berisi tiga event, dua event ID unik,
dan satu duplicate.

![Publish batch](image/01/12-batch-publish-main-endpoint.png)

**Gambar 11.** Endpoint utama menerima tiga event dalam satu request batch.

![Hasil event batch](image/01/13-batch-events-dedup.png)

**Gambar 12.** Hanya dua event unik terlihat pada query topic batch.

![Audit batch](image/01/14-batch-dedup-audit.png)

**Gambar 13.** Audit batch menunjukkan dua `processed` dan satu
`duplicate_dropped`.

Kebijakan batch adalah partial asynchronous acceptance pada broker: seluruh
schema array divalidasi sebelum handler berjalan, tetapi setiap item menjadi
message Redis tersendiri dan diproses dalam transaksi database masing-masing.
Kebijakan ini mempertahankan throughput dan isolasi kegagalan antar-item.

## 4.5 Retry, Pending Recovery, dan Dead-Letter

Worker memiliki konfigurasi:

- `MAX_RETRIES=3`
- `RETRY_BACKOFF_BASE_SECONDS=0.5`
- `PENDING_IDLE_MS=10000`
- `READ_COUNT=10`

![Konfigurasi worker](image/01/15-worker-retry-pending-config.png)

**Gambar 14.** Kedua worker aktif dengan retry dan pending recovery.

Jika `process_event` gagal, worker tidak mengirim `XACK`. Retry count disimpan
pada Redis hash. Message tetap pending dan setelah idle 10 detik dapat
diklaim oleh worker yang sama atau worker lain menggunakan `XAUTOCLAIM`.
Backoff dihitung sebagai `base * 2^(attempt-1)`. Pada attempt ketiga, worker
menulis message ke `events:deadletter`, menambah counter `process_errors` dan
`dead_lettered`, menulis audit log, lalu mengirim ACK agar poison message
tidak berulang tanpa batas.

![Retry dan dead-letter](image/01/25-retry-pending-deadletter.png)

**Gambar 15.** Fault injection memperlihatkan attempt 1–3, pending recovery
lintas worker, dead-letter, audit database, dan status PASS.

## 4.6 Persistence

PostgreSQL menggunakan named volume:

```yaml
volumes:
  - pg_data:/var/lib/postgresql/data
```

Redis menggunakan `redis_data:/data` dan AOF. Uji persistence membuat marker
event, memastikan row tersedia, menjalankan `docker compose up -d
--force-recreate postgres`, menunggu healthcheck, kemudian menghitung marker
yang sama.

![Persistence recreate](image/01/16-postgres-recreate-persistence.png)

**Gambar 16.** Jumlah marker tetap satu sebelum dan sesudah container
PostgreSQL di-recreate.

## 4.7 Observability

Observability terdiri dari:

- Structured log dengan worker, topic, event ID, Redis ID, dan status.
- Audit log persisten.
- `/health` dan `/ready`.
- `/stats` untuk counter global/per-topic.
- `/metrics` untuk rate dan queue state.

![Metrics](image/01/17-observability-metrics.png)

**Gambar 17.** Metrics menampilkan throughput, duplicate rate, pending,
consumer-group lag, unprocessed estimate, dan dead-letter length.

Nilai `process_errors` dan `dead_lettered` pada gambar berasal dari intentional
fault injection untuk membuktikan recovery, bukan error pada load test normal.

## 4.8 Docker, Jaringan, dan Security

Application image menggunakan `python:3.11-slim`, dependency version tetap,
dan user non-root. PostgreSQL dan Redis tidak diekspos ke host. API menjadi
satu-satunya entry point pada `localhost:8080`. Compose healthcheck dan
`service_healthy` mengurangi startup race. Konfigurasi database saat ini
memakai credential demonstrasi di Compose; untuk deployment produksi,
credential harus dipindahkan ke secret manager atau Docker secrets.

---

# 5. Pengujian

## 5.1 Strategi Pengujian

Pengujian dibagi menjadi:

1. Unit test untuk schema validation dan helper worker.
2. Repository integration test menggunakan PostgreSQL.
3. HTTP integration test terhadap API dan worker aktual.
4. Concurrency test.
5. Host integration script untuk recreate dan Redis fault injection.
6. Load test k6.

## 5.2 Hasil Pytest

| Kelompok | Jumlah | Fokus |
|---|---:|---|
| API | 9 | Health, ready, single, batch, events, stats, metrics |
| Concurrency | 2 | Duplicate race dan unique parallel events |
| Dedup repository | 3 | Unique, duplicate, satu row |
| Event validation | 6 | Topic, ID, timestamp, payload |
| Persistence reconnect | 1 | Data tersedia melalui pool baru |
| Stats | 3 | Counter global dan topic |
| Worker idempotency | 2 | Repeated processing dan audit |
| Retry/pending | 4 | ACK, no ACK on fail, dead-letter, XAUTOCLAIM |
| **Total** | **30** | **Seluruh test lulus** |

![Pytest](image/01/11-pytest-30-passed.png)

**Gambar 18.** Seluruh 30 test lulus dalam sekitar 1,55 detik.

## 5.3 Uji Konkurensi

Test `test_concurrent_duplicate_processing_saves_one_row` menjalankan 20
coroutine terhadap object event yang sama. Ekspektasi dan hasilnya:

```text
processed             = 1
duplicate_dropped     = 19
processed_events rows = 1
```

Test `test_concurrent_unique_events_are_all_processed` mengirim 10 event unik
secara bersamaan dan menghasilkan 10 row. Kedua hasil membuktikan bahwa unique
constraint tidak menghambat event berbeda, tetapi mencegah race event identik.

## 5.4 Uji Crash/Retry

Test unit memastikan message gagal sebelum max retry tidak di-ACK. Fault
injection Redis nyata memasukkan payload `not-json`. Message pertama kali
diterima worker-2, tetap pending, kemudian diklaim dan akhirnya
dead-lettered oleh worker-1. Perpindahan worker membuktikan recovery tidak
bergantung pada consumer awal.

## 5.5 Uji Persistence

Test pytest memverifikasi data tetap tersedia setelah database pool ditutup
dan dibuat ulang. Script host melengkapi test dengan recreate container
PostgreSQL sebenarnya. Volume tidak dihapus, sehingga row marker tetap ada.
Pengujian sengaja tidak memakai `docker compose down -v` karena opsi `-v`
memang diperuntukkan menghapus volume beserta data.

---

# 6. Pengujian Performa

## 6.1 Skenario k6

Konfigurasi:

| Parameter | Nilai |
|---|---:|
| Total request | 20.000 |
| Unique event | 14.000 |
| Duplicate event | 6.000 |
| Duplicate ratio | 30% |
| Virtual users | 20 |
| Executor | `shared-iterations` |
| Maximum duration | 5 menit |
| Threshold failure | `< 1%` |
| Threshold p95 | `< 1.000 ms` |

Unique index untuk 6.000 request terakhir diarahkan kembali ke 14.000 ID awal,
sehingga duplicate ratio deterministik dan dapat diverifikasi di database.

## 6.2 Hasil

| Metrik | Hasil |
|---|---:|
| Completed iterations | 20.000 |
| Checks | 100% |
| HTTP request failed | 0,00% |
| Throughput request | ±341 request/detik |
| Average HTTP duration | 47,68 ms |
| p90 HTTP duration | 74,54 ms |
| p95 HTTP duration | 89,38 ms |
| Maximum HTTP duration | 312,61 ms |
| Total waktu | ±58,6 detik |

![Hasil k6](image/01/18-k6-20000-events.png)

**Gambar 19.** k6 menyelesaikan 20.000 iterations dengan 100% check dan 0%
request failure.

## 6.3 Verifikasi Correctness Setelah Load Test

Endpoint `/stats` menunjukkan:

```text
received          = 20000
stream_enqueued   = 20000
unique_processed  = 14000
duplicate_dropped = 6000
process_errors    = 0
```

![Stats setelah k6](image/01/19-stats-after-k6.png)

**Gambar 20.** Statistik setelah load test sesuai komposisi 70% unique dan 30%
duplicate.

Query database menghitung 14.000 event unik berdasarkan prefix run:

![Unique k6 database](image/01/20-k6-14000-unique-db.png)

**Gambar 21.** PostgreSQL berisi tepat 14.000 event unik untuk run k6.

Audit log menunjukkan 14.000 `processed` dan 6.000 `duplicate_dropped`:

![Audit k6](image/01/21-k6-dedup-audit.png)

**Gambar 22.** Audit database membuktikan seluruh 20.000 delivery
terklasifikasi tanpa kehilangan event.

## 6.4 Parallel Worker

![Worker 1](image/01/22-parallel-worker-1.png)

**Gambar 23.** Worker-1 memproses message dari consumer group.

![Worker 2](image/01/23-parallel-worker-2.png)

**Gambar 24.** Worker-2 memproses message pada waktu yang sama.

Kedua gambar membuktikan workload tidak hanya dijalankan satu worker. Consumer
group membagi message ke dua process, sedangkan unique constraint
mempertahankan konsistensi ketika duplicate diproses secara parallel.

---

# 7. Analisis Hasil

Implementasi memenuhi spesifikasi API, persistent deduplication,
idempotency, transaction, concurrency control, persistence, dan minimum
performance. Hasil 14.000 unique dan 6.000 duplicate sama pada `/stats`,
database, dan audit log. Kesamaan tiga sumber observasi menunjukkan tidak ada
lost update atau unclassified delivery pada load test.

Unique constraint terbukti lebih kuat dibanding application-level duplicate
check. Dua worker dapat berjalan tanpa distributed lock, dan test concurrency
tetap menghasilkan satu row. Transaction boundary juga memastikan counter
serta audit mengikuti keputusan insert. Penggunaan `READ COMMITTED` cukup
karena invariant tidak bergantung pada repeated read.

Reliability diuji pada dua tingkat. Unit test memeriksa aturan ACK dan retry
secara deterministik, sedangkan fault injection membuktikan pending message
dapat berpindah consumer dan masuk dead-letter. Persistence recreate
membuktikan data tidak terikat pada lifecycle container.

Keterbatasan utama adalah Redis Stream belum menerapkan retention (`MAXLEN`),
sehingga stream dapat terus membesar. Dead-letter belum memiliki reprocessing
endpoint. Metrics bersifat application snapshot dan belum diekspor dalam
format Prometheus. Strict ordering, broker/database replication, TLS,
authentication, dan secret management juga belum diterapkan. Keterbatasan
tersebut tidak mengubah correctness ruang lingkup tugas, tetapi menjadi
pekerjaan lanjutan untuk deployment production.

---

# 8. Kesimpulan

Distributed Pub-Sub Log Aggregator berhasil dibangun sebagai sistem
multi-service lokal menggunakan Docker Compose. Redis Streams memisahkan
publisher dan consumer, sedangkan PostgreSQL menyediakan durable source of
truth. Kombinasi unique constraint `(topic, event_id)`, `ON CONFLICT`,
transaction `READ COMMITTED`, atomic counter, dan ACK setelah commit
menghasilkan idempotent processing yang tahan race condition.

Sistem mendukung single dan batch publish, dua worker paralel, retry,
exponential backoff, pending recovery, dead-letter, persistent volume,
healthcheck, readiness, logging, audit, dan metrics. Seluruh 30 automated
tests lulus. Load test 20.000 event dengan 30% duplikat menghasilkan 14.000
event unik dan 6.000 duplicate dropped, 100% check sukses, 0% HTTP failure,
serta p95 sekitar 89,38 ms. Container PostgreSQL dapat di-recreate tanpa
kehilangan marker event.

Hasil tersebut menunjukkan tujuan tugas tercapai: event duplicate tidak
diproses sebagai row baru, pemrosesan concurrent tetap konsisten, dan data
bertahan melewati lifecycle application container. Desain juga memiliki batas
jaminan yang eksplisit, yaitu at-least-once delivery dengan exactly-once
persistent effect melalui idempotency.

---

# Daftar Pustaka

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012).
*Distributed systems: Concepts and design* (5th ed.). Pearson Education.

Docker, Inc. (n.d.-a). *Control startup and shutdown order in Compose*.
Docker Documentation. Retrieved June 14, 2026, from
https://docs.docker.com/compose/how-tos/startup-order/

Docker, Inc. (n.d.-b). *Volumes*. Docker Documentation. Retrieved June 14,
2026, from https://docs.docker.com/engine/storage/volumes/

PostgreSQL Global Development Group. (n.d.-a). *Transaction isolation*.
PostgreSQL 18 Documentation. Retrieved June 14, 2026, from
https://www.postgresql.org/docs/current/transaction-iso.html

PostgreSQL Global Development Group. (n.d.-b). *INSERT*. PostgreSQL 18
Documentation. Retrieved June 14, 2026, from
https://www.postgresql.org/docs/current/sql-insert.html

Redis Ltd. (n.d.-a). *Redis Streams*. Redis Documentation. Retrieved June 14,
2026, from https://redis.io/docs/latest/develop/data-types/streams/

Redis Ltd. (n.d.-b). *XAUTOCLAIM*. Redis Documentation. Retrieved June 14,
2026, from https://redis.io/docs/latest/commands/xautoclaim/

---

# Lampiran A. Cara Menjalankan

```powershell
docker compose up --build -d postgres redis aggregator-api aggregator-worker-1 aggregator-worker-2
docker compose ps
```

Akses API:

```text
http://localhost:8080
```

Menjalankan test:

```powershell
docker compose exec -T aggregator-api pytest -v
```

Menjalankan k6:

```powershell
docker compose --profile tools run --rm k6
```

Uji persistence:

```powershell
.\scripts\test-persistence-recreate.ps1
```

Uji retry/dead-letter:

```powershell
.\scripts\test-retry-deadletter.ps1
```

# Lampiran B. Struktur Repository

![Struktur repository](image/01/24-folder-structure.png)

**Gambar 25.** Struktur repository final.

```text
aggregator/          FastAPI, worker, repository, dan tests
publisher/           Simulator publisher
db/                  Inisialisasi schema PostgreSQL
k6/                  Load test 20.000 event
scripts/             Demo dan integration scripts
docs/                Dokumentasi, laporan, dan bukti gambar
docker-compose.yaml  Orkestrasi service
README.md            Instruksi project
```

