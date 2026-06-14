# Architecture

## Ringkasan

Sistem adalah Pub-Sub Log Aggregator multi-service berbasis Docker Compose.
API menerima event, Redis Streams menjadi broker internal, dua worker
memproses message secara paralel, dan PostgreSQL menyimpan hasil secara
persisten.

## Komponen

### Aggregator API

FastAPI menyediakan:

- `POST /publish` untuk satu object event atau array 1-1000 event.
- `POST /publish/batch` sebagai alias deprecated.
- `GET /events`, `/stats`, `/metrics`, `/health`, dan `/ready`.

Pydantic memvalidasi `topic`, `event_id`, `timestamp`, `source`, dan `payload`
sebelum message dimasukkan ke Redis.

### Redis Streams

Stream `events` digunakan sebagai broker. Consumer group `log-workers`
membagikan message kepada dua worker. Redis menggunakan AOF dan named volume
`redis_data`.

Jika pemrosesan gagal, message tidak di-ACK dan tetap berada di Pending
Entries List. Worker menggunakan `XAUTOCLAIM` untuk mengambil message yang
idle minimal 10 detik.

### Workers

Setiap worker:

1. Membaca message baru atau mengambil pending message.
2. Memvalidasi JSON menjadi `EventIn`.
3. Menjalankan transaksi database.
4. Mengirim ACK hanya setelah transaksi berhasil.
5. Menjadwalkan retry eksponensial ketika gagal.
6. Memindahkan kegagalan permanen ke `events:deadletter` setelah tiga attempt.

Retry count disimpan pada Redis hash `events:retry-count`. Dead-letter juga
dicatat pada tabel `audit_logs`.

### PostgreSQL

Tabel utama:

| Tabel | Fungsi |
|---|---|
| `processed_events` | Event unik |
| `stats` | Counter global |
| `topic_stats` | Counter per topic |
| `audit_logs` | Audit processed, duplicate, dan dead-letter |

## Deduplication dan Idempotency

Database memiliki constraint:

```sql
UNIQUE (topic, event_id)
```

Worker menggunakan:

```sql
INSERT INTO processed_events (...)
VALUES (...)
ON CONFLICT (topic, event_id)
DO NOTHING
RETURNING id;
```

Insert event, update counter, topic stats, dan audit log berada dalam satu
transaksi. Jika dua worker memproses event sama, hanya satu insert yang
berhasil; lainnya tercatat sebagai `duplicate_dropped`.

## Isolation dan Kontrol Konkurensi

Isolation level adalah `READ COMMITTED`. Level ini cukup karena keputusan
deduplication tidak memakai pola read-then-write. Unique constraint dan
`ON CONFLICT` menjadi titik sinkronisasi atomik sehingga race antar-worker
tetap menghasilkan satu row tanpa biaya `SERIALIZABLE`.

Counter memakai atomic upsert:

```sql
value = stats.value + delta
```

Pola tersebut mencegah lost update.

## Ordering

Sistem tidak menjanjikan total ordering karena message dibagi ke dua worker.
Redis Stream mempertahankan urutan append, tetapi completion order dapat
berbeda. Sistem menerima event out-of-order dengan kebijakan:

- Timestamp producer disimpan sebagai `event_timestamp`.
- Waktu proses disimpan sebagai `created_at`.
- Deduplication hanya berdasarkan `(topic, event_id)`.
- Query default memakai ID database agar hasil deterministik.

Jika strict per-topic ordering diperlukan, topic harus dipartisi secara
deterministik sehingga seluruh event topic tersebut diproses oleh satu worker.

## Persistence

PostgreSQL dan Redis menggunakan named volumes:

```yaml
volumes:
  pg_data:
  redis_data:
```

Verifikasi recreate container dilakukan dari host:

```powershell
.\scripts\test-persistence-recreate.ps1
```

Skrip membuat marker event, menjalankan `docker compose up -d
--force-recreate postgres`, lalu membuktikan marker tetap ada.

## Observability

`GET /metrics` menampilkan:

- Accepted dan processed events per second.
- Duplicate rate.
- Redis stream length.
- Pending-message count.
- Consumer-group lag.
- Estimasi message belum diproses.
- Dead-letter stream length.
- Counter global. Statistik per topic tersedia melalui `/stats` atau
  `/metrics?include_topics=true`.

Log worker mencatat nama worker, topic, event ID, Redis ID, retry attempt,
pending recovery, dan dead-letter.

## Jaringan dan Healthcheck

Semua service berada pada network `uas_net`. Hanya API port `8080` yang
diekspos ke host. PostgreSQL dan Redis tidak memiliki host port.

PostgreSQL, Redis, dan aggregator API memiliki healthcheck. Worker, publisher,
dan k6 menunggu API berstatus healthy sebelum dimulai.
