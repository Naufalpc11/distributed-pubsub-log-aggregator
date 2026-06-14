# Distributed Pub-Sub Log Aggregator

Project UAS Sistem Paralel dan Terdistribusi.

Sistem ini membangun log aggregator multi-service dengan FastAPI, Redis
Streams, PostgreSQL, dan Docker Compose. Fitur utamanya:

- Idempotent consumer dan persistent deduplication `(topic, event_id)`.
- Dua worker paralel dengan transaksi `READ COMMITTED`.
- Retry eksponensial, pending-message recovery, dan dead-letter stream.
- `POST /publish` menerima satu event atau batch 1-1000 event.
- Named volumes untuk PostgreSQL dan Redis.
- Health, readiness, statistics, queue metrics, dan audit log.
- Load test k6 sebanyak 20.000 event dengan 30% duplikat.

## Arsitektur

```text
publisher / k6
     |
     v
aggregator-api:8080
     |
     v
Redis Stream: events
     |
     +--> aggregator-worker-1
     +--> aggregator-worker-2
     |
     v
PostgreSQL

failed repeatedly --> Redis Stream: events:deadletter
```

Hanya API port `8080` yang dipublikasikan ke host. PostgreSQL dan Redis hanya
tersedia pada network Compose `uas_net`.

## Menjalankan Sistem

```powershell
docker compose up --build -d postgres redis aggregator-api aggregator-worker-1 aggregator-worker-2
docker compose ps
```

`aggregator-api` memiliki healthcheck `/ready`. Worker, publisher, dan k6 baru
dimulai setelah API dinyatakan sehat.

## Endpoint

| Method | Endpoint | Fungsi |
|---|---|---|
| `GET` | `/health` | Liveness API |
| `GET` | `/ready` | Koneksi PostgreSQL dan Redis |
| `POST` | `/publish` | Single event atau array event |
| `POST` | `/publish/batch` | Alias batch lama, deprecated |
| `GET` | `/events?topic=...` | Event unik yang telah diproses |
| `GET` | `/stats` | Counter global dan per topic |
| `GET` | `/metrics` | Throughput, duplicate rate, queue, pending, dead-letter |

Single event:

```powershell
curl.exe -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  -d "{\"topic\":\"auth.login\",\"event_id\":\"manual-001\",\"timestamp\":\"2026-06-11T10:00:00Z\",\"source\":\"manual-curl\",\"payload\":{\"user_id\":\"u001\"}}"
```

Batch menggunakan endpoint yang sama:

```powershell
curl.exe -X POST http://localhost:8080/publish `
  -H "Content-Type: application/json" `
  --data-binary "@scripts/batch-demo.json"
```

## Idempotency dan Reliability

Worker memasukkan event dengan:

```sql
INSERT ... ON CONFLICT (topic, event_id) DO NOTHING
```

Insert event, update statistik, dan audit log dilakukan dalam satu transaksi.
ACK Redis hanya diberikan setelah transaksi sukses. Jika gagal, message tetap
pending dan diambil kembali dengan `XAUTOCLAIM`. Setelah tiga kegagalan,
message ditulis ke `events:deadletter`, dicatat pada audit log, lalu di-ACK.

## Ordering

Sistem tidak menjanjikan total ordering karena dua worker memproses event
secara paralel. Redis Stream mempertahankan urutan masuk, tetapi completion
order dapat berbeda. Aggregator menerima event out-of-order dan menyimpan
`event_timestamp` asli serta `created_at`. Deduplication tidak bergantung pada
urutan, melainkan pada `(topic, event_id)`. Jika strict per-topic ordering
diperlukan, topic harus dipartisi secara deterministik ke satu worker.

## Tests

```powershell
docker compose exec -T aggregator-api pytest -v
```

Hasil terakhir:

```text
30 passed
```

Test mencakup validasi schema, API single/batch, deduplication, idempotency,
transaksi konkurensi, reconnect database, ACK/retry, pending recovery,
dead-letter, stats, dan metrics.

Recreate container PostgreSQL secara nyata:

```powershell
.\scripts\test-persistence-recreate.ps1
```

Uji retry dan dead-letter dengan Redis nyata:

```powershell
.\scripts\test-retry-deadletter.ps1
```

## Load Test

```powershell
docker compose --profile tools run --rm k6
```

Konfigurasi: 20.000 request, 14.000 event unik, 6.000 duplikat, dan 20 VUs.
Hasil bukti terakhir menunjukkan 100% check berhasil dan 0% HTTP failure.

## Dokumentasi

- `docs/architecture.md`
- `docs/testing.md`
- `docs/report.md`
- `scripts/demo-commands.md`
- `scripts/screenshot-checklist.md`
