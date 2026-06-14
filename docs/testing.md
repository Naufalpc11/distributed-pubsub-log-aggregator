# Testing

## Ringkasan

Pengujian terdiri dari:

1. Sebanyak 30 test pytest.
2. Integration script persistence recreate.
3. Integration script retry/pending/dead-letter.
4. Load test k6 sebanyak 20.000 event.

## Pytest

```powershell
docker compose exec -T aggregator-api pytest -v
```

Hasil terakhir:

```text
30 passed
```

| File | Fokus |
|---|---|
| `test_event_validation.py` | Schema dan timestamp |
| `test_dedup_repository.py` | Deduplication repository |
| `test_stats.py` | Counter global dan per topic |
| `test_worker_idempotency.py` | Idempotent processing dan audit |
| `test_api_publish.py` | Single/batch API, events, stats, metrics |
| `test_concurrency.py` | Race duplicate dan unique event |
| `test_worker_retry_pending.py` | ACK, retry, dead-letter, XAUTOCLAIM |
| `test_persistence_recreate.py` | Data tersedia setelah reconnect pool |

## Retry dan Pending Recovery

Test worker memastikan:

- Message sukses baru di-ACK setelah `process_event`.
- Message gagal tidak di-ACK sebelum batas retry.
- Message permanen gagal masuk dead-letter setelah tiga attempt.
- Pending message diambil kembali menggunakan `XAUTOCLAIM`.

Verifikasi dengan Redis nyata:

```powershell
.\scripts\test-retry-deadletter.ps1
```

Output harus memuat `retry_scheduled`, `pending_recovery`, `dead_lettered`,
audit row, dan baris `PASS`.

## Persistence Recreate

Pytest memverifikasi data tetap terbaca setelah pool database ditutup dan
dibuat ulang. Recreate container sebenarnya dijalankan dari host karena
container pytest tidak diberi akses ke Docker daemon:

```powershell
.\scripts\test-persistence-recreate.ps1
```

Ekspektasi:

```text
Before recreate: 1 row
After recreate : 1 row
PASS: named volume preserved data after PostgreSQL container recreate.
```

## Load Test k6

```powershell
docker compose --profile tools run --rm k6
```

Konfigurasi:

```text
Total event      : 20.000
Event unik       : 14.000
Event duplikat   : 6.000
Duplicate ratio  : 30%
Virtual users    : 20
```

Target:

```text
checks           : 100%
http_req_failed  : 0.00%
p(95)            : < 1000 ms
```

Verifikasi database:

```powershell
docker compose exec postgres psql -U appuser -d appdb -c "SELECT COUNT(*) FROM processed_events WHERE event_id LIKE 'k6-uasfinal001-%';"
docker compose exec postgres psql -U appuser -d appdb -c "SELECT status, COUNT(*) FROM audit_logs WHERE event_id LIKE 'k6-uasfinal001-%' GROUP BY status ORDER BY status;"
```

Target adalah 14.000 `processed` dan 6.000 `duplicate_dropped`.

## Screenshot

Daftar bukti lama dan screenshot yang perlu diperbarui tersedia di
`scripts/screenshot-checklist.md`.
