# Demo Commands

## 1. Menjalankan Sistem

```powershell
docker compose up --build -d postgres redis aggregator-api aggregator-worker-1 aggregator-worker-2
docker compose ps
```

## 2. Health dan Readiness

```powershell
curl.exe http://localhost:8080/health
curl.exe http://localhost:8080/ready
```

## 3. Publish Single Event

```powershell
curl.exe -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d "{\"topic\":\"auth.login\",\"event_id\":\"manual-001\",\"timestamp\":\"2026-06-11T10:00:00Z\",\"source\":\"manual-curl\",\"payload\":{\"user_id\":\"u001\"}}"
```

Jalankan dua kali untuk menunjukkan deduplication.

## 4. Publish Batch pada Endpoint Utama

```powershell
curl.exe -X POST http://localhost:8080/publish -H "Content-Type: application/json" --data-binary "@scripts/batch-demo.json"
```

## 5. Events, Stats, dan Metrics

```powershell
curl.exe "http://localhost:8080/events?topic=batch.demo"
curl.exe http://localhost:8080/stats
curl.exe http://localhost:8080/metrics
```

## 6. Log Dua Worker

```powershell
docker compose logs aggregator-worker-1 --tail=100
docker compose logs aggregator-worker-2 --tail=100
```

## 7. Tests

```powershell
docker compose exec -T aggregator-api pytest -v
```

## 8. Persistence Container Recreate

```powershell
.\scripts\test-persistence-recreate.ps1
```

## 9. Retry, Pending Recovery, dan Dead-Letter

```powershell
.\scripts\test-retry-deadletter.ps1
```

## 10. Load Test

```powershell
docker compose --profile tools run --rm k6
```

## 11. Verifikasi k6

```powershell
docker compose exec postgres psql -U appuser -d appdb -c "SELECT COUNT(*) AS total_unique_k6_events FROM processed_events WHERE event_id LIKE 'k6-uasfinal001-%';"
docker compose exec postgres psql -U appuser -d appdb -c "SELECT status, COUNT(*) FROM audit_logs WHERE event_id LIKE 'k6-uasfinal001-%' GROUP BY status ORDER BY status;"
```
