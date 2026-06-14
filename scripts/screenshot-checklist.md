# Screenshot Checklist Laporan Akhir

Seluruh bukti final berada di `docs/image/01`.

## Deployment dan Jaringan

1. `01-compose-services-health.png`
2. `02-health-ready-endpoints.png`
3. `03-compose-internal-network.png`

## Single Event dan Deduplication

4. `04-single-event-publish.png`
5. `05-single-event-processed.png`
6. `06-dedup-stats.png`
7. `07-worker-logs-dedup.png`
8. `08-postgres-tables.png`
9. `09-postgres-unique-event.png`
10. `10-dedup-audit-log.png`

## Automated Tests

11. `11-pytest-30-passed.png`

## Batch Processing

12. `12-batch-publish-main-endpoint.png`
13. `13-batch-events-dedup.png`
14. `14-batch-dedup-audit.png`

## Reliability, Persistence, dan Observability

15. `15-worker-retry-pending-config.png`
16. `16-postgres-recreate-persistence.png`
17. `17-observability-metrics.png`

## Load Test dan Parallel Processing

18. `18-k6-20000-events.png`
19. `19-stats-after-k6.png`
20. `20-k6-14000-unique-db.png`
21. `21-k6-dedup-audit.png`
22. `22-parallel-worker-1.png`
23. `23-parallel-worker-2.png`

## Lampiran dan Fault Injection

24. `24-folder-structure.png`
25. `25-retry-pending-deadletter.png`

## Penempatan pada Laporan

Seluruh gambar sudah digunakan pada `docs/report.md`. Gambar 24 ditempatkan
pada lampiran struktur repository. Gambar 25 ditempatkan pada pembahasan
retry, pending recovery, dan dead-letter.

## Catatan Caption

- Jelaskan hasil yang dibuktikan, bukan hanya command.
- Gambar worker 1 dan worker 2 sebaiknya diletakkan berdekatan.
- Pada gambar metrics, jelaskan bahwa `process_errors` dan `dead_lettered`
  berasal dari intentional fault-injection test.
- Bukti detail dapat dipindahkan ke lampiran ketika versi PDF menjadi terlalu
  panjang, tetapi referensinya tetap dipertahankan pada pembahasan utama.
