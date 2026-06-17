Write-Host "GET /health"
curl.exe http://localhost:8080/health

Write-Host "`nGET /ready"
curl.exe http://localhost:8080/ready

Write-Host "`nPOST /publish manual event"
curl.exe -X POST http://localhost:8080/publish -H "Content-Type: application/json" -d '{\"topic\":\"auth.login\",\"event_id\":\"manual-001\",\"timestamp\":\"2026-06-11T10:00:00Z\",\"source\":\"manual-curl\",\"payload\":{\"user_id\":\"u001\",\"action\":\"login\"}}'

Write-Host "`nGET /events"
curl.exe http://localhost:8080/events

Write-Host "`nGET /stats"
curl.exe http://localhost:8080/stats
