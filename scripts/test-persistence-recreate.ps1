$ErrorActionPreference = "Stop"

$suffix = [guid]::NewGuid().ToString("N").Substring(0, 12)
$topic = "persistence.recreate.$suffix"
$eventId = "persist-$suffix"
$timestamp = (Get-Date).ToUniversalTime().ToString("o")

$body = @{
    topic = $topic
    event_id = $eventId
    timestamp = $timestamp
    source = "persistence-recreate-test"
    payload = @{
        marker = $suffix
    }
} | ConvertTo-Json -Depth 4 -Compress

Write-Host "Publishing marker event: $eventId"
Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/publish" `
    -ContentType "application/json" `
    -Body $body | Out-Null

$before = 0
for ($attempt = 1; $attempt -le 20; $attempt++) {
    Start-Sleep -Milliseconds 500
    $before = docker compose exec -T postgres psql `
        -U appuser `
        -d appdb `
        -Atc "SELECT COUNT(*) FROM processed_events WHERE topic='$topic' AND event_id='$eventId';"

    if ($before.Trim() -eq "1") {
        break
    }
}

if ($before.Trim() -ne "1") {
    throw "Marker event was not processed before container recreate."
}

Write-Host "Before recreate: $($before.Trim()) row"
docker compose up -d --force-recreate postgres

$healthy = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    Start-Sleep -Seconds 1
    $status = docker inspect --format "{{.State.Health.Status}}" uas-postgres

    if ($status -eq "healthy") {
        $healthy = $true
        break
    }
}

if (-not $healthy) {
    throw "PostgreSQL did not become healthy after recreate."
}

$after = docker compose exec -T postgres psql `
    -U appuser `
    -d appdb `
    -Atc "SELECT COUNT(*) FROM processed_events WHERE topic='$topic' AND event_id='$eventId';"

Write-Host "After recreate : $($after.Trim()) row"

if ($after.Trim() -ne "1") {
    throw "Persistence failed: marker event disappeared after recreate."
}

Write-Host "PASS: named volume preserved data after PostgreSQL container recreate."
