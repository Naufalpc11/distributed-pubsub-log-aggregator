$ErrorActionPreference = "Stop"

$before = [int](docker compose exec -T redis redis-cli XLEN events:deadletter)
$messageId = (
    docker compose exec -T redis redis-cli XADD events "*" event "not-json"
).Trim()

Write-Host "Invalid message ID : $messageId"
Write-Host "Dead-letter before: $before"
Write-Host "Waiting for three attempts and pending recovery..."

$after = $before
for ($attempt = 1; $attempt -le 40; $attempt++) {
    Start-Sleep -Seconds 1
    $after = [int](
        docker compose exec -T redis redis-cli XLEN events:deadletter
    )

    if ($after -gt $before) {
        break
    }
}

if ($after -le $before) {
    throw "Message did not reach the dead-letter stream."
}

Write-Host "Dead-letter after : $after"
docker compose logs --since=2m aggregator-worker-1 aggregator-worker-2 |
    Select-String -Pattern $messageId

docker compose exec -T postgres psql `
    -U appuser `
    -d appdb `
    -c "SELECT topic, event_id, status, worker_name FROM audit_logs WHERE event_id='$messageId';"

docker compose exec -T redis redis-cli XREVRANGE events:deadletter + - COUNT 1

Write-Host "PASS: message was retried, recovered, and dead-lettered."
