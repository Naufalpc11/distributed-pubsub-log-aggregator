Write-Host "Stopping containers and removing volumes..."
docker compose down -v --remove-orphans

Write-Host "Rebuilding and starting core services..."
docker compose up --build -d postgres redis aggregator-api aggregator-worker-1 aggregator-worker-2

Write-Host "Current containers:"
docker compose ps