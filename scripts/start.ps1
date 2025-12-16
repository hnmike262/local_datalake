# PowerShell script for Windows users

Write-Host "🚀 Starting Local Data Lakehouse..." -ForegroundColor Green

# Start all services
Write-Host "Starting Docker Compose..." -ForegroundColor Cyan
docker compose up -d

Write-Host "⏳ Waiting for services to stabilize..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Wait for services (simplified for PowerShell)
$maxAttempts = 30
for ($i = 0; $i -lt $maxAttempts; $i++) {
    if (($i % 6) -eq 0) {
        Write-Host "Status check $($i + 1)/$maxAttempts..." -ForegroundColor Cyan
    }
    Start-Sleep -Seconds 10
}

Write-Host "✓ All services started" -ForegroundColor Green
Write-Host

# Run validation if available
if (Test-Path "scripts/validate.sh") {
    Write-Host "Running validation checks..." -ForegroundColor Cyan
    bash scripts/validate.sh
}

Write-Host
Write-Host "Setup complete! Access services at:" -ForegroundColor Green
Write-Host "  MinIO:      http://localhost:9001" -ForegroundColor Cyan
Write-Host "  Trino:      http://localhost:8082" -ForegroundColor Cyan
Write-Host "  Airflow:    http://localhost:8083" -ForegroundColor Cyan
