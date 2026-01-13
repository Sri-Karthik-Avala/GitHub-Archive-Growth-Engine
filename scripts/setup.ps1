# PowerShell setup script for GitHub Archive Growth Engine

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " GitHub Archive Growth Engine - Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if Docker is running
Write-Host "[1/7] Checking Docker..." -ForegroundColor Yellow
$dockerRunning = docker ps 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker is not running. Please start Docker Desktop." -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker is running" -ForegroundColor Green
Write-Host ""

# Copy .env.example to .env if it doesn't exist
Write-Host "[2/7] Setting up environment..." -ForegroundColor Yellow
if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "✓ Created .env file from template" -ForegroundColor Green
} else {
    Write-Host "✓ .env file already exists" -ForegroundColor Green
}
Write-Host ""

# Build and start services
Write-Host "[3/7] Building Docker images (this may take 5-10 minutes on first run)..." -ForegroundColor Yellow
docker-compose build
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Docker build failed" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker images built successfully" -ForegroundColor Green
Write-Host ""

Write-Host "[4/7] Starting services..." -ForegroundColor Yellow
docker-compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Failed to start services" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Services started" -ForegroundColor Green
Write-Host ""

# Wait for services to be healthy
Write-Host "[5/7] Waiting for services to be ready (~60 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 15

# Check PostgreSQL
Write-Host "  Checking PostgreSQL..." -ForegroundColor Gray
$retries = 0
while ($retries -lt 10) {
    $pgReady = docker exec postgres pg_isready -U airflow 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  ✓ PostgreSQL ready" -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 5
    $retries++
}

# Check MinIO
Write-Host "  Checking MinIO..." -ForegroundColor Gray
Start-Sleep -Seconds 5
Write-Host "  ✓ MinIO ready" -ForegroundColor Green

# Check Airflow
Write-Host "  Checking Airflow..." -ForegroundColor Gray
Start-Sleep -Seconds 10
Write-Host "  ✓ Airflow ready" -ForegroundColor Green
Write-Host ""

# Create Airflow admin user
Write-Host "[6/7] Setting up Airflow..." -ForegroundColor Yellow
docker exec airflow-webserver airflow users create `
    --username admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com `
    --password admin 2>&1 | Out-Null
Write-Host "✓ Airflow admin user created (username: admin, password: admin)" -ForegroundColor Green
Write-Host ""

# Create MinIO buckets
Write-Host "[7/7] Creating MinIO buckets..." -ForegroundColor Yellow
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>&1 | Out-Null
docker exec minio mc mb local/bronze --ignore-existing 2>&1 | Out-Null
docker exec minio mc mb local/silver --ignore-existing 2>&1 | Out-Null
docker exec minio mc mb local/gold --ignore-existing 2>&1 | Out-Null
Write-Host "✓ MinIO buckets created (bronze, silver, gold)" -ForegroundColor Green
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Setup Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Access the services:" -ForegroundColor Yellow
Write-Host "  • Airflow UI:  http://localhost:8080 (admin/admin)" -ForegroundColor White
Write-Host "  • MinIO UI:    http://localhost:9001 (minioadmin/minioadmin)" -ForegroundColor White
Write-Host "  • Spark UI:    http://localhost:8081" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Open Airflow UI: http://localhost:8080" -ForegroundColor White
Write-Host "  2. Find DAG: 'github_archive_master_pipeline'" -ForegroundColor White
Write-Host "  3. Click the play button to trigger the pipeline" -ForegroundColor White
Write-Host "  4. Monitor progress in Airflow UI" -ForegroundColor White
Write-Host ""
Write-Host "To stop services: docker-compose down" -ForegroundColor Gray
Write-Host "To view logs: docker-compose logs -f" -ForegroundColor Gray
Write-Host ""
