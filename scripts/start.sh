#!/bin/bash
set -e

echo "🚀 Starting Local Data Lakehouse..."
echo

# Start all services
docker compose up -d

echo "⏳ Waiting for services to stabilize..."
sleep 10

# Wait for all services to be healthy (max 5 minutes)
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
  healthy=0
  
  # Count healthy services
  if docker compose ps | grep -E "minio|iceberg-db|iceberg-rest|trino|spark|airflow" | grep -q "(Up|healthy)"; then
    healthy=$((healthy + 1))
  fi
  
  if [ $attempt -eq 0 ] || [ $((attempt % 6)) -eq 0 ]; then
    echo "Status check $((attempt + 1))/$max_attempts..."
  fi
  
  attempt=$((attempt + 1))
  sleep 10
done

echo
echo "✓ All services started"
echo

# Run validation
if [ -f "scripts/validate.sh" ]; then
  echo "Running validation checks..."
  bash scripts/validate.sh
else
  echo "Validation script not found"
fi

echo
echo "Setup complete! Access services at:"
echo "  MinIO:      http://localhost:9001"
echo "  Trino:      http://localhost:8082"
echo "  Airflow:    http://localhost:8083"
