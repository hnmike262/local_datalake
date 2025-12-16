#!/bin/bash
set -e

echo "=== Local Data Lakehouse Validation ==="
echo

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'  # No Color

# Check MinIO
echo -n "✓ Checking MinIO health... "
if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
  echo -e "${GREEN}OK${NC}"
else
  echo -e "${RED}FAILED${NC}"
  exit 1
fi

# Check Iceberg REST
echo -n "✓ Checking Iceberg REST... "
if curl -s http://localhost:8181/v1/config > /dev/null 2>&1; then
  echo -e "${GREEN}OK${NC}"
else
  echo -e "${RED}FAILED${NC}"
  exit 1
fi

# Check Trino
echo -n "✓ Checking Trino... "
if curl -s http://localhost:8082/ui/ > /dev/null 2>&1; then
  echo -e "${GREEN}OK${NC}"
else
  echo -e "${RED}FAILED${NC}"
  exit 1
fi

# Check Airflow
echo -n "✓ Checking Airflow... "
if curl -s http://localhost:8083/health > /dev/null 2>&1; then
  echo -e "${GREEN}OK${NC}"
else
  echo -e "${RED}FAILED${NC}"
  exit 1
fi

echo
echo "=== All services healthy ==="
echo
echo "Access URLs:"
echo "  MinIO:      http://localhost:9001"
echo "  Trino:      http://localhost:8082"
echo "  Airflow:    http://localhost:8083"
echo "  Iceberg:    http://localhost:8181"
echo
