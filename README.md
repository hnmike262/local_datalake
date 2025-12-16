# Local Data Lakehouse

A complete, Docker-based data lakehouse stack replicating AWS analytics services locally.

**Status:** MVP (Ready for development and testing)  

---

## Quick Start

### Prerequisites

- Docker Desktop (4.0+) with 8GB+ memory, 4+ CPUs
- WSL2 (for Windows)
- ~30GB free disk space

### 1-Minute Setup

```bash
# Start all services
bash scripts/start.sh  # (or start.ps1 on Windows)

# Wait for healthy status (~2-3 minutes)
bash scripts/validate.sh
```

### Access Services

| Service | URL | User | Pass |
|---------|-----|------|------|
| MinIO Console | http://localhost:9001 | minioadmin | miniopassword123 |
| Trino Web UI | http://localhost:8082 | — | — |
| Airflow | http://localhost:8083 | airflow | airflow |
| Iceberg REST API | http://localhost:8181 | — | — |

---

## Architecture

```
Raw CSV/JSON (tests/data/)
  ↓ [Spark ETL or direct load]
MinIO (S3-compatible) - bronze/silver/gold buckets
  ↓ [Iceberg REST Catalog]
Iceberg Tables (structured metadata)
  ↓ [Trino SQL Engine]
SQL Queries → dbt Transformations
  ↓ [Bronze → Silver → Gold medallion layers]
Airflow Orchestration (DAG scheduling)
  ↓
Analytics & Reporting
```

---

## Services

### MinIO (Object Storage)
- **Port:** 9000 (API), 9001 (Console)
- **Role:** S3-compatible storage for raw data and Iceberg tables
- **Buckets:** `raw`, `bronze`, `silver`, `gold`, `lakehouse`
- **Health Check:** `curl http://localhost:9000/minio/health/live`

### Iceberg REST Catalog
- **Port:** 8181
- **Role:** Metadata management for Iceberg tables
- **Backend:** PostgreSQL
- **Health Check:** `curl http://localhost:8181/v1/config`

### Trino
- **Port:** 8082
- **Role:** SQL query engine for interactive analytics
- **Catalog:** Iceberg (via REST)
- **Access:** `docker exec -it trino trino`

### Spark (Optional)
- **Jupyter:** http://localhost:8888
- **Spark UI:** http://localhost:8084
- **Role:** Optional ETL compute for Bronze ingestion
- **Config:** `spark/spark_config.py`

### dbt
- **Location:** `dbt/`
- **Models:** Bronze (views) → Silver (cleaned) → Gold (aggregated)
- **Connection:** Trino
- **Run:** `dbt run --profiles-dir dbt/`

### Airflow
- **Port:** 8083
- **Role:** Workflow orchestration and scheduling
- **DAGs:** `airflow/dags/`
- **Location:** `airflow/`

---

## Common Tasks

### 1. Ingest Sample Data

```bash
# Copy sample CSVs to MinIO raw bucket
aws s3 cp tests/data/sample_customers.csv s3://raw/sample_customers.csv \
  --endpoint-url http://localhost:9000 \
  --no-sign-request

# Or use MinIO console at http://localhost:9001
```

### 2. Run dbt Transformations

```bash
# From Bronze to Gold via dbt
cd dbt

# Run all models
dbt run --profiles-dir .

# Or run by layer
dbt run --select path:models/bronze
dbt run --select path:models/silver
dbt run --select path:models/gold

# Test data quality
dbt test --profiles-dir .
```

### 3. Query Data via Trino

```bash
# Open Trino CLI
docker exec -it trino trino

# List catalogs and tables
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.gold.daily_sales LIMIT 5;
```

### 4. Schedule with Airflow

```bash
# Access Airflow UI at http://localhost:8083
# Trigger DAG: medallion_pipeline
# Monitor task execution in Graph/Gantt view
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Services won't start | Check Docker Desktop resources, run `docker compose logs` |
| Trino can't find Iceberg catalog | Wait 60+ seconds, Trino is slow to start. Check `docker compose logs trino` |
| dbt can't connect to Trino | Verify `profiles.yml` has `host: trino` (not localhost). From Airflow, use service name. |
| MinIO buckets missing | Run `docker compose restart minio-init` to recreate buckets |
| Out of memory errors | Reduce Docker Desktop memory allocation or disable Spark |
| Airflow DAGs not visible | Restart Airflow: `docker compose restart airflow`. Check `docker compose logs airflow` |

---

## Project Structure

```
lakehouse/
├── docker-compose.yml          # Service definitions
├── .env                        # Credentials (gitignored)
├── scripts/
│   ├── start.sh                # Startup orchestration
│   ├── validate.sh             # Health checks
│   ├── init-minio.sh           # MinIO bucket setup
│   └── init-catalog.sh         # Iceberg schema setup
├── config/
│   ├── trino/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   └── catalog/iceberg.properties
│   └── airflow/Dockerfile
├── spark/
│   ├── jobs/                   # ETL job scripts
│   ├── notebooks/              # Jupyter notebooks
│   └── spark_config.py         # Session builder
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── bronze/             # Raw views
│       ├── silver/             # Cleaned tables
│       └── gold/               # Aggregations
├── airflow/
│   ├── dags/
│   │   ├── medallion_pipeline.py
│   │   └── ingest_raw.py
│   └── logs/
├── tests/
│   ├── data/
│   │   ├── sample_customers.csv
│   │   ├── sample_orders.csv
│   │   └── sample_products.csv
│   └── properties/             # Property-based tests
└── docs/
    ├── IMPLEMENTATION_PLAN.md
    └── ARCHITECTURE.md
```

---

## Next Steps

1. **Explore Services:** Access MinIO, Trino, Airflow UIs to familiarize yourself
2. **Load Sample Data:** Use `tests/data/*.csv` files to test ingestion
3. **Run dbt:** Execute transformations and query results in Trino
4. **Schedule DAGs:** Trigger Airflow DAGs to orchestrate end-to-end workflow
5. **Add Data Sources:** Extend `dbt/models/` and `airflow/dags/` for your use cases

---

## Cloud Migration

To deploy to AWS:

1. **Replace MinIO → S3:** Update `docker-compose.yml` to remove MinIO
2. **Replace Iceberg REST → Glue Catalog:** Use AWS Glue instead
3. **Replace Trino → Athena:** Use AWS Athena for queries
4. **Replace Airflow (LocalExecutor) → MWAA:** Use managed Airflow
5. **Keep dbt:** Same dbt config with different connection profile

See `docs/CLOUD_MIGRATION.md` for detailed steps.

---

## Support

- **Docker Issues:** Check `docker compose logs <service>`
- **Trino Queries:** Test in Trino CLI; check table existence
- **dbt Errors:** Run `dbt debug` to verify connections
- **Airflow Issues:** Check scheduler and executor logs

---

**Version:** 1.0.0-MVP  
**License:** MIT  
**Maintained by:** Data Engineering Team
