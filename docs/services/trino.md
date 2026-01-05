# Trino (SQL Query Engine)

## Overview

Trino là Query Engine trung tâm của hệ thống, kết nối tất cả thành phần từ lưu trữ (MinIO) đến quản lý bảng (Iceberg) và phục vụ dữ liệu cho dbt/Power BI.

> 💡 **Ẩn dụ:** Nếu MinIO là kho chứa linh kiện rời rạc, thì Trino là người thợ lắp ráp. Người thợ dùng cuốn sổ hướng dẫn (Iceberg Catalog) để biết linh kiện nằm ở đâu và lắp thành sản phẩm hoàn chỉnh (Gold Tables) để giao cho khách hàng (Power BI).

---

## Services Integration

```
┌─────────────────────────────────────────────────────────────────────────┐
│                               Trino                                      │
│                         (Query Engine)                                   │
│                                                                          │
│   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    │
│   │  Hive Catalog   │    │ Iceberg Catalog │    │  System Catalog │    │
│   │  (raw Parquet)  │    │ (ACID tables)   │    │  (internal)     │    │
│   └────────┬────────┘    └────────┬────────┘    └─────────────────┘    │
│            │                      │                                      │
└────────────┼──────────────────────┼──────────────────────────────────────┘
             │                      │
             ▼                      ▼
    ┌────────────────┐     ┌────────────────┐
    │ lol-bronze     │     │ Iceberg REST   │──► PostgreSQL (metadata)
    │ (MinIO bucket) │     │ Catalog        │
    └────────────────┘     └───────┬────────┘
                                   │
                                   ▼
                           ┌────────────────┐
                           │   lakehouse    │
                           │ (MinIO bucket) │
                           └────────────────┘
```

| Service | Kết nối với Trino | Vai trò |
|---------|-------------------|---------|
| **MinIO** | S3 endpoint `http://minio:9000` | Lưu trữ file Parquet |
| **Iceberg REST Catalog** | REST `http://iceberg-rest:8181` | Quản lý metadata, snapshot, ACID |
| **PostgreSQL** | Internal (via Iceberg) | Lưu danh sách bảng |
| **dbt-trino** | JDBC `localhost:8082` | Gửi SQL transforms |
| **Airflow** | Orchestrates dbt runs | Lập lịch ETL |
| **Power BI** | ODBC `localhost:8082` | Dashboard & Reports |

---

## Data Flow through Trino

```
Step 1: Đăng ký dữ liệu thô (Hive Catalog)
        ┌─────────────────────────────────────────────────────────┐
        │ CREATE TABLE hive.lol_bronze.ladder_ext (...)           │
        │ WITH (external_location = 's3://lol-bronze/ladder/')    │
        └─────────────────────────────────────────────────────────┘
                                   │
                                   ▼
Step 2: Chuyển đổi sang Iceberg (Bronze)
        ┌─────────────────────────────────────────────────────────┐
        │ INSERT INTO iceberg.bronze.ladder                       │
        │ SELECT * FROM hive.lol_bronze.ladder_ext                │
        └─────────────────────────────────────────────────────────┘
                                   │
                                   ▼
Step 3: dbt transforms (Silver → Gold)
        ┌─────────────────────────────────────────────────────────┐
        │ dbt run → SQL via Trino                                 │
        │ iceberg.bronze → iceberg.silver → iceberg.gold          │
        └─────────────────────────────────────────────────────────┘
```

---

## Quick Reference

| Property | Value |
|----------|-------|
| **Ports** | `8082` (host) → `8080` (container) |
| **Config** | `trino/etc/`, `trino/catalog/` |
| **Healthcheck** | `curl http://localhost:8082/v1/info` |
| **Depends On** | `iceberg-rest`, `minio` |

---

## Getting Started

### Step 1: Start Trino Service

```bash
docker compose up -d trino
```

**Expected Output:**

```
[+] Running 1/1
 ✔ Container trino  Started
```

### Step 2: Wait for Healthy Status

Trino takes about 60-90 seconds to fully start.

```bash
docker compose ps trino
```

**Expected Output:**

```
NAME    IMAGE                   STATUS          PORTS
trino   trinodb/trino:latest    Up (healthy)    0.0.0.0:8082->8080/tcp
```

### Step 3: Verify API Endpoint

```bash
curl -s http://localhost:8082/v1/info | jq
```

**Expected Output:**

```json
{
  "nodeVersion": {
    "version": "442"
  },
  "environment": "docker",
  "coordinator": true,
  "starting": false,
  "uptime": "5.00m"
}
```

### Step 3: Connect with DataGrip

DataGrip là IDE database chuyên nghiệp từ JetBrains, hỗ trợ kết nối Trino để explore Iceberg tables.

#### 3.1. Tạo Data Source

1. Mở DataGrip → **File** → **New** → **Data Source** → **Trino**
2. Nếu chưa có driver, click **Download** để tải Trino JDBC driver

#### 3.2. Cấu hình Connection

| Property | Value |
|----------|-------|
| **Host** | `localhost` |
| **Port** | `8082` |
| **User** | `admin` (hoặc bất kỳ tên nào) |
| **Password** | (để trống) |
| **Database** | `iceberg` |

**JDBC URL:**
```
jdbc:trino://localhost:8082/iceberg
```

#### 3.3. Test Connection

1. Click **Test Connection** → Expected: ✅ Successful
2. Click **OK** để lưu

![DataGrip Trino Connection](../../images/datagrip-trino-connection.png)

#### 3.4. Explore Iceberg Tables

Sau khi kết nối, DataGrip hiển thị cấu trúc catalog:

```
iceberg (catalog)
├── bronze (schema)
│   ├── ladder
│   ├── match_ids
│   ├── matches_participants
│   └── matches_teams
├── silver (schema)
│   ├── stg_ladder
│   ├── stg_matches_participants
│   └── stg_matches_teams
└── gold (schema)
    ├── dim_champion
    ├── dim_date
    ├── dim_player
    ├── dim_queue
    ├── dim_rank
    ├── dim_region
    └── fct_participant_match
```

#### 3.5. Xem Table Structure

**Cách 1:** Double-click vào table để xem schema

**Cách 2:** Chuột phải → **Navigate** → **Go to DDL**

```sql
-- DDL của dim_champion
CREATE TABLE iceberg.gold.dim_champion (
    champion_key INTEGER,
    champion_id VARCHAR,
    champion_name VARCHAR
) WITH (format = 'PARQUET');
```

#### 3.6. Query Data

Mở **Console** và chạy SQL:

```sql
-- Xem top 10 players
SELECT * FROM iceberg.gold.dim_player LIMIT 10;

-- Count participants per match
SELECT match_id, COUNT(*) as participants
FROM iceberg.gold.fct_participant_match
GROUP BY match_id
LIMIT 5;

-- Join dimension tables
SELECT 
    p.summoner_name,
    c.champion_name,
    f.kills,
    f.deaths,
    f.assists
FROM iceberg.gold.fct_participant_match f
JOIN iceberg.gold.dim_player p ON f.player_key = p.player_key
JOIN iceberg.gold.dim_champion c ON f.champion_key = c.champion_key
LIMIT 20;
```

![DataGrip Query Results](../../images/datagrip-query-results.png)

#### 3.7. Xem Iceberg Metadata

Trino cung cấp các system tables để xem metadata của Iceberg:

```sql
-- Xem snapshots (version history)
SELECT * FROM iceberg.gold."fct_participant_match$snapshots";

-- Xem files trong table
SELECT * FROM iceberg.gold."fct_participant_match$files";

-- Xem partitions
SELECT * FROM iceberg.gold."fct_participant_match$partitions";

-- Xem table properties
SELECT * FROM iceberg.gold."fct_participant_match$properties";

-- Xem manifest files
SELECT * FROM iceberg.gold."fct_participant_match$manifests";
```

**Snapshots Output Example:**

| committed_at | snapshot_id | parent_id | operation | summary |
|--------------|-------------|-----------|-----------|---------|
| 2025-01-05 10:30:00 | 123456789 | NULL | append | {"added-records":"2000"} |

#### 3.8. Time Travel Queries

Iceberg hỗ trợ query dữ liệu tại thời điểm cũ:

```sql
-- Query by snapshot ID
SELECT * FROM iceberg.gold.fct_participant_match
FOR VERSION AS OF 123456789
LIMIT 10;

-- Query by timestamp
SELECT * FROM iceberg.gold.fct_participant_match
FOR TIMESTAMP AS OF TIMESTAMP '2025-01-04 00:00:00'
LIMIT 10;
```

---

### Step 4: Access Web UI

1. Open browser: http://localhost:8082
2. No authentication required
3. View active queries and cluster info

![Trino Web UI](../../images/trino-ui.png)

### Step 5: Test Trino CLI

```bash
docker exec -it trino trino --catalog iceberg --schema gold
```

**Expected Output:**

```
trino:gold>
```

---

## Configuration

```
trino/
├── etc/
│   ├── config.properties    # Coordinator settings
│   └── jvm.config           # JVM memory (default: 2G heap)
└── catalog/
    ├── iceberg.properties   # Iceberg catalog (Silver/Gold)
    └── hive.properties      # Hive catalog (Bronze raw)
```

### Catalog 1: iceberg.properties (Silver & Gold)

Quản lý các bảng Iceberg với ACID và Time Travel:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://lakehouse/warehouse

# MinIO S3 configuration
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=minioadmin
s3.aws-secret-key=miniopassword123
s3.path-style-access=true
s3.region=us-east-1
```

### Catalog 2: hive.properties (Bronze Raw)

Đọc file Parquet thô từ `lol-bronze` bucket:

```properties
connector.name=hive
hive.metastore=file
hive.metastore.catalog.dir=s3://lol-bronze/

# MinIO S3 configuration
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.aws-access-key=minioadmin
s3.aws-secret-key=miniopassword123
s3.path-style-access=true
s3.region=us-east-1
```

### dbt Connection (profiles.yml)

```yaml
# ~/.dbt/profiles.yml
lol_lakehouse:
  target: dev
  outputs:
    dev:
      type: trino
      method: none           # No auth for local
      user: admin
      host: localhost        # hoặc 'trino' nếu trong Docker
      port: 8082
      database: iceberg      # Tên catalog
      schema: gold           # Default schema
      threads: 4
```

### Power BI Connection

| Property | Value |
|----------|-------|
| Driver | Trino ODBC / Simba ODBC |
| Host | `localhost` |
| Port | `8082` |
| Catalog | `iceberg` |
| Schema | `gold` |
| User | `admin` (hoặc bất kỳ) |
| Password | (để trống) |

---

## Common Tasks

### List Catalogs

```bash
docker exec trino trino --execute "SHOW CATALOGS"
```

**Expected Output:**

```
 Catalog
---------
 iceberg
 system
(2 rows)
```

### List Schemas

```bash
docker exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
```

**Expected Output:**

```
       Schema
--------------------
 bronze
 silver
 gold
 information_schema
(4 rows)
```

### List Tables

```bash
docker exec trino trino --execute "SHOW TABLES FROM iceberg.gold"
```

**Expected Output:**

```
        Table
--------------------
 dim_champion
 dim_date
 dim_player
 fct_participant_match
(4 rows)
```

### Describe Table

```bash
docker exec trino trino --execute "DESCRIBE iceberg.gold.dim_champion"
```

**Expected Output:**

```
    Column     |   Type    | Extra | Comment
---------------+-----------+-------+---------
 champion_key  | integer   |       |
 champion_id   | varchar   |       |
 champion_name | varchar   |       |
(3 rows)
```

### Run Query

```bash
docker exec trino trino --execute "SELECT * FROM iceberg.gold.dim_champion LIMIT 5"
```

**Expected Output:**

```
 champion_key | champion_id | champion_name
--------------+-------------+---------------
            1 | Aatrox      | Aatrox
            2 | Ahri        | Ahri
            3 | Akali       | Akali
            4 | Akshan      | Akshan
            5 | Alistar     | Alistar
(5 rows)
```

---

## Creating Tables

### Create Schema

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.test;
```

### Create Table

```sql
CREATE TABLE iceberg.test.sample (
    id INT,
    name VARCHAR,
    created_at TIMESTAMP
) WITH (format = 'PARQUET');
```

### Insert Data

```sql
INSERT INTO iceberg.test.sample VALUES (1, 'test', current_timestamp);
```

### Query Data

```sql
SELECT * FROM iceberg.test.sample;
```

**Expected Output:**

```
 id | name |        created_at
----+------+---------------------------
  1 | test | 2025-01-01 10:00:00.000
(1 row)
```

---

## JDBC Connection

For Power BI, DBeaver, or other BI tools:

| Property | Value |
|----------|-------|
| JDBC URL | `jdbc:trino://localhost:8082/iceberg/gold` |
| User | `admin` (or any username) |
| Password | (leave empty) |

### Power BI Setup

1. Install Trino ODBC driver from [Trino Downloads](https://trino.io/download.html)
2. Create DSN with:
   - Host: `localhost`
   - Port: `8082`
   - Catalog: `iceberg`
   - Schema: `gold`
3. Connect using ODBC data source

![Power BI Connection](../../images/powerbi-trino.png)

---

## Performance Tips

### Partitioning

```sql
CREATE TABLE iceberg.gold.events (
    event_date DATE,
    event_type VARCHAR,
    data VARCHAR
) WITH (
    format = 'PARQUET',
    partitioning = ARRAY['event_date']
);
```

### Query Optimization

```sql
-- Use WHERE clauses on partition columns
SELECT * FROM iceberg.gold.fct_participant_match
WHERE match_date >= DATE '2024-01-01';

-- Check query plan
EXPLAIN SELECT * FROM iceberg.gold.dim_champion;
```

---

## Troubleshooting

| Symptom | Check | Fix |
|---------|-------|-----|
| UI not accessible | `docker compose logs trino` | Wait for startup (90s), check port 8082 |
| "Catalog iceberg does not exist" | `cat trino/catalog/iceberg.properties` | Verify REST URI, restart Trino |
| "Table not found" | Run `SHOW TABLES` | Check schema name, run dbt first |
| Out of memory errors | Check `trino/etc/jvm.config` | Increase `-Xmx` or reduce concurrent queries |
| Slow queries | Check Trino UI → Query Details | Add `WHERE` clauses, check partitioning |

### View Trino Logs

```bash
docker compose logs trino -f
```

### Test Iceberg Connectivity

```bash
docker exec -it trino curl http://iceberg-rest:8181/v1/config
```

**Expected Output:**

```json
{"defaults":{},"overrides":{"warehouse":"s3://lakehouse/warehouse"}}
```

### Restart Trino

```bash
docker compose restart trino
```

---

## Iceberg REST Catalog

> 📦 **Iceberg** cung cấp metadata management cho Trino, cho phép ACID transactions và time-travel queries.

### Quick Reference

| Property | Value |
|----------|-------|
| **Port** | `8181` (REST API) |
| **Healthcheck** | `curl http://localhost:8181/v1/config` |
| **Depends On** | `minio`, `iceberg-db` (PostgreSQL) |

### Start Iceberg Services

```bash
# Step 1: Start PostgreSQL (lưu metadata)
docker compose up -d iceberg-db

# Step 2: Wait for healthy
docker compose ps iceberg-db
# Expected: STATUS = Up (healthy)

# Step 3: Start Iceberg REST
docker compose up -d iceberg-rest

# Step 4: Verify
curl -s http://localhost:8181/v1/config | jq
```

**Expected Output:**

```json
{
  "defaults": {},
  "overrides": {
    "warehouse": "s3://lakehouse/warehouse"
  }
}
```

![Iceberg REST API](../../images/iceberg-rest.png)

### Iceberg API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/config` | GET | Catalog configuration |
| `/v1/namespaces` | GET | List all namespaces (schemas) |
| `/v1/namespaces/{ns}/tables` | GET | List tables in namespace |
| `/v1/namespaces/{ns}/tables/{table}` | GET | Get table metadata |

### List Tables via REST API

```bash
# List namespaces
curl -s http://localhost:8181/v1/namespaces | jq

# List bronze tables
curl -s http://localhost:8181/v1/namespaces/bronze/tables | jq

# List gold tables
curl -s http://localhost:8181/v1/namespaces/gold/tables | jq
```

### How Iceberg Maps to S3

| Iceberg Table | S3 Path |
|---------------|---------|
| `iceberg.bronze.ladder` | `s3://lakehouse/warehouse/bronze.db/ladder/` |
| `iceberg.silver.stg_ladder` | `s3://lakehouse/warehouse/silver.db/stg_ladder/` |
| `iceberg.gold.dim_champion` | `s3://lakehouse/warehouse/gold.db/dim_champion/` |

### S3 Directory Structure

```
s3://lakehouse/warehouse/
├── bronze.db/
│   ├── ladder/
│   │   ├── metadata/
│   │   │   └── 00000-xxxxx.metadata.json
│   │   └── data/
│   │       └── 00000-xxxxx.parquet
│   └── ...
├── silver.db/
│   └── stg_ladder/
└── gold.db/
    ├── dim_champion/
    └── fct_participant_match/
```

### Iceberg Troubleshooting

| Symptom | Check | Fix |
|---------|-------|-----|
| 500 error on `/v1/config` | `docker compose logs iceberg-rest` | Check S3 credentials |
| "Catalog not available" in Trino | Verify REST is healthy | Restart `iceberg-rest` |
| Database connection failed | `docker compose ps iceberg-db` | Restart PostgreSQL |

```bash
# Check PostgreSQL connection
docker exec -it iceberg-db psql -U iceberg -d iceberg -c "\dt"

# Restart Iceberg
docker compose restart iceberg-rest
```

> ⚠️ **Production Note:** `apache/iceberg-rest-fixture` is for dev/testing only. In production, use AWS Glue, Nessie, or Polaris.

---

[← Back to Services](../README.md#service-guides)
