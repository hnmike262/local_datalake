# MinIO (Object Storage)

## Overview

MinIO là nơi lưu trữ dữ liệu của hệ thống, cung cấp S3-compatible object storage cho data lakehouse.

### Bucket Architecture

| Bucket | Mục đích | Quản lý bởi |
|--------|----------|-------------|
| `lol-bronze` | **Raw Landing Zone** - Parquet thô từ Riot API | Python scriptnơi
| `lakehouse` | **Iceberg Warehouse** - Tables với ACID & Time Travel | Iceberg Catalog |


---

## Services Integration

| Service | Protocol | Bucket | Action |
|---------|----------|--------|--------|
| **Python Scripts** | `minio` lib | `lol-bronze` | `fput_object()` upload Parquet |
| **Spark** | `s3a://` | Both | Read raw → Write Iceberg |
| **Trino** | Hive/Iceberg Catalog | Both | Query & Transform |
| **dbt** | SQL via Trino | `lakehouse` | Bronze → Silver → Gold |
| **Iceberg Catalog** | REST API | `lakehouse` | Manage table metadata |
| **Power BI** | ODBC via Trino | `lakehouse` | Read Gold tables |

---

## Data Flow

```
Step 1: Python scripts upload Parquet → lol-bronze
            │
            ▼
Step 2: Spark reads lol-bronze, writes Iceberg → lakehouse/bronze
            │
            ▼
Step 3: dbt transforms via Trino: bronze → silver → gold
            │
            ▼
Step 4: Power BI queries Trino → reads from lakehouse/gold
```

---

## Quick Reference

| Property | Value |
|----------|-------|
| **Ports** | `9000` (S3 API), `9001` (Console) |
| **Config** | `.env`  |



## Getting Started

### Step 1: Configure Credentials

Create or update the `.env` file with MinIO credentials:

```bash
# .env
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=miniopassword123

# AWS S3 (for Iceberg)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=miniopassword123
```

### Step 2: Start MinIO Service

```bash
docker compose up -d minio
```


### Step 3: Access MinIO Console

1. Open browser: http://localhost:9001
2. Login with: `minioadmin` / `miniopassword123`
3. Verify `lakehouse` bucket exists

![MinIO Console](../../images/minio-console.png)

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_ROOT_USER` | `minioadmin` | Admin username |
| `MINIO_ROOT_PASSWORD` | `miniopassword123` | Admin password |

### Critical Configurations

Để các services kết nối được với MinIO, cần đảm bảo các config sau:

| Config | Value | Mô tả |
|--------|-------|-------|
| **Endpoint (internal)** | `http://minio:9000` | Các container gọi MinIO qua Docker network |
| **Endpoint (host)** | `http://localhost:9000` | Python scripts chạy từ máy host |
| **Credentials** | Đồng nhất | Phải giống nhau trong `.env`, `docker-compose.yml`, và catalog configs |
| **Path-Style Access** | `true` | Bắt buộc: dùng `endpoint/bucket` thay vì `bucket.endpoint` |
| **Region** | `us-east-1` | MinIO local vẫn yêu cầu region |

**Trong Spark/Trino config:**
```properties
# iceberg.properties
s3.endpoint = http://minio:9000
s3.path-style-access = true
s3.region = us-east-1
```



| Path | Source | Description |
|------|--------|-------------|
| `lol-bronze/ladder/data.parquet` | `01_ladder.py` | Ranked ladder data |
| `lol-bronze/match_ids/data.parquet` | `02_matchids.py` | Match ID list |
| `lol-bronze/matches_participants/data.parquet` | `03_matches.py` | Player stats per match |
| `lol-bronze/matches_teams/data.parquet` | `03_matches.py` | Team stats per match |

### lakehouse (Iceberg Warehouse)

Chứa Iceberg tables với metadata quản lý:

| Path | Layer | Description |
|------|-------|-------------|
| `lakehouse/warehouse/bronze.db/` | Bronze | Raw tables with schema |
| `lakehouse/warehouse/silver.db/` | Silver | Cleaned/staged tables |
| `lakehouse/warehouse/gold.db/` | Gold | Star schema (dim/fct) |

---


## Implementation

### Prerequisites

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start MinIO:**
   ```bash
   docker compose up -d minio minio-init
   ```

3. **Run Riot API extraction first:**
   ```bash
   cd data/extraction
   python 01_ladder.py
   python 02_matchids.py --count 20
   python 03_matches.py --limit 200
   ```

### Step 1: Verify Local Bronze Data

**Command:**
```bash
ls -la data/bronze/
```

### Step 2: Upload to MinIO (lol-bronze bucket)

**Command:**
```bash
docker exec spark spark-submit /opt/spark/jobs/load_bronze.py
```
### Step 3: Verify Upload in MinIO

**Command:**
```bash
aws --endpoint-url http://localhost:9000 s3 ls s3://lol-bronze/ 
```


---

```

### Upload File

```bash
aws --endpoint-url http://localhost:9000 s3 cp data/sample.csv s3://lakehouse/raw/
```



---

[← Back to Services](../README.md#service-guides)
