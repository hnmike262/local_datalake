# Apache Airflow (Orchestration)

## Overview

Airflow orchestrates the data pipeline, scheduling and monitoring dbt transformations through the medallion layers (Bronze → Silver → Gold).

## Quick Reference

| Property | Value |
|----------|-------|
| **Ports** | `8083` (Web UI), `8793` (worker logs) |
| **Config** | `docker-compose.yml` environment |
| **DAGs** | `airflow/dags/` |
| **Depends On** | `airflow-db`, `trino`, `minio` |
| **Login** | `admin` / `admin` |

---


## Getting Started

### Step 1: Start Airflow Database

```bash
docker compose up -d airflow-db
```


### Step 2: Start Airflow Webserver & Scheduler

```bash
docker compose up -d airflow
```


### Step 3: Access Airflow UI

1. Open browser: http://localhost:8083
2. Login with: `admin` / `admin`
3. View DAGs in the dashboard

![Airflow DAGs](../../images/3irflow-dags.png)

---

## Configuration Verification

---

### Verify dbt được cài trong Airflow container

```bash
docker exec -it airflow dbt --version
```
---

### Verify DAGs folder được mount

```bash
docker exec -it airflow ls /opt/airflow/dags/
```
---

### Verify dbt project được mount

```bash
docker exec -it airflow ls /opt/dbt/
```

---

## DAG Structure

### Xem DAG file

```bash
cat airflow/dags/pipeline.py
```

#### 4. Task Dependencies - Thứ tự chạy

```python
dbt_bronze >> dbt_silver >> dbt_gold
```


---

[← Back to Services](../README.md#service-guides)
