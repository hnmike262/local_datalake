# dbt (Data Transformation)

## Overview

dbt (data build tool) handles SQL transformations in the data lakehouse, transforming data through Bronze → Silver → Gold layers using modular SQL models.

## Quick Reference

| Property | Value |
|----------|-------|
| **Location** | `./dbt` (host) hoặc `/opt/dbt` (Airflow container) |
| **Config** | `dbt/dbt_project.yml`, `dbt/profiles.yml` |
| **Adapter** | `dbt-trino==1.10.0` |
| **Connection** | Trino tại `localhost:8082` (local) / `trino:8080` (docker) |
| **Catalog** | `iceberg` |
| **Run From** | Host machine (`--target local`) hoặc Airflow (`--target docker`) |

---

## Table of Contents

1. [Project Initialization](#1-project-initialization)
3. [Configuration ](#3-configuration-verification)
4. [Medallion Layers Verification](#4-medallion-layers-verification)
5. [Model Dependencies & Lineage](#5-model-dependencies--lineage)
6. [Documentation](#6-documentation)

---

## 1. Project Initialization

### 1.1 Khởi Tạo Project Mới (Fresh Setup)

```bash
pip install "dbt-trino==1.10.0"
dbt init "project-name"
```


## 2. Config2ration Verification

### 3.1 Verify profiles.yml

**Mục đích:** Định nghĩa cách dbt kết nối Trino



---

### 3.2 Verify dbt_project.yml

**Mục đích:** Định nghĩa materialization cho từng layer

**Kiểm tra materialization config:**



---

### 3.4 Verify sources.yml

**Mục đích:** Định nghĩa các bảng Bronze từ Spark


---

## 4. Medallion Layers Verification

### 4.1 Verify Schemas tồn tại trong Trino

```bash
docker exec -it trino trino -e "SHOW SCHEMAS FROM iceberg"
```

---

### 4.2 Bronze Layer Verification

**a) dbt thấy Bronze models:**

```bash
dbt ls --select path:models/bronze --profiles-dir . 
```


### 4.3 Silver Layer Verification

**a) dbt thấy Silver models:**

```bash
dbt ls --select path:models/silver --profiles-dir . 
```


### 4.4 Gold Layer Verification

**a Gold models:**

```bash
dbt ls --select path:models/gold --profiles-dir . 
```
---

### 4.5 Verify toàn bộ Pipeline

**Chạy full pipeline:**

```bash
dbt run 
```


**Chạy tests:**

```bash
dbt test --profiles-dir . 
```



---

## 5. Model Dependencies & Lineage

### 5.1 Xem dependency graph bằng dbt ls

```bash
dbt ls --select +fct_participant_match --profiles-dir . --target local
```

### 5.2 Xem lineage trong dbt Docs

```bash
dbt docs generate --profiles-dir . --target local
dbt docs serve --port 8080 --profiles-dir . --target local
```

Mở browser: `http://localhost:8080`


---

[← Back to Services](../README.md#service-guides)
