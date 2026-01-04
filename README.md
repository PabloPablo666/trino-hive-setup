# Discogs Lakehouse
## Local Trino + Hive Metastore

This repository contains a **fully reproducible local lakehouse setup** built on
**Trino**, **Hive Metastore**, and **external Parquet tables**, designed for
analytical workloads on large Discogs datasets.

The system is **local-first**, **compute-stateless**, and **explicitly decoupled**
from storage. All data lives **outside containers** and is queried directly via **SQL**.

---

## Architecture

The project follows a **standard lakehouse architecture**:

- **Query engine**: Trino  
- **Metastore**: Hive Metastore backed by Postgres  
- **Storage**: External Parquet data lake  
- **Orchestration**: Docker Compose  

Compute and metadata layers can be destroyed and rebuilt **without affecting the underlying data**.

---

## Repository contents

```
trino-hive-setup/
├── docker-compose.yml
├── trino-catalog/
│   └── hive.properties
├── bootstrap_discogs.sql
└── README.md
```

The **data lake itself** is stored **outside this repository**:

```
discogs_data_lake/
└── hive-data/
    ├── artists_v1/
    ├── artist_aliases_v1/
    ├── artist_memberships_v1/
    ├── releases_v6/
    ├── labels_v10/
    ├── masters_v1/
    ├── collection/
    └── warehouse_discogs/
        └── artist_name_map_v1/
```

---

## Data model

The lake is organized around **normalized Discogs entities and relationships**:

- **artists_v1**
- **artist_aliases_v1**
- **artist_memberships_v1**
- **releases_ref_v6**
- **labels_ref_v10**
- **masters_v1**
- **collection**

Derived datasets are stored under **warehouse_discogs**, for example:

- **artist_name_map_v1**  
  Normalized artist name → artist ID mapping

All tables are defined as **external Parquet tables** and queried directly by **Trino**.

---

## Reproducibility model

- **Storage** is persistent and external to Docker  
- **Compute** layer is stateless  
- **Schema and tables** are managed via an **idempotent SQL bootstrap**  
- **No reliance on temporary directories**

If the Hive Metastore is reset, the entire schema can be recreated **without rebuilding the data lake**.

---

## Quick start

### Start services

```
docker compose up -d
```

### Bootstrap schema and tables

```
docker exec -it trino trino --catalog hive --file /etc/trino/bootstrap_discogs.sql
```

### Verify tables

```
docker exec -it trino trino --catalog hive --schema discogs --execute "SHOW TABLES"
```

---

## Operations

### Stop services safely (no data loss)

```
docker compose down
```

### Reset metastore only (do not use casually)

```
docker compose down -v
```

This deletes **metastore metadata only**.  
Data files remain intact and can be re-registered using the bootstrap SQL.

---

## Design principles

- **Externalized storage**
- **Stateless compute**
- **SQL-first analytics**
- **Explicit schema definitions**
- **Reproducible infrastructure**

---

## Licensing note

Discogs data is subject to **Discogs terms and licensing**.
This repository does **not** distribute Discogs datasets.

It focuses exclusively on **infrastructure, schema, and tooling**.
