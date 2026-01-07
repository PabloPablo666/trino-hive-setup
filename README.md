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

trino-hive-setup/
├── docker-compose.yml
├── .env
├── trino-catalog/
│ └── hive.properties
├── bootstrap_discogs.sql
├── sanity_checks_trino.sql
└── README.md


The `.env` file defines the external data lake location, for example:

DISCOGS_DATA_LAKE=/absolute/path/to/discogs_data_lake/hive-data


---

## Data lake layout (external to this repository)

The **data lake itself** is stored **outside this repository**, following a
typed, canonical layout:

discogs_data_lake/
└── hive-data/
├── artists_v1_typed/
├── artist_aliases_v1_typed/
├── artist_memberships_v1_typed/
├── masters_v1_typed/
├── releases_v6/
├── labels_v10/
├── collection/
└── warehouse_discogs/
├── artist_name_map_v1/
├── release_artists_v1/
├── release_label_xref_v1/
└── ...


All datasets are stored as **external Parquet files** and are never written inside containers.

---

## Data model

This lakehouse follows a **typed-first physical model** with a **stable logical API**.

### Physical datasets (BASE TABLES)

These tables point directly to Parquet storage and use **typed, consistent IDs**:

- `artists_v1_typed`
- `artist_aliases_v1_typed`
- `artist_memberships_v1_typed`
- `masters_v1_typed`
- `releases_ref_v6`
- `labels_ref_v10`
- `collection`

### Derived / warehouse datasets

Stored under `warehouse_discogs`, for example:

- `artist_name_map_v1`
- `release_artists_v1`
- `release_label_xref_v1`
- other analytical bridge or fact-style tables

### Logical API (VIEWs)

For stability and backward compatibility, the following **logical views** are defined:

- `artists_v1`
- `artist_aliases_v1`
- `artist_memberships_v1`
- `masters_v1`

These views point to the corresponding `*_v1_typed` physical tables and should be
treated as the **canonical query interface**.

---

## Reproducibility model

- **Storage** is external and persistent
- **Compute** (Trino) is stateless
- **Metadata** (Hive Metastore) can be safely destroyed and recreated
- **Schema and tables** are created via an **idempotent bootstrap SQL**

If the metastore is reset, running `bootstrap_discogs.sql` fully restores:
- schemas
- external tables
- logical views

without rebuilding or moving the underlying data.

---

## Data guarantees & known anomalies

This lakehouse reflects Discogs data *as-is*.

The following conditions are expected and valid:
- Artist aliases may reference artist IDs not present in `artists_v1`
- Some parent label references may point to missing labels
- Multiple artist entities may share identical real names

These are upstream data characteristics, not ingestion errors.

Sanity checks are designed to:
- detect structural corruption
- quantify upstream inconsistencies
- prevent silent schema drift

---

## Quick start

### Start services

docker compose up -d


### Bootstrap schema, tables, and views

docker exec -it trino trino --catalog hive --file /etc/trino/bootstrap_discogs.sql


### Run sanity checks (optional but recommended)

docker exec -it trino trino --catalog hive --file /etc/trino/sanity_checks_trino.sql


### Verify tables

docker exec -it trino trino --catalog hive --schema discogs --execute "SHOW TABLES"

---

## Operations

### Stop services safely (no data loss)

docker compose down


### Reset metastore only (do not use casually)

docker compose down -v


This removes **metastore metadata only**.  
All Parquet data remains intact and can be re-registered using the bootstrap SQL.

---

## Design principles

- **Externalized storage**
- **Stateless compute**
- **Typed canonical datasets**
- **Logical API via views**
- **SQL-first analytics**
- **Reproducible infrastructure**

---

## Licensing note

Discogs data is subject to **Discogs terms and licensing**.
This repository does **not** distribute Discogs datasets.

It focuses exclusively on **infrastructure, schema, and tooling**.
