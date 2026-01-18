Discogs Lakehouse

Local Trino + Hive Metastore (Run-based, reproducible)

This repository contains a fully reproducible local lakehouse setup built on:
	•	Trino (SQL engine)
	•	Hive Metastore (Postgres-backed)
	•	External Parquet data lake

The system is designed to behave like a real data platform, not a demo:
	•	storage is immutable per run
	•	compute is stateless
	•	metadata is rebuildable
	•	validation is explicit and versioned

================================================================================


Core idea

This lakehouse follows three strict rules:
	1.	Data is never written inside containers
	2.	Every ingestion produces a versioned run
	3.	Only one run is “active” at a time

Everything else derives from this.

================================================================================


Architecture

┌─────────────────────────────┐
│        Trino (SQL)          │
│   stateless, replaceable    │
└─────────────┬───────────────┘
              │
┌─────────────▼───────────────┐
│      Hive Metastore         │
│   Postgres-backed metadata  │
└─────────────┬───────────────┘
              │
┌─────────────▼────────────────────────────────────┐
│            External Data Lake                     │
│  (mounted read-only inside containers)            │
└────────────────────────────────────────────────────┘

• Trino can be restarted or replaced freely

• Metastore can be dropped and rebuilt

• Data always remains intact on disk

================================================================================


Repositories

This project is intentionally split:


1️⃣ Infrastructure (this repo)

trino-hive-setup

• Docker Compose

• Trino runtime configuration

• Hive Metastore

• Bootstrap SQL

•  Trino sanity checks

This repo never parses data.


2️⃣ Pipelines & tests (separate repo)

discogs_tools_refactor

• XML → Parquet ingestion

• Typed schemas

• DuckDB tests

• Run-based lake layout

• Trino sanity report generation

Infrastructure and pipelines evolve independently.

================================================================================


Data lake layout

The data lake lives outside Docker.

Example:

discogs_data_lake/
└── hive-data/
    ├── _runs/
    │   ├── 20260118_004418/
    │   │   ├── artists_v1_typed/
    │   │   ├── masters_v1_typed/
    │   │   ├── releases_v6/
    │   │   ├── labels_v10/
    │   │   ├── warehouse_discogs/
    │   │   └── _reports/
    │   │       └── trino_sanity_active_*.csv
    │   │
    │   └── 20251215_231122/
    │
    ├── active -> _runs/20260118_004418
    └── active__prev_20260117_192144

    Key concepts

    • _runs/<timestamp>
      Immutable snapshot of a full ingestion.

    • active (symlink)
      Points to the currently selected run.

    • active__prev_*
      Automatic rollback pointer.

    Trino always queries active.

    Changing the active dataset is instant and does not touch data.

================================================================================

Why this matters

This gives you:

• reproducible historical snapshots

• zero-copy rollback

• deterministic analytics

• safe experimentation

• time-based comparisons (month vs month)

You can keep:

• November

• December

• January

…and switch between them with a symlink.

No rebuild. No rebootstrap. No SQL changes.

================================================================================


Trino configuration

Trino is configured via explicit config files mounted individually:

trino-config/
├── config.properties
├── jvm.config
└── node.properties

This avoids Docker creating empty directories or shadowing defaults.


Memory configuration (example)

-Xmx8G

query.max-memory=10GB
query.max-memory-per-node=6GB
memory.heap-headroom-per-node=1GB

================================================================================


Catalog

Catalogs live in:

trino-catalog/
└── hive.properties

Example:

connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.non-managed-table-writes-enabled=true

================================================================================


Bootstrap model

All metadata is created via SQL:

bootstrap_discogs.sql

This script is idempotent.

It creates:

schemas

external tables

logical views

If the metastore is deleted, running this file fully restores the lakehouse.

================================================================================


Logical data model


Physical tables (Parquet)
	•	artists_v1_typed
	•	artist_aliases_v1_typed
	•	artist_memberships_v1_typed
	•	masters_v1_typed
	•	releases_ref_v6
	•	labels_ref_v10
	•	warehouse_discogs/*


Logical API (views)
	•	artists_v1
	•	artist_aliases_v1
	•	artist_memberships_v1
	•	masters_v1

Queries should target views, not physical paths.

This allows schema evolution without breaking analytics.

================================================================================


Sanity checks

Two layers of validation exist:


1️⃣ Pipeline tests (DuckDB)

Run during ingestion.
	•	row counts
	•	null checks
	•	structural correctness


2️⃣ Trino sanity report (this repo)

Executed against the active run.

Produces:

_runs/<run_id>/_reports/trino_sanity_active_YYYYMMDD_HHMMSS.csv

Each row contains:
	•	check name
	•	severity (CRITICAL / WARN / INFO)
	•	value
	•	pass/fail flag
	•	optional details

The pipeline fails hard on CRITICAL.

================================================================================


Operations


Start stack

docker compose up -d


Bootstrap metadata

docker exec -it trino trino \
  --catalog hive \
  --file /etc/trino/bootstrap_discogs.sql


Stop safely

docker compose down


Reset metastore only

docker compose down -v

Data is untouched

================================================================================


Design principles

	•	external immutable storage
	•	run-based versioning
	•	pointer-based activation
	•	deterministic ingestion
	•	explicit validation
	•	zero hidden state
	•	SQL-first visibility

No magic.
No mutable tables.
No silent corruption.

================================================================================


What this is NOT
	•	not a scraper
	•	not a data warehouse SaaS
	•	not a toy project
	•	not shipping Discogs data

This is infrastructure and engineering discipline, applied locally.

================================================================================


License note

Discogs data is subject to Discogs licensing.

This repository contains only infrastructure, configuration, and SQL.

No datasets are distributed.
