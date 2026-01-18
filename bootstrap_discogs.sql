-- Discogs Data Lake bootstrap (ACTIVE pointer mode)
-- Physical Parquet lives under: file:/data/hive-data/active/...
-- "active" is a symlink updated by promote: active -> _runs/<run_id>
-- Drops are metadata-only if tables are external.

-- =========================================================
-- DROP VIEWS (first) - because views depend on tables
-- =========================================================

DROP VIEW IF EXISTS hive.discogs.release_label_xref_v1_dedup;
DROP VIEW IF EXISTS hive.discogs.release_label_xref_v1_canon;

DROP VIEW IF EXISTS hive.discogs.artist_memberships_v1_typed_dedup;

DROP VIEW IF EXISTS hive.discogs.artists_v1;
DROP VIEW IF EXISTS hive.discogs.artist_aliases_v1;
DROP VIEW IF EXISTS hive.discogs.artist_memberships_v1;
DROP VIEW IF EXISTS hive.discogs.masters_v1;

-- =========================================================
-- DROP TABLES (metadata only)
-- =========================================================

-- Derived warehouse tables
DROP TABLE IF EXISTS hive.discogs.release_genre_xref_v1;
DROP TABLE IF EXISTS hive.discogs.release_style_xref_v1;
DROP TABLE IF EXISTS hive.discogs.label_release_counts_v1;
DROP TABLE IF EXISTS hive.discogs.release_label_xref_v1;
DROP TABLE IF EXISTS hive.discogs.release_artists_v1;
DROP TABLE IF EXISTS hive.discogs.artist_name_map_v1;

-- Base/ref tables
DROP TABLE IF EXISTS hive.discogs.collection;
DROP TABLE IF EXISTS hive.discogs.labels_ref_v10;
DROP TABLE IF EXISTS hive.discogs.releases_ref_v6;

DROP TABLE IF EXISTS hive.discogs.masters_v1_typed;
DROP TABLE IF EXISTS hive.discogs.artist_memberships_v1_typed;
DROP TABLE IF EXISTS hive.discogs.artist_aliases_v1_typed;
DROP TABLE IF EXISTS hive.discogs.artists_v1_typed;

-- =========================================================
-- Schema (points to ACTIVE metadata)
-- =========================================================

CREATE SCHEMA IF NOT EXISTS hive.discogs
WITH (location = 'file:/data/hive-data/active/_meta/discogs');

-- =========================================================
-- BASE TABLES (PHYSICAL) - under /active
-- =========================================================

CREATE TABLE hive.discogs.artists_v1_typed (
  artist_id      BIGINT,
  name           VARCHAR,
  realname       VARCHAR,
  profile        VARCHAR,
  data_quality   VARCHAR,
  urls           VARCHAR,
  namevariations VARCHAR,
  aliases        VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/artists_v1_typed', format = 'PARQUET');

CREATE TABLE hive.discogs.artist_aliases_v1_typed (
  artist_id  BIGINT,
  alias_id   BIGINT,
  alias_name VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/artist_aliases_v1_typed', format = 'PARQUET');

CREATE TABLE hive.discogs.artist_memberships_v1_typed (
  group_id    BIGINT,
  group_name  VARCHAR,
  member_id   BIGINT,
  member_name VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/artist_memberships_v1_typed', format = 'PARQUET');

CREATE TABLE hive.discogs.masters_v1_typed (
  master_id         BIGINT,
  main_release_id   BIGINT,
  title             VARCHAR,
  year              BIGINT,
  master_artists    VARCHAR,
  master_artist_ids VARCHAR,
  genres            VARCHAR,
  styles            VARCHAR,
  data_quality      VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/masters_v1_typed', format = 'PARQUET');

-- =========================================================
-- REF TABLES - under /active
-- =========================================================

CREATE TABLE hive.discogs.releases_ref_v6 (
  release_id           BIGINT,
  master_id            BIGINT,
  title                VARCHAR,
  artists              VARCHAR,
  labels               VARCHAR,
  label_catnos         VARCHAR,
  country              VARCHAR,
  formats              VARCHAR,
  genres               VARCHAR,
  styles               VARCHAR,
  credits_flat         VARCHAR,
  status               VARCHAR,
  released             VARCHAR,
  data_quality         VARCHAR,
  format_qtys          VARCHAR,
  format_texts         VARCHAR,
  format_descriptions  VARCHAR,
  identifiers_flat     VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/releases_v6', format = 'PARQUET');

CREATE TABLE hive.discogs.labels_ref_v10 (
  label_id           BIGINT,
  name               VARCHAR,
  profile            VARCHAR,
  contact_info       VARCHAR,
  data_quality       VARCHAR,
  parent_label_id    BIGINT,
  parent_label_name  VARCHAR,
  urls_csv           VARCHAR,
  sublabel_ids_csv   VARCHAR,
  sublabel_names_csv VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/labels_v10', format = 'PARQUET');

CREATE TABLE hive.discogs.collection (
  instance_id BIGINT,
  release_id  BIGINT,
  title       VARCHAR,
  artists     VARCHAR,
  labels      VARCHAR,
  year        BIGINT,
  formats     VARCHAR,
  genres      VARCHAR,
  styles      VARCHAR,
  date_added  VARCHAR,
  rating      BIGINT
)
WITH (external_location = 'file:/data/hive-data/active/collection', format = 'PARQUET');

-- =========================================================
-- DERIVED / WAREHOUSE TABLES - under /active/warehouse_discogs
-- =========================================================

CREATE TABLE hive.discogs.artist_name_map_v1 (
  norm_name VARCHAR,
  artist_id BIGINT
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/artist_name_map_v1', format = 'PARQUET');

CREATE TABLE hive.discogs.release_artists_v1 (
  release_id  BIGINT,
  artist_norm VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/release_artists_v1', format = 'PARQUET');

CREATE TABLE hive.discogs.release_label_xref_v1 (
  release_id BIGINT,
  label_name VARCHAR,
  label_norm VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/release_label_xref_v1', format = 'PARQUET');

CREATE TABLE hive.discogs.label_release_counts_v1 (
  label_norm        VARCHAR,
  label_name_sample VARCHAR,
  n_total_releases  BIGINT
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/label_release_counts_v1', format = 'PARQUET');

CREATE TABLE hive.discogs.release_style_xref_v1 (
  release_id  BIGINT,
  style       VARCHAR,
  style_norm  VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/release_style_xref_v1', format = 'PARQUET');

CREATE TABLE hive.discogs.release_genre_xref_v1 (
  release_id  BIGINT,
  genre       VARCHAR,
  genre_norm  VARCHAR
)
WITH (external_location = 'file:/data/hive-data/active/warehouse_discogs/release_genre_xref_v1', format = 'PARQUET');

-- =========================================================
-- CANONICAL VIEWS (stable API) - *_v1
-- =========================================================

CREATE OR REPLACE VIEW hive.discogs.artists_v1 AS
SELECT * FROM hive.discogs.artists_v1_typed;

CREATE OR REPLACE VIEW hive.discogs.artist_aliases_v1 AS
SELECT * FROM hive.discogs.artist_aliases_v1_typed;

CREATE OR REPLACE VIEW hive.discogs.artist_memberships_v1 AS
SELECT * FROM hive.discogs.artist_memberships_v1_typed;

CREATE OR REPLACE VIEW hive.discogs.masters_v1 AS
SELECT * FROM hive.discogs.masters_v1_typed;

-- =========================================================
-- LEGACY/UTILITY VIEWS (to match your old project)
-- =========================================================

-- Dedup memberships (likely reason: duplicates in source)
CREATE OR REPLACE VIEW hive.discogs.artist_memberships_v1_typed_dedup AS
SELECT DISTINCT
  group_id,
  group_name,
  member_id,
  member_name
FROM hive.discogs.artist_memberships_v1_typed;

-- "canon" view: keep as a stable alias for the warehouse xref
-- If you later want canon = normalized/deduped logic, change it here.
CREATE OR REPLACE VIEW hive.discogs.release_label_xref_v1_canon AS
SELECT
  release_id,
  label_name,
  label_norm
FROM hive.discogs.release_label_xref_v1;

-- Dedup labels xref
CREATE OR REPLACE VIEW hive.discogs.release_label_xref_v1_dedup AS
SELECT DISTINCT
  release_id,
  label_name,
  label_norm
FROM hive.discogs.release_label_xref_v1;
