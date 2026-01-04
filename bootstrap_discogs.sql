-- Discogs Data Lake bootstrap (SAFE / IDEMPOTENT)

CREATE SCHEMA IF NOT EXISTS hive.discogs
WITH (location = 'file:/data/hive-data/warehouse_discogs');

CREATE TABLE IF NOT EXISTS hive.discogs.artists_v1 (
  artist_id      VARCHAR,
  name           VARCHAR,
  realname       VARCHAR,
  profile        VARCHAR,
  data_quality   VARCHAR,
  urls           VARCHAR,
  namevariations VARCHAR,
  aliases        VARCHAR
)
WITH (external_location = 'file:/data/hive-data/artists_v1', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.artist_aliases_v1 (
  artist_id  VARCHAR,
  alias_id   VARCHAR,
  alias_name VARCHAR
)
WITH (external_location = 'file:/data/hive-data/artist_aliases_v1', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.artist_memberships_v1 (
  group_id    VARCHAR,
  group_name  VARCHAR,
  member_id   VARCHAR,
  member_name VARCHAR
)
WITH (external_location = 'file:/data/hive-data/artist_memberships_v1', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.releases_ref_v6 (
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
WITH (external_location = 'file:/data/hive-data/releases_v6', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.labels_ref_v10 (
  label_id           BIGINT,
  name               VARCHAR,
  profile            VARCHAR,
  contact_info       VARCHAR,
  data_quality       VARCHAR,
  parent_label_id    BIGINT,
  parent_label_name  VARCHAR,
  urls_csv           VARCHAR,
  sublabel_ids_csv   VARBINARY,
  sublabel_names_csv VARBINARY
)
WITH (external_location = 'file:/data/hive-data/labels_v10', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.masters_v1 (
  master_id         VARCHAR,
  main_release_id   VARCHAR,
  title             VARCHAR,
  year              BIGINT,
  master_artists    VARCHAR,
  master_artist_ids VARCHAR,
  genres            VARCHAR,
  styles            VARCHAR,
  data_quality      VARCHAR
)
WITH (external_location = 'file:/data/hive-data/masters_v1', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.collection (
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
WITH (external_location = 'file:/data/hive-data/collection', format = 'PARQUET');

CREATE TABLE IF NOT EXISTS hive.discogs.artist_name_map_v1 (
  norm_name VARCHAR,
  artist_id VARCHAR
)
WITH (external_location = 'file:/data/hive-data/warehouse_discogs/artist_name_map_v1', format = 'PARQUET');

