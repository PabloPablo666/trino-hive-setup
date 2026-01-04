-- =========================================================
-- Discogs Data Lake · Trino Sanity Checks
-- Catalog: hive
-- Schema:  discogs
-- =========================================================

USE hive.discogs;

-- =========================================================
-- 1️⃣ artists_v1
-- =========================================================

-- Row count
SELECT count(*) AS rows
FROM artists_v1;

-- Primary key sanity
SELECT
  count(*)                          AS rows,
  count(DISTINCT artist_id)         AS distinct_ids,
  count_if(artist_id IS NULL)       AS null_ids
FROM artists_v1;

-- Name sanity (Discogs may contain edge cases)
SELECT
  count_if(name IS NULL OR trim(name) = '') AS empty_names
FROM artists_v1;


-- =========================================================
-- 2️⃣ artist_aliases_v1
-- =========================================================

SELECT
  count(*)                                           AS rows,
  count_if(artist_id IS NULL)                        AS null_artist_id,
  count_if(alias_name IS NULL OR trim(alias_name)='') AS null_alias
FROM artist_aliases_v1;

-- Referential integrity: aliases → artists
SELECT count(*) AS orphan_aliases
FROM artist_aliases_v1 a
LEFT JOIN artists_v1 ar
  ON a.artist_id = ar.artist_id
WHERE ar.artist_id IS NULL;


-- =========================================================
-- 3️⃣ artist_memberships_v1
-- =========================================================

SELECT
  count(*)                       AS rows,
  count_if(group_id IS NULL)     AS null_group_id,
  count_if(member_id IS NULL)    AS null_member_id
FROM artist_memberships_v1;


-- =========================================================
-- 4️⃣ labels_ref_v10
-- =========================================================

SELECT
  count(*)                   AS rows,
  count(DISTINCT label_id)   AS distinct_ids,
  count_if(label_id IS NULL) AS null_label_id
FROM labels_ref_v10;

-- Duplicate label_id sanity (should be 0)
SELECT count(*) AS duplicate_label_ids
FROM (
  SELECT label_id
  FROM labels_ref_v10
  GROUP BY 1
  HAVING count(*) > 1
);

-- Parent label sanity (Discogs does NOT guarantee integrity)
SELECT count(*) AS broken_parent_refs
FROM labels_ref_v10 l
LEFT JOIN labels_ref_v10 p
  ON l.parent_label_id = p.label_id
WHERE l.parent_label_id IS NOT NULL
  AND p.label_id IS NULL;


-- =========================================================
-- 5️⃣ masters_v1
-- =========================================================

SELECT
  count(*)                      AS rows,
  count(DISTINCT master_id)     AS distinct_ids,
  count_if(master_id IS NULL)   AS null_master_id
FROM masters_v1;

-- Duplicate master_id sanity (should be 0)
SELECT count(*) AS duplicate_master_ids
FROM (
  SELECT master_id
  FROM masters_v1
  GROUP BY 1
  HAVING count(*) > 1
);

-- Title sanity
SELECT
  count_if(title IS NULL OR trim(title)='') AS empty_titles
FROM masters_v1;

-- main_release_id numeric sanity (should be 0 in your case)
SELECT count(*) AS non_numeric_main_release_id
FROM masters_v1
WHERE main_release_id IS NOT NULL
  AND NOT regexp_like(main_release_id, '^[0-9]+$');


-- =========================================================
-- 6️⃣ releases_ref_v6
-- =========================================================

SELECT
  count(*)                      AS rows,
  count(DISTINCT release_id)    AS distinct_ids,
  count_if(release_id IS NULL)  AS null_release_id,
  count_if(title IS NULL OR trim(title)='')   AS empty_title,
  count_if(artists IS NULL OR trim(artists)='') AS empty_artists
FROM releases_ref_v6;

-- Duplicate release_id sanity (should be 0)
SELECT count(*) AS duplicate_release_ids
FROM (
  SELECT release_id
  FROM releases_ref_v6
  GROUP BY 1
  HAVING count(*) > 1
);

-- Optional: how many releases have no labels (can be >0)
SELECT count_if(labels IS NULL OR trim(labels)='') AS empty_labels
FROM releases_ref_v6;


-- =========================================================
-- 7️⃣ Cross-table sanity: masters → releases
-- =========================================================

-- Orphan masters (expected >0, must be quantified)
-- Robust join: only cast numeric main_release_id, otherwise ignore that row in the join key.
SELECT count(*) AS orphan_masters
FROM masters_v1 m
LEFT JOIN releases_ref_v6 r
  ON try_cast(m.main_release_id AS bigint) = r.release_id
WHERE m.main_release_id IS NOT NULL
  AND r.release_id IS NULL;


-- =========================================================
-- 8️⃣ Human sanity check (NON OPTIONAL)
-- =========================================================

SELECT master_id, title, year, master_artists
FROM masters_v1
WHERE year BETWEEN 1995 AND 2005
ORDER BY random()
LIMIT 10;

SELECT release_id, title, artists, labels, country, released, data_quality
FROM releases_ref_v6
ORDER BY random()
LIMIT 10;
