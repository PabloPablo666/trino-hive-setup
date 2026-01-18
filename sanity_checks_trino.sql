-- =========================================================
-- Discogs Data Lake · Trino Sanity Checks (v3)
-- Catalog: hive
-- Schema:  discogs
--
-- Changes vs v2:
-- - artist_aliases_v1: enforce FK only on artist_id (strong)
-- - alias_id may be orphan (Discogs behavior): measure + profile it, do not fail the pipeline
-- - keep labels_ref_v10 duplicate checks, but interpret results (it may be canonical)
-- =========================================================

USE hive.discogs;

-- =========================================================
-- 0️⃣ Inventory
-- =========================================================
SHOW TABLES;

-- =========================================================
-- 1️⃣ artists_v1
-- =========================================================
SELECT count(*) AS rows
FROM artists_v1;

SELECT
  count(*)                    AS rows,
  count(DISTINCT artist_id)   AS distinct_ids,
  count_if(artist_id IS NULL) AS null_ids
FROM artists_v1;

SELECT
  count_if(name IS NULL OR trim(name) = '') AS empty_names
FROM artists_v1;

-- Optional: identify the empty name row(s)
SELECT artist_id, name
FROM artists_v1
WHERE name IS NULL OR trim(name) = ''
LIMIT 50;

-- =========================================================
-- 2️⃣ artist_aliases_v1
-- =========================================================

-- Basic nullability
SELECT
  count(*)                                            AS rows,
  count_if(artist_id IS NULL)                         AS null_artist_id,
  count_if(alias_id IS NULL)                          AS null_alias_id,
  count_if(alias_name IS NULL OR trim(alias_name)='') AS empty_alias_name
FROM artist_aliases_v1;

-- 2A) STRONG FK check: artist_id MUST exist in artists_v1
SELECT count(*) AS orphan_artist_id_rows
FROM artist_aliases_v1 a
LEFT JOIN artists_v1 ar
  ON a.artist_id = ar.artist_id
WHERE a.artist_id IS NOT NULL
  AND ar.artist_id IS NULL;

-- 2B) WEAK reference profiling: alias_id MAY be orphan (expected in Discogs)
-- Measure rows + distinct alias_id that do NOT exist as artist entities.
SELECT
  count(*)                    AS orphan_alias_rows,
  count(DISTINCT a.alias_id)  AS orphan_distinct_alias_ids
FROM artist_aliases_v1 a
LEFT JOIN artists_v1 ar
  ON a.alias_id = ar.artist_id
WHERE a.alias_id IS NOT NULL
  AND ar.artist_id IS NULL;

-- 2C) Show top orphan alias_id (impact + sample names + sample artist_ids)
-- (This is for understanding, not for failing.)
SELECT
  a.alias_id,
  count(*) AS n_rows,
  array_agg(DISTINCT a.alias_name) FILTER (WHERE a.alias_name IS NOT NULL) AS alias_names_sample,
  array_agg(DISTINCT a.artist_id)  AS artist_ids_sample
FROM artist_aliases_v1 a
LEFT JOIN artists_v1 ar
  ON a.alias_id = ar.artist_id
WHERE a.alias_id IS NOT NULL
  AND ar.artist_id IS NULL
GROUP BY 1
ORDER BY n_rows DESC
LIMIT 25;

-- 2D) Sanity: alias edges should not be self-null in the important direction
SELECT
  count_if(artist_id IS NULL OR alias_id IS NULL) AS edges_with_missing_id
FROM artist_aliases_v1;

-- =========================================================
-- 3️⃣ artist_memberships_v1
-- =========================================================
SELECT
  count(*)                    AS rows,
  count_if(group_id IS NULL)  AS null_group_id,
  count_if(member_id IS NULL) AS null_member_id
FROM artist_memberships_v1;

-- Optional: membership integrity (both ids should exist as artists)
SELECT count(*) AS orphan_member_ids
FROM artist_memberships_v1 m
LEFT JOIN artists_v1 a
  ON m.member_id = a.artist_id
WHERE a.artist_id IS NULL;

SELECT count(*) AS orphan_group_ids
FROM artist_memberships_v1 m
LEFT JOIN artists_v1 a
  ON m.group_id = a.artist_id
WHERE a.artist_id IS NULL;

SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT (group_id, member_id)) AS distinct_pairs,
  CAST(COUNT(*) AS DOUBLE) / COUNT(DISTINCT (group_id, member_id)) AS dup_ratio
FROM artist_memberships_v1_typed;


-- =========================================================
-- 4️⃣ labels_ref_v10
-- =========================================================

-- Basic integrity
SELECT
  count(*)                   AS rows,
  count(DISTINCT label_id)   AS distinct_label_ids,
  count_if(label_id IS NULL) AS null_label_id
FROM labels_ref_v10;

-- Duplicate label_id distribution (should be 0 if canonical)
SELECT
  count(*) AS label_ids_with_duplicates,
  max(n_rows) AS max_rows_per_label_id
FROM (
  SELECT label_id, count(*) AS n_rows
  FROM labels_ref_v10
  GROUP BY 1
  HAVING count(*) > 1
);

-- Parent presence / sublabel presence (must be >0 if parsing correct)
SELECT
  count(*) AS rows,
  count_if(parent_label_id IS NOT NULL) AS with_parent,
  count_if(sublabel_ids_csv IS NOT NULL AND trim(sublabel_ids_csv) <> '') AS with_sublabels
FROM labels_ref_v10;

-- Broken parent refs (validated against distinct label_id set)
WITH label_ids AS (
  SELECT DISTINCT label_id
  FROM labels_ref_v10
),
parents AS (
  SELECT DISTINCT parent_label_id AS parent_id
  FROM labels_ref_v10
  WHERE parent_label_id IS NOT NULL
)
SELECT count(*) AS broken_parent_refs
FROM parents p
LEFT JOIN label_ids i
  ON p.parent_id = i.label_id
WHERE i.label_id IS NULL;

-- Spot-check a few parent edges
SELECT label_id, name, parent_label_id, parent_label_name
FROM labels_ref_v10
WHERE parent_label_id IS NOT NULL
LIMIT 25;

-- =========================================================
-- 5️⃣ masters_v1
-- =========================================================
SELECT
  count(*)                    AS rows,
  count(DISTINCT master_id)   AS distinct_ids,
  count_if(master_id IS NULL) AS null_master_id
FROM masters_v1;

SELECT count(*) AS duplicate_master_ids
FROM (
  SELECT master_id
  FROM masters_v1
  GROUP BY 1
  HAVING count(*) > 1
);

SELECT
  count_if(title IS NULL OR trim(title)='') AS empty_titles
FROM masters_v1;

SELECT
  count_if(main_release_id IS NULL) AS null_main_release_id
FROM masters_v1;

SELECT
  count(*) AS non_positive_main_release_id
FROM masters_v1
WHERE main_release_id IS NOT NULL
  AND main_release_id <= 0;

-- =========================================================
-- 6️⃣ releases_ref_v6
-- =========================================================
SELECT
  count(*)                                         AS rows,
  count(DISTINCT release_id)                       AS distinct_ids,
  count_if(release_id IS NULL)                     AS null_release_id,
  count_if(title IS NULL OR trim(title)='')        AS empty_title,
  count_if(artists IS NULL OR trim(artists)='')    AS empty_artists
FROM releases_ref_v6;

SELECT count(*) AS duplicate_release_ids
FROM (
  SELECT release_id
  FROM releases_ref_v6
  GROUP BY 1
  HAVING count(*) > 1
);

SELECT count_if(labels IS NULL OR trim(labels)='') AS empty_labels
FROM releases_ref_v6;

-- =========================================================
-- 7️⃣ Cross-table sanity: masters ↔ releases
-- =========================================================

-- Orphan masters by main_release_id (expected small >0)
SELECT count(*) AS orphan_masters
FROM masters_v1 m
LEFT JOIN releases_ref_v6 r
  ON m.main_release_id = r.release_id
WHERE m.main_release_id IS NOT NULL
  AND r.release_id IS NULL;

-- Critical join used in analytics: releases.master_id -> masters.master_id
SELECT COUNT(*) AS n_join_releases_to_masters
FROM releases_ref_v6 r
JOIN masters_v1 m
  ON r.master_id = m.master_id;

-- =========================================================
-- 8️⃣ Human sanity check (cheap sampling)
-- =========================================================
SELECT master_id, title, year, master_artists
FROM masters_v1
WHERE year BETWEEN 1995 AND 2005
ORDER BY random()
LIMIT 10;

SELECT release_id, title, artists, labels, country, released, data_quality
FROM releases_ref_v6
WHERE release_id % 100000 = 0
ORDER BY random()
LIMIT 10;

-- =========================================================
-- 9️⃣ artist_name_map_v1 (warehouse)
-- =========================================================
SELECT
  count(*) AS rows,
  count_if(norm_name IS NULL OR trim(norm_name)='') AS empty_norm_name,
  count_if(artist_id IS NULL) AS null_artist_id
FROM artist_name_map_v1;

SELECT count(*) AS orphan_mapped_artist_ids
FROM artist_name_map_v1 nm
LEFT JOIN artists_v1 a
  ON nm.artist_id = a.artist_id
WHERE a.artist_id IS NULL;

SELECT count(*) AS duplicate_pairs
FROM (
  SELECT norm_name, artist_id
  FROM artist_name_map_v1
  GROUP BY 1,2
  HAVING count(*) > 1
);

-- =========================================================
-- 🔟 release_artists_v1 (warehouse)
-- =========================================================
SELECT
  count(*) AS rows,
  count_if(release_id IS NULL) AS null_release_id,
  count_if(artist_norm IS NULL OR trim(artist_norm)='') AS empty_artist_norm
FROM release_artists_v1;

SELECT count(*) AS orphan_release_ids
FROM release_artists_v1 ra
LEFT JOIN releases_ref_v6 r
  ON ra.release_id = r.release_id
WHERE r.release_id IS NULL;

SELECT count(*) AS duplicate_pairs
FROM (
  SELECT release_id, artist_norm
  FROM release_artists_v1
  GROUP BY 1,2
  HAVING count(*) > 1
);

-- =========================================================
-- 1️⃣1️⃣ release_label_xref_v1 + label_release_counts_v1
-- =========================================================
SELECT
  count(*) AS rows,
  count_if(release_id IS NULL) AS null_release_id,
  count_if(label_norm IS NULL OR trim(label_norm)='') AS empty_label_norm
FROM release_label_xref_v1;

SELECT count(*) AS orphan_release_ids
FROM release_label_xref_v1 lx
LEFT JOIN releases_ref_v6 r
  ON lx.release_id = r.release_id
WHERE r.release_id IS NULL;

SELECT count(*) AS rows_dedup
FROM release_label_xref_v1_dedup;

SELECT
  count(*) AS rows,
  count_if(label_norm IS NULL OR trim(label_norm)='') AS empty_label_norm,
  count_if(n_total_releases IS NULL) AS null_counts
FROM label_release_counts_v1;

SELECT
  (SELECT count(DISTINCT label_norm) FROM release_label_xref_v1) AS distinct_label_norm_in_xref,
  (SELECT count(*) FROM label_release_counts_v1)                 AS rows_in_counts;

WITH recomputed AS (
  SELECT label_norm, count(DISTINCT release_id) AS n
  FROM release_label_xref_v1
  GROUP BY 1
)
SELECT count(*) AS mismatches_sample
FROM (
  SELECT c.label_norm
  FROM label_release_counts_v1 c
  JOIN recomputed r ON c.label_norm = r.label_norm
  WHERE c.n_total_releases <> r.n
  LIMIT 1000
) t;

-- =========================================================
-- 1️⃣2️⃣ genre/style xref integrity (warehouse)
-- =========================================================
SELECT
  count(*) AS rows,
  count_if(style_norm IS NULL OR trim(style_norm)='') AS empty_style_norm
FROM release_style_xref_v1;

SELECT
  count(*) AS rows,
  count_if(genre_norm IS NULL OR trim(genre_norm)='') AS empty_genre_norm
FROM release_genre_xref_v1;

SELECT count(*) AS orphan_style_release_ids
FROM release_style_xref_v1 x
LEFT JOIN releases_ref_v6 r
  ON x.release_id = r.release_id
WHERE r.release_id IS NULL;

SELECT count(*) AS orphan_genre_release_ids
FROM release_genre_xref_v1 x
LEFT JOIN releases_ref_v6 r
  ON x.release_id = r.release_id
WHERE r.release_id IS NULL;