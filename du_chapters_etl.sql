CREATE OR REPLACE TABLE du_dataset.du_chapters_clean AS
SELECT
  feature.attributes.OBJECTID AS object_id,
  feature.attributes.University_Chapter AS university_chapter,
  feature.attributes.City AS city,
  feature.attributes.State AS state,
  feature.attributes.ChapterID AS chapter_id,
  feature.attributes.MEVR_RD AS regional_director,
  CAST(feature.geometry.x AS FLOAT64) AS longitude,
  CAST(feature.geometry.y AS FLOAT64) AS latitude
FROM
  du_dataset.du_chapters_raw,
  UNNEST(features) AS feature;
