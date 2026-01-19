-- 1) Should be zero: negative tons
SELECT COUNT(*) AS negative_tons_rows
FROM staging.rdrs_long
WHERE tons < 0;

-- 2) Duplicate grain in fact table (should be impossible with PK)
-- If this query returns rows BEFORE you build mart.fact_tons, you have duplicates in staging
SELECT yearquarter, source, stream, entity, COUNT(*) AS c
FROM staging.rdrs_long
GROUP BY 1,2,3,4
HAVING COUNT(*) > 1
ORDER BY c DESC
LIMIT 50;

-- 3) Missing keys (should be zero)
SELECT
  SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS missing_year,
  SUM(CASE WHEN quarter IS NULL THEN 1 ELSE 0 END) AS missing_quarter,
  SUM(CASE WHEN yearquarter IS NULL OR yearquarter = '' THEN 1 ELSE 0 END) AS missing_yearquarter,
  SUM(CASE WHEN source IS NULL OR source = '' THEN 1 ELSE 0 END) AS missing_source,
  SUM(CASE WHEN entity IS NULL OR entity = '' THEN 1 ELSE 0 END) AS missing_entity,
  SUM(CASE WHEN stream IS NULL OR stream = '' THEN 1 ELSE 0 END) AS missing_stream
FROM staging.rdrs_long;

-- 4) Quarter sanity
SELECT quarter, COUNT(*) FROM staging.rdrs_long GROUP BY 1 ORDER BY 1;
