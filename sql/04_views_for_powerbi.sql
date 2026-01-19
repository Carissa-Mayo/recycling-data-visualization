CREATE OR REPLACE VIEW mart.vw_powerbi_fact AS
SELECT
  f.yearquarter,
  q.year,
  q.quarter,
  f.source,
  f.stream,
  f.entity,
  f.tons
FROM mart.fact_tons f
JOIN mart.dim_quarter q USING (yearquarter);

SELECT * FROM mart.vw_powerbi_fact;
