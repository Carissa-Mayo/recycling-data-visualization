-- Dimensions
DROP TABLE IF EXISTS mart.dim_quarter;
CREATE TABLE mart.dim_quarter AS
SELECT DISTINCT yearquarter, year, quarter
FROM staging.rdrs_long;

ALTER TABLE mart.dim_quarter ADD PRIMARY KEY (yearquarter);

DROP TABLE IF EXISTS mart.dim_source;
CREATE TABLE mart.dim_source AS
SELECT DISTINCT source FROM staging.rdrs_long;
ALTER TABLE mart.dim_source ADD PRIMARY KEY (source);

DROP TABLE IF EXISTS mart.dim_stream;
CREATE TABLE mart.dim_stream AS
SELECT DISTINCT stream FROM staging.rdrs_long;
ALTER TABLE mart.dim_stream ADD PRIMARY KEY (stream);

DROP TABLE IF EXISTS mart.dim_entity;
CREATE TABLE mart.dim_entity AS
SELECT DISTINCT entity FROM staging.rdrs_long;
ALTER TABLE mart.dim_entity ADD PRIMARY KEY (entity);

-- Fact (aggregated to unique grain)
DROP TABLE IF EXISTS mart.fact_tons;
CREATE TABLE mart.fact_tons AS
SELECT
  yearquarter, source, stream, entity,
  SUM(tons) AS tons
FROM staging.rdrs_long
GROUP BY 1,2,3,4;

ALTER TABLE mart.fact_tons
  ADD PRIMARY KEY (yearquarter, source, stream, entity);

-- Foreign keys (optional but good practice)
ALTER TABLE mart.fact_tons
  ADD CONSTRAINT fk_q FOREIGN KEY (yearquarter) REFERENCES mart.dim_quarter(yearquarter);

ALTER TABLE mart.fact_tons
  ADD CONSTRAINT fk_source FOREIGN KEY (source) REFERENCES mart.dim_source(source);

ALTER TABLE mart.fact_tons
  ADD CONSTRAINT fk_stream FOREIGN KEY (stream) REFERENCES mart.dim_stream(stream);

ALTER TABLE mart.fact_tons
  ADD CONSTRAINT fk_entity FOREIGN KEY (entity) REFERENCES mart.dim_entity(entity);
