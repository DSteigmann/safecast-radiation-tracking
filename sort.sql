-- Job 1: sort raw measurements by Captured Time and materialize the result in Kafka.
-- The single job parallelism and single-partition sink make the emitted Kafka order
-- deterministic for downstream consumers.
SET 'parallelism.default' = '1';

CREATE TABLE safecast_raw (
  `Captured Time` STRING,
  `Latitude` STRING,
  `Longitude` STRING,
  `Value` STRING,
  `Unit` STRING,
  `Location Name` STRING,
  `Device ID` STRING,
  `MD5Sum` STRING,
  `Height` STRING,
  `Surface` STRING,
  `Radiation` STRING,
  `Uploaded Time` STRING,
  `Loader ID` STRING,
  captured_ts AS TRY_CAST(`Captured Time` AS TIMESTAMP(3)),
  WATERMARK FOR captured_ts AS captured_ts - INTERVAL '60' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-measurements',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE VIEW safecast_clean AS
SELECT
  NULLIF(`Device ID`, '') AS sensor_id,
  `Captured Time` AS captured_at,
  `Uploaded Time` AS uploaded_at,
  captured_ts,
  `Location Name` AS location_name,
  TRY_CAST(`Latitude` AS DOUBLE) AS lat,
  TRY_CAST(`Longitude` AS DOUBLE) AS lon,
  TRY_CAST(`Value` AS DOUBLE) AS cpm
FROM safecast_raw
WHERE `Unit` = 'cpm'
  AND captured_ts IS NOT NULL
  AND TRY_CAST(`Latitude` AS DOUBLE) IS NOT NULL
  AND TRY_CAST(`Longitude` AS DOUBLE) IS NOT NULL
  AND TRY_CAST(`Value` AS DOUBLE) IS NOT NULL;

CREATE TABLE safecast_buffered_output_v2 (
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location_name STRING,
  lat DOUBLE,
  lon DOUBLE,
  cpm DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-buffered-output-v2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Keep the first 100 rows in each captured-time window, ordered ascending.
INSERT INTO safecast_buffered_output_v2
SELECT
  sensor_id,
  captured_at,
  uploaded_at,
  location_name,
  lat,
  lon,
  cpm
FROM (
  SELECT
    sensor_id,
    captured_at,
    uploaded_at,
    location_name,
    lat,
    lon,
    cpm,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end
      ORDER BY captured_ts ASC
    ) AS row_num
  FROM TABLE(
    TUMBLE(
      TABLE safecast_clean,
      DESCRIPTOR(captured_ts),
      INTERVAL '10' SECOND
    )
  )
)
WHERE row_num <= 100;
