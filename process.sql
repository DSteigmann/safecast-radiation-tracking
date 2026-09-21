-- Job 2: consume Job 1's materialized, captured-time ordered Kafka stream.
SET 'parallelism.default' = '1';

CREATE TEMPORARY FUNCTION h3_index AS 'com.tuhh.bd26.H3Index';

CREATE TABLE safecast_buffered_input_v2 (
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location_name STRING,
  lat DOUBLE,
  lon DOUBLE,
  cpm DOUBLE,
  captured_ts AS TRY_CAST(captured_at AS TIMESTAMP(3))
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-buffered-output-v2',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset',
  'properties.group.id' = 'safecast-downstream-v2'
);

CREATE TABLE safecast_map_points (
  marker_id STRING,
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location ROW<name STRING, lat DOUBLE, lon DOUBLE>,
  cpm DOUBLE,
  action STRING,
  PRIMARY KEY (marker_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'safecast-map-points',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE safecast_hotspots (
  marker_id STRING,
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location ROW<name STRING, lat DOUBLE, lon DOUBLE>,
  cpm DOUBLE,
  status STRING,
  color STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-hotspots',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE safecast_cells_r2 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r2',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

CREATE TABLE safecast_cells_r4 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r4',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

CREATE TABLE safecast_cells_r6 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r6',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

CREATE TABLE safecast_cells_r8 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r8',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

CREATE TABLE safecast_cells_r12 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r12',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO safecast_map_points
  SELECT
    'device-' || normalized_sensor_id AS marker_id,
    normalized_sensor_id AS sensor_id,
    captured_at,
    uploaded_at,
    CAST(ROW(location_name, lat, lon) AS ROW<name STRING, lat DOUBLE, lon DOUBLE>),
    cpm,
    'upsert'
  FROM (
    SELECT
      COALESCE(
        sensor_id,
        'generated-' ||
          CAST(lat AS STRING) || '-' ||
          CAST(lon AS STRING) || '-' ||
          COALESCE(captured_at, 'unknown-captured') || '-' ||
          COALESCE(uploaded_at, 'unknown-uploaded') || '-' ||
          CAST(cpm AS STRING)
      ) AS normalized_sensor_id,
      captured_at,
      uploaded_at,
      location_name,
      lat,
      lon,
      cpm
    FROM safecast_buffered_input_v2
  );

  INSERT INTO safecast_hotspots
  SELECT
    COALESCE('device-' || sensor_id,
             'loc-' || CAST(lat AS STRING) || ',' || CAST(lon AS STRING)),
    sensor_id,
    captured_at,
    uploaded_at,
    CAST(ROW(location_name, lat, lon) AS ROW<name STRING, lat DOUBLE, lon DOUBLE>),
    cpm,
    'critical',
    '#c62828'
  FROM safecast_buffered_input_v2
  WHERE cpm > 1000;

  INSERT INTO safecast_cells_r2
  SELECT
    'h3-2-' || h3, h3, 2,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    'upsert'
  FROM (SELECT h3_index(lat, lon, 2) AS h3, lat, lon, cpm FROM safecast_buffered_input_v2)
  GROUP BY h3;

  INSERT INTO safecast_cells_r4
  SELECT
    'h3-4-' || h3, h3, 4,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    'upsert'
  FROM (SELECT h3_index(lat, lon, 4) AS h3, lat, lon, cpm FROM safecast_buffered_input_v2)
  GROUP BY h3;

  INSERT INTO safecast_cells_r6
  SELECT
    'h3-6-' || h3, h3, 6,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    'upsert'
  FROM (SELECT h3_index(lat, lon, 6) AS h3, lat, lon, cpm FROM safecast_buffered_input_v2)
  GROUP BY h3;

  INSERT INTO safecast_cells_r8
  SELECT
    'h3-8-' || h3, h3, 8,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    'upsert'
  FROM (SELECT h3_index(lat, lon, 8) AS h3, lat, lon, cpm FROM safecast_buffered_input_v2)
  GROUP BY h3;

  INSERT INTO safecast_cells_r12
  SELECT
    'h3-12-' || h3, h3, 12,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    'upsert'
  FROM (SELECT h3_index(lat, lon, 12) AS h3, lat, lon, cpm FROM safecast_buffered_input_v2)
  GROUP BY h3;
END;
