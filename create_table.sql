-- Deprecated: Docker Compose now starts sort.sql (Job 1) and process.sql (Job 2).
-- This legacy file is retained temporarily for reference only.
--
-- register our own UDF that turns a lat/lon into an H3 cell id
CREATE TEMPORARY FUNCTION h3_index AS 'com.tuhh.bd26.H3Index';

-- raw table and view, without producer filtering
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
  uploaded_ts AS TRY_CAST(`Uploaded Time` AS TIMESTAMP(3)),
  WATERMARK FOR captured_ts AS captured_ts - INTERVAL '60' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-measurements',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'scan.startup.mode' = 'earliest-offset'
);

CREATE VIEW safecast AS
SELECT
  `Captured Time` AS captured_at,
  `Uploaded Time` AS uploaded_at,
  ROW(`Location Name`, `Latitude`, `Longitude`) AS location,
  `Value` AS radiation
FROM safecast_raw;

-- main cleaned view we use for everything below.
-- keeps only cpm readings, casts the text fields to real numbers, drops rows with missing values,
-- and adds a status + color based on how high the cpm is.
CREATE VIEW safecast_clean AS
SELECT
  device_id,
  captured_at,
  uploaded_at,
  captured_ts,
  location_name,
  lat,
  lon,
  cpm,
  CASE WHEN cpm > 200 THEN 'critical'
       WHEN cpm >= 100 THEN 'warning'
       ELSE 'safe' END AS status,
  CASE WHEN cpm > 200 THEN '#c62828'
       WHEN cpm >= 100 THEN '#f9a825'
       ELSE '#2e7d32' END AS color
FROM (
  SELECT
    NULLIF(`Device ID`, '')         AS device_id,
    `Captured Time`                 AS captured_at,
    `Uploaded Time`                 AS uploaded_at,
    captured_ts,
    `Location Name`                 AS location_name,
    TRY_CAST(`Latitude`  AS DOUBLE) AS lat,
    TRY_CAST(`Longitude` AS DOUBLE) AS lon,
    TRY_CAST(`Value`     AS DOUBLE) AS cpm
  FROM safecast_raw
  WHERE `Unit` = 'cpm'
) typed
WHERE captured_ts IS NOT NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL
  AND cpm IS NOT NULL;

-- Buffered output sink
CREATE TABLE buffered_sorted_output (
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location_name STRING,
  cpm DOUBLE,
  status STRING,
  color STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'safecast-buffered-output',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- Shared captured-time stream for every downstream topic. The watermark is
-- based on Captured Time, so Uploaded Time and Kafka arrival order cannot
-- change the ordering inside a 10-second window.
CREATE VIEW safecast_buffered AS
SELECT
  device_id,
  captured_at,
  uploaded_at,
  captured_ts,
  location_name,
  lat,
  lon,
  cpm,
  status,
  color
FROM (
  SELECT
    device_id,
    captured_at,
    uploaded_at,
    location_name,
    lat,
    lon,
    cpm,
    status,
    color,
    captured_ts,
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

-- main map output. upsert-kafka means each sensor_id is a key, so a new reading
-- overwrites the old marker instead of adding a duplicate one.
CREATE TABLE safecast_map_points (
  marker_id STRING,
  sensor_id STRING,
  captured_at STRING,
  uploaded_at STRING,
  location ROW<name STRING, lat DOUBLE, lon DOUBLE>,
  cpm DOUBLE,
  status STRING,
  color STRING,
  action STRING,
  PRIMARY KEY (sensor_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'safecast-map-points',
  'properties.bootstrap.servers' = 'kafka:9092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- five map zoom layers using H3 hexagon cells. one topic per resolution.
-- r2 = big hexagons (zoomed out), r12 = tiny hexagons (zoomed in). h3 cell id is the upsert key.
CREATE TABLE safecast_cells_r2 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  status STRING, color STRING, action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r2',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

-- same table, medium-coarse hexagons
CREATE TABLE safecast_cells_r4 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  status STRING, color STRING, action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r4',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

-- same table, medium hexagons
CREATE TABLE safecast_cells_r6 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  status STRING, color STRING, action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r6',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

-- same table, fine hexagons
CREATE TABLE safecast_cells_r8 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  status STRING, color STRING, action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r8',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');

-- same table, smallest hexagons (most zoomed in)
CREATE TABLE safecast_cells_r12 (
  marker_id STRING, h3 STRING, resolution INT,
  center ROW<lat DOUBLE, lon DOUBLE>,
  avg_cpm DOUBLE, max_cpm DOUBLE, sample_count BIGINT,
  status STRING, color STRING, action STRING,
  PRIMARY KEY (h3) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='safecast-cells-r12',
        'properties.bootstrap.servers'='kafka:9092','key.format'='json','value.format'='json');


-- separate topic that only gets critical high-radiation readings, used for alerts
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

--  run all the INSERTs below as one Flink job. this way Flink reads the Kafka
--  topic once and feeds all the sinks from it, instead of reading it once per INSERT.
EXECUTE STATEMENT SET
BEGIN

  -- Publish the same buffered stream for consumers that need the individual
  -- measurements in captured-time order.
  INSERT INTO buffered_sorted_output
  SELECT
    device_id AS sensor_id,
    captured_at,
    uploaded_at,
    location_name,
    cpm,
    status,
    color
  FROM safecast_buffered;

  -- sensor-state replacement: for each device keep only its newest reading.
  -- ROW_NUMBER ordered by captured_ts DESC -> rn=1 is the latest, and upsert-kafka
  -- replaces the old marker instead of leaving a duplicate on the map.
  INSERT INTO safecast_map_points
  SELECT
    marker_id, sensor_id, captured_at, uploaded_at,
    CAST(ROW(location_name, lat, lon) AS ROW<name STRING, lat DOUBLE, lon DOUBLE>),
    cpm, status, color, 'upsert'
  FROM (
    SELECT
      'device-' || device_id AS marker_id,
      device_id              AS sensor_id,
      captured_at, uploaded_at, location_name, lat, lon, cpm, status, color,
      ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY captured_ts DESC) AS rn
    FROM safecast_buffered
    WHERE device_id IS NOT NULL
  ) t
  WHERE rn = 1;

  -- H3 layer r2: group all readings that fall in the same big hexagon and average them
  INSERT INTO safecast_cells_r2
  SELECT
    'h3-2-' || h3 AS marker_id, h3, 2 AS resolution,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>) AS center,
    AVG(cpm) AS avg_cpm, MAX(cpm) AS max_cpm, COUNT(*) AS sample_count,
    CASE WHEN AVG(cpm) > 200 THEN 'critical' WHEN AVG(cpm) >= 100 THEN 'warning' ELSE 'safe' END,
    CASE WHEN AVG(cpm) > 200 THEN '#c62828' WHEN AVG(cpm) >= 100 THEN '#f9a825' ELSE '#2e7d32' END,
    'upsert'
  FROM (SELECT h3_index(lat, lon, 2) AS h3, lat, lon, cpm FROM safecast_buffered)   -- tag each point with its r2 cell
  GROUP BY h3;

  -- same as above but smaller hexagons (r4)
  INSERT INTO safecast_cells_r4
  SELECT
    'h3-4-' || h3, h3, 4,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    CASE WHEN AVG(cpm) > 200 THEN 'critical' WHEN AVG(cpm) >= 100 THEN 'warning' ELSE 'safe' END,
    CASE WHEN AVG(cpm) > 200 THEN '#c62828' WHEN AVG(cpm) >= 100 THEN '#f9a825' ELSE '#2e7d32' END,
    'upsert'
  FROM (SELECT h3_index(lat, lon, 4) AS h3, lat, lon, cpm FROM safecast_buffered)
  GROUP BY h3;

  -- smaller hexagons again (r6)
  INSERT INTO safecast_cells_r6
  SELECT
    'h3-6-' || h3, h3, 6,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    CASE WHEN AVG(cpm) > 200 THEN 'critical' WHEN AVG(cpm) >= 100 THEN 'warning' ELSE 'safe' END,
    CASE WHEN AVG(cpm) > 200 THEN '#c62828' WHEN AVG(cpm) >= 100 THEN '#f9a825' ELSE '#2e7d32' END,
    'upsert'
  FROM (SELECT h3_index(lat, lon, 6) AS h3, lat, lon, cpm FROM safecast_buffered)
  GROUP BY h3;

  -- smaller hexagons again (r8)
  INSERT INTO safecast_cells_r8
  SELECT
    'h3-8-' || h3, h3, 8,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    CASE WHEN AVG(cpm) > 200 THEN 'critical' WHEN AVG(cpm) >= 100 THEN 'warning' ELSE 'safe' END,
    CASE WHEN AVG(cpm) > 200 THEN '#c62828' WHEN AVG(cpm) >= 100 THEN '#f9a825' ELSE '#2e7d32' END,
    'upsert'
  FROM (SELECT h3_index(lat, lon, 8) AS h3, lat, lon, cpm FROM safecast_buffered)
  GROUP BY h3;

  -- smallest hexagons (r12, most zoomed in)
  INSERT INTO safecast_cells_r12
  SELECT
    'h3-12-' || h3, h3, 12,
    CAST(ROW(AVG(lat), AVG(lon)) AS ROW<lat DOUBLE, lon DOUBLE>),
    AVG(cpm), MAX(cpm), COUNT(*),
    CASE WHEN AVG(cpm) > 200 THEN 'critical' WHEN AVG(cpm) >= 100 THEN 'warning' ELSE 'safe' END,
    CASE WHEN AVG(cpm) > 200 THEN '#c62828' WHEN AVG(cpm) >= 100 THEN '#f9a825' ELSE '#2e7d32' END,
    'upsert'
  FROM (SELECT h3_index(lat, lon, 12) AS h3, lat, lon, cpm FROM safecast_buffered)
  GROUP BY h3;

  -- alerts feed: send every critical reading to the hotspots topic.
  -- use the device id for the marker, or fall back to the coordinates if there is no device id.
  INSERT INTO safecast_hotspots
  SELECT
    COALESCE('device-' || device_id,
             'loc-' || CAST(lat AS STRING) || ',' || CAST(lon AS STRING)) AS marker_id,
    device_id, captured_at, uploaded_at,
    CAST(ROW(location_name, lat, lon) AS ROW<name STRING, lat DOUBLE, lon DOUBLE>),
    cpm, status, color
  FROM safecast_buffered
  WHERE status = 'critical';

END;
