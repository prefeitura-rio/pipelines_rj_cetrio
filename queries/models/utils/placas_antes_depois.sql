DECLARE plate STRING DEFAULT ''; -- plate of the vehicle to be monitored
DECLARE start_datetime DATETIME DEFAULT '2024-01-01T00:00:00'; -- Start time of the detection range
DECLARE end_datetime DATETIME DEFAULT   '2024-01-01T00:00:00'; -- End time of the detection range
DECLARE N INT64 DEFAULT 5; -- Number of minutes to look for records before and after the selected time

-- Select all radar readings 
WITH all_readings AS (
  SELECT
    placa,
    velocidade,
    DATETIME(datahora, 'America/Sao_Paulo') AS datahora_local,
    camera_numero,
    empresa,
    camera_latitude AS latitude,
    camera_longitude AS longitude,
    DATETIME(datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    ROW_NUMBER() OVER (PARTITION BY placa, datahora ORDER BY datahora) AS row_num_duplicate
  FROM `rj-cetrio.ocr_radar.readings_2024*`
  WHERE
    DATETIME(datahora, "America/Sao_Paulo")
      BETWEEN DATETIME_SUB(start_datetime, INTERVAL 1 DAY)
      AND DATETIME_ADD(end_datetime, INTERVAL 1 DAY)
  QUALIFY(row_num_duplicate) = 1
  -- ORDER BY datahora
),

all_loc AS (
  SELECT
    t1.codcet,
    t2.camera_numero,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(t1.locequip, r'^(.*?) -.*', r'\1'), -- Remove the part after " -"
         r'\s+', ' ') -- Remove extra spaces
      ) AS locequip,
    COALESCE(CONCAT(' - SENTIDO ', sentido), '') AS sentido,
    TO_BASE64(
      MD5(
        CONCAT(
          LEFT(t1.codcet, LENGTH(t1.codcet) -1), 
          COALESCE(t1.sentido, '') -- Combine codcet and sentido, omitting the last character of codcet
        )
      )
    ) AS hashed_coordinates, -- Generate a unique hash for the location
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),
-- Select unique coordinates for each location
loc AS (
  SELECT
    hashed_coordinates,
    locequip,
    ROW_NUMBER() OVER(PARTITION BY hashed_coordinates) rn
  FROM all_loc
  QUALIFY(rn) = 1
),
-- Group radar information with readings
group_radars AS (
  SELECT
    l.camera_numero,
    l.codcet,
    l.bairro,
    l.latitude,
    l.longitude,
    b.locequip,
    l.sentido,
    l.hashed_coordinates
  FROM
    all_loc l
    JOIN loc b ON l.hashed_coordinates = b.hashed_coordinates
  WHERE
  -- Ensure there is at least one reading for each radar
    EXISTS (
      SELECT
        1
      FROM
        all_readings c
      WHERE l.camera_numero = c.camera_numero
    )
),
  -- Select specific readings for the desired license plate
selected AS (
  SELECT
    b.hashed_coordinates,
    a.placa,
    a.velocidade,
    a.datahora_local,
    a.camera_numero,
    a.empresa,
    a.latitude,
    a.longitude,
    a.datahora_captura,
  FROM all_readings a
  JOIN group_radars b ON a.camera_numero = b.camera_numero
  WHERE 
    placa = plate
    AND datahora_local
      BETWEEN start_datetime
      AND end_datetime
),
-- Look for records before and after the selected reading time
before_and_after AS (
  SELECT
    l.codcet,
    l.hashed_coordinates,
    l.locequip,
    l.bairro,
    l.sentido,
    a.*
  FROM 
    all_readings a
  JOIN group_radars l ON a.camera_numero = l.camera_numero
  JOIN selected s ON l.hashed_coordinates = s.hashed_coordinates
    AND (
      a.datahora_local BETWEEN 
        DATETIME_SUB(s.datahora_local, INTERVAL N MINUTE) 
        AND DATETIME_ADD(s.datahora_local, INTERVAL N MINUTE)
    )
),
-- Count occurrences of plates in 'before_and_after'
qty_occurrences AS (
  SELECT
    placa,
    COUNT(placa) `count`
  FROM
    before_and_after
  GROUP BY ALL
),
-- Aggregate final results
aggregations AS (
  SELECT
    b.hashed_coordinates AS id_camera_groups,
    ARRAY(
      SELECT
        g.camera_numero
      FROM
        group_radars g
      WHERE g.hashed_coordinates = b.hashed_coordinates
    ) AS radars,
    DATETIME_SUB(s.datahora_local, INTERVAL N MINUTE) AS start_time,
    DATETIME_ADD(s.datahora_local, INTERVAL N MINUTE) AS end_time,
    CONCAT(b.locequip, b.sentido, ' - ', b.bairro) AS location,
    b.latitude AS latitude,
    b.longitude AS longitude,
    ARRAY_AGG(
      STRUCT(
        b.datahora_local AS `timestamp`,
        b.placa AS plate,
        b.camera_numero,
        RIGHT(b.codcet, 1) AS lane,
        b.velocidade AS speed,
        o.`count`
      )
      ORDER BY 
        b.datahora_local) as detections -- Organize detections by date/time
  FROM before_and_after b
  JOIN qty_occurrences o ON b.placa = o.placa
  JOIN selected s ON b.hashed_coordinates = s.hashed_coordinates
  GROUP BY all
)
-- Select the final results from the aggregation
SELECT 
  id_camera_groups,
  radars,
  start_time,
  end_time,
  location,
  latitude,
  longitude,
  ARRAY_LENGTH(detections) AS total_detection,
  detections,
FROM 
  aggregations
ORDER BY
  start_time
