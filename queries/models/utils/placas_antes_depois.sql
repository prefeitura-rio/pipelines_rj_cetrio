DECLARE plate STRING DEFAULT ''; -- plate of the vehicle to be monitored
DECLARE start_datetime TIMESTAMP DEFAULT '2024-10-15T01:00:00.000Z'; -- Start timestamp of the detection range
DECLARE end_datetime TIMESTAMP DEFAULT   '2024-10-15T01:00:00.000Z'; -- End timestamp of the detection range
DECLARE N_minutes INT64 DEFAULT 5; -- Number of minutes to look for records before and after the selected_readings time
DECLARE N_plates INT64 DEFAULT 5; -- Quantity of plates to look for records before and after the selected_readings time

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
  FROM `rj-cetrio.ocr_radar.readings_*`
  WHERE
    DATETIME(datahora, "America/Sao_Paulo")
      BETWEEN DATETIME(DATETIME_SUB(start_datetime, INTERVAL 1 DAY), "America/Sao_Paulo")
      AND DATETIME(DATETIME_ADD(end_datetime, INTERVAL 1 DAY), "America/Sao_Paulo")
  QUALIFY(row_num_duplicate) = 1
),

-- Get all unique locations and associated information
unique_locations AS (
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
unique_location_coordinates  AS (
  SELECT
    hashed_coordinates,
    locequip,
    ROW_NUMBER() OVER(PARTITION BY hashed_coordinates) rn
  FROM unique_locations
  QUALIFY(rn) = 1
),

-- Group radar information with readings
radar_group AS (
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
    unique_locations l
    JOIN unique_location_coordinates  b ON l.hashed_coordinates = b.hashed_coordinates
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
selected_readings AS (
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
    ROW_NUMBER() OVER(PARTITION BY a.placa ORDER BY a.datahora_local) n_deteccao,
    DATETIME_SUB(a.datahora_local, INTERVAL N_minutes MINUTE) AS datahora_inicio,
    DATETIME_ADD(a.datahora_local, INTERVAL N_minutes MINUTE) AS datahora_fim
  FROM all_readings a
  JOIN radar_group b ON a.camera_numero = b.camera_numero
  WHERE 
    a.placa = plate
    AND datahora_local
      BETWEEN DATETIME(start_datetime, "America/Sao_Paulo")
      AND DATETIME(end_datetime, "America/Sao_Paulo")
),

-- Look for records before and after the selected_readings reading time
before_and_after AS (
  SELECT
    l.codcet,
    l.hashed_coordinates,
    l.locequip,
    l.bairro,
    l.sentido,
    a.*,
    s.n_deteccao
  FROM 
    all_readings a
  JOIN radar_group l ON a.camera_numero = l.camera_numero
  JOIN selected_readings s ON l.hashed_coordinates = s.hashed_coordinates
    AND (
      a.datahora_local BETWEEN 
        s.datahora_inicio AND s.datahora_fim
    )
),

-- Aggregate final results
aggregations AS (
  SELECT
    b.n_deteccao AS id_detection,
    s.datahora_local AS detection_time, -- group by each plate detection
    b.hashed_coordinates AS id_camera_groups,
    ARRAY(
      SELECT
        g.camera_numero
      FROM
        radar_group g
      WHERE g.hashed_coordinates = b.hashed_coordinates
    ) AS radars,
    CONCAT(b.locequip, b.sentido, ' - ', b.bairro) AS location,
    b.latitude AS latitude,
    b.longitude AS longitude,
    ARRAY_AGG(
      STRUCT(
        b.datahora_local AS `timestamp`,
        b.placa,
        b.camera_numero,
        RIGHT(b.codcet, 1) AS lane,
        b.velocidade AS speed
      )
      ORDER BY 
        b.datahora_local) as detections -- Organize detections by date/time
  FROM before_and_after b
  JOIN selected_readings s ON b.hashed_coordinates = s.hashed_coordinates AND b.n_deteccao = s.n_deteccao
  GROUP BY all
),

-- Order detection results
detection_orders AS (
  SELECT
    a.id_detection,
    a.id_camera_groups,
    a.radars,
    a.detection_time,
    a.location,
    a.latitude,
    a.longitude,
    d.*,
    ROW_NUMBER() OVER(PARTITION BY a.id_detection ORDER BY d.timestamp) AS detection_order
  FROM
    aggregations a
    JOIN UNNEST(a.detections) d
),

-- Select the specific detection orders for the target plate
selected_orders AS (
  SELECT
    id_detection,
    detection_order
  FROM
  -- Order detection results
    detection_orders
  WHERE
    placa = plate
),

-- Final query to aggregate results
final_results AS (
  SELECT
    a.id_detection,
    a.id_camera_groups,
    a.radars,
    a.detection_time,
    DATETIME_SUB(a.detection_time, INTERVAL N_minutes MINUTE) AS start_time,
    DATETIME_ADD(a.detection_time, INTERVAL N_minutes MINUTE) AS end_time,
    a.location,
    a.latitude,
    a.longitude,
    a.timestamp,
    a.placa AS plate,
    a.camera_numero,
    a.lane,
    a.speed,
    COUNT(a.placa) AS `count`
  FROM
  -- Order detection results
    detection_orders a
  JOIN 
    selected_orders b
  ON
    a.id_detection = b.id_detection
  WHERE
    a.detection_order BETWEEN b.detection_order - N_plates AND b.detection_order + N_plates
  GROUP BY ALL
),

-- Count plates in the final results
plates_count AS  (
  SELECT
    plate,
    COUNT(plate) AS `count`
  FROM
    final_results
  GROUP BY ALL
),

-- Final aggregation of results into an array
final_array_agg AS (
  SELECT
    a.id_detection,
    a.id_camera_groups,
    a.radars,
    a.detection_time,
    a.start_time,
    a.end_time,
    a.location,
    a.latitude,
    a.longitude,
    ARRAY_AGG(
      STRUCT(
        a.timestamp,
        a.plate,
        a.camera_numero,
        a.lane,
        a.speed,
        b.count
      )
    ORDER BY a.timestamp
    ) AS detections
  FROM
    final_results a
  JOIN
    plates_count b
  ON a.plate = b.plate
  GROUP BY ALL
)

-- Final selection to retrieve results
SELECT
  id_camera_groups,
  radars,
  detection_time,
  start_time,
  end_time,
  location,
  latitude,
  longitude,
  ARRAY_LENGTH(detections) AS total_detection,
  detections
FROM
  final_array_agg
