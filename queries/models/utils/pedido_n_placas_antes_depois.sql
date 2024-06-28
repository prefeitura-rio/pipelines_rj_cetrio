DECLARE plate STRING DEFAULT '';
DECLARE start_datetime DATETIME DEFAULT '2024-06-15T17:12:18';
DECLARE end_datetime DATETIME DEFAULT '2024-06-15T17:15:52';
DECLARE N INT64 DEFAULT 1;

WITH all_readings AS (
  SELECT
    placa,
    tipoveiculo,
    velocidade,
    DATETIME(datahora, 'America/Sao_Paulo') AS datahora_local,
    camera_numero,
    empresa,
    camera_latitude AS latitude,
    camera_longitude AS longitude,
    DATETIME(datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    ROW_NUMBER() OVER (PARTITION BY camera_numero ORDER BY datahora) AS row_num
  FROM `rj-cetrio.ocr_radar.readings_*`
  WHERE
    DATETIME(datahora, "America/Sao_Paulo")
      BETWEEN DATETIME_SUB(start_datetime, INTERVAL 1 DAY)
      AND DATETIME_ADD(end_datetime, INTERVAL 1 DAY)
  ORDER BY datahora
),

selected AS (
  SELECT
    placa,
    tipoveiculo,
    velocidade,
    datahora_local,
    camera_numero,
    empresa,
    latitude,
    longitude,
    datahora_captura,
    row_num AS selected_row_num,
  FROM all_readings
  WHERE placa = plate
    AND datahora_local
      BETWEEN start_datetime
      AND end_datetime
),

before_and_after AS (
  SELECT
    a.*
  FROM all_readings a
  JOIN selected s
    ON a.camera_numero = s.camera_numero
    AND (a.row_num BETWEEN s.selected_row_num - N AND s.selected_row_num + N)
),

loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
)

SELECT
  b.*,
  l.locequip,
  l.bairro,
  COALESCE(b.latitude, l.latitude) AS latitude,
  COALESCE(b.longitude, l.longitude) AS longitude
FROM before_and_after b
LEFT JOIN loc l
  ON b.camera_numero = l.camera_numero
ORDER BY b.camera_numero, b.datahora_local;
