WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),

tb AS (
  SELECT
    *,
    DATETIME(datahora, "America/Sao_Paulo") AS datahora_local
  FROM `rj-cetrio.ocr_radar.readings_*`
  WHERE placa = ""
  AND DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-01-01T00:00:00" AND "2024-01-01T00:00:00"
  ORDER BY datahora
),

selected AS (
  SELECT
    t1.placa,
    t1.tipoveiculo,
    t1.velocidade,
    t1.datahora_local,
    t1.camera_numero,
    t1.empresa,
    COALESCE(t2.latitude, t1.camera_latitude) AS latitude,
    COALESCE(t2.longitude, t1.camera_longitude) AS longitude,
    t1.datahora_captura,
    t2.locequip,
    t2.bairro
  FROM tb t1
  JOIN loc t2
    ON t1.camera_numero = t2.camera_numero
),

all_readings AS (
  SELECT
    t1.placa,
    t1.tipoveiculo,
    t1.velocidade,
    DATETIME(t1.datahora, 'America/Sao_Paulo') AS datahora_local,
    t1.camera_numero,
    t1.empresa,
    COALESCE(t2.latitude, t1.camera_latitude) AS latitude,
    COALESCE(t2.longitude, t1.camera_longitude) AS longitude,
    DATETIME(t1.datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    t2.locequip,
    t2.bairro,
    ROW_NUMBER() OVER (PARTITION BY t1.camera_numero ORDER BY t1.datahora) AS row_num
  FROM `rj-cetrio.ocr_radar.readings_*` t1
  JOIN loc t2
    ON t1.camera_numero = t2.camera_numero
),

selected_with_row_num AS (
  SELECT
    s.*,
    a.row_num AS selected_row_num
  FROM selected s
  JOIN all_readings a
    ON s.camera_numero = a.camera_numero
    AND s.datahora_local = a.datahora_local
),

before_and_after AS (
  SELECT
    a.*
  FROM all_readings a
  JOIN selected_with_row_num s
    ON a.camera_numero = s.camera_numero
    AND (a.row_num BETWEEN s.selected_row_num - 20 AND s.selected_row_num + 20)
)

SELECT *
FROM before_and_after
ORDER BY camera_numero, datahora_local;
