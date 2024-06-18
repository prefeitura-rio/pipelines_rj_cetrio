WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),

tb AS (
  SELECT
    *
  FROM `rj-cetrio.ocr_radar.readings_*`  t1
  WHERE placa = ""
  AND DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-01-01T00:00:00" AND "2024-01-01T00:00:00"
  ORDER BY datahora
)


SELECT
    t1.placa,
    t1.tipoveiculo,
    t1.velocidade,
    DATETIME(t1.datahora, 'America/Sao_Paulo') AS datahora,
    t1.camera_numero,
    t1.empresa,
    COALESCE(t2.latitude, t1.camera_latitude) AS latitude,
    COALESCE(t2.longitude, t1.camera_longitude) AS longitude,
    DATETIME(t1.datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    t2.locequip,
    t2.bairro
FROM tb t1
  JOIN loc t2
  ON t1.camera_numero = t2.camera_numero



----
-- TOP N RESULTS
WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
),

tb AS (
  SELECT
    *
  FROM `rj-cetrio.ocr_radar.readings_*`  t1
  WHERE camera_numero = ""
  AND DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-01-01T00:00:00" AND "2024-01-01T00:00:00"
  ORDER BY datahora
)


SELECT
    t1.placa,
    t1.tipoveiculo,
    t1.velocidade,
    DATETIME(t1.datahora, 'America/Sao_Paulo') AS datahora,
    t1.camera_numero,
    t1.empresa,
    COALESCE(t2.latitude, t1.camera_latitude) AS latitude,
    COALESCE(t2.longitude, t1.camera_longitude) AS longitude,
    DATETIME(t1.datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    t2.locequip,
    t2.bairro
FROM tb t1
  JOIN loc t2
  ON t1.camera_numero = t2.camera_numero
ORDER BY datahora
LIMIT 21



