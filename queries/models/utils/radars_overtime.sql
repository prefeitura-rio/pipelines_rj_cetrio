-- RADARS ORVER TIME

WITH tb AS (
  SELECT
    empresa,
    placa,
    camera_numero,
    CONCAT(camera_latitude, camera_longitude) AS position,
    DATETIME(datahora, "America/Sao_Paulo") AS datahora,
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) / 60.0 AS datahora_diff_minutes,
  FROM `rj-cetrio.ocr_radar.readings_*`

),

radars_hour AS (
  SELECT
    empresa,
    camera_numero,
    position,
    DATE_TRUNC(datahora, HOUR) AS datahora,
    COUNT(placa) quantidade_placas,
    COUNT(DISTINCT placa) quantidade_placas_unicas,
    AVG(datahora_diff_minutes) AS media_tempo_minutos,
    APPROX_QUANTILES(datahora_diff_minutes, 2)[OFFSET(1)] AS mediana_tempo_minutos,
  FROM tb
  GROUP BY
    empresa,
    camera_numero,
    position,
    datahora
),

duplicates AS (
  SELECT
    placa,
    camera_numero,
    empresa,
    position,
    datahora,
    COUNT(*) as occurrences
  FROM tb
  GROUP BY
    placa,
    camera_numero,
    datahora,
    empresa,
    position
),

duplicates_hour AS (
  SELECT
    camera_numero,
    empresa,
    position,
    DATE_TRUNC(datahora, HOUR) AS datahora,
    SUM(occurrences) AS duplicate_occurrences_hour,
  FROM duplicates
  GROUP BY 1, 2, 3, 4
)

SELECT
  t1.empresa,
  t1.camera_numero,
  t1.position,
  t1.datahora,
  t1.quantidade_placas,
  t1.quantidade_placas_unicas,
  t1.media_tempo_minutos,
  t1.mediana_tempo_minutos,
  t2.duplicate_occurrences_hour,
FROM radars_hour t1
JOIN duplicates_hour t2
  ON  t1.camera_numero = t2.camera_numero
  AND t1.empresa = t2.empresa
  AND t1.position = t2.position
  AND t1.datahora = t2.datahora
ORDER BY duplicate_occurrences_hour DESC