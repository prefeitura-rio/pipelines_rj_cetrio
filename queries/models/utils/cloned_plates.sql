WITH sample AS (
    SELECT
      placa,
      tipoveiculo,
      velocidade,
      DATETIME(datahora, "America/Sao_Paulo") AS datahora,
      camera_numero,
      camera_latitude,
      camera_longitude,
      empresa,
      DATETIME(datahora_captura, "America/Sao_Paulo") AS datahora_captura,
      CASE
          WHEN camera_longitude BETWEEN -43.74818 AND -43.09615
            AND camera_latitude BETWEEN -23.06016 AND -22.74337
          THEN 1
          ELSE 0
      END AS inside_rio,
      ROW_NUMBER() OVER(PARTITION BY placa ORDER BY  DATETIME(datahora, "America/Sao_Paulo")) AS seq_num
    FROM `rj-cetrio.ocr_radar.readings_*`
    WHERE placa != ""
),

placas_duplicadas AS (
  SELECT
    a.placa AS id,
    a.inside_rio AS inside_rio_a,
    b.inside_rio AS inside_rio_b,
    a.datahora AS datahora_a,
    b.datahora AS datahora_b,
    a.camera_numero AS camera_a,
    b.camera_numero AS camera_b,
    a.datahora_captura AS datahora_captura_a,
    b.datahora_captura AS datahora_captura_b,
    ST_GEOGPOINT(a.camera_longitude, a.camera_latitude) AS ponto_a,
    ST_GEOGPOINT(b.camera_longitude, b.camera_latitude) AS ponto_b,
    CONCAT(a.camera_latitude,a.camera_longitude) AS position_a,
    CONCAT(b.camera_latitude, b.camera_longitude) AS position_b,
    1.4 * ST_DISTANCE(ST_GEOGPOINT(a.camera_longitude, a.camera_latitude), ST_GEOGPOINT(b.camera_longitude, b.camera_latitude)) AS distancia,
    TIMESTAMP_DIFF(b.datahora, a.datahora, SECOND) AS diferenca_tempo_segundos,
    a.empresa AS empresa_a,
    b.empresa AS empresa_b
  FROM sample a
  JOIN sample b
  ON a.placa = b.placa
     # get only sequential detections
     AND a.seq_num + 1 = b.seq_num
     # must occur in a interval less then 1 hour
     AND TIMESTAMP_DIFF(b.datahora, a.datahora, SECOND) < 60 * 60
     # only dates that are in the same day
     AND TIMESTAMP_TRUNC(a.datahora, DAY) = TIMESTAMP_TRUNC(b.datahora, DAY)
)

SELECT
    id,
    inside_rio_a,
    inside_rio_b,
    CASE
        WHEN inside_rio_a = 1 AND inside_rio_b = 1 THEN 1
        ELSE 0
    END AS inside_rio_ab,
    empresa_a,
    camera_a,
    empresa_b,
    camera_b,
    CONCAT(camera_a, " | ", camera_b) AS camera_ab,
    CONCAT(empresa_a, " | ", empresa_b) AS empresa_ab,
    datahora_a,
    datahora_b,
    diferenca_tempo_segundos,
    datahora_captura_a,
    datahora_captura_b,
    TIMESTAMP_DIFF(datahora_captura_a, datahora_a, SECOND) AS time_diff_a,
    TIMESTAMP_DIFF(datahora_captura_b, datahora_b, SECOND) AS time_diff_b,
    distancia,
    3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) AS velocidade_km_hora,
    position_a,
    position_b,
    ponto_a,
    ponto_b,
    CASE
      WHEN diferenca_tempo_segundos <= 10 THEN '1. 0-10 segundos'
      WHEN diferenca_tempo_segundos > 10 AND diferenca_tempo_segundos <= 30 THEN '2. 10-30 segundos'
      WHEN diferenca_tempo_segundos > 30 AND diferenca_tempo_segundos <= 60 THEN '3. 30-60 segundos'
      WHEN diferenca_tempo_segundos > 60 AND diferenca_tempo_segundos <= 60 * 2 THEN '4. 1-2 minutos'
      WHEN diferenca_tempo_segundos > 60 * 2 AND diferenca_tempo_segundos <= 60 * 5 THEN '5. 2-5 minutos'
      WHEN diferenca_tempo_segundos > 60 * 5 AND diferenca_tempo_segundos <= 60 * 10 THEN '6. 5-10 minutos'
      WHEN diferenca_tempo_segundos > 60 * 10 AND diferenca_tempo_segundos <= 60 * 30 THEN '7. 10-30 minutos'
      WHEN diferenca_tempo_segundos > 60 * 30 AND diferenca_tempo_segundos <= 60 * 60 THEN '8. 30-60 minutos'
      WHEN diferenca_tempo_segundos > 60 * 60 AND diferenca_tempo_segundos <= 60 * 60 *2 THEN '9. 1-2 horas'
      WHEN diferenca_tempo_segundos > 60 * 60 *2 AND diferenca_tempo_segundos <= 60 * 60 *5 THEN '10. 2-5 horas'
      ELSE '11. >5 horas'
  END AS bin_diferenca_tempo_segundos,
  CASE
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 200 THEN '0-200 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 200 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 250 THEN '0. 200-250 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 250 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 300 THEN '1. 250-300 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 300 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 400 THEN '2. 300-400 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 400 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 500 THEN '3. 400-500 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 500 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 750 THEN '4. 500-750 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 750 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 1000 THEN '5. 750-1000 km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 1000 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 5000 THEN '6. 1-5 mil km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 5000 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 10000 THEN '7. 5-10 mil km/h'
      WHEN 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 10000 AND 3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) <= 50000 THEN '8. 10-50 mil km/h'
      ELSE '9. >50 mil km/h'
  END AS bin_velocidade,
FROM placas_duplicadas
WHERE
  3.6 * SAFE_DIVIDE(distancia, diferenca_tempo_segundos) > 250
ORDER BY datahora_a;