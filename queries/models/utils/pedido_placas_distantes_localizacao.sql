WITH loc AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
    ST_GEOGPOINT(CAST(latitude AS FLOAT64), CAST(longitude AS FLOAT64)) AS location,
    ST_DISTANCE(
      ST_GEOGPOINT(latitude, longitude), -- posicao
      ST_GEOGPOINT(CAST(t1.latitude AS FLOAT64), CAST(t1.longitude AS FLOAT64))
    ) AS distance,
  FROM `rj-cetrio.ocr_radar.equipamento` t1
  JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
    ON t1.codcet = t2.codcet
  WHERE ST_DWithin(
    ST_GEOGPOINT(latitude, longitude), -- posicao
    ST_GEOGPOINT(CAST(t1.latitude AS FLOAT64), CAST(t1.longitude AS FLOAT64)),
    300
    )
)

SELECT
  t2.camera_numero,
  t2.locequip,
  t2.bairro,
  t2.distance as distancia,
  t1.placa,
  DATETIME(datahora, "America/Sao_Paulo") AS datahora,
  COALESCE(t2.latitude,t1.camera_latitude) AS latitude,
  COALESCE(t2.longitude,t1.camera_longitude) AS longitude,
FROM `rj-cetrio.ocr_radar.readings_2024_07` t1
JOIN loc t2
  ON t1.camera_numero = t2.camera_numero
WHERE DATETIME(datahora, "America/Sao_Paulo") BETWEEN "2024-07-01T00:00:00" AND "2024-07-01T00:00:00"
  AND t2.locequip LIKE "%SENTIDO CENTRO%"
ORDER BY distance, t2.camera_numero, datahora